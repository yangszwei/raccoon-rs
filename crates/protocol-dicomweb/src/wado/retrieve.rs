use std::collections::VecDeque;
use std::pin::Pin;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Uri, header};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt, stream};
use raccoon_contract_dicom::{DicomInstanceIdentity, TransferSyntaxUid};
use raccoon_contract_object_store::{Bytes, ObjectStoreError};
use raccoon_service_retrieve::{
    RetrieveError, RetrieveRequest, RetrieveResult, RetrieveScope, RetrieveService,
    RetrievedInstance,
};
use tracing::{Instrument, Span, info_span};

use crate::instrumentation::record_error;
use crate::media::{
    self, AvailableRepresentation, MediaType, MediaTypeParams, SelectedRepresentation,
};
use crate::wado::{TranscodeError, TransferSyntaxPolicy};
use crate::{DicomWebError, DicomWebUrlBase};

#[derive(Debug)]
pub(crate) struct CollectedInstance {
    pub(crate) identity: DicomInstanceIdentity,
    pub(crate) transfer_syntax_uid: Option<TransferSyntaxUid>,
    pub(crate) body: Bytes,
}

type ObjectBodyStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, ObjectStoreError>> + Send + 'static>>;

struct MultipartBodyState {
    instances:
        Pin<Box<dyn Stream<Item = Result<RetrievedInstance, RetrieveError>> + Send + 'static>>,
    first_instance: Option<RetrievedInstance>,
    current_body: Option<ObjectBodyStream>,
    pending: VecDeque<Bytes>,
    base: Option<DicomWebUrlBase>,
    boundary: String,
    requested_transfer_syntax: Option<TransferSyntaxUid>,
    policy: Option<TransferSyntaxPolicy>,
    finished: bool,
}

pub(crate) async fn retrieve_response(
    state: crate::DicomWebState,
    headers: &HeaderMap,
    uri: &Uri,
    scope: RetrieveScope,
) -> Result<Response, DicomWebError> {
    record_scope(&scope);
    let selected = negotiate_dicom_accept(headers).map_err(record_error)?;
    let requested_transfer_syntax = selected
        .transfer_syntax_uid("accept transfer-syntax")
        .map_err(record_error)?;
    record_selected(&selected, requested_transfer_syntax.as_ref());

    let service = state.retrieve.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "WADO-RS retrieve service is not registered".to_string(),
        ))
    })?;
    let result = retrieve_result(service.as_ref(), scope)
        .await
        .map_err(record_error)?;
    Span::current().record("dicomweb.retrieve.instance_count", result.instance_count);

    match selected.media_type {
        MediaType::MultipartRelated => Ok(multipart_response(
            result,
            headers,
            uri,
            requested_transfer_syntax,
            state.wado_rs_transfer_syntax_policy.as_ref(),
        )
        .await
        .map_err(record_error)?),
        MediaType::ApplicationDicom => {
            single_instance_response(
                result,
                requested_transfer_syntax,
                state.wado_rs_transfer_syntax_policy.as_ref(),
            )
            .await
        }
        _ => unreachable!("WADO object negotiation only offers DICOM representations"),
    }
}

fn negotiate_dicom_accept(headers: &HeaderMap) -> Result<SelectedRepresentation, DicomWebError> {
    media::negotiate_representation(
        headers,
        None,
        &[
            AvailableRepresentation {
                media_type: MediaType::MultipartRelated,
                params: MediaTypeParams {
                    type_: Some(media::APPLICATION_DICOM.to_string()),
                    transfer_syntax: None,
                    charset: None,
                },
            },
            AvailableRepresentation::from(MediaType::ApplicationDicom),
        ],
    )
}

pub(crate) async fn collect_instances(
    retrieve: &dyn RetrieveService,
    scope: RetrieveScope,
) -> Result<Vec<CollectedInstance>, DicomWebError> {
    let result = retrieve_result(retrieve, scope).await?;
    let mut stream = result.stream;
    let mut instances = Vec::with_capacity(result.instance_count);
    while let Some(item) = stream.next().await {
        let instance = item.map_err(map_stream_error)?;
        instances.push(collect_instance(instance).await?);
    }
    if instances.is_empty() {
        return Err(DicomWebError::NotFound(
            "no matching DICOM instances".to_string(),
        ));
    }
    Ok(instances)
}

pub(crate) async fn retrieve_result(
    retrieve: &dyn RetrieveService,
    scope: RetrieveScope,
) -> Result<RetrieveResult, DicomWebError> {
    let result = retrieve
        .retrieve(RetrieveRequest::new(scope))
        .instrument(info_span!("wado.retrieve.service_call"))
        .await
        .map_err(|error| DicomWebError::Internal(format!("retrieve failed: {error}")))?;
    if result.instance_count == 0 {
        return Err(DicomWebError::NotFound(
            "no matching DICOM instances".to_string(),
        ));
    }
    Ok(result)
}

fn map_stream_error(error: RetrieveError) -> DicomWebError {
    DicomWebError::Internal(format!("retrieve stream failed: {error}"))
}

pub(crate) async fn collect_instance(
    instance: RetrievedInstance,
) -> Result<CollectedInstance, DicomWebError> {
    let mut body = instance.body.into_stream();
    let mut bytes = Vec::with_capacity(instance.content_length as usize);
    while let Some(chunk) = body.next().await {
        let chunk = chunk
            .map_err(|error| DicomWebError::Internal(format!("object stream failed: {error}")))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(CollectedInstance {
        identity: instance.identity,
        transfer_syntax_uid: instance.transfer_syntax_uid,
        body: Bytes::from(bytes),
    })
}

async fn multipart_response(
    result: RetrieveResult,
    headers: &HeaderMap,
    uri: &Uri,
    requested_transfer_syntax: Option<TransferSyntaxUid>,
    policy: Option<&TransferSyntaxPolicy>,
) -> Result<Response, DicomWebError> {
    let base = DicomWebUrlBase::from_request(headers, uri);
    let mut instances = result.stream;
    let first_instance =
        next_prepared_instance(&mut instances, requested_transfer_syntax.as_ref(), policy).await?;
    let boundary = media::multipart_boundary();
    let body = multipart_body_stream(
        first_instance,
        instances,
        base,
        boundary.clone(),
        requested_transfer_syntax,
        policy.cloned(),
    );
    Ok(media::multipart_related_response(
        Body::from_stream(body),
        &boundary,
        MediaType::ApplicationDicom,
        None,
    ))
}

pub(crate) async fn single_instance_response(
    result: RetrieveResult,
    requested_transfer_syntax: Option<TransferSyntaxUid>,
    policy: Option<&TransferSyntaxPolicy>,
) -> Result<Response, DicomWebError> {
    if result.instance_count != 1 {
        return Err(DicomWebError::not_acceptable(
            "application/dicom response is only available for single instance retrieve",
        ));
    }
    let mut stream = result.stream;
    let instance =
        next_prepared_instance(&mut stream, requested_transfer_syntax.as_ref(), policy).await?;
    let content_type = media::content_type(
        MediaType::ApplicationDicom,
        &media::MediaTypeParams {
            type_: None,
            transfer_syntax: instance
                .transfer_syntax_uid
                .as_ref()
                .map(|uid| uid.as_str().to_string()),
            charset: None,
        },
    );
    Ok((
        [
            (header::CONTENT_TYPE, header_value(&content_type)?),
            (
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&instance.content_length.to_string())
                    .expect("content length is a valid header value"),
            ),
        ],
        Body::from_stream(instance.body.into_stream().map(|chunk| {
            chunk.map_err(|error| DicomWebError::Internal(format!("object stream failed: {error}")))
        })),
    )
        .into_response())
}

async fn next_prepared_instance(
    stream: &mut Pin<Box<dyn Stream<Item = Result<RetrievedInstance, RetrieveError>> + Send>>,
    requested_transfer_syntax: Option<&TransferSyntaxUid>,
    policy: Option<&TransferSyntaxPolicy>,
) -> Result<RetrievedInstance, DicomWebError> {
    let instance = stream
        .next()
        .await
        .ok_or_else(|| DicomWebError::NotFound("no matching DICOM instances".to_string()))?
        .map_err(map_stream_error)?;
    prepare_instance(instance, requested_transfer_syntax, policy).await
}

async fn prepare_instance(
    instance: RetrievedInstance,
    requested: Option<&TransferSyntaxUid>,
    policy: Option<&TransferSyntaxPolicy>,
) -> Result<RetrievedInstance, DicomWebError> {
    record_stored_transfer_syntax_for_instance(&instance);
    let Some(requested) = requested else {
        Span::current().record("dicomweb.transcode.required", false);
        record_native_transfer_syntax_for_instance(&instance);
        return Ok(instance);
    };
    if instance
        .transfer_syntax_uid
        .as_ref()
        .is_some_and(|stored| stored == requested)
    {
        Span::current().record("dicomweb.transcode.required", false);
        record_native_transfer_syntax_for_instance(&instance);
        return Ok(instance);
    }

    Span::current().record("dicomweb.transcode.required", true);
    let Some(policy) = policy else {
        Span::current().record("dicomweb.transcode.result", "unsupported");
        return Err(DicomWebError::not_acceptable(format!(
            "transfer syntax {requested} requires transcoding, which is not supported"
        )));
    };
    let Some(transcoder) = policy.transcoder() else {
        Span::current().record("dicomweb.transcode.result", "unsupported");
        return Err(DicomWebError::not_acceptable(format!(
            "transfer syntax {requested} requires transcoding, which is not supported"
        )));
    };
    if !policy.allows_target(requested) {
        Span::current().record("dicomweb.transcode.backend", transcoder.backend());
        Span::current().record("dicomweb.transcode.result", "unsupported");
        return Err(DicomWebError::not_acceptable(format!(
            "transfer syntax {requested} requires transcoding, which is not supported"
        )));
    }
    if !transcoder.supports(instance.transfer_syntax_uid.as_ref(), requested) {
        Span::current().record("dicomweb.transcode.backend", transcoder.backend());
        Span::current().record("dicomweb.transcode.result", "unsupported");
        return Err(DicomWebError::not_acceptable(format!(
            "transfer syntax {requested} requires transcoding, which is not supported"
        )));
    }

    Span::current().record("dicomweb.transcode.backend", transcoder.backend());
    match transcoder.transcode(instance, requested).await {
        Ok(transcoded) => {
            Span::current().record("dicomweb.transcode.result", "success");
            record_native_transfer_syntax_for_instance(&transcoded.instance);
            Ok(transcoded.instance)
        }
        Err(TranscodeError::Unsupported { .. }) => {
            Span::current().record("dicomweb.transcode.result", "unsupported");
            Err(DicomWebError::not_acceptable(format!(
                "transfer syntax {requested} requires transcoding, which is not supported"
            )))
        }
        Err(error) => {
            Span::current().record("dicomweb.transcode.result", "failed");
            Err(DicomWebError::Internal(format!(
                "transcode failed: {error}"
            )))
        }
    }
}

fn multipart_body_stream(
    first_instance: RetrievedInstance,
    instances: Pin<
        Box<dyn Stream<Item = Result<RetrievedInstance, RetrieveError>> + Send + 'static>,
    >,
    base: Option<DicomWebUrlBase>,
    boundary: String,
    requested_transfer_syntax: Option<TransferSyntaxUid>,
    policy: Option<TransferSyntaxPolicy>,
) -> impl Stream<Item = Result<Bytes, DicomWebError>> + Send + 'static {
    stream::try_unfold(
        MultipartBodyState {
            instances,
            first_instance: Some(first_instance),
            current_body: None,
            pending: VecDeque::new(),
            base,
            boundary,
            requested_transfer_syntax,
            policy,
            finished: false,
        },
        next_multipart_chunk,
    )
}

async fn next_multipart_chunk(
    mut state: MultipartBodyState,
) -> Result<Option<(Bytes, MultipartBodyState)>, DicomWebError> {
    loop {
        if let Some(chunk) = state.pending.pop_front() {
            return Ok(Some((chunk, state)));
        }

        if let Some(body) = state.current_body.as_mut() {
            match body.next().await {
                Some(Ok(chunk)) => return Ok(Some((chunk, state))),
                Some(Err(error)) => {
                    return Err(DicomWebError::Internal(format!(
                        "object stream failed: {error}"
                    )));
                }
                None => {
                    state.current_body = None;
                    state.pending.push_back(Bytes::from_static(b"\r\n"));
                    continue;
                }
            }
        }

        if let Some(instance) = state.first_instance.take() {
            begin_multipart_part(&mut state, instance);
            continue;
        }

        if state.finished {
            return Ok(None);
        }

        match state.instances.next().await {
            Some(Ok(instance)) => {
                let instance = prepare_instance(
                    instance,
                    state.requested_transfer_syntax.as_ref(),
                    state.policy.as_ref(),
                )
                .await?;
                begin_multipart_part(&mut state, instance);
            }
            Some(Err(error)) => return Err(map_stream_error(error)),
            None => {
                state.finished = true;
                state
                    .pending
                    .push_back(Bytes::from(format!("--{}--\r\n", state.boundary)));
            }
        }
    }
}

fn begin_multipart_part(state: &mut MultipartBodyState, instance: RetrievedInstance) {
    let part_header = format!(
        "--{}\r\n{}\r\n{}\r\n\r\n",
        state.boundary,
        part_content_type(instance.transfer_syntax_uid.as_ref()),
        content_location(&instance.identity, state.base.as_ref()),
    );
    state.pending.push_back(Bytes::from(part_header));
    state.current_body = Some(instance.body.into_stream());
}

fn part_content_type(transfer_syntax_uid: Option<&TransferSyntaxUid>) -> String {
    let mut value = "Content-Type: application/dicom".to_string();
    if let Some(transfer_syntax_uid) = transfer_syntax_uid {
        value.push_str("; transfer-syntax=\"");
        value.push_str(transfer_syntax_uid.as_str());
        value.push('"');
    }
    value
}

fn content_location(identity: &DicomInstanceIdentity, base: Option<&DicomWebUrlBase>) -> String {
    let value = if let Some(base) = base {
        base.instance_retrieve_url(
            &identity.study_instance_uid,
            &identity.series_instance_uid,
            &identity.sop_instance_uid,
        )
        .to_string()
    } else {
        format!(
            "/studies/{}/series/{}/instances/{}",
            identity.study_instance_uid, identity.series_instance_uid, identity.sop_instance_uid
        )
    };
    format!("Content-Location: {value}")
}

fn header_value(value: &str) -> Result<HeaderValue, DicomWebError> {
    HeaderValue::from_str(value)
        .map_err(|error| DicomWebError::Internal(format!("invalid response header: {error}")))
}

fn record_selected(
    selected: &SelectedRepresentation,
    requested_transfer_syntax: Option<&TransferSyntaxUid>,
) {
    let span = Span::current();
    span.record("dicomweb.selected_media_type", selected.content_type());
    if let Some(transfer_syntax) = requested_transfer_syntax {
        span.record(
            "dicomweb.requested_transfer_syntax_uid",
            transfer_syntax.as_str(),
        );
    }
}

fn record_native_transfer_syntax_for_instance(instance: &RetrievedInstance) {
    if let Some(transfer_syntax_uid) = instance.transfer_syntax_uid.as_ref() {
        Span::current().record(
            "dicomweb.returned_transfer_syntax_uid",
            transfer_syntax_uid.as_str(),
        );
    }
}

fn record_stored_transfer_syntax_for_instance(instance: &RetrievedInstance) {
    if let Some(transfer_syntax_uid) = instance.transfer_syntax_uid.as_ref() {
        Span::current().record(
            "dicomweb.stored_transfer_syntax_uid",
            transfer_syntax_uid.as_str(),
        );
    }
}

pub(crate) fn record_scope(scope: &RetrieveScope) {
    let span = Span::current();
    match scope {
        RetrieveScope::Study { study_instance_uid } => {
            span.record("dicom.study_instance_uid", study_instance_uid.as_str());
        }
        RetrieveScope::Series {
            study_instance_uid,
            series_instance_uid,
        } => {
            if let Some(study_instance_uid) = study_instance_uid {
                span.record("dicom.study_instance_uid", study_instance_uid.as_str());
            }
            span.record("dicom.series_instance_uid", series_instance_uid.as_str());
        }
        RetrieveScope::Instance {
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        } => {
            if let Some(study_instance_uid) = study_instance_uid {
                span.record("dicom.study_instance_uid", study_instance_uid.as_str());
            }
            if let Some(series_instance_uid) = series_instance_uid {
                span.record("dicom.series_instance_uid", series_instance_uid.as_str());
            }
            span.record("dicom.sop_instance_uid", sop_instance_uid.as_str());
        }
        RetrieveScope::Patient { .. } => {}
    }
    span.record("dicomweb.retrieve.scope", scope.label());
}
