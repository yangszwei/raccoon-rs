use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Uri, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use raccoon_contract_dicom::{DicomInstanceIdentity, TransferSyntaxUid};
use raccoon_contract_object_store::Bytes;
use raccoon_service_retrieve::{
    RetrieveError, RetrieveRequest, RetrieveResult, RetrieveScope, RetrieveService,
    RetrievedInstance,
};
use tracing::Span;

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
    let instances = prepare_instances(result, requested_transfer_syntax.as_ref(), policy).await?;
    let boundary = media::multipart_boundary();
    let body = multipart_body(instances, base, &boundary).await?;
    Ok(media::multipart_related_response(
        Body::from(body),
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
    let mut instances =
        prepare_instances(result, requested_transfer_syntax.as_ref(), policy).await?;
    let instance = instances
        .pop()
        .ok_or_else(|| DicomWebError::NotFound("no matching DICOM instances".to_string()))?;
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
        [(header::CONTENT_TYPE, header_value(&content_type)?)],
        Body::from_stream(instance.body.into_stream().map(|chunk| {
            chunk.map_err(|error| DicomWebError::Internal(format!("object stream failed: {error}")))
        })),
    )
        .into_response())
}

async fn prepare_instances(
    result: RetrieveResult,
    requested_transfer_syntax: Option<&TransferSyntaxUid>,
    policy: Option<&TransferSyntaxPolicy>,
) -> Result<Vec<RetrievedInstance>, DicomWebError> {
    let mut stream = result.stream;
    let mut instances = Vec::with_capacity(result.instance_count);
    while let Some(item) = stream.next().await {
        let instance = item.map_err(map_stream_error)?;
        instances.push(prepare_instance(instance, requested_transfer_syntax, policy).await?);
    }
    if instances.is_empty() {
        return Err(DicomWebError::NotFound(
            "no matching DICOM instances".to_string(),
        ));
    }
    Ok(instances)
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

async fn multipart_body(
    instances: Vec<RetrievedInstance>,
    base: Option<DicomWebUrlBase>,
    boundary: &str,
) -> Result<Bytes, DicomWebError> {
    let mut bytes = Vec::new();
    for instance in instances {
        bytes.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        bytes
            .extend_from_slice(part_content_type(instance.transfer_syntax_uid.as_ref()).as_bytes());
        bytes.extend_from_slice(b"\r\n");
        bytes.extend_from_slice(content_location(&instance.identity, base.as_ref()).as_bytes());
        bytes.extend_from_slice(b"\r\n\r\n");
        let mut body = instance.body.into_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| {
                DicomWebError::Internal(format!("object stream failed: {error}"))
            })?;
            bytes.extend_from_slice(&chunk);
        }
        bytes.extend_from_slice(b"\r\n");
    }
    bytes.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    Ok(Bytes::from(bytes))
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
