use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Uri, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use raccoon_contract_dicom::{DicomInstanceIdentity, TransferSyntaxUid};
use raccoon_contract_object_store::Bytes;
use raccoon_service_retrieve::{
    RetrieveError, RetrieveRequest, RetrieveScope, RetrieveService, RetrievedInstance,
};
use tracing::Span;

use crate::media::{
    self, AvailableRepresentation, MediaType, MediaTypeParams, SelectedRepresentation,
};
use crate::{DicomWebError, DicomWebUrlBase};

const BOUNDARY: &str = "raccoon-dicomweb-boundary";

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
    let instances = collect_instances(service.as_ref(), scope)
        .await
        .map_err(record_error)?;

    validate_transfer_syntaxes(&instances, requested_transfer_syntax.as_ref())
        .map_err(record_error)?;
    Span::current().record("dicomweb.retrieve.instance_count", instances.len());
    record_native_transfer_syntax(&instances);

    match selected.media_type {
        MediaType::MultipartRelated => Ok(multipart_response(instances, headers, uri)),
        MediaType::ApplicationDicom => single_instance_response(instances).map_err(record_error),
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
    let result = retrieve
        .retrieve(RetrieveRequest::new(scope))
        .await
        .map_err(|error| DicomWebError::Internal(format!("retrieve failed: {error}")))?;
    if result.instance_count == 0 {
        return Err(DicomWebError::NotFound(
            "no matching DICOM instances".to_string(),
        ));
    }

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

fn map_stream_error(error: RetrieveError) -> DicomWebError {
    DicomWebError::Internal(format!("retrieve stream failed: {error}"))
}

async fn collect_instance(instance: RetrievedInstance) -> Result<CollectedInstance, DicomWebError> {
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

pub(crate) fn validate_transfer_syntaxes(
    instances: &[CollectedInstance],
    requested: Option<&TransferSyntaxUid>,
) -> Result<(), DicomWebError> {
    let Some(requested) = requested else {
        return Ok(());
    };
    if instances.iter().all(|instance| {
        instance
            .transfer_syntax_uid
            .as_ref()
            .is_some_and(|stored| stored == requested)
    }) {
        return Ok(());
    }
    Err(DicomWebError::not_acceptable(format!(
        "transfer syntax {requested} requires transcoding, which is not supported"
    )))
}

fn multipart_response(
    instances: Vec<CollectedInstance>,
    headers: &HeaderMap,
    uri: &Uri,
) -> Response {
    let base = DicomWebUrlBase::from_request(headers, uri);
    let mut body = Vec::new();
    for instance in instances {
        body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        body.extend_from_slice(part_content_type(instance.transfer_syntax_uid.as_ref()).as_bytes());
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(content_location(&instance.identity, base.as_ref()).as_bytes());
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(&instance.body);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    media::multipart_related_response(
        Body::from(body),
        BOUNDARY,
        MediaType::ApplicationDicom,
        None,
    )
}

pub(crate) fn single_instance_response(
    instances: Vec<CollectedInstance>,
) -> Result<Response, DicomWebError> {
    if instances.len() != 1 {
        return Err(DicomWebError::not_acceptable(
            "application/dicom response is only available for single instance retrieve",
        ));
    }
    let instance = instances.into_iter().next().expect("length checked");
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
        Body::from(instance.body),
    )
        .into_response())
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

pub(crate) fn record_native_transfer_syntax(instances: &[CollectedInstance]) {
    let first = instances
        .first()
        .and_then(|instance| instance.transfer_syntax_uid.as_ref());
    if let Some(first) = first
        && instances
            .iter()
            .all(|instance| instance.transfer_syntax_uid.as_ref() == Some(first))
    {
        Span::current().record("dicomweb.returned_transfer_syntax_uid", first.as_str());
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

fn record_error(error: DicomWebError) -> DicomWebError {
    Span::current().record("error.type", error.http_error_class());
    error
}
