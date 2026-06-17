use axum::body::Body;
use axum::extract::{OriginalUri, Path, Request, State};
use axum::http::{HeaderMap, Uri, header};
use axum::response::Response;
use axum::routing::post;
use futures_util::TryStreamExt;
use raccoon_contract_dicom::StudyInstanceUid;
use raccoon_service_ingest::{
    IngestBatchRepositoryStatus, IngestPayloadRepresentation, IngestRequest, IngestSource,
    IngestUploadId,
};
use serde::Deserialize;
use tracing::Instrument;

use super::{metadata, response, spool};
use crate::instrumentation::record_error;
use crate::{
    APPLICATION_DICOM, APPLICATION_DICOM_JSON, APPLICATION_DICOM_XML, DicomWebError,
    DicomWebRouteRegistry, DicomWebState, MULTIPART_RELATED, RouteTelemetry,
};

pub(crate) fn register(registry: &mut DicomWebRouteRegistry) {
    registry.route(
        "/studies",
        post(store_instances),
        RouteTelemetry::new("STOW-RS", "studies", "/studies"),
    );
    registry.route(
        "/studies/{study}",
        post(store_study_instances),
        RouteTelemetry::new("STOW-RS", "studies", "/studies/{study}"),
    );
}

#[derive(Debug, Deserialize)]
struct StudyPath {
    study: String,
}

#[derive(Debug, Clone)]
struct StowRequestMedia {
    content_type: String,
    boundary: String,
    part_type: StowRequestPartType,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StowRequestPartType {
    Dicom,
    Metadata(metadata::MetadataMedia),
}

async fn store_instances(
    State(state): State<DicomWebState>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Result<Response, DicomWebError> {
    let span = stow_span("/studies", uri.path());
    async move {
        let (_parts, body) = request.into_parts();
        store(state, headers, uri, body, None).await
    }
    .instrument(span)
    .await
}

async fn store_study_instances(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    request: Request,
) -> Result<Response, DicomWebError> {
    let span = stow_span("/studies/{study}", uri.path());
    async move {
        let study = StudyInstanceUid::new(path.study).map_err(|error| {
            record_error(DicomWebError::invalid_uid("path StudyInstanceUID", error))
        })?;
        tracing::Span::current().record("dicom.study_instance_uid", study.as_str());
        let (_parts, body) = request.into_parts();
        store(state, headers, uri, body, Some(study)).await
    }
    .instrument(span)
    .await
}

async fn store(
    state: DicomWebState,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
    expected_study: Option<StudyInstanceUid>,
) -> Result<Response, DicomWebError> {
    let media = parse_stow_request_media(&headers).map_err(record_error)?;
    tracing::Span::current().record("dicomweb.request_media_type", media.content_type.as_str());
    if let StowRequestPartType::Metadata(metadata_media) = media.part_type {
        return metadata::store(metadata::StoreMetadataRequest {
            state,
            headers,
            uri,
            body,
            expected_study,
            metadata_media,
            boundary: media.boundary,
            request_content_type: media.content_type,
        })
        .await;
    }

    let service = state.ingest.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "STOW-RS ingest service is not registered".to_string(),
        ))
    })?;
    let body_stream = body
        .into_data_stream()
        .map_err(|error| std::io::Error::other(error.to_string()));
    let mut multipart = multer::Multipart::new(body_stream, media.boundary.clone());
    let upload_id = IngestUploadId::new();
    let options = state.stow.unwrap_or_default();
    let mut results = Vec::new();
    let mut repository_status = IngestBatchRepositoryStatus::Recorded;
    let mut part_index = 0_u64;

    while let Some(field) = multipart.next_field().await.map_err(|error| {
        record_error(DicomWebError::bad_request(format!(
            "invalid multipart body: {error}"
        )))
    })? {
        validate_part_content_type(&field).map_err(record_error)?;
        part_index = part_index.saturating_add(1);
        if let Some(max_part_count) = options.max_part_count()
            && part_index as usize > max_part_count
        {
            return Err(record_error(DicomWebError::payload_too_large(format!(
                "STOW-RS request exceeds configured maximum of {max_part_count} DICOM parts"
            ))));
        }
        let spool = spool::spool_field(field, part_index, options.max_part_size_bytes())
            .await
            .map_err(record_error)?;
        tracing::Span::current().record("dicomweb.spool_bytes", spool.size_bytes());

        let mut request = IngestRequest::new(upload_id.clone(), spool.byte_stream().await?)
            .with_payload_representation(IngestPayloadRepresentation::DicomFile)
            .with_source(IngestSource {
                protocol: Some("dicomweb".to_string()),
                content_type: Some(media.content_type.clone()),
                ..IngestSource::default()
            });
        if let Some(study) = expected_study.as_ref() {
            request = request.with_expected_study_instance_uid(study.as_str());
        }
        let batch = service.ingest_upload_objects(vec![request]).await;
        results.extend(batch.object_results);
        if matches!(
            batch.repository_status,
            IngestBatchRepositoryStatus::Failed { .. }
        ) {
            repository_status = batch.repository_status;
            break;
        }
    }

    if results.is_empty() {
        return Err(record_error(DicomWebError::bad_request(
            "STOW-RS request did not contain any DICOM parts",
        )));
    }

    Ok(response::storage_response(
        &headers,
        &uri,
        &results,
        &repository_status,
    ))
}

fn parse_stow_request_media(headers: &HeaderMap) -> Result<StowRequestMedia, DicomWebError> {
    let value = headers
        .get(header::CONTENT_TYPE)
        .ok_or_else(|| DicomWebError::unsupported_media_type("missing Content-Type header"))?
        .to_str()
        .map_err(|_| DicomWebError::unsupported_media_type("invalid Content-Type header"))?;
    let parsed = parse_content_type(value)?;
    if parsed.media_type != MULTIPART_RELATED {
        return Err(DicomWebError::unsupported_media_type(format!(
            "unsupported STOW-RS Content-Type {value:?}"
        )));
    }
    let part_type = parsed
        .params
        .iter()
        .find_map(|(name, value)| (name == "type").then_some(value.as_str()))
        .ok_or_else(|| {
            DicomWebError::unsupported_media_type("missing STOW-RS multipart type parameter")
        })?;
    let part_type = if part_type.eq_ignore_ascii_case(APPLICATION_DICOM) {
        StowRequestPartType::Dicom
    } else if part_type.eq_ignore_ascii_case(APPLICATION_DICOM_JSON) {
        StowRequestPartType::Metadata(metadata::MetadataMedia::Json)
    } else if part_type.eq_ignore_ascii_case(APPLICATION_DICOM_XML) {
        StowRequestPartType::Metadata(metadata::MetadataMedia::Xml)
    } else {
        return Err(DicomWebError::unsupported_media_type(format!(
            "unsupported STOW-RS request media type {part_type}"
        )));
    };
    Ok(StowRequestMedia {
        content_type: value.to_string(),
        boundary: multipart_boundary(parsed.params)?,
        part_type,
    })
}

fn multipart_boundary(params: Vec<(String, String)>) -> Result<String, DicomWebError> {
    params
        .into_iter()
        .find_map(|(name, value)| (name == "boundary").then_some(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DicomWebError::bad_request("missing STOW-RS multipart boundary"))
}

fn validate_part_content_type(field: &multer::Field<'_>) -> Result<(), DicomWebError> {
    let Some(part_content_type) = field.content_type() else {
        return Err(DicomWebError::unsupported_media_type(
            "missing STOW-RS part Content-Type",
        ));
    };
    if part_content_type.type_() != "application" || part_content_type.subtype() != "dicom" {
        return Err(DicomWebError::unsupported_media_type(format!(
            "unsupported STOW-RS part Content-Type {part_content_type}"
        )));
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedContentType {
    media_type: String,
    params: Vec<(String, String)>,
}

fn parse_content_type(value: &str) -> Result<ParsedContentType, DicomWebError> {
    let mut parts = value.split(';');
    let media_type = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DicomWebError::unsupported_media_type("invalid Content-Type header"))?
        .to_ascii_lowercase();
    let mut params = Vec::new();
    for part in parts {
        let (name, value) = part.trim().split_once('=').ok_or_else(|| {
            DicomWebError::unsupported_media_type("invalid Content-Type parameter")
        })?;
        params.push((
            name.trim().to_ascii_lowercase(),
            unquote(value.trim()).to_string(),
        ));
    }
    Ok(ParsedContentType { media_type, params })
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

fn stow_span(route: &'static str, path: &str) -> tracing::Span {
    tracing::info_span!(
        "stow-rs store",
        http.request.method = "POST",
        http.route = route,
        url.path = path,
        dicomweb.service = "STOW-RS",
        dicomweb.resource = "studies",
        dicom.study_instance_uid = tracing::field::Empty,
        dicomweb.request_media_type = tracing::field::Empty,
        dicomweb.store.metadata_media_type = tracing::field::Empty,
        dicomweb.store.bulk_part_count = tracing::field::Empty,
        dicomweb.store.reconstructed_instance_count = tracing::field::Empty,
        dicomweb.store.missing_bulk_part_count = tracing::field::Empty,
        dicomweb.object_count = tracing::field::Empty,
        dicomweb.successful_object_count = tracing::field::Empty,
        dicomweb.failed_object_count = tracing::field::Empty,
        dicomweb.spool_bytes = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        dicomweb.error_type = tracing::field::Empty,
        error.message = tracing::field::Empty,
    )
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, header};

    use super::*;

    #[test]
    fn parses_quoted_boundary_from_multipart_related() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "multipart/related; type=\"application/dicom\"; boundary=\"abc\""
                .parse()
                .unwrap(),
        );

        assert_eq!(parse_stow_request_media(&headers).unwrap().boundary, "abc");
    }

    #[test]
    fn accepts_metadata_plus_bulk_data_request_media() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "multipart/related; type=\"application/dicom+json\"; boundary=abc"
                .parse()
                .unwrap(),
        );

        assert_eq!(
            parse_stow_request_media(&headers).unwrap().part_type,
            StowRequestPartType::Metadata(metadata::MetadataMedia::Json)
        );
    }

    #[test]
    fn preserves_case_sensitive_boundary_value() {
        let parsed =
            parse_content_type("multipart/related; type=\"application/dicom\"; boundary=BOUNDARY")
                .unwrap();

        assert_eq!(
            parsed
                .params
                .iter()
                .find(|(name, _)| name == "boundary")
                .unwrap()
                .1,
            "BOUNDARY"
        );
    }
}
