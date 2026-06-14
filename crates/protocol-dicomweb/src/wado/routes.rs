use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use tracing::Instrument;

use super::{bulkdata, metadata, retrieve, scope};
use crate::instrumentation::record_error;
use crate::{DicomWebError, DicomWebRouteRegistry, DicomWebState, FrameList, RouteTelemetry};

pub(crate) fn register(registry: &mut DicomWebRouteRegistry) {
    registry.route(
        "/studies/{study}",
        get(retrieve_study),
        RouteTelemetry::new("WADO-RS", "studies", "/studies/{study}"),
    );
    registry.route(
        "/studies/{study}/metadata",
        get(study_metadata),
        RouteTelemetry::new("WADO-RS", "metadata", "/studies/{study}/metadata"),
    );
    registry.route(
        "/studies/{study}/pixeldata",
        get(study_pixel_data),
        RouteTelemetry::new("WADO-RS", "pixeldata", "/studies/{study}/pixeldata"),
    );
    registry.route(
        "/studies/{study}/series/{series}",
        get(retrieve_series),
        RouteTelemetry::new("WADO-RS", "series", "/studies/{study}/series/{series}"),
    );
    registry.route(
        "/studies/{study}/series/{series}/metadata",
        get(series_metadata),
        RouteTelemetry::new(
            "WADO-RS",
            "metadata",
            "/studies/{study}/series/{series}/metadata",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/pixeldata",
        get(series_pixel_data),
        RouteTelemetry::new(
            "WADO-RS",
            "pixeldata",
            "/studies/{study}/series/{series}/pixeldata",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}",
        get(retrieve_instance),
        RouteTelemetry::new(
            "WADO-RS",
            "instances",
            "/studies/{study}/series/{series}/instances/{instance}",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/metadata",
        get(instance_metadata),
        RouteTelemetry::new(
            "WADO-RS",
            "metadata",
            "/studies/{study}/series/{series}/instances/{instance}/metadata",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/bulkdata/{*path}",
        get(instance_bulk_data),
        RouteTelemetry::new(
            "WADO-RS",
            "bulkdata",
            "/studies/{study}/series/{series}/instances/{instance}/bulkdata/{path}",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/pixeldata",
        get(instance_pixel_data),
        RouteTelemetry::new(
            "WADO-RS",
            "pixeldata",
            "/studies/{study}/series/{series}/instances/{instance}/pixeldata",
        ),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}",
        get(instance_frames),
        RouteTelemetry::new(
            "WADO-RS",
            "frames",
            "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}",
        ),
    );
}

#[derive(Debug, Deserialize)]
struct StudyPath {
    study: String,
}

#[derive(Debug, Deserialize)]
struct SeriesPath {
    study: String,
    series: String,
}

#[derive(Debug, Deserialize)]
struct InstancePath {
    study: String,
    series: String,
    instance: String,
}

#[derive(Debug, Deserialize)]
struct BulkDataPath {
    study: String,
    series: String,
    instance: String,
    path: String,
}

#[derive(Debug, Deserialize)]
struct FramesPath {
    study: String,
    series: String,
    instance: String,
    frames: String,
}

async fn retrieve_study(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span("dicom", "/studies/{study}", &uri);
    async move {
        let scope = scope::study_scope(path.study).map_err(record_error)?;
        retrieve::retrieve_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn retrieve_series(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span("dicom", "/studies/{study}/series/{series}", &uri);
    async move {
        let scope = scope::series_scope(path.study, path.series).map_err(record_error)?;
        retrieve::retrieve_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn retrieve_instance(
    State(state): State<DicomWebState>,
    Path(path): Path<InstancePath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "dicom",
        "/studies/{study}/series/{series}/instances/{instance}",
        &uri,
    );
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        retrieve::retrieve_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn study_metadata(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span("metadata", "/studies/{study}/metadata", &uri);
    async move {
        let scope = scope::study_scope(path.study).map_err(record_error)?;
        metadata::metadata_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn study_pixel_data(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span("pixeldata", "/studies/{study}/pixeldata", &uri);
    async move {
        let scope = scope::study_scope(path.study).map_err(record_error)?;
        bulkdata::pixel_data_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn series_metadata(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "metadata",
        "/studies/{study}/series/{series}/metadata",
        &uri,
    );
    async move {
        let scope = scope::series_scope(path.study, path.series).map_err(record_error)?;
        metadata::metadata_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn series_pixel_data(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "pixeldata",
        "/studies/{study}/series/{series}/pixeldata",
        &uri,
    );
    async move {
        let scope = scope::series_scope(path.study, path.series).map_err(record_error)?;
        bulkdata::pixel_data_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn instance_metadata(
    State(state): State<DicomWebState>,
    Path(path): Path<InstancePath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "metadata",
        "/studies/{study}/series/{series}/instances/{instance}/metadata",
        &uri,
    );
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        metadata::metadata_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn instance_bulk_data(
    State(state): State<DicomWebState>,
    Path(path): Path<BulkDataPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "bulkdata",
        "/studies/{study}/series/{series}/instances/{instance}/bulkdata/{path}",
        &uri,
    );
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        bulkdata::bulk_data_response(state, &headers, &uri, scope, &path.path).await
    }
    .instrument(span)
    .await
}

async fn instance_pixel_data(
    State(state): State<DicomWebState>,
    Path(path): Path<InstancePath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "pixeldata",
        "/studies/{study}/series/{series}/instances/{instance}/pixeldata",
        &uri,
    );
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        bulkdata::pixel_data_response(state, &headers, &uri, scope).await
    }
    .instrument(span)
    .await
}

async fn instance_frames(
    State(state): State<DicomWebState>,
    Path(path): Path<FramesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, DicomWebError> {
    let span = wado_span(
        "frames",
        "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}",
        &uri,
    );
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        let frames = FrameList::parse(&path.frames).map_err(record_error)?;
        bulkdata::frames_response(state, &headers, &uri, scope, &frames).await
    }
    .instrument(span)
    .await
}

fn wado_span(resource: &'static str, route: &'static str, uri: &Uri) -> tracing::Span {
    tracing::info_span!(
        "wado-rs retrieve",
        http.request.method = "GET",
        http.route = route,
        url.path = uri.path(),
        dicomweb.service = "WADO-RS",
        dicomweb.resource = resource,
        dicomweb.retrieve.scope = tracing::field::Empty,
        dicom.study_instance_uid = tracing::field::Empty,
        dicom.series_instance_uid = tracing::field::Empty,
        dicom.sop_instance_uid = tracing::field::Empty,
        dicomweb.selected_media_type = tracing::field::Empty,
        dicomweb.requested_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.stored_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.returned_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.transcode.required = tracing::field::Empty,
        dicomweb.transcode.backend = tracing::field::Empty,
        dicomweb.transcode.result = tracing::field::Empty,
        dicomweb.retrieve.instance_count = tracing::field::Empty,
        dicomweb.metadata.row_count = tracing::field::Empty,
        dicomweb.metadata.bulk_data_uri_count = tracing::field::Empty,
        dicomweb.requested_frame_count = tracing::field::Empty,
        dicomweb.returned_part_count = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        dicomweb.error_type = tracing::field::Empty,
        error.message = tracing::field::Empty,
    )
}
