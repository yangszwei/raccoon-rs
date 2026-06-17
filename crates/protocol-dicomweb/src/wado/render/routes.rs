use axum::body::Body;
use axum::extract::{OriginalUri, Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use raccoon_service_retrieve::RetrieveScope;
use serde::Deserialize;
use tracing::Instrument;

use super::{
    RenderParams, RenderRequest, RenderedImage, render_error, validate_render_params,
    validate_thumbnail_params,
};
use crate::media::{self, MediaType, MediaTypeParams, SelectedRepresentation};
use crate::wado::{record_scope, scope};
use crate::{DicomWebError, DicomWebRouteRegistry, DicomWebState, FrameList};

const BOUNDARY: &str = "raccoon-dicomweb-rendered-boundary";

pub(crate) fn register(registry: &mut DicomWebRouteRegistry) {
    registry.route("/studies/{study}/rendered", get(study_rendered));
    registry.route("/studies/{study}/thumbnail", get(study_thumbnail));
    registry.route(
        "/studies/{study}/series/{series}/rendered",
        get(series_rendered),
    );
    registry.route(
        "/studies/{study}/series/{series}/thumbnail",
        get(series_thumbnail),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/rendered",
        get(instance_rendered),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/thumbnail",
        get(instance_thumbnail),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}/rendered",
        get(frames_rendered),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}/thumbnail",
        get(frames_thumbnail),
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
struct FramesPath {
    study: String,
    series: String,
    instance: String,
    frames: String,
}

async fn study_rendered(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/rendered";
    let span = render_span("rendered", route, &uri);
    async move {
        let scope = scope::study_scope(path.study).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, false, false).await
    }
    .instrument(span)
    .await
}

async fn study_thumbnail(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/thumbnail";
    let span = render_span("thumbnail", route, &uri);
    async move {
        let scope = scope::study_scope(path.study).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, true, true).await
    }
    .instrument(span)
    .await
}

async fn series_rendered(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/rendered";
    let span = render_span("rendered", route, &uri);
    async move {
        let scope = scope::series_scope(path.study, path.series).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, false, false).await
    }
    .instrument(span)
    .await
}

async fn series_thumbnail(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/thumbnail";
    let span = render_span("thumbnail", route, &uri);
    async move {
        let scope = scope::series_scope(path.study, path.series).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, true, true).await
    }
    .instrument(span)
    .await
}

async fn instance_rendered(
    State(state): State<DicomWebState>,
    Path(path): Path<InstancePath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/instances/{instance}/rendered";
    let span = render_span("rendered", route, &uri);
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, false, true).await
    }
    .instrument(span)
    .await
}

async fn instance_thumbnail(
    State(state): State<DicomWebState>,
    Path(path): Path<InstancePath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/instances/{instance}/thumbnail";
    let span = render_span("thumbnail", route, &uri);
    async move {
        let scope =
            scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
        render_response(state, &headers, query.as_deref(), scope, None, true, true).await
    }
    .instrument(span)
    .await
}

async fn frames_rendered(
    State(state): State<DicomWebState>,
    Path(path): Path<FramesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}/rendered";
    let span = render_span("rendered", route, &uri);
    async move {
        let (scope, frames) = frame_scope(path)?;
        render_response(
            state,
            &headers,
            query.as_deref(),
            scope,
            Some(frames),
            false,
            false,
        )
        .await
    }
    .instrument(span)
    .await
}

async fn frames_thumbnail(
    State(state): State<DicomWebState>,
    Path(path): Path<FramesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let route = "/studies/{study}/series/{series}/instances/{instance}/frames/{frames}/thumbnail";
    let span = render_span("thumbnail", route, &uri);
    async move {
        let (scope, frames) = frame_scope(path)?;
        render_response(
            state,
            &headers,
            query.as_deref(),
            scope,
            Some(frames),
            true,
            true,
        )
        .await
    }
    .instrument(span)
    .await
}

async fn render_response(
    state: DicomWebState,
    headers: &HeaderMap,
    query: Option<&str>,
    scope: RetrieveScope,
    frames: Option<Vec<u32>>,
    thumbnail: bool,
    single: bool,
) -> Result<Response, DicomWebError> {
    record_scope(&scope);
    let default_media_type = state
        .render_default_media_type
        .as_deref()
        .unwrap_or(media::IMAGE_JPEG);
    let selected = negotiate_render_accept(headers, default_media_type).map_err(record_error)?;
    let media_type = selected_render_media_type(&selected).map_err(record_error)?;
    tracing::Span::current().record("dicomweb.selected_media_type", media_type.as_str());
    let params = parse_render_params(query).map_err(record_error)?;
    if thumbnail {
        validate_thumbnail_params(&params).map_err(record_error)?;
    } else {
        validate_render_params(&params).map_err(record_error)?;
    }
    let frame_count = frames.as_ref().map_or(0, Vec::len);
    tracing::Span::current().record("dicomweb.requested_frame_count", frame_count as u64);
    let render = state.render.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "WADO-RS render service is not registered".to_string(),
        ))
    })?;
    let response = render
        .render(RenderRequest {
            scope,
            frames,
            media_type: media_type.clone(),
            params,
            thumbnail,
            single,
        })
        .await
        .map_err(|error| record_error(render_error(error)))?;
    tracing::Span::current().record("dicomweb.returned_part_count", response.images.len() as u64);
    if single || response.images.len() == 1 {
        let image = response
            .images
            .into_iter()
            .next()
            .ok_or_else(|| record_error(DicomWebError::not_acceptable("no rendered image")))?;
        return single_image_response(image);
    }
    multipart_image_response(response.images, &media_type)
}

fn negotiate_render_accept(
    headers: &HeaderMap,
    default_media_type: &str,
) -> Result<SelectedRepresentation, DicomWebError> {
    if !headers.contains_key(header::ACCEPT) {
        return Ok(SelectedRepresentation {
            media_type: match default_media_type {
                media::IMAGE_PNG => MediaType::ImagePng,
                _ => MediaType::ImageJpeg,
            },
            params: MediaTypeParams::default(),
        });
    }
    media::negotiate_response(
        headers,
        None,
        &[
            MediaType::ImageJpeg,
            MediaType::ImagePng,
            MediaType::MultipartRelated,
        ],
    )
}

fn selected_render_media_type(selected: &SelectedRepresentation) -> Result<String, DicomWebError> {
    match selected.media_type {
        MediaType::ImageJpeg | MediaType::ImagePng => Ok(selected.media_type.as_str().to_string()),
        MediaType::MultipartRelated => match selected.params.type_.as_deref() {
            Some(media::IMAGE_JPEG) | Some(media::IMAGE_PNG) => {
                Ok(selected.params.type_.clone().expect("type checked"))
            }
            _ => Err(DicomWebError::not_acceptable(
                "WADO-RS rendered multipart response requires type=\"image/jpeg\" or type=\"image/png\"",
            )),
        },
        _ => unreachable!("render negotiation only offers image representations"),
    }
}

fn single_image_response(image: RenderedImage) -> Result<Response, DicomWebError> {
    Ok((
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&image.media_type)
                .map_err(|error| DicomWebError::Internal(format!("invalid header: {error}")))?,
        )],
        Body::from(image.bytes),
    )
        .into_response())
}

fn multipart_image_response(
    images: Vec<RenderedImage>,
    media_type: &str,
) -> Result<Response, DicomWebError> {
    let mut body = Vec::new();
    for image in images {
        body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        body.extend_from_slice(format!("Content-Type: {}\r\n\r\n", image.media_type).as_bytes());
        body.extend_from_slice(&image.bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    Ok(media::multipart_related_response(
        Body::from(body),
        BOUNDARY,
        match media_type {
            media::IMAGE_PNG => MediaType::ImagePng,
            _ => MediaType::ImageJpeg,
        },
        None,
    ))
}

fn parse_render_params(query: Option<&str>) -> Result<RenderParams, DicomWebError> {
    let Some(query) = query else {
        return Ok(RenderParams::default());
    };
    let mut params = RenderParams::default();
    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        match name.as_ref() {
            "viewport" => {
                validate_viewport(&value)?;
                assign_once(&mut params.viewport, value.into_owned(), "viewport")?;
            }
            "window" => {
                validate_window(&value)?;
                assign_once(&mut params.window, value.into_owned(), "window")?;
            }
            "quality" => {
                let quality = value
                    .parse::<u8>()
                    .map_err(|_| DicomWebError::bad_request("quality must be an integer"))?;
                if !(1..=100).contains(&quality) {
                    return Err(DicomWebError::bad_request("quality must be 1..100"));
                }
                if params.quality.replace(quality).is_some() {
                    return Err(DicomWebError::bad_request(
                        "duplicate rendered parameter quality",
                    ));
                }
            }
            "annotation" => assign_once(&mut params.annotation, value.into_owned(), "annotation")?,
            "iccprofile" => assign_once(&mut params.iccprofile, value.into_owned(), "iccprofile")?,
            "presentationUID" | "presentationSeriesUID" | "presentationInstanceUID" => assign_once(
                &mut params.presentation_state,
                value.into_owned(),
                "presentation state",
            )?,
            "region" => {
                return Err(DicomWebError::not_acceptable(
                    "region rendering is not supported",
                ));
            }
            _ => {}
        }
    }
    Ok(params)
}

fn assign_once(
    target: &mut Option<String>,
    value: String,
    name: &'static str,
) -> Result<(), DicomWebError> {
    if target.replace(value).is_some() {
        return Err(DicomWebError::bad_request(format!(
            "duplicate rendered parameter {name}"
        )));
    }
    Ok(())
}

fn validate_viewport(value: &str) -> Result<(), DicomWebError> {
    let parts = value.split(',').collect::<Vec<_>>();
    if !matches!(parts.len(), 2 | 6) || parts.iter().any(|part| part.is_empty()) {
        return Err(DicomWebError::bad_request("invalid viewport parameter"));
    }
    let values = parts
        .iter()
        .map(|part| {
            part.parse::<f64>()
                .map_err(|_| DicomWebError::bad_request("invalid viewport parameter"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values[0] <= 0.0 || values[1] <= 0.0 {
        return Err(DicomWebError::bad_request(
            "viewport output dimensions must be positive",
        ));
    }
    if values.len() == 6 && (values[2] < 0.0 || values[3] < 0.0) {
        return Err(DicomWebError::bad_request(
            "viewport source origin must be non-negative",
        ));
    }
    if values.len() == 6 && (values[4] <= 0.0 || values[5] <= 0.0) {
        return Err(DicomWebError::bad_request(
            "viewport source dimensions must be positive",
        ));
    }
    Ok(())
}

fn validate_window(value: &str) -> Result<(), DicomWebError> {
    let parts = value.split(',').collect::<Vec<_>>();
    if !matches!(parts.len(), 2 | 3) {
        return Err(DicomWebError::bad_request("window requires center,width"));
    }
    let _center = parts[0]
        .parse::<f64>()
        .map_err(|_| DicomWebError::bad_request("invalid window center"))?;
    let width = parts[1]
        .parse::<f64>()
        .map_err(|_| DicomWebError::bad_request("invalid window width"))?;
    if width <= 0.0 {
        return Err(DicomWebError::bad_request("window width must be positive"));
    }
    match parts.get(2).copied().unwrap_or("linear") {
        "linear" | "linear-exact" | "sigmoid" | "LINEAR" | "LINEAR_EXACT" | "SIGMOID" => Ok(()),
        _ => Err(DicomWebError::bad_request(
            "window function must be linear, linear-exact, or sigmoid",
        )),
    }
}

fn frame_scope(path: FramesPath) -> Result<(RetrieveScope, Vec<u32>), DicomWebError> {
    let frames = FrameList::parse(&path.frames).map_err(record_error)?;
    let scope =
        scope::instance_scope(path.study, path.series, path.instance).map_err(record_error)?;
    Ok((scope, frames.frames().to_vec()))
}

fn render_span(resource: &'static str, route: &'static str, uri: &Uri) -> tracing::Span {
    tracing::info_span!(
        "wado-rs render",
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
        dicomweb.requested_frame_count = tracing::field::Empty,
        dicomweb.returned_part_count = tracing::field::Empty,
        dicomweb.renderer_backend = tracing::field::Empty,
        dicomweb.render_cache_result = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}

fn record_error(error: DicomWebError) -> DicomWebError {
    tracing::Span::current().record("error.type", error.http_error_class());
    error
}
