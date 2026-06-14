use axum::extract::{OriginalUri, Path, RawQuery, State};
use axum::http::{HeaderMap, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use raccoon_contract_dicom::{SeriesInstanceUid, StudyInstanceUid, StudyRootQueryRetrieveLevel};
use raccoon_service_query::{DicomQuery, Predicate, QueryError, QueryScope};
use serde::Deserialize;
use tracing::Instrument;

use crate::instrumentation::record_error;
use crate::media::{self, DicomJsonOrXmlMultipart};
use crate::qido::params::{QidoQueryParams, uid_predicate};
use crate::qido::response::{RetrieveUrlLevel, query_page_json};
use crate::{DicomWebError, DicomWebRouteRegistry, DicomWebState, DicomWebUrlBase, RouteTelemetry};

pub(crate) fn register(registry: &mut DicomWebRouteRegistry) {
    registry.route(
        "/studies",
        get(search_studies),
        RouteTelemetry::new("QIDO-RS", "studies", "/studies"),
    );
    registry.route(
        "/studies/{study}/series",
        get(search_study_series),
        RouteTelemetry::new("QIDO-RS", "study_series", "/studies/{study}/series"),
    );
    registry.route(
        "/studies/{study}/instances",
        get(search_study_instances),
        RouteTelemetry::new("QIDO-RS", "study_instances", "/studies/{study}/instances"),
    );
    registry.route(
        "/studies/{study}/series/{series}/instances",
        get(search_series_instances),
        RouteTelemetry::new(
            "QIDO-RS",
            "series_instances",
            "/studies/{study}/series/{series}/instances",
        ),
    );
    registry.route(
        "/series",
        get(search_series),
        RouteTelemetry::new("QIDO-RS", "series", "/series"),
    );
    registry.route(
        "/instances",
        get(search_instances),
        RouteTelemetry::new("QIDO-RS", "instances", "/instances"),
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

async fn search_studies(
    State(state): State<DicomWebState>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    query_route(
        state,
        headers,
        uri,
        raw,
        RouteMeta {
            resource: "studies",
            route: "/studies",
            scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
            retrieve_url_level: RetrieveUrlLevel::Study,
        },
        Vec::new(),
    )
    .await
}

async fn search_study_series(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    let span = qido_span("study_series", "/studies/{study}/series", uri.path());
    async move {
        let study = parse_study_uid(path.study)?;
        tracing::Span::current().record("dicom.study_instance_uid", study.as_str());
        query_route_inner(
            state,
            headers,
            uri,
            raw,
            RouteMeta {
                resource: "study_series",
                route: "/studies/{study}/series",
                scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
                retrieve_url_level: RetrieveUrlLevel::Series,
            },
            vec![study_predicate(&study)],
        )
        .await
    }
    .instrument(span)
    .await
}

async fn search_study_instances(
    State(state): State<DicomWebState>,
    Path(path): Path<StudyPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    let span = qido_span("study_instances", "/studies/{study}/instances", uri.path());
    async move {
        let study = parse_study_uid(path.study)?;
        tracing::Span::current().record("dicom.study_instance_uid", study.as_str());
        query_route_inner(
            state,
            headers,
            uri,
            raw,
            RouteMeta {
                resource: "study_instances",
                route: "/studies/{study}/instances",
                scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
                retrieve_url_level: RetrieveUrlLevel::Instance,
            },
            vec![study_predicate(&study)],
        )
        .await
    }
    .instrument(span)
    .await
}

async fn search_series_instances(
    State(state): State<DicomWebState>,
    Path(path): Path<SeriesPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    let span = qido_span(
        "series_instances",
        "/studies/{study}/series/{series}/instances",
        uri.path(),
    );
    async move {
        let study = parse_study_uid(path.study)?;
        let series = parse_series_uid(path.series)?;
        let span = tracing::Span::current();
        span.record("dicom.study_instance_uid", study.as_str());
        span.record("dicom.series_instance_uid", series.as_str());
        query_route_inner(
            state,
            headers,
            uri,
            raw,
            RouteMeta {
                resource: "series_instances",
                route: "/studies/{study}/series/{series}/instances",
                scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
                retrieve_url_level: RetrieveUrlLevel::Instance,
            },
            vec![study_predicate(&study), series_predicate(&series)],
        )
        .await
    }
    .instrument(span)
    .await
}

async fn search_series(
    State(state): State<DicomWebState>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    query_route(
        state,
        headers,
        uri,
        raw,
        RouteMeta {
            resource: "series",
            route: "/series",
            scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
            retrieve_url_level: RetrieveUrlLevel::Series,
        },
        Vec::new(),
    )
    .await
}

async fn search_instances(
    State(state): State<DicomWebState>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    RawQuery(raw): RawQuery,
) -> Result<Response, DicomWebError> {
    query_route(
        state,
        headers,
        uri,
        raw,
        RouteMeta {
            resource: "instances",
            route: "/instances",
            scope: QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            retrieve_url_level: RetrieveUrlLevel::Instance,
        },
        Vec::new(),
    )
    .await
}

#[derive(Clone, Copy)]
struct RouteMeta {
    resource: &'static str,
    route: &'static str,
    scope: QueryScope,
    retrieve_url_level: RetrieveUrlLevel,
}

async fn query_route(
    state: DicomWebState,
    headers: HeaderMap,
    uri: Uri,
    raw: Option<String>,
    meta: RouteMeta,
    path_predicates: Vec<Predicate>,
) -> Result<Response, DicomWebError> {
    let span = qido_span(meta.resource, meta.route, uri.path());
    query_route_inner(state, headers, uri, raw, meta, path_predicates)
        .instrument(span)
        .await
}

async fn query_route_inner(
    state: DicomWebState,
    headers: HeaderMap,
    uri: Uri,
    raw: Option<String>,
    meta: RouteMeta,
    path_predicates: Vec<Predicate>,
) -> Result<Response, DicomWebError> {
    let selected = media::negotiate_dicom_json_or_xml_multipart(&headers).map_err(record_error)?;
    tracing::Span::current().record(
        "dicomweb.selected_media_type",
        selected.selected_media_type(),
    );

    let params = QidoQueryParams::parse(raw.as_deref()).map_err(record_error)?;
    record_query_controls(&params, path_predicates.len());

    let service = state.query.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "QIDO-RS query service is not registered".to_string(),
        ))
    })?;

    let mut query = DicomQuery::new(meta.scope, params.projection)
        .map_err(|error| record_error(DicomWebError::bad_request(error.to_string())))?;
    let mut predicates = path_predicates;
    predicates.extend(params.predicates);
    if !predicates.is_empty() {
        query = query
            .with_predicate(Predicate::All(predicates))
            .map_err(|error| record_error(DicomWebError::bad_request(error.to_string())))?;
    }
    if let Some(paging) = params.paging {
        query = query.with_paging(paging);
    }
    if params.fuzzy_matching {
        query = query.with_fuzzy_matching();
    }
    if let Some(timezone_offset) = params.timezone_offset {
        query = query
            .with_timezone_offset(timezone_offset)
            .map_err(|error| record_error(DicomWebError::bad_request(error.to_string())))?;
    }
    if let Some(specific_character_set) = params.specific_character_set {
        query = query
            .with_specific_character_set(specific_character_set)
            .map_err(|error| record_error(DicomWebError::bad_request(error.to_string())))?;
    }

    let page = service.query(query).await.map_err(query_error)?;
    let url_base = DicomWebUrlBase::from_request(&headers, &uri);
    let payload = query_page_json(page.items, url_base.as_ref(), meta.retrieve_url_level);

    match selected {
        DicomJsonOrXmlMultipart::Json => {
            let body = axum::Json(payload);
            let mut response = body.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                media::APPLICATION_DICOM_JSON
                    .parse()
                    .expect("valid DICOM JSON content type"),
            );
            Ok(response)
        }
        DicomJsonOrXmlMultipart::XmlMultipart => {
            let boundary = media::multipart_boundary();
            let body = dicom_xml_multipart(&payload, &boundary).map_err(record_error)?;
            Ok(media::multipart_related_response(
                body,
                &boundary,
                media::MediaType::ApplicationDicomXml,
                None,
            ))
        }
    }
}

fn dicom_xml_multipart(
    payload: &serde_json::Value,
    boundary: &str,
) -> Result<String, DicomWebError> {
    let datasets = payload
        .as_array()
        .ok_or_else(|| DicomWebError::Internal("QIDO XML payload is not an array".to_string()))?;
    let mut body = String::new();
    for dataset in datasets {
        body.push_str("--");
        body.push_str(boundary);
        body.push_str("\r\nContent-Type: application/dicom+xml\r\n\r\n");
        body.push_str(&crate::xml::native_dicom_model_xml(dataset)?);
        body.push_str("\r\n");
    }
    body.push_str("--");
    body.push_str(boundary);
    body.push_str("--\r\n");
    Ok(body)
}

fn qido_span(resource: &'static str, route: &'static str, path: &str) -> tracing::Span {
    tracing::info_span!(
        "qido-rs search",
        http.request.method = "GET",
        http.route = route,
        url.path = path,
        dicomweb.service = "QIDO-RS",
        dicomweb.resource = resource,
        dicom.study_instance_uid = tracing::field::Empty,
        dicom.series_instance_uid = tracing::field::Empty,
        dicomweb.query.limit = tracing::field::Empty,
        dicomweb.query.offset = tracing::field::Empty,
        dicomweb.query.predicate_count = tracing::field::Empty,
        dicomweb.query.fuzzy_matching = tracing::field::Empty,
        dicom.timezone_offset = tracing::field::Empty,
        dicom.specific_character_set = tracing::field::Empty,
        dicomweb.charset.source = tracing::field::Empty,
        dicomweb.charset.supported = tracing::field::Empty,
        dicomweb.charset.result = tracing::field::Empty,
        dicomweb.selected_media_type = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        dicomweb.error_type = tracing::field::Empty,
        error.message = tracing::field::Empty,
    )
}

fn record_query_controls(params: &QidoQueryParams, path_predicate_count: usize) {
    let span = tracing::Span::current();
    if let Some(limit) = params.limit_for_span {
        span.record("dicomweb.query.limit", limit);
    }
    span.record("dicomweb.query.offset", params.offset_for_span);
    span.record(
        "dicomweb.query.predicate_count",
        path_predicate_count + params.predicates.len(),
    );
    span.record("dicomweb.query.fuzzy_matching", params.fuzzy_matching);
    if let Some(offset) = params.timezone_offset.as_deref() {
        span.record("dicom.timezone_offset", offset);
    }
    if let Some(charsets) = params.specific_character_set.as_ref() {
        span.record("dicom.specific_character_set", charsets.join("\\"));
        span.record("dicomweb.charset.source", "query");
        span.record("dicomweb.charset.supported", true);
        span.record("dicomweb.charset.result", "decoded");
    } else {
        span.record("dicomweb.charset.source", "absent");
        span.record("dicomweb.charset.supported", true);
    }
}

fn parse_study_uid(value: String) -> Result<StudyInstanceUid, DicomWebError> {
    StudyInstanceUid::new(value)
        .map_err(|error| record_error(DicomWebError::invalid_uid("path StudyInstanceUID", error)))
}

fn parse_series_uid(value: String) -> Result<SeriesInstanceUid, DicomWebError> {
    SeriesInstanceUid::new(value)
        .map_err(|error| record_error(DicomWebError::invalid_uid("path SeriesInstanceUID", error)))
}

fn study_predicate(uid: &StudyInstanceUid) -> Predicate {
    uid_predicate(dicom_dictionary_std::tags::STUDY_INSTANCE_UID, uid.as_str())
}

fn series_predicate(uid: &SeriesInstanceUid) -> Predicate {
    uid_predicate(
        dicom_dictionary_std::tags::SERIES_INSTANCE_UID,
        uid.as_str(),
    )
}

fn query_error(error: QueryError) -> DicomWebError {
    match error {
        QueryError::InvalidQuery(message) => record_error(DicomWebError::bad_request(message)),
        QueryError::Repository(error) => record_error(DicomWebError::Internal(error.to_string())),
    }
}
