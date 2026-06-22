use axum::body::Body;
use axum::extract::{OriginalUri, Path, RawQuery, State};
use axum::http::{HeaderMap, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use dicom_core::Tag;
use dicom_dictionary_std::tags;
use raccoon_contract_dicom::{
    SeriesInstanceUid, SopInstanceUid, StudyInstanceUid, StudyRootQueryRetrieveLevel,
};
use raccoon_service_query::{
    AttributePathSegment, DicomQuery, MatchingRule, Predicate, QueryError, QueryScope, QueryService,
};
use serde::Deserialize;
use tracing::Instrument;

use crate::instrumentation::record_error;
use crate::media::{self, DicomJsonOrXmlMultipart};
use crate::qido::cache::QidoJsonCacheKey;
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
    let service = state.query.clone().ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "QIDO-RS query service is not registered".to_string(),
        ))
    })?;

    let route_span = tracing::Span::current();
    let (selected, query, cache_key, url_base) = {
        let span = tracing::info_span!(
            "qido.route.parse",
            dicomweb.resource = meta.resource,
            http.route = meta.route,
            dicomweb.path_predicate_count = path_predicates.len(),
            dicomweb.selected_media_type = tracing::field::Empty,
        );
        let _guard = span.enter();

        let selected =
            media::negotiate_dicom_json_or_xml_multipart(&headers).map_err(record_error)?;
        tracing::Span::current().record(
            "dicomweb.selected_media_type",
            selected.selected_media_type(),
        );
        route_span.record(
            "dicomweb.selected_media_type",
            selected.selected_media_type(),
        );

        let params = QidoQueryParams::parse(raw.as_deref()).map_err(record_error)?;
        record_query_controls(&params, path_predicates.len());
        record_query_controls_on(&route_span, &params, path_predicates.len());
        let url_base = DicomWebUrlBase::from_request(&headers, &uri);
        let cache_key = exact_json_cache_key(
            &selected,
            meta,
            &path_predicates,
            &params,
            url_base.as_ref(),
        );

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

        (selected, query, cache_key, url_base)
    };
    let cache_eligible = cache_key.is_some();
    route_span.record("qido.cache_eligible", cache_eligible);

    if cache_key.is_some() {
        refresh_qido_cache_revision_if_due(&state, service.as_ref()).await?;
        if let Some(cache_key) = cache_key.as_ref()
            && let Some(bytes) = state.qido_json_cache.get(cache_key)
        {
            route_span.record("qido.cache_hit", true);
            return Ok(json_bytes_response(bytes));
        }
    }
    route_span.record("qido.cache_hit", false);

    let query_revision_before = if cache_key.is_some() {
        Some(read_and_record_qido_cache_revision(&state, service.as_ref()).await?)
    } else {
        None
    };

    let query_span = tracing::info_span!(
        "qido.gateway.query_service",
        dicomweb.resource = meta.resource,
        query.scope = ?query.scope(),
        qido.result_count = tracing::field::Empty,
    );
    let page = service
        .query(query)
        .instrument(query_span.clone())
        .await
        .map_err(query_error)?;
    let result_count = page.items.len();
    query_span.record("qido.result_count", page.items.len());
    route_span.record("qido.result_count", result_count);

    let payload = {
        let span = tracing::info_span!(
            "qido.response.build_payload",
            dicomweb.resource = meta.resource,
            qido.result_count = result_count,
        );
        let _guard = span.enter();
        query_page_json(page.items, url_base.as_ref(), meta.retrieve_url_level)
    };

    let response = {
        let span = tracing::info_span!(
            "qido.response.encode",
            dicomweb.resource = meta.resource,
            dicomweb.selected_media_type = selected.selected_media_type(),
            qido.result_count = result_count,
        );
        async move {
            match selected {
                DicomJsonOrXmlMultipart::Json => {
                    let bytes = serde_json::to_vec(&payload).map_err(|error| {
                        record_error(DicomWebError::Internal(format!(
                            "QIDO JSON serialization failed: {error}"
                        )))
                    })?;
                    if let Some(cache_key) = cache_key {
                        let revision_after =
                            read_and_record_qido_cache_revision(&state, service.as_ref()).await?;
                        if query_revision_before == Some(revision_after) {
                            state.qido_json_cache.insert(cache_key, bytes.clone());
                        }
                    }
                    Ok(json_bytes_response(bytes))
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
        .instrument(span)
        .await?
    };

    Ok(response)
}

async fn refresh_qido_cache_revision_if_due(
    state: &DicomWebState,
    service: &dyn QueryService,
) -> Result<(), DicomWebError> {
    if state.qido_json_cache.revision_check_due() {
        read_and_record_qido_cache_revision(state, service)
            .instrument(tracing::info_span!("qido.cache.refresh_revision"))
            .await?;
    }
    Ok(())
}

async fn read_and_record_qido_cache_revision(
    state: &DicomWebState,
    service: &dyn QueryService,
) -> Result<u64, DicomWebError> {
    let revision = service
        .read_model_revision()
        .instrument(tracing::info_span!("qido.cache.read_model_revision"))
        .await
        .map_err(query_error)?;
    state.qido_json_cache.record_read_model_revision(revision);
    Ok(revision)
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

fn json_bytes_response(bytes: Vec<u8>) -> Response {
    let mut response = Body::from(bytes).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        media::APPLICATION_DICOM_JSON
            .parse()
            .expect("valid DICOM JSON content type"),
    );
    response
}

fn exact_json_cache_key(
    selected: &DicomJsonOrXmlMultipart,
    meta: RouteMeta,
    path_predicates: &[Predicate],
    params: &QidoQueryParams,
    url_base: Option<&DicomWebUrlBase>,
) -> Option<QidoJsonCacheKey> {
    if !matches!(selected, DicomJsonOrXmlMultipart::Json)
        || !params.projection.is_default()
        || params.paging.is_some()
        || params.fuzzy_matching
        || params.timezone_offset.is_some()
        || params.specific_character_set.is_some()
    {
        return None;
    }

    let (origin, base_path) = url_base
        .map(|base| (base.origin().to_string(), base.base_path().to_string()))
        .unwrap_or_else(|| (String::new(), String::new()));

    match meta.resource {
        "studies" if path_predicates.is_empty() && params.predicates.len() == 1 => {
            let study = StudyInstanceUid::new(single_uid_for_tag(
                &params.predicates,
                tags::STUDY_INSTANCE_UID,
            )?)
            .ok()?;
            Some(QidoJsonCacheKey::study(
                meta.route, &study, origin, base_path,
            ))
        }
        "study_series" if path_predicates.len() == 1 && params.predicates.len() == 1 => {
            let study = StudyInstanceUid::new(single_uid_for_tag(
                path_predicates,
                tags::STUDY_INSTANCE_UID,
            )?)
            .ok()?;
            let series = SeriesInstanceUid::new(single_uid_for_tag(
                &params.predicates,
                tags::SERIES_INSTANCE_UID,
            )?)
            .ok()?;
            Some(QidoJsonCacheKey::series(
                meta.route, &study, &series, origin, base_path,
            ))
        }
        "series_instances" if path_predicates.len() == 2 && params.predicates.len() == 1 => {
            let study = StudyInstanceUid::new(single_uid_for_tag(
                path_predicates,
                tags::STUDY_INSTANCE_UID,
            )?)
            .ok()?;
            let series = SeriesInstanceUid::new(single_uid_for_tag(
                path_predicates,
                tags::SERIES_INSTANCE_UID,
            )?)
            .ok()?;
            let sop = SopInstanceUid::new(single_uid_for_tag(
                &params.predicates,
                tags::SOP_INSTANCE_UID,
            )?)
            .ok()?;
            Some(QidoJsonCacheKey::instance(
                meta.route, &study, &series, &sop, origin, base_path,
            ))
        }
        _ => None,
    }
}

fn single_uid_for_tag(predicates: &[Predicate], tag: Tag) -> Option<String> {
    predicates
        .iter()
        .find_map(|predicate| single_uid_for_tag_in_predicate(predicate, tag))
}

fn single_uid_for_tag_in_predicate(predicate: &Predicate, tag: Tag) -> Option<String> {
    match predicate {
        Predicate::All(items) => single_uid_for_tag(items, tag),
        Predicate::Attribute(path, MatchingRule::SingleValue(value))
            if path_has_tag(path.segments(), tag) =>
        {
            Some(value.clone())
        }
        Predicate::Attribute(path, MatchingRule::UidList(values))
            if path_has_tag(path.segments(), tag) && values.len() == 1 =>
        {
            values.first().cloned()
        }
        _ => None,
    }
}

fn path_has_tag(segments: &[AttributePathSegment], tag: Tag) -> bool {
    matches!(segments, [AttributePathSegment::Tag(path_tag)] if *path_tag == tag)
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
        qido.cache_hit = tracing::field::Empty,
        qido.cache_eligible = tracing::field::Empty,
        qido.result_count = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        dicomweb.error_type = tracing::field::Empty,
        error.message = tracing::field::Empty,
    )
}

fn record_query_controls(params: &QidoQueryParams, path_predicate_count: usize) {
    record_query_controls_on(&tracing::Span::current(), params, path_predicate_count);
}

fn record_query_controls_on(
    span: &tracing::Span,
    params: &QidoQueryParams,
    path_predicate_count: usize,
) {
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
