use std::sync::Arc;

use axum::extract::{OriginalUri, RawQuery, State};
use axum::http::Uri;
use axum::response::Response;
use axum::routing::get;
use raccoon_contract_dicom::TransferSyntaxUid;
use raccoon_service_retrieve::{RetrieveScope, RetrieveService};
use tracing::Instrument;

use crate::instrumentation::record_error;
use crate::media::{self, MediaType, MediaTypeParams};
use crate::wado::{TransferSyntaxPolicy, record_scope, retrieve_result, single_instance_response};
use crate::{
    DicomWebError, DicomWebProvider, DicomWebRouteRegistry, DicomWebState, RouteTelemetry,
    series_instance_uid, sop_instance_uid, study_instance_uid, transfer_syntax_uid,
};

/// WADO-URI provider for query-parameter based DICOM object retrieval.
pub struct WadoUriProvider {
    retrieve: Arc<dyn RetrieveService>,
    transfer_syntax_policy: TransferSyntaxPolicy,
}

impl WadoUriProvider {
    pub fn new(retrieve: Arc<dyn RetrieveService>) -> Self {
        Self {
            retrieve,
            transfer_syntax_policy: TransferSyntaxPolicy::native_little_endian(),
        }
    }

    pub fn with_transfer_syntax_policy(mut self, policy: TransferSyntaxPolicy) -> Self {
        self.transfer_syntax_policy = policy;
        self
    }
}

impl DicomWebProvider for WadoUriProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_wado_uri();
        registry.feature_set_mut().set_wado_uri_transfer_syntaxes(
            self.transfer_syntax_policy.advertised_transfer_syntaxes(),
        );
        registry.state_mut().retrieve = Some(self.retrieve.clone());
        registry.state_mut().wado_uri_transfer_syntax_policy =
            Some(self.transfer_syntax_policy.clone());
        registry.route(
            "/wado",
            get(wado_uri),
            RouteTelemetry::new("WADO-URI", "object", "/wado"),
        );
    }
}

#[derive(Debug)]
struct WadoUriQuery {
    study_uid: String,
    series_uid: String,
    object_uid: String,
    content_type: Option<String>,
    transfer_syntax: Option<String>,
    charset: Option<String>,
}

async fn wado_uri(
    State(state): State<DicomWebState>,
    OriginalUri(uri): OriginalUri,
    RawQuery(query): RawQuery,
) -> Result<Response, DicomWebError> {
    let span = wado_uri_span(&uri);
    async move {
        let query = parse_query(query.as_deref()).map_err(record_error)?;
        let content_type = query
            .content_type
            .as_deref()
            .unwrap_or(media::APPLICATION_DICOM);
        tracing::Span::current().record("dicomweb.requested_content_type", content_type);

        match content_type {
            media::APPLICATION_DICOM => object_response(state, query).await,
            media::IMAGE_JPEG | media::IMAGE_PNG => Err(record_error(
                DicomWebError::not_acceptable("WADO-URI rendered responses are not supported"),
            )),
            _ => Err(record_error(DicomWebError::not_acceptable(format!(
                "unsupported WADO-URI contentType {content_type}"
            )))),
        }
    }
    .instrument(span)
    .await
}

async fn object_response(
    state: DicomWebState,
    query: WadoUriQuery,
) -> Result<Response, DicomWebError> {
    let study_instance_uid =
        study_instance_uid(query.study_uid, "query studyUID").map_err(record_error)?;
    let series_instance_uid =
        series_instance_uid(query.series_uid, "query seriesUID").map_err(record_error)?;
    let sop_instance_uid =
        sop_instance_uid(query.object_uid, "query objectUID").map_err(record_error)?;
    let requested_transfer_syntax = query
        .transfer_syntax
        .map(|uid| transfer_syntax_uid(uid, "query transferSyntax"))
        .transpose()
        .map_err(record_error)?;
    if let Some(charset) = &query.charset {
        tracing::Span::current().record("dicomweb.requested_charset", charset.as_str());
        tracing::Span::current().record("dicomweb.charset.source", "query");
        tracing::Span::current().record("dicomweb.charset.supported", false);
        tracing::Span::current().record("dicomweb.charset.result", "unsupported");
        return Err(record_error(DicomWebError::not_acceptable(
            "WADO-URI charset recoding is not supported for native DICOM object retrieval",
        )));
    } else {
        tracing::Span::current().record("dicomweb.charset.result", "pass_through");
    }

    let scope = RetrieveScope::Instance {
        study_instance_uid: Some(study_instance_uid),
        series_instance_uid: Some(series_instance_uid),
        sop_instance_uid,
    };
    record_scope(&scope);
    if let Some(transfer_syntax) = requested_transfer_syntax.as_ref() {
        tracing::Span::current().record(
            "dicomweb.requested_transfer_syntax_uid",
            transfer_syntax.as_str(),
        );
    }

    let service = state.retrieve.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "WADO-URI retrieve service is not registered".to_string(),
        ))
    })?;
    let result = retrieve_result(service.as_ref(), scope)
        .await
        .map_err(record_error)?;
    tracing::Span::current().record("dicomweb.retrieve.instance_count", result.instance_count);
    record_selected_media_type(
        requested_transfer_syntax
            .as_ref()
            .map(TransferSyntaxUid::as_str),
    );
    single_instance_response(
        result,
        requested_transfer_syntax,
        state.wado_uri_transfer_syntax_policy.as_ref(),
    )
    .await
    .map_err(record_error)
}

fn parse_query(query: Option<&str>) -> Result<WadoUriQuery, DicomWebError> {
    let Some(query) = query else {
        return Err(DicomWebError::bad_request(
            "missing WADO-URI query parameters",
        ));
    };
    let mut request_type = None;
    let mut study_uid = None;
    let mut series_uid = None;
    let mut object_uid = None;
    let mut content_type = None;
    let mut transfer_syntax = None;
    let mut charset = None;

    for (name, value) in form_urlencoded::parse(query.as_bytes()) {
        match name.as_ref() {
            "requestType" => assign_once(&mut request_type, value.into_owned(), "requestType")?,
            "studyUID" => assign_once(&mut study_uid, value.into_owned(), "studyUID")?,
            "seriesUID" => assign_once(&mut series_uid, value.into_owned(), "seriesUID")?,
            "objectUID" => assign_once(&mut object_uid, value.into_owned(), "objectUID")?,
            "contentType" => assign_once(&mut content_type, value.into_owned(), "contentType")?,
            "transferSyntax" => {
                assign_once(&mut transfer_syntax, value.into_owned(), "transferSyntax")?
            }
            "charset" => assign_once(&mut charset, value.into_owned(), "charset")?,
            _ => {}
        }
    }

    match request_type.as_deref() {
        Some("WADO") => {}
        Some(_) => return Err(DicomWebError::bad_request("requestType must be WADO")),
        None => return Err(DicomWebError::bad_request("missing requestType")),
    }

    Ok(WadoUriQuery {
        study_uid: required(study_uid, "studyUID")?,
        series_uid: required(series_uid, "seriesUID")?,
        object_uid: required(object_uid, "objectUID")?,
        content_type,
        transfer_syntax,
        charset,
    })
}

fn assign_once(
    target: &mut Option<String>,
    value: String,
    name: &'static str,
) -> Result<(), DicomWebError> {
    if target.is_some() {
        return Err(DicomWebError::bad_request(format!(
            "duplicate WADO-URI parameter {name}"
        )));
    }
    *target = Some(value);
    Ok(())
}

fn required(value: Option<String>, name: &'static str) -> Result<String, DicomWebError> {
    value.filter(|value| !value.is_empty()).ok_or_else(|| {
        DicomWebError::bad_request(format!("missing required WADO-URI parameter {name}"))
    })
}

fn record_selected_media_type(transfer_syntax: Option<&str>) {
    let content_type = media::content_type(
        MediaType::ApplicationDicom,
        &MediaTypeParams {
            type_: None,
            transfer_syntax: transfer_syntax.map(str::to_string),
            charset: None,
        },
    );
    tracing::Span::current().record("dicomweb.selected_media_type", content_type.as_str());
}

fn wado_uri_span(uri: &Uri) -> tracing::Span {
    tracing::info_span!(
        "wado-uri retrieve",
        http.request.method = "GET",
        http.route = "/wado",
        url.path = uri.path(),
        dicomweb.service = "WADO-URI",
        dicomweb.resource = "object",
        dicomweb.retrieve.scope = tracing::field::Empty,
        dicom.study_instance_uid = tracing::field::Empty,
        dicom.series_instance_uid = tracing::field::Empty,
        dicom.sop_instance_uid = tracing::field::Empty,
        dicomweb.requested_content_type = tracing::field::Empty,
        dicomweb.requested_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.stored_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.requested_charset = tracing::field::Empty,
        dicomweb.charset.source = tracing::field::Empty,
        dicomweb.charset.supported = tracing::field::Empty,
        dicomweb.charset.result = tracing::field::Empty,
        dicomweb.selected_media_type = tracing::field::Empty,
        dicomweb.returned_transfer_syntax_uid = tracing::field::Empty,
        dicomweb.transcode.required = tracing::field::Empty,
        dicomweb.transcode.backend = tracing::field::Empty,
        dicomweb.transcode.result = tracing::field::Empty,
        dicomweb.retrieve.instance_count = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
        dicomweb.error_type = tracing::field::Empty,
        error.message = tracing::field::Empty,
    )
}
