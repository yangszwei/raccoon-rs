use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, options};
use axum::{Json, Router};
use tracing::Instrument;

use crate::{DicomWebFeatureSet, DicomWebState};

/// Registers one DICOMweb transaction family into a route registry.
pub trait DicomWebProvider: Send + Sync + 'static {
    fn register(&self, registry: &mut DicomWebRouteRegistry);
}

/// Builder for composing DICOMweb providers into one Axum router.
#[derive(Debug, Default)]
pub struct DicomWebRouter {
    registry: DicomWebRouteRegistry,
}

impl DicomWebRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<P>(mut self, provider: P) -> Self
    where
        P: DicomWebProvider,
    {
        provider.register(&mut self.registry);
        self
    }

    pub fn into_router(self) -> Router {
        self.registry.into_router()
    }

    pub fn feature_set(&self) -> &DicomWebFeatureSet {
        self.registry.feature_set()
    }
}

/// Mutable route registry passed to DICOMweb providers.
#[derive(Debug, Clone)]
pub struct DicomWebRouteRegistry {
    routes: Router<DicomWebState>,
    state: DicomWebState,
}

/// DICOMweb route metadata used for protocol-oriented completion events.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RouteTelemetry {
    pub service: &'static str,
    pub resource: &'static str,
    pub route: &'static str,
}

impl RouteTelemetry {
    pub const fn new(service: &'static str, resource: &'static str, route: &'static str) -> Self {
        Self {
            service,
            resource,
            route,
        }
    }
}

impl Default for DicomWebRouteRegistry {
    fn default() -> Self {
        Self {
            routes: Router::new(),
            state: DicomWebState::default(),
        }
    }
}

impl DicomWebRouteRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feature_set(&self) -> &DicomWebFeatureSet {
        &self.state.features
    }

    pub fn feature_set_mut(&mut self) -> &mut DicomWebFeatureSet {
        &mut self.state.features
    }

    pub fn state(&self) -> &DicomWebState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut DicomWebState {
        &mut self.state
    }

    pub fn route(
        &mut self,
        path: &'static str,
        method_router: MethodRouter<DicomWebState>,
        _telemetry: RouteTelemetry,
    ) {
        self.routes = std::mem::take(&mut self.routes).route(path, method_router);
    }

    pub fn merge(&mut self, router: Router<DicomWebState>) {
        self.routes = std::mem::take(&mut self.routes).merge(router);
    }

    pub fn into_router(self) -> Router {
        Router::new()
            .route("/", options(options_root))
            .merge(self.routes)
            .with_state(self.state)
    }
}

/// Retained for gateway builders; request completion logging is owned by orchestration HTTP middleware.
pub fn log_dicomweb_requests(_service_name: &'static str, router: Router) -> Router {
    router
}

async fn options_root(
    State(state): State<DicomWebState>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    let base = capabilities_base_url(&headers, &uri);
    let span = tracing::info_span!(
        "dicomweb capabilities",
        dicomweb.service = "capabilities",
        http.route = "OPTIONS /",
        dicomweb.transaction_count = state.features.transaction_count(),
        dicomweb.resource_count = state.features.resource_count(),
        dicomweb.media_type_count = state.features.media_type_count(),
    );

    async move {
        let mut response = (
            StatusCode::OK,
            Json(state.features.capabilities_description(base)),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("OPTIONS"));
        response
    }
    .instrument(span)
    .await
}

fn capabilities_base_url(headers: &HeaderMap, uri: &Uri) -> String {
    let Some(host) = forwarded_or_host(headers) else {
        return "/".to_string();
    };
    let scheme = header_str(headers, "x-forwarded-proto").unwrap_or("http");
    let path = uri.path().trim_end_matches('/');
    if path.is_empty() {
        format!("{scheme}://{host}")
    } else {
        format!("{scheme}://{host}{path}")
    }
}

fn forwarded_or_host(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "x-forwarded-host").or_else(|| header_str(headers, header::HOST.as_str()))
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::to_bytes;
    use axum::http::{Method, Request, StatusCode};
    use axum::routing::get;
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use tracing::Metadata;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::Interest;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    use super::{
        DicomWebProvider, DicomWebRouteRegistry, DicomWebRouter, RouteTelemetry,
        log_dicomweb_requests,
    };

    struct QidoTestProvider;

    impl DicomWebProvider for QidoTestProvider {
        fn register(&self, registry: &mut DicomWebRouteRegistry) {
            registry.feature_set_mut().enable_qido_rs();
        }
    }

    struct StowTestProvider;

    impl DicomWebProvider for StowTestProvider {
        fn register(&self, registry: &mut DicomWebRouteRegistry) {
            registry.feature_set_mut().enable_stow_rs();
        }
    }

    struct WadoTestProvider;

    impl DicomWebProvider for WadoTestProvider {
        fn register(&self, registry: &mut DicomWebRouteRegistry) {
            registry.feature_set_mut().enable_wado_rs();
        }
    }

    struct AllCapabilitiesTestProvider;

    impl DicomWebProvider for AllCapabilitiesTestProvider {
        fn register(&self, registry: &mut DicomWebRouteRegistry) {
            registry.feature_set_mut().enable_qido_rs();
            registry.feature_set_mut().enable_stow_rs();
            registry.feature_set_mut().enable_wado_rs();
            registry.feature_set_mut().enable_wado_rs_metadata();
            registry.feature_set_mut().enable_rendered();
            registry.feature_set_mut().enable_thumbnail();
            registry.feature_set_mut().enable_wado_uri();
        }
    }

    struct TelemetryTestProvider;

    impl DicomWebProvider for TelemetryTestProvider {
        fn register(&self, registry: &mut DicomWebRouteRegistry) {
            registry.route(
                "/studies",
                get(|| async { (StatusCode::ACCEPTED, "ok") }),
                RouteTelemetry::new("QIDO-RS", "studies", "/studies"),
            );
        }
    }

    #[test]
    fn empty_router_builds() {
        let _router = DicomWebRouter::new().into_router();
    }

    #[test]
    fn empty_capabilities_advertise_no_features() {
        let features = DicomWebRouteRegistry::new().feature_set().clone();

        assert!(features.qido_rs.is_none());
        assert!(features.stow_rs.is_none());
        assert!(features.wado_rs.is_none());
        assert!(features.wado_uri.is_none());
    }

    #[test]
    fn registering_provider_can_mutate_feature_set() {
        let router = DicomWebRouter::new().register(QidoTestProvider);

        assert!(router.feature_set().qido_rs.is_some());
        assert!(router.feature_set().stow_rs.is_none());
    }

    #[tokio::test]
    async fn options_root_returns_empty_wadl_description_for_empty_router() {
        let router = DicomWebRouter::new().into_router();
        let response = options_root_response(router).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("allow").unwrap(), "OPTIONS");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            payload,
            json!({
                "application": {
                    "resources": {
                        "@base": "http://dicom.example.test"
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn options_root_advertises_qido_only_capabilities() {
        let payload = options_root_payload(DicomWebRouter::new().register(QidoTestProvider)).await;

        assert_has_method(&payload, "SearchForStudies");
        assert_has_method(&payload, "SearchForSeries");
        assert_has_method(&payload, "SearchForInstances");
        assert_has_representation(&payload, "application/dicom+json");
        assert_has_query_param(&payload, "limit");
        assert_has_query_param(&payload, "offset");
        assert_absent(&payload, "StoreInstances");
        assert_absent(&payload, "RetrieveStudy");
        assert_has_representation(
            &payload,
            "multipart/related; type=\\\"application/dicom+xml\\\"",
        );
        assert_absent(&payload, "rendered");
        assert_absent(&payload, "thumbnail");
    }

    #[tokio::test]
    async fn options_root_advertises_stow_only_capabilities() {
        let payload = options_root_payload(DicomWebRouter::new().register(StowTestProvider)).await;

        assert_has_method(&payload, "StoreInstances");
        assert_has_method(&payload, "StoreStudyInstances");
        assert_has_representation(&payload, "application/dicom");
        assert_has_representation(&payload, "application/dicom+json");
        assert_absent(&payload, "SearchForStudies");
        assert_absent(&payload, "RetrieveStudy");
        assert_absent(&payload, "multipart/related");
    }

    #[tokio::test]
    async fn options_root_advertises_wado_only_capabilities() {
        let payload = options_root_payload(DicomWebRouter::new().register(WadoTestProvider)).await;

        assert_has_method(&payload, "RetrieveStudy");
        assert_has_method(&payload, "RetrieveSeries");
        assert_has_method(&payload, "RetrieveBulkData");
        assert_has_method(&payload, "RetrieveStudyPixelData");
        assert_has_method(&payload, "RetrieveFrames");
        assert_has_representation(&payload, "application/dicom");
        assert_has_representation(&payload, "application/octet-stream");
        assert_has_plain_param_default(&payload, "transfer-syntax", "*");
        assert_absent(&payload, "SearchForStudies");
        assert_absent(&payload, "StoreInstances");
        assert_absent(&payload, "metadata");
        assert_absent(&payload, "rendered");
        assert_absent(&payload, "thumbnail");
    }

    #[tokio::test]
    async fn options_root_advertises_all_registered_capabilities() {
        let payload = options_root_payload(
            DicomWebRouter::new()
                .register(QidoTestProvider)
                .register(StowTestProvider)
                .register(WadoTestProvider),
        )
        .await;

        assert_has_method(&payload, "SearchForStudies");
        assert_has_method(&payload, "StoreInstances");
        assert_has_method(&payload, "RetrieveStudy");
        assert_has_method(&payload, "RetrieveSeries");
        assert_has_representation(&payload, "application/dicom");
        assert_has_representation(&payload, "application/dicom+json");
        assert_has_representation(
            &payload,
            "multipart/related; type=\\\"application/dicom+xml\\\"",
        );
        assert_absent(&payload, "wado");
        assert_absent(&payload, "rendered");
        assert_absent(&payload, "thumbnail");
    }

    #[tokio::test]
    async fn wado_options_claims_match_conformance_document() {
        let payload =
            options_root_payload(DicomWebRouter::new().register(AllCapabilitiesTestProvider)).await;
        let text = payload.to_string();
        let expected_method_ids = [
            "SearchForStudies",
            "SearchForSeries",
            "SearchForInstances",
            "SearchForStudySeries",
            "SearchForStudyInstances",
            "SearchForStudySeriesInstances",
            "StoreInstances",
            "StoreStudyInstances",
            "RetrieveStudy",
            "RetrieveSeries",
            "RetrieveInstance",
            "RetrieveStudyMetadata",
            "RetrieveSeriesMetadata",
            "RetrieveInstanceMetadata",
            "RetrieveBulkData",
            "RetrieveStudyPixelData",
            "RetrieveSeriesPixelData",
            "RetrieveInstancePixelData",
            "RetrieveFrames",
            "RetrieveStudyRendered",
            "RetrieveSeriesRendered",
            "RetrieveInstanceRendered",
            "RetrieveRenderedFrames",
            "RetrieveStudyThumbnail",
            "RetrieveSeriesThumbnail",
            "RetrieveInstanceThumbnail",
            "RetrieveFrameThumbnail",
            "RetrieveDicomInstance",
        ];

        for method_id in expected_method_ids {
            assert_has_method(&payload, method_id);
        }

        for supported_media_type in [
            "application/dicom",
            "application/dicom+json",
            "application/dicom+xml",
            "application/octet-stream",
            "image/jpeg",
            "image/png",
        ] {
            assert!(
                text.contains(supported_media_type),
                "OPTIONS / missing {supported_media_type}: {text}"
            );
        }

        for rendered_param in ["viewport", "window", "quality"] {
            assert_has_query_param(&payload, rendered_param);
        }

        for wado_uri_param in [
            "requestType",
            "studyUID",
            "seriesUID",
            "objectUID",
            "contentType",
            "transferSyntax",
        ] {
            assert_has_query_param(&payload, wado_uri_param);
        }

        assert_has_plain_param_default(&payload, "transfer-syntax", "*");
        assert!(text.contains("\"@path\":\"wado\""), "{text}");
        assert!(!text.contains("application/vnd.sun.wadl+xml"), "{text}");
        assert!(!text.contains("1.2.840.10008.1.2.4.50"), "{text}");
        assert!(!text.contains("application/pdf"), "{text}");
        assert!(!text.contains("video/"), "{text}");
    }

    #[tokio::test]
    async fn capabilities_route_is_not_registered() {
        let router = DicomWebRouter::new()
            .register(QidoTestProvider)
            .into_router();
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/capabilities")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_completion_is_owned_by_outer_http_middleware() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            records: records.clone(),
        });
        let _guard = tracing::subscriber::set_default(subscriber);
        let router = log_dicomweb_requests(
            "dicomweb-gateway",
            DicomWebRouter::new()
                .register(TelemetryTestProvider)
                .into_router(),
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/studies?PatientName=SMITH")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let records = records.lock().expect("records lock").join("\n");
        assert!(
            !records.contains("message=DICOMweb response sent"),
            "{records}"
        );
    }

    async fn options_root_payload(builder: DicomWebRouter) -> Value {
        let response = options_root_response(builder.into_router()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn options_root_response(router: axum::Router) -> axum::response::Response {
        router
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/")
                    .header("accept", "application/json")
                    .header("host", "dicom.example.test")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    fn assert_has_method(payload: &Value, id: &str) {
        assert_contains(payload, "\"@id\":\"", id);
    }

    fn assert_has_representation(payload: &Value, media_type: &str) {
        assert_contains(payload, "\"@mediaType\":\"", media_type);
    }

    fn assert_has_query_param(payload: &Value, name: &str) {
        assert_contains(payload, "\"@style\":\"query\"", name);
    }

    fn assert_has_plain_param_default(payload: &Value, name: &str, default_value: &str) {
        let text = payload.to_string();
        assert!(
            text.contains(&format!("\"@name\":\"{name}\"")),
            "{text} missing param {name}"
        );
        assert!(
            text.contains(&format!("\"@default\":\"{default_value}\"")),
            "{text} missing default {default_value}"
        );
    }

    fn assert_absent(payload: &Value, needle: &str) {
        let text = payload.to_string();
        assert!(
            !text.contains(needle),
            "{text} unexpectedly contained {needle}"
        );
    }

    fn assert_contains(payload: &Value, context: &str, needle: &str) {
        let text = payload.to_string();
        assert!(
            text.contains(context) && text.contains(needle),
            "{text} missing {needle}"
        );
    }

    struct CaptureLayer {
        records: Arc<Mutex<Vec<String>>>,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber,
        S: for<'lookup> LookupSpan<'lookup>,
    {
        fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
            Interest::always()
        }

        fn enabled(&self, _metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
            true
        }

        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = CaptureVisitor::default();
            event.record(&mut visitor);
            self.records
                .lock()
                .expect("records lock")
                .extend(visitor.records);
        }
    }

    #[derive(Default)]
    struct CaptureVisitor {
        records: Vec<String>,
    }

    impl Visit for CaptureVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.records.push(format!("{}={value:?}", field.name()));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.records.push(format!("{}={value}", field.name()));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.records.push(format!("{}={value}", field.name()));
        }
    }
}
