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

    pub fn route(&mut self, path: &'static str, method_router: MethodRouter<DicomWebState>) {
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
    use axum::body::to_bytes;
    use axum::http::{Method, Request, StatusCode};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::{DicomWebProvider, DicomWebRouteRegistry, DicomWebRouter};

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
}
