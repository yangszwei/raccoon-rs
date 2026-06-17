use std::future::IntoFuture;
use std::net::SocketAddr;
use std::time::Instant;

use axum::Router;
use axum::extract::Request;
use axum::http::uri::PathAndQuery;
use axum::middleware::{Next, from_fn};
use axum::response::Response;
use raccoon_platform_runtime::FatalError;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use tracing::{debug, info};

use crate::error::OrchestrationError;

/// Bind an HTTP listener from a configured socket address.
pub async fn bind_http_listener(
    service_name: &str,
    bind_address: &str,
) -> Result<(SocketAddr, TcpListener), OrchestrationError> {
    let bind_address: SocketAddr = bind_address.parse()?;
    let listener = TcpListener::bind(bind_address).await?;
    let local_addr = listener.local_addr()?;

    info!(
        service.name = service_name,
        server.address = %local_addr,
        "HTTP server listening"
    );

    Ok((local_addr, listener))
}

/// Mount routes below a configured base path.
pub fn mount_base_path(base_path: &str, router: Router) -> Router {
    let base_path = base_path.trim_end_matches('/');
    if base_path.is_empty() || base_path == "/" {
        info!(http.base_path = "/", "DICOMweb routes mounted");
        router
    } else {
        info!(http.base_path = base_path, "DICOMweb routes mounted");
        Router::new()
            .nest_service(base_path, router)
            .layer(from_fn(trim_trailing_slash))
    }
}

/// Log one completion event for each HTTP request served by the router.
pub fn log_http_requests(service_name: &'static str, router: Router) -> Router {
    router.layer(from_fn(move |request, next| {
        log_http_request(service_name, request, next)
    }))
}

async fn log_http_request(service_name: &'static str, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let started_at = Instant::now();

    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

    info!(
        service.name = service_name,
        http.request.method = %method,
        url.path = path_and_query,
        http.response.status_code = status.as_u16(),
        http.server.duration_ms = elapsed_ms,
        "HTTP request completed"
    );

    response
}

async fn trim_trailing_slash(mut request: Request, next: Next) -> Response {
    let uri = request.uri();
    let path = uri.path();
    if path != "/" && path.ends_with('/') {
        let mut parts = uri.clone().into_parts();
        let trimmed = path.trim_end_matches('/');
        let path_and_query = match uri.query() {
            Some(query) => format!("{trimmed}?{query}"),
            None => trimmed.to_string(),
        };
        if let Ok(path_and_query) = path_and_query.parse::<PathAndQuery>() {
            parts.path_and_query = Some(path_and_query);
            if let Ok(uri) = parts.try_into() {
                *request.uri_mut() = uri;
            }
        }
    }
    next.run(request).await
}

/// Register a long-running HTTP server future with the runtime task tracker.
pub fn start_http_server<F>(
    service_name: &'static str,
    server: F,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) where
    F: IntoFuture<Output = std::io::Result<()>> + Send + 'static,
    F::IntoFuture: Send,
{
    task_tracker.spawn(async move {
        if let Err(error) = server.into_future().await {
            let _ = fatal_tx.send(FatalError::new(service_name, "http-server", error));
        }
        debug!(service.name = service_name, "HTTP server task finished");
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;
    use tracing::Metadata;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::Interest;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    use super::*;

    #[tokio::test]
    async fn mount_base_path_nests_routes_under_configured_prefix() {
        let router = Router::new().route("/studies", get(|| async { "ok" }));
        let router = mount_base_path("/dicom-web", router);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/dicom-web/studies")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mount_base_path_matches_root_with_trailing_slash() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let router = mount_base_path("/dicom-web", router);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/dicom-web/")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mount_base_path_matches_nested_route_with_trailing_slash() {
        let router = Router::new().route("/studies", get(|| async { "ok" }));
        let router = mount_base_path("/dicom-web", router);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/dicom-web/studies/")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mount_base_path_keeps_root_routes_when_prefix_is_root() {
        let router = Router::new().route("/studies", get(|| async { "ok" }));
        let router = mount_base_path("/", router);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/studies")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn log_http_requests_records_completion_event() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            records: records.clone(),
        });
        let _guard = tracing::subscriber::set_default(subscriber);
        let router =
            Router::new().route("/studies", get(|| async { (StatusCode::ACCEPTED, "ok") }));
        let router = log_http_requests("dicomweb-gateway", router);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/studies?PatientName=SMITH")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let records = records.lock().expect("records lock").join("\n");
        assert!(
            records.contains("message=HTTP request completed"),
            "{records}"
        );
        assert!(
            records.contains("service.name=dicomweb-gateway"),
            "{records}"
        );
        assert!(records.contains("http.request.method=GET"), "{records}");
        assert!(
            records.contains("url.path=/studies?PatientName=SMITH"),
            "{records}"
        );
        assert!(
            records.contains("http.response.status_code=202"),
            "{records}"
        );
        assert!(records.contains("http.server.duration_ms="), "{records}");
    }

    struct CaptureLayer {
        records: Arc<Mutex<Vec<String>>>,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber,
        S: for<'a> LookupSpan<'a>,
    {
        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if metadata
                .target()
                .starts_with("raccoon_platform_orchestration")
            {
                Interest::always()
            } else {
                Interest::never()
            }
        }

        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = EventVisitor::default();
            event.record(&mut visitor);

            self.records
                .lock()
                .expect("records lock")
                .push(visitor.fields.join(" "));
        }
    }

    #[derive(Default)]
    struct EventVisitor {
        fields: Vec<String>,
    }

    impl Visit for EventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields.push(format!("{}={:?}", field.name(), value));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields.push(format!("{}={value}", field.name()));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields.push(format!("{}={value}", field.name()));
        }
    }
}
