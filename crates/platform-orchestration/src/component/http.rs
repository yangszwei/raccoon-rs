use std::future::IntoFuture;
use std::net::SocketAddr;

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
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

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
}
