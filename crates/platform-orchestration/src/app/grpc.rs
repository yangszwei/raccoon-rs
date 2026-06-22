use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use raccoon_platform_config::component::grpc::{GrpcClientConfig, GrpcServerConfig};
use raccoon_platform_runtime::FatalError;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::task::TaskTracker;
use tonic::server::NamedService;

use crate::error::OrchestrationError;

const GRPC_TCP_KEEPALIVE: Duration = Duration::from_secs(30);
const GRPC_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const GRPC_TCP_KEEPALIVE_RETRIES: u32 = 3;
const GRPC_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const GRPC_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Incoming gRPC TCP stream wrapper that applies low-latency socket defaults.
pub struct GrpcIncoming {
    inner: TcpListenerStream,
}

impl GrpcIncoming {
    fn new(listener: TcpListener) -> Self {
        Self {
            inner: TcpListenerStream::new(listener),
        }
    }
}

impl Stream for GrpcIncoming {
    type Item = Result<TcpStream, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(stream))) => {
                if let Err(error) = stream.set_nodelay(true) {
                    tracing::warn!(
                        error = %error,
                        "failed to enable TCP_NODELAY for accepted gRPC connection"
                    );
                }
                Poll::Ready(Some(Ok(stream)))
            }
            other => other,
        }
    }
}

/// Bind a gRPC listener from service configuration.
pub async fn bind_grpc_listener(
    service_name: &'static str,
    config: &GrpcServerConfig,
) -> Result<(SocketAddr, GrpcIncoming), OrchestrationError> {
    let bind_address: SocketAddr = config.bind_address.parse()?;
    let listener = TcpListener::bind(bind_address).await?;
    let local_addr = listener.local_addr()?;

    tracing::info!(
        service.name = service_name,
        server.address = %local_addr,
        "gRPC server listening"
    );

    Ok((local_addr, GrpcIncoming::new(listener)))
}

/// Build a low-latency gRPC client endpoint from service configuration.
pub fn grpc_client_endpoint(
    config: &GrpcClientConfig,
) -> Result<tonic::transport::Endpoint, OrchestrationError> {
    let mut endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())?
        .tcp_nodelay(true)
        .tcp_keepalive(Some(GRPC_TCP_KEEPALIVE))
        .tcp_keepalive_interval(Some(GRPC_TCP_KEEPALIVE_INTERVAL))
        .tcp_keepalive_retries(Some(GRPC_TCP_KEEPALIVE_RETRIES))
        .http2_keep_alive_interval(GRPC_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(GRPC_HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true);

    if let Some(seconds) = config.connect_timeout_seconds {
        endpoint = endpoint.connect_timeout(Duration::from_secs(seconds));
    }

    if let Some(seconds) = config.request_timeout_seconds {
        endpoint = endpoint.timeout(Duration::from_secs(seconds));
    }

    Ok(endpoint)
}

/// Build a gRPC server builder with transport-level keepalive defaults.
pub fn grpc_server_builder() -> tonic::transport::Server {
    tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(GRPC_HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(GRPC_HTTP2_KEEPALIVE_TIMEOUT))
}

/// Register a long-running gRPC server future with the runtime task tracker.
pub fn start_grpc_server<F>(
    service_name: &'static str,
    server: F,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) where
    F: Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
{
    task_tracker.spawn(async move {
        if let Err(error) = server.await {
            let _ = fatal_tx.send(FatalError::new(service_name, "grpc-server", error));
        }
    });
}

/// Build the standard gRPC health service for a Raccoon service server.
pub async fn serving_health_service<S>()
-> tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health>
where
    S: NamedService,
{
    let (reporter, health_service) = tonic_health::server::health_reporter();
    reporter.set_serving::<S>().await;
    health_service
}
