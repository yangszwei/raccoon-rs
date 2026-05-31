use std::future::Future;
use std::net::SocketAddr;

use raccoon_platform_config::component::grpc::GrpcServerConfig;
use raccoon_platform_runtime::FatalError;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::task::TaskTracker;

use crate::error::OrchestrationError;

/// Bind a gRPC listener from service configuration.
pub async fn bind_grpc_listener(
    config: &GrpcServerConfig,
) -> Result<(SocketAddr, TcpListenerStream), OrchestrationError> {
    let bind_address: SocketAddr = config.bind_address.parse()?;
    let listener = TcpListener::bind(bind_address).await?;
    let local_addr = listener.local_addr()?;

    Ok((local_addr, TcpListenerStream::new(listener)))
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
