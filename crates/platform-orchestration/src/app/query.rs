use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use raccoon_platform_config::app::QueryServiceConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_service_query::{DicomQueryServiceServer, QueryGrpcService, QueryService};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::transport::Server;

use crate::app::grpc::{bind_grpc_listener, serving_health_service, start_grpc_server};
use crate::contract::read_repository::build_read_repository_handles;
use crate::error::OrchestrationError;
use crate::service::query::build_query_service;

/// Runnable query gRPC service application.
pub struct QueryApp {
    local_addr: SocketAddr,
    incoming: Mutex<Option<TcpListenerStream>>,
    service: Arc<dyn QueryService>,
}

impl QueryApp {
    /// Return the socket address where the query gRPC server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the query service application from loaded configuration.
pub async fn build_query_app(config: &QueryServiceConfig) -> Result<QueryApp, OrchestrationError> {
    let repositories = build_read_repository_handles(&config.filesystem).await?;
    let service = build_query_service(repositories.query_repository);
    let (local_addr, incoming) = bind_grpc_listener("query", &config.grpc).await?;

    Ok(QueryApp {
        local_addr,
        incoming: Mutex::new(Some(incoming)),
        service,
    })
}

impl App for QueryApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        let Some(incoming) = self.incoming.lock().expect("query listener lock").take() else {
            let _ = fatal_tx.send(FatalError::new(
                "query",
                "grpc-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "gRPC server already started",
                ),
            ));
            return;
        };
        let service = QueryGrpcService::from_shared(self.service.clone()).into_server();

        let server = async move {
            let health_service =
                serving_health_service::<DicomQueryServiceServer<QueryGrpcService>>().await;
            Server::builder()
                .add_service(service)
                .add_service(health_service)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown.cancelled().await;
                })
                .await
        };

        start_grpc_server("query", server, task_tracker, fatal_tx);
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}
