use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use raccoon_platform_config::app::RetrieveServiceConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_service_retrieve::{DicomRetrieveServiceServer, RetrieveGrpcService, RetrieveService};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::grpc::{
    GrpcIncoming, bind_grpc_listener, grpc_server_builder, serving_health_service,
    start_grpc_server,
};
use crate::component::object_store::ingest_object_store_root;
use crate::contract::object_store::build_object_store;
use crate::contract::read_repository::build_read_repository_handles;
use crate::error::OrchestrationError;
use crate::service::retrieve::build_retrieve_service;

/// Runnable retrieve gRPC service application.
pub struct RetrieveApp {
    local_addr: SocketAddr,
    incoming: Mutex<Option<GrpcIncoming>>,
    service: Arc<dyn RetrieveService>,
}

impl RetrieveApp {
    /// Return the socket address where the retrieve gRPC server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the retrieve service application from loaded configuration.
pub async fn build_retrieve_app(
    config: &RetrieveServiceConfig,
) -> Result<RetrieveApp, OrchestrationError> {
    let object_store = build_object_store(
        &config.storage,
        ingest_object_store_root(&config.filesystem),
    );
    let repositories = build_read_repository_handles(&config.filesystem).await?;
    let service = build_retrieve_service(repositories.retrieve_repository, object_store);
    let (local_addr, incoming) = bind_grpc_listener("retrieve", &config.grpc).await?;

    Ok(RetrieveApp {
        local_addr,
        incoming: Mutex::new(Some(incoming)),
        service,
    })
}

impl App for RetrieveApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        let Some(incoming) = self.incoming.lock().expect("retrieve listener lock").take() else {
            let _ = fatal_tx.send(FatalError::new(
                "retrieve",
                "grpc-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "gRPC server already started",
                ),
            ));
            return;
        };
        let service = RetrieveGrpcService::from_shared(self.service.clone()).into_server();

        let server = async move {
            let health_service =
                serving_health_service::<DicomRetrieveServiceServer<RetrieveGrpcService>>().await;
            grpc_server_builder()
                .add_service(service)
                .add_service(health_service)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown.cancelled().await;
                })
                .await
        };

        start_grpc_server("retrieve", server, task_tracker, fatal_tx);
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}
