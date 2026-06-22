use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use raccoon_platform_config::app::IngestServiceConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_service_ingest::{
    IngestService, IngestTransportGrpcService, IngestTransportServiceServer,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::app::grpc::{
    GrpcIncoming, bind_grpc_listener, grpc_server_builder, serving_health_service,
    start_grpc_server,
};
use crate::component::object_store::{ingest_object_store_root, quarantine_object_store_root};
use crate::contract::ingest_repository::build_ingest_repository_handles;
use crate::contract::object_store::build_object_store;
use crate::error::OrchestrationError;
use crate::service::ingest::build_ingest_service;

/// Runnable ingest gRPC service application.
pub struct IngestApp {
    local_addr: SocketAddr,
    incoming: Mutex<Option<GrpcIncoming>>,
    service: Arc<dyn IngestService>,
}

impl IngestApp {
    /// Return the socket address where the ingest gRPC server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the ingest service application from loaded configuration.
pub async fn build_ingest_app(
    config: &IngestServiceConfig,
) -> Result<IngestApp, OrchestrationError> {
    let object_store = build_object_store(
        &config.storage,
        ingest_object_store_root(&config.filesystem),
    );
    let quarantine_object_store = build_object_store(
        &config.storage,
        quarantine_object_store_root(&config.filesystem),
    );
    let repositories =
        build_ingest_repository_handles(&config.database, &config.filesystem).await?;
    let ingest_service = build_ingest_service(
        object_store,
        quarantine_object_store,
        repositories.ingest_repository,
    );
    let service = ingest_service;
    let (local_addr, incoming) = bind_grpc_listener("ingest", &config.grpc).await?;

    Ok(IngestApp {
        local_addr,
        incoming: Mutex::new(Some(incoming)),
        service,
    })
}

impl App for IngestApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        let Some(incoming) = self.incoming.lock().expect("ingest listener lock").take() else {
            let _ = fatal_tx.send(FatalError::new(
                "ingest",
                "grpc-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "gRPC server already started",
                ),
            ));
            return;
        };
        let service = IngestTransportGrpcService::from_shared(self.service.clone()).into_server();

        let server = async move {
            let health_service = serving_health_service::<
                IngestTransportServiceServer<IngestTransportGrpcService>,
            >()
            .await;
            grpc_server_builder()
                .add_service(service)
                .add_service(health_service)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown.cancelled().await;
                })
                .await
        };

        start_grpc_server("ingest", server, task_tracker, fatal_tx);
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}
