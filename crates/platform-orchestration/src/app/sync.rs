use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use raccoon_platform_config::app::SyncServiceConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_service_sync::{DicomSyncServiceServer, SyncGrpcService, SyncService, SyncWorkerId};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::transport::Server;
use tracing::info;

use crate::app::grpc::{bind_grpc_listener, serving_health_service, start_grpc_server};
use crate::component::object_store::{ingest_object_store_root, quarantine_object_store_root};
use crate::contract::ingest_repository::build_ingest_repository_handles;
use crate::contract::object_store::build_object_store;
use crate::contract::read_repository::build_read_repository_handles;
use crate::error::OrchestrationError;
use crate::service::sync::build_sync_service;

/// Runnable sync gRPC service application.
pub struct SyncApp {
    local_addr: SocketAddr,
    incoming: Mutex<Option<TcpListenerStream>>,
    service: Arc<dyn SyncService>,
}

impl SyncApp {
    /// Return the socket address where the sync gRPC server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the sync service application from loaded configuration.
pub async fn build_sync_app(config: &SyncServiceConfig) -> Result<SyncApp, OrchestrationError> {
    let object_store = build_object_store(
        &config.storage,
        ingest_object_store_root(&config.filesystem),
    );
    let quarantine_object_store = build_object_store(
        &config.storage,
        quarantine_object_store_root(&config.filesystem),
    );
    let ingest_repositories = build_ingest_repository_handles(&config.filesystem).await?;
    let read_repositories = build_read_repository_handles(&config.filesystem).await?;
    let service = build_sync_service(
        ingest_repositories.sync_source_repository,
        read_repositories.sync_read_model_writer,
        ingest_repositories.sync_quarantine_repository,
        object_store,
        quarantine_object_store,
    );
    let (local_addr, incoming) = bind_grpc_listener("sync", &config.grpc).await?;

    Ok(SyncApp {
        local_addr,
        incoming: Mutex::new(Some(incoming)),
        service,
    })
}

impl App for SyncApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        start_sync_worker(
            self.service.clone(),
            shutdown.clone(),
            task_tracker,
            fatal_tx.clone(),
        );

        let Some(incoming) = self.incoming.lock().expect("sync listener lock").take() else {
            let _ = fatal_tx.send(FatalError::new(
                "sync",
                "grpc-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "gRPC server already started",
                ),
            ));
            return;
        };
        let service = SyncGrpcService::from_shared(self.service.clone()).into_server();

        let server = async move {
            let health_service =
                serving_health_service::<DicomSyncServiceServer<SyncGrpcService>>().await;
            Server::builder()
                .add_service(service)
                .add_service(health_service)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown.cancelled().await;
                })
                .await
        };

        start_grpc_server("sync", server, task_tracker, fatal_tx);
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}

fn start_sync_worker(
    service: Arc<dyn SyncService>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) {
    task_tracker.spawn(async move {
        let worker_id = SyncWorkerId::new("sync-service-1");
        info!(sync.worker_id = %worker_id, "sync worker started");
        if let Err(error) = service.run_until_shutdown(worker_id, shutdown).await {
            let _ = fatal_tx.send(FatalError::new("sync", "worker", error));
        }
    });
}
