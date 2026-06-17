use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::Router;
use raccoon_platform_config::app::MonolithConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_protocol_dicomweb::log_dicomweb_requests;
use raccoon_protocol_dimse::{DimseListener, ServiceClassRegistry};
use raccoon_service_application_entity_registry::ApplicationEntityRegistry;
use raccoon_service_sync::{SyncService, SyncWorkerId};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::component::http::{bind_http_listener, mount_base_path, start_http_server};
use crate::component::object_store::{ingest_object_store_root, quarantine_object_store_root};
use crate::contract::ingest_repository::build_ingest_repository_handles;
use crate::contract::object_store::build_object_store;
use crate::contract::read_repository::build_read_repository_handles;
use crate::error::OrchestrationError;
use crate::protocol::dicomweb::build_dicomweb_router;
use crate::protocol::dimse::{bind_dimse_listener, build_dimse_service_registry};
use crate::service::application_entity_registry::build_application_entity_registry;
use crate::service::ingest::build_ingest_service;
use crate::service::query::build_query_service;
use crate::service::retrieve::build_retrieve_service;
use crate::service::sync::build_sync_service;

/// Runnable monolith application assembled from concrete Raccoon components.
pub struct MonolithApp {
    sync_service: Arc<dyn SyncService>,
    dimse_endpoints: Vec<DimseEndpoint>,
    dicomweb_local_addr: SocketAddr,
    dicomweb_listener: Mutex<Option<TcpListener>>,
    dicomweb_router: Router,
}

struct DimseEndpoint {
    listener: Arc<DimseListener>,
    registry: Arc<ServiceClassRegistry>,
    max_concurrent_associations: usize,
}

impl MonolithApp {
    /// Return the socket address where the DICOMweb HTTP server is bound.
    pub fn dicomweb_local_addr(&self) -> SocketAddr {
        self.dicomweb_local_addr
    }
}

/// Build the monolith application from loaded configuration.
pub async fn build_monolith_app(
    config: &MonolithConfig,
) -> Result<MonolithApp, OrchestrationError> {
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

    let ingest_service = build_ingest_service(
        object_store.clone(),
        quarantine_object_store.clone(),
        ingest_repositories.ingest_repository.clone(),
    );
    let query_service = build_query_service(read_repositories.query_repository);
    let metadata_repository = read_repositories.metadata_repository;
    let retrieve_service =
        build_retrieve_service(read_repositories.retrieve_repository, object_store.clone());
    let sync_service = build_sync_service(
        ingest_repositories.sync_source_repository,
        read_repositories.sync_read_model_writer,
        ingest_repositories.sync_quarantine_repository,
        object_store,
        quarantine_object_store,
    );
    let application_entity_registry =
        build_application_entity_registry(&config.application_entities)?;
    let local_aes = application_entity_registry
        .locals()
        .cloned()
        .collect::<Vec<_>>();
    let application_entity_registry: Arc<dyn ApplicationEntityRegistry + Send + Sync> =
        Arc::new(application_entity_registry);

    let mut dimse_endpoints = Vec::with_capacity(local_aes.len());
    for local_ae in local_aes {
        let max_concurrent_associations = local_ae.max_concurrent_associations();
        let registry = Arc::new(build_dimse_service_registry(
            ingest_service.clone(),
            query_service.clone(),
            retrieve_service.clone(),
            application_entity_registry.clone(),
            local_ae.clone(),
        ));
        let listener = Arc::new(bind_dimse_listener(&local_ae, registry.as_ref()).await?);

        dimse_endpoints.push(DimseEndpoint {
            listener,
            registry,
            max_concurrent_associations,
        });
    }

    let dicomweb_router = build_dicomweb_router(
        ingest_service.clone(),
        query_service.clone(),
        retrieve_service.clone(),
        metadata_repository,
        &config.dicomweb,
        &config.dcmtk,
    );
    let dicomweb_router = mount_base_path(&config.dicomweb.base_path, dicomweb_router);
    let dicomweb_router = log_dicomweb_requests("dicomweb", dicomweb_router);
    let (dicomweb_local_addr, dicomweb_listener) =
        bind_http_listener(&config.app.name, &config.dicomweb.bind_address).await?;

    Ok(MonolithApp {
        sync_service,
        dimse_endpoints,
        dicomweb_local_addr,
        dicomweb_listener: Mutex::new(Some(dicomweb_listener)),
        dicomweb_router,
    })
}

impl App for MonolithApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        start_sync_worker(
            self.sync_service.clone(),
            shutdown.clone(),
            task_tracker,
            fatal_tx.clone(),
        );

        start_dicomweb_server(
            &self.dicomweb_listener,
            self.dicomweb_router.clone(),
            shutdown.clone(),
            task_tracker,
            fatal_tx,
        );

        for endpoint in &self.dimse_endpoints {
            start_dimse_workers(endpoint, shutdown.clone(), task_tracker);
        }
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}

fn start_dicomweb_server(
    listener: &Mutex<Option<TcpListener>>,
    router: Router,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) {
    let Some(listener) = listener.lock().expect("dicomweb listener lock").take() else {
        let _ = fatal_tx.send(FatalError::new(
            "dicomweb",
            "http-server",
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "HTTP server already started",
            ),
        ));
        return;
    };

    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        info!("DICOMweb HTTP server graceful shutdown started");
        shutdown.cancelled().await;
        info!("DICOMweb HTTP server graceful shutdown finished");
    });
    start_http_server("dicomweb", server, task_tracker, fatal_tx);
}

fn start_sync_worker(
    sync_service: Arc<dyn SyncService>,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) {
    task_tracker.spawn(async move {
        let worker_id = SyncWorkerId::new("monolith-sync-1");
        info!(sync.worker_id = %worker_id, "sync worker started");
        if let Err(error) = sync_service.run_until_shutdown(worker_id, shutdown).await {
            let _ = fatal_tx.send(FatalError::new("sync", "worker", error));
        }
    });
}

fn start_dimse_workers(
    endpoint: &DimseEndpoint,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
) {
    for worker_index in 0..endpoint.max_concurrent_associations {
        let listener = endpoint.listener.clone();
        let registry = endpoint.registry.clone();
        let shutdown = shutdown.clone();

        task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        debug!(dimse.worker_index = worker_index, "DIMSE worker stopping");
                        return;
                    }
                    result = listener.accept_and_serve(registry.as_ref()) => {
                        if let Err(error) = result {
                            warn!(
                                dimse.worker_index = worker_index,
                                error = %error,
                                "DIMSE association failed"
                            );
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use raccoon_platform_config::app::MonolithConfig;
    use raccoon_platform_config::component::application_entities::LocalApplicationEntityConfig;
    use raccoon_platform_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;

    fn test_config(filesystem_root: &TempDir) -> MonolithConfig {
        let mut config = MonolithConfig::default();
        config.filesystem.root = filesystem_root.path().to_path_buf();
        config.application_entities.local = vec![LocalApplicationEntityConfig {
            bind_address: "127.0.0.1:0".to_string(),
            max_concurrent_associations: 1,
            ..LocalApplicationEntityConfig::default()
        }];
        config.dicomweb.bind_address = "127.0.0.1:0".to_string();
        config
    }

    #[tokio::test]
    async fn build_monolith_app_binds_configured_dimse_listener() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let config = test_config(&filesystem_root);

        let app = build_monolith_app(&config)
            .await
            .expect("monolith app builds");

        assert_eq!(app.dimse_endpoints.len(), 1);
        let local_addr = app.dimse_endpoints[0]
            .listener
            .local_addr()
            .expect("listener has local address");
        assert_eq!(local_addr.ip().to_string(), "127.0.0.1");
        assert_ne!(local_addr.port(), 0);
        assert_eq!(app.dicomweb_local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.dicomweb_local_addr().port(), 0);
        assert!(filesystem_root.path().join("ingest/ingest.db").exists());
        assert!(filesystem_root.path().join("read/read.db").exists());

        let supported_syntaxes = app.dimse_endpoints[0]
            .registry
            .supported_abstract_syntax_uids();
        assert!(
            supported_syntaxes
                .iter()
                .any(|uid| uid == "1.2.840.10008.5.1.4.1.2.1.2")
        );
        assert!(
            supported_syntaxes
                .iter()
                .any(|uid| uid == "1.2.840.10008.5.1.4.1.2.2.2")
        );
    }

    #[tokio::test]
    async fn build_monolith_app_rejects_duplicate_application_entities() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = test_config(&filesystem_root);
        config
            .application_entities
            .local
            .push(LocalApplicationEntityConfig {
                bind_address: "127.0.0.1:0".to_string(),
                ..LocalApplicationEntityConfig::default()
            });

        let error = match build_monolith_app(&config).await {
            Ok(_) => panic!("duplicate AE should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            OrchestrationError::ApplicationEntityRegistry(_)
        ));
    }

    #[tokio::test]
    async fn monolith_app_shuts_down_under_runtime() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let config = test_config(&filesystem_root);
        let app = build_monolith_app(&config)
            .await
            .expect("monolith app builds");
        let runtime = Arc::new(Runtime::new(
            app,
            RuntimeConfig {
                shutdown_timeout_seconds: 1,
                force_exit_on_timeout: true,
            },
        ));

        let runner = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.start().await })
        };

        tokio::task::yield_now().await;
        runtime.shutdown();

        runner
            .await
            .expect("runtime task joins")
            .expect("runtime shuts down");
    }
}
