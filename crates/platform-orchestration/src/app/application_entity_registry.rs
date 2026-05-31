use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use raccoon_platform_config::app::ApplicationEntityRegistryServiceConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_service_application_entity_registry::{
    ApplicationEntityRegistryGrpcService, InMemoryApplicationEntityRegistry,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::transport::Server;

use crate::app::grpc::{bind_grpc_listener, start_grpc_server};
use crate::error::OrchestrationError;
use crate::service::application_entity_registry::build_application_entity_registry;

/// Runnable Application Entity registry gRPC service application.
pub struct ApplicationEntityRegistryApp {
    local_addr: SocketAddr,
    incoming: Mutex<Option<TcpListenerStream>>,
    registry: Arc<tokio::sync::Mutex<InMemoryApplicationEntityRegistry>>,
}

impl ApplicationEntityRegistryApp {
    /// Return the socket address where the registry gRPC server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the Application Entity registry app from loaded configuration.
pub async fn build_application_entity_registry_app(
    config: &ApplicationEntityRegistryServiceConfig,
) -> Result<ApplicationEntityRegistryApp, OrchestrationError> {
    let registry = build_application_entity_registry(&config.application_entities)?;
    let (local_addr, incoming) = bind_grpc_listener(&config.grpc).await?;

    Ok(ApplicationEntityRegistryApp {
        local_addr,
        incoming: Mutex::new(Some(incoming)),
        registry: Arc::new(tokio::sync::Mutex::new(registry)),
    })
}

impl App for ApplicationEntityRegistryApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        let Some(incoming) = self
            .incoming
            .lock()
            .expect("application entity registry listener lock")
            .take()
        else {
            let _ = fatal_tx.send(FatalError::new(
                "application-entity-registry",
                "grpc-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "gRPC server already started",
                ),
            ));
            return;
        };
        let service =
            ApplicationEntityRegistryGrpcService::from_shared(self.registry.clone()).into_server();

        let server = async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    shutdown.cancelled().await;
                })
                .await
        };

        start_grpc_server(
            "application-entity-registry",
            server,
            task_tracker,
            fatal_tx,
        );
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}
