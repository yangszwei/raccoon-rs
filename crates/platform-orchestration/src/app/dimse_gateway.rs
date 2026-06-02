use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use raccoon_platform_config::app::DimseGatewayConfig;
use raccoon_platform_config::component::grpc::GrpcClientConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_protocol_dimse::{DimseListener, ServiceClassRegistry};
use raccoon_service_application_entity_registry::{
    ApplicationEntityRegistry, LocalApplicationEntity,
};
use raccoon_service_ingest::{
    GrpcIngestTransportClient, IngestService, IngestTransportServiceClient,
};
use raccoon_service_query::{DicomQueryServiceClient, GrpcQueryServiceClient, QueryService};
use raccoon_service_retrieve::{
    DicomRetrieveServiceClient, GrpcRetrieveServiceClient, RetrieveService,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, warn};

use crate::error::OrchestrationError;
use crate::protocol::dimse::{bind_dimse_listener, build_dimse_service_registry};
use crate::service::application_entity_registry::build_grpc_application_entity_registry_client;

/// Runnable DIMSE gateway application backed by distributed gRPC services.
pub struct DimseGatewayApp {
    dimse_endpoints: Vec<DimseEndpoint>,
}

struct DimseEndpoint {
    listener: Arc<DimseListener>,
    registry: Arc<ServiceClassRegistry>,
    max_concurrent_associations: usize,
}

/// Build the DIMSE gateway application from loaded configuration.
pub async fn build_dimse_gateway_app(
    config: &DimseGatewayConfig,
) -> Result<DimseGatewayApp, OrchestrationError> {
    let application_entity_registry =
        build_grpc_application_entity_registry_client(&config.application_entity_registry).await?;
    let local_aes = application_entity_registry.list_locals().await?;

    if local_aes.is_empty() {
        return Err(OrchestrationError::NoLocalApplicationEntities);
    }

    let query_service = build_grpc_query_service(&config.query).await?;
    let retrieve_service = build_grpc_retrieve_service(&config.retrieve).await?;
    let ingest_service = build_grpc_ingest_service(&config.ingest).await?;

    let application_entity_registry: Arc<dyn ApplicationEntityRegistry + Send + Sync> =
        Arc::new(application_entity_registry);

    let mut dimse_endpoints = Vec::with_capacity(local_aes.len());
    for local_ae in local_aes {
        let max_concurrent_associations = local_ae.max_concurrent_associations();
        let registry = Arc::new(build_gateway_dimse_service_registry(
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

    Ok(DimseGatewayApp { dimse_endpoints })
}

impl DimseGatewayApp {
    /// Return the DIMSE socket addresses bound by this gateway.
    pub fn local_aes(&self) -> impl Iterator<Item = Option<std::net::SocketAddr>> + '_ {
        self.dimse_endpoints
            .iter()
            .map(|endpoint| endpoint.listener.local_addr().ok())
    }
}

impl App for DimseGatewayApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        for endpoint in &self.dimse_endpoints {
            start_dimse_workers(endpoint, shutdown.clone(), task_tracker, fatal_tx.clone());
        }
    }

    async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
        Ok(())
    }
}

async fn build_grpc_query_service(
    config: &GrpcClientConfig,
) -> Result<Arc<dyn QueryService>, OrchestrationError> {
    let endpoint = grpc_client_endpoint(config)?;
    let server_address = endpoint.uri().to_string();
    let inner = DicomQueryServiceClient::connect(endpoint).await?;
    Ok(Arc::new(GrpcQueryServiceClient::with_server_address(
        inner,
        server_address,
    )))
}

async fn build_grpc_retrieve_service(
    config: &GrpcClientConfig,
) -> Result<Arc<dyn RetrieveService>, OrchestrationError> {
    let endpoint = grpc_client_endpoint(config)?;
    let server_address = endpoint.uri().to_string();
    let inner = DicomRetrieveServiceClient::connect(endpoint).await?;
    Ok(Arc::new(GrpcRetrieveServiceClient::with_server_address(
        inner,
        server_address,
    )))
}

async fn build_grpc_ingest_service(
    config: &GrpcClientConfig,
) -> Result<Arc<dyn IngestService>, OrchestrationError> {
    let endpoint = grpc_client_endpoint(config)?;
    let server_address = endpoint.uri().to_string();
    let inner = IngestTransportServiceClient::connect(endpoint).await?;
    Ok(Arc::new(GrpcIngestTransportClient::with_server_address(
        inner,
        server_address,
    )))
}

fn build_gateway_dimse_service_registry(
    ingest_service: Arc<dyn IngestService>,
    query_service: Arc<dyn QueryService>,
    retrieve_service: Arc<dyn RetrieveService>,
    application_entity_registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
    local_ae: LocalApplicationEntity,
) -> ServiceClassRegistry {
    build_dimse_service_registry(
        ingest_service,
        query_service,
        retrieve_service,
        application_entity_registry,
        local_ae,
    )
}

fn start_dimse_workers(
    endpoint: &DimseEndpoint,
    shutdown: CancellationToken,
    task_tracker: &TaskTracker,
    fatal_tx: mpsc::UnboundedSender<FatalError>,
) {
    for worker_index in 0..endpoint.max_concurrent_associations {
        let listener = endpoint.listener.clone();
        let registry = endpoint.registry.clone();
        let shutdown = shutdown.clone();
        let fatal_tx = fatal_tx.clone();

        task_tracker.spawn(async move {
            let _fatal_tx = fatal_tx;

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        debug!(dimse.worker_index = worker_index, "DIMSE gateway worker stopping");
                        return;
                    }
                    result = listener.accept_and_serve(registry.as_ref()) => {
                        if let Err(error) = result {
                            warn!(
                                dimse.worker_index = worker_index,
                                error = %error,
                                "DIMSE gateway association failed"
                            );
                        }
                    }
                }
            }
        });
    }
}

fn grpc_client_endpoint(
    config: &GrpcClientConfig,
) -> Result<tonic::transport::Endpoint, OrchestrationError> {
    let mut endpoint = tonic::transport::Endpoint::from_shared(config.endpoint.clone())?;

    if let Some(seconds) = config.connect_timeout_seconds {
        endpoint = endpoint.connect_timeout(Duration::from_secs(seconds));
    }

    if let Some(seconds) = config.request_timeout_seconds {
        endpoint = endpoint.timeout(Duration::from_secs(seconds));
    }

    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use raccoon_platform_config::app::DimseGatewayConfig;
    use raccoon_service_application_entity_registry::{
        ApplicationEntityRegistryGrpcService, InMemoryApplicationEntityRegistry,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;

    #[tokio::test]
    async fn build_dimse_gateway_app_rejects_empty_registry_locals() {
        let registry_endpoint = start_empty_registry_server().await;
        let mut config = DimseGatewayConfig::default();
        config.application_entity_registry.endpoint = registry_endpoint;

        let error = match build_dimse_gateway_app(&config).await {
            Ok(_) => panic!("empty local AE registry should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            OrchestrationError::NoLocalApplicationEntities
        ));
    }

    async fn start_empty_registry_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("registry listener binds");
        let local_addr = listener.local_addr().expect("registry local address");
        let registry = InMemoryApplicationEntityRegistry::default();
        let service = ApplicationEntityRegistryGrpcService::new(registry).into_server();

        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("registry server runs");
        });

        format!("http://{local_addr}")
    }
}
