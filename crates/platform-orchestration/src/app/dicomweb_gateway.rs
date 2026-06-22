use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::Router;
use raccoon_platform_config::app::DicomWebGatewayConfig;
use raccoon_platform_config::component::grpc::GrpcClientConfig;
use raccoon_platform_runtime::{App, FatalError};
use raccoon_protocol_dicomweb::log_dicomweb_requests;
use raccoon_service_ingest::{
    GrpcIngestTransportClient, IngestService, IngestTransportServiceClient,
};
use raccoon_service_query::{DicomQueryServiceClient, GrpcQueryServiceClient, QueryService};
use raccoon_service_retrieve::{
    DicomRetrieveServiceClient, GrpcRetrieveServiceClient, RetrieveService,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::app::grpc::grpc_client_endpoint;
use crate::component::http::{bind_http_listener, mount_base_path, start_http_server};
use crate::contract::read_repository::build_read_repository_handles;
use crate::error::OrchestrationError;
use crate::protocol::dicomweb::build_dicomweb_router;

/// Runnable DICOMweb gateway backed by distributed gRPC services.
pub struct DicomWebGatewayApp {
    local_addr: SocketAddr,
    listener: Mutex<Option<TcpListener>>,
    router: Router,
}

impl DicomWebGatewayApp {
    /// Return the socket address where the DICOMweb HTTP server is bound.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Build the DICOMweb gateway application from loaded configuration.
pub async fn build_dicomweb_gateway_app(
    config: &DicomWebGatewayConfig,
) -> Result<DicomWebGatewayApp, OrchestrationError> {
    let query_service = build_grpc_query_service(&config.query).await?;
    let retrieve_service = build_grpc_retrieve_service(&config.retrieve).await?;
    let ingest_service = build_grpc_ingest_service(&config.ingest).await?;
    let read_repositories =
        build_read_repository_handles(&config.database, &config.filesystem).await?;
    let router = build_dicomweb_router(
        ingest_service,
        query_service,
        retrieve_service,
        read_repositories.metadata_repository,
        &config.dicomweb,
        &config.dcmtk,
    );
    let router = mount_base_path(&config.dicomweb.base_path, router);
    let router = log_dicomweb_requests("dicomweb-gateway", router);
    let (local_addr, listener) =
        bind_http_listener("dicomweb-gateway", &config.dicomweb.bind_address).await?;

    Ok(DicomWebGatewayApp {
        local_addr,
        listener: Mutex::new(Some(listener)),
        router,
    })
}

impl App for DicomWebGatewayApp {
    type ShutdownError = Infallible;

    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    ) {
        let Some(listener) = self.listener.lock().expect("dicomweb listener lock").take() else {
            let _ = fatal_tx.send(FatalError::new(
                "dicomweb-gateway",
                "http-server",
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "HTTP server already started",
                ),
            ));
            return;
        };
        let router = self.router.clone();

        let server = axum::serve(listener, router).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            info!("DICOMweb gateway graceful shutdown started");
        });
        start_http_server("dicomweb-gateway", server, task_tracker, fatal_tx);
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
