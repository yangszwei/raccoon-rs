#[cfg(feature = "grpc")]
mod application_entity_registry;
#[cfg(feature = "grpc")]
mod dicomweb_gateway;
#[cfg(feature = "grpc")]
mod dimse_gateway;
#[cfg(feature = "grpc")]
mod grpc;
#[cfg(feature = "grpc")]
mod ingest;
mod monolith;
#[cfg(feature = "grpc")]
mod query;
#[cfg(feature = "grpc")]
mod retrieve;
#[cfg(feature = "grpc")]
mod sync;

#[cfg(feature = "grpc")]
pub use application_entity_registry::{
    ApplicationEntityRegistryApp, build_application_entity_registry_app,
};
#[cfg(feature = "grpc")]
pub use dicomweb_gateway::{DicomWebGatewayApp, build_dicomweb_gateway_app};
#[cfg(feature = "grpc")]
pub use dimse_gateway::{DimseGatewayApp, build_dimse_gateway_app};
#[cfg(feature = "grpc")]
pub use ingest::{IngestApp, build_ingest_app};
pub use monolith::{MonolithApp, build_monolith_app};
#[cfg(feature = "grpc")]
pub use query::{QueryApp, build_query_app};
#[cfg(feature = "grpc")]
pub use retrieve::{RetrieveApp, build_retrieve_app};
#[cfg(feature = "grpc")]
pub use sync::{SyncApp, build_sync_app};

#[cfg(all(test, feature = "grpc"))]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use raccoon_platform_config::app::{
        ApplicationEntityRegistryServiceConfig, IngestServiceConfig, QueryServiceConfig,
        RetrieveServiceConfig, SyncServiceConfig,
    };
    use raccoon_platform_config::component::application_entities::LocalApplicationEntityConfig;
    use raccoon_platform_runtime::{App, Runtime, RuntimeConfig};
    use raccoon_service_application_entity_registry::{
        ApplicationEntityRegistryGrpcService, ApplicationEntityRegistryServiceServer,
        InMemoryApplicationEntityRegistry,
    };
    use raccoon_service_ingest::{IngestTransportGrpcService, IngestTransportServiceServer};
    use raccoon_service_query::{DicomQueryServiceServer, QueryGrpcService};
    use raccoon_service_retrieve::{DicomRetrieveServiceServer, RetrieveGrpcService};
    use raccoon_service_sync::{DicomSyncServiceServer, SyncGrpcService};
    use tempfile::TempDir;
    use tonic::server::NamedService;
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;

    use super::*;
    use crate::error::OrchestrationError;

    fn bind_ephemeral(bind_address: &mut String) {
        *bind_address = "127.0.0.1:0".to_string();
    }

    fn set_root(root: &TempDir, path: &mut std::path::PathBuf) {
        *path = root.path().to_path_buf();
    }

    async fn assert_app_health<A>(app: A, local_addr: SocketAddr, service_name: &'static str)
    where
        A: App + 'static,
    {
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

        let endpoint = format!("http://{local_addr}");
        assert_health_serving(&endpoint, "").await;
        assert_health_serving(&endpoint, service_name).await;

        runtime.shutdown();
        runner
            .await
            .expect("runtime task joins")
            .expect("runtime shuts down");
    }

    async fn assert_health_serving(endpoint: &str, service_name: &str) {
        let mut client = health_client(endpoint).await;
        let response = client
            .check(HealthCheckRequest {
                service: service_name.to_string(),
            })
            .await
            .expect("health check succeeds")
            .into_inner();

        let status = ServingStatus::try_from(response.status).expect("known health status");
        assert_eq!(status, ServingStatus::Serving);
    }

    async fn health_client(endpoint: &str) -> HealthClient<tonic::transport::Channel> {
        let mut last_error = None;

        for _ in 0..20 {
            match tonic::transport::Endpoint::from_shared(endpoint.to_string())
                .expect("valid health endpoint")
                .connect_timeout(Duration::from_millis(100))
                .timeout(Duration::from_secs(1))
                .connect()
                .await
            {
                Ok(channel) => return HealthClient::new(channel),
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        panic!("health client connects: {last_error:?}");
    }

    #[tokio::test]
    async fn build_ingest_app_binds_grpc_listener() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = IngestServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);

        let app = build_ingest_app(&config).await.expect("ingest app builds");

        assert_eq!(app.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.local_addr().port(), 0);
        assert!(filesystem_root.path().join("ingest/ingest.db").exists());
    }

    #[tokio::test]
    async fn build_query_app_binds_grpc_listener() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = QueryServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);

        let app = build_query_app(&config).await.expect("query app builds");

        assert_eq!(app.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.local_addr().port(), 0);
        assert!(filesystem_root.path().join("read/read.db").exists());
    }

    #[tokio::test]
    async fn build_retrieve_app_binds_grpc_listener() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = RetrieveServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);

        let app = build_retrieve_app(&config)
            .await
            .expect("retrieve app builds");

        assert_eq!(app.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.local_addr().port(), 0);
        assert!(filesystem_root.path().join("read/read.db").exists());
    }

    #[tokio::test]
    async fn build_sync_app_binds_grpc_listener() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = SyncServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);

        let app = build_sync_app(&config).await.expect("sync app builds");

        assert_eq!(app.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.local_addr().port(), 0);
        assert!(filesystem_root.path().join("ingest/ingest.db").exists());
        assert!(filesystem_root.path().join("read/read.db").exists());
    }

    #[tokio::test]
    async fn build_application_entity_registry_app_binds_grpc_listener() {
        let mut config = ApplicationEntityRegistryServiceConfig::default();
        bind_ephemeral(&mut config.grpc.bind_address);

        let app = build_application_entity_registry_app(&config)
            .await
            .expect("application entity registry app builds");

        assert_eq!(app.local_addr().ip().to_string(), "127.0.0.1");
        assert_ne!(app.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn build_application_entity_registry_app_rejects_duplicate_aes() {
        let mut config = ApplicationEntityRegistryServiceConfig::default();
        bind_ephemeral(&mut config.grpc.bind_address);
        config
            .application_entities
            .local
            .push(LocalApplicationEntityConfig::default());

        let error = match build_application_entity_registry_app(&config).await {
            Ok(_) => panic!("duplicate AE should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            OrchestrationError::ApplicationEntityRegistry(_)
        ));
    }

    #[tokio::test]
    async fn sync_app_shuts_down_under_runtime() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = SyncServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_sync_app(&config).await.expect("sync app builds");
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

    #[tokio::test]
    async fn ingest_app_serves_standard_grpc_health() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = IngestServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_ingest_app(&config).await.expect("ingest app builds");
        let local_addr = app.local_addr();

        assert_app_health(
            app,
            local_addr,
            <IngestTransportServiceServer<IngestTransportGrpcService> as NamedService>::NAME,
        )
        .await;
    }

    #[tokio::test]
    async fn query_app_serves_standard_grpc_health() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = QueryServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_query_app(&config).await.expect("query app builds");
        let local_addr = app.local_addr();

        assert_app_health(
            app,
            local_addr,
            <DicomQueryServiceServer<QueryGrpcService> as NamedService>::NAME,
        )
        .await;
    }

    #[tokio::test]
    async fn retrieve_app_serves_standard_grpc_health() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = RetrieveServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_retrieve_app(&config)
            .await
            .expect("retrieve app builds");
        let local_addr = app.local_addr();

        assert_app_health(
            app,
            local_addr,
            <DicomRetrieveServiceServer<RetrieveGrpcService> as NamedService>::NAME,
        )
        .await;
    }

    #[tokio::test]
    async fn sync_app_serves_standard_grpc_health() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = SyncServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_sync_app(&config).await.expect("sync app builds");
        let local_addr = app.local_addr();

        assert_app_health(
            app,
            local_addr,
            <DicomSyncServiceServer<SyncGrpcService> as NamedService>::NAME,
        )
        .await;
    }

    #[tokio::test]
    async fn application_entity_registry_app_serves_standard_grpc_health() {
        let mut config = ApplicationEntityRegistryServiceConfig::default();
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_application_entity_registry_app(&config)
            .await
            .expect("application entity registry app builds");
        let local_addr = app.local_addr();

        assert_app_health(
            app,
            local_addr,
            <ApplicationEntityRegistryServiceServer<
                ApplicationEntityRegistryGrpcService<InMemoryApplicationEntityRegistry>,
            > as NamedService>::NAME,
        )
        .await;
    }

    #[tokio::test]
    async fn grpc_health_rejects_unknown_service_name() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let mut config = QueryServiceConfig::default();
        set_root(&filesystem_root, &mut config.filesystem.root);
        bind_ephemeral(&mut config.grpc.bind_address);
        let app = build_query_app(&config).await.expect("query app builds");
        let endpoint = format!("http://{}", app.local_addr());
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

        let error = health_client(&endpoint)
            .await
            .check(HealthCheckRequest {
                service: "raccoon.unknown.v1.Unknown".to_string(),
            })
            .await
            .expect_err("unknown service is rejected");

        assert_eq!(error.code(), tonic::Code::NotFound);

        runtime.shutdown();
        runner
            .await
            .expect("runtime task joins")
            .expect("runtime shuts down");
    }
}
