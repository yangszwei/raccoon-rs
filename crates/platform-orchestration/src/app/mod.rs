#[cfg(feature = "grpc")]
mod application_entity_registry;
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
    use std::sync::Arc;

    use raccoon_platform_config::app::{
        ApplicationEntityRegistryServiceConfig, IngestServiceConfig, QueryServiceConfig,
        RetrieveServiceConfig, SyncServiceConfig,
    };
    use raccoon_platform_config::component::application_entities::LocalApplicationEntityConfig;
    use raccoon_platform_runtime::{Runtime, RuntimeConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::error::OrchestrationError;

    fn bind_ephemeral(bind_address: &mut String) {
        *bind_address = "127.0.0.1:0".to_string();
    }

    fn set_root(root: &TempDir, path: &mut std::path::PathBuf) {
        *path = root.path().to_path_buf();
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
}
