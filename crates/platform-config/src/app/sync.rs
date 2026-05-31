use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::database::DatabaseConfig;
use crate::component::filesystem::FilesystemConfig;
use crate::component::grpc::GrpcServerConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::storage::StorageConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the sync gRPC service.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct SyncServiceConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// Database backend configuration.
    pub database: DatabaseConfig,

    /// Local filesystem configuration.
    pub filesystem: FilesystemConfig,

    /// gRPC server listener settings for the sync service.
    pub grpc: GrpcServerConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Object storage backend configuration.
    pub storage: StorageConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl SyncServiceConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/sync").required(false))
            .add_source(File::with_name("sync").required(false))
            .add_source(Environment::with_prefix("RACCOON_SYNC").separator("__"))
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use config::{Config, File};

    use super::SyncServiceConfig;

    #[test]
    fn default_constructs_with_shared_component_defaults() {
        let config = SyncServiceConfig::default();

        assert_eq!(config.app.name, "raccoon");
        assert_eq!(config.grpc.bind_address, "127.0.0.1:50051");
    }

    #[test]
    fn deserializes_top_level_components_from_example_config() {
        let config: SyncServiceConfig = Config::builder()
            .add_source(File::from(example_config_path()))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.app.name, "raccoon-sync");
        assert_eq!(config.filesystem.root.to_string_lossy(), "data");
        assert_eq!(config.grpc.bind_address, "127.0.0.1:50055");
    }

    fn example_config_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/sync.example.toml")
    }
}
