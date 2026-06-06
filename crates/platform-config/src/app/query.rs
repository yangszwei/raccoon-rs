use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::database::DatabaseConfig;
use crate::component::filesystem::FilesystemConfig;
use crate::component::grpc::GrpcServerConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the query gRPC service.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct QueryServiceConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// Database backend configuration.
    pub database: DatabaseConfig,

    /// Local filesystem configuration.
    pub filesystem: FilesystemConfig,

    /// gRPC server listener settings for the query service.
    pub grpc: GrpcServerConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl QueryServiceConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/query").required(false))
            .add_source(File::with_name("query").required(false))
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .add_source(Environment::with_prefix("RACCOON_QUERY").separator("__"))
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use config::{Config, Environment, File};

    use super::QueryServiceConfig;

    #[test]
    fn default_constructs_with_shared_component_defaults() {
        let config = QueryServiceConfig::default();

        assert_eq!(config.app.name, "raccoon");
        assert_eq!(config.grpc.bind_address, "127.0.0.1:50051");
    }

    #[test]
    fn deserializes_top_level_components_from_example_config() {
        let config: QueryServiceConfig = Config::builder()
            .add_source(File::from(example_config_path()))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.app.name, "raccoon-query");
        assert_eq!(config.filesystem.root.to_string_lossy(), "data");
        assert_eq!(config.grpc.bind_address, "127.0.0.1:50053");
    }

    #[test]
    fn service_environment_overrides_common_environment() {
        unsafe {
            std::env::set_var("RACCOON__RUNTIME__SHUTDOWN_TIMEOUT_SECONDS", "60");
            std::env::set_var("RACCOON__TELEMETRY__LOG_LEVEL", "debug");
            std::env::set_var("RACCOON_QUERY__TELEMETRY__LOG_LEVEL", "warn");
        }

        let config: QueryServiceConfig = Config::builder()
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .add_source(Environment::with_prefix("RACCOON_QUERY").separator("__"))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.runtime.shutdown_timeout_seconds, 60);
        assert_eq!(
            config.telemetry.log_level,
            crate::component::telemetry::LogLevel::Warn
        );

        unsafe {
            std::env::remove_var("RACCOON__RUNTIME__SHUTDOWN_TIMEOUT_SECONDS");
            std::env::remove_var("RACCOON__TELEMETRY__LOG_LEVEL");
            std::env::remove_var("RACCOON_QUERY__TELEMETRY__LOG_LEVEL");
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/query.example.toml")
    }
}
