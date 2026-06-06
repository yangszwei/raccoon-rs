use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::grpc::GrpcClientConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the DIMSE gateway.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DimseGatewayConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// gRPC client settings for the Application Entity registry service.
    pub application_entity_registry: GrpcClientConfig,

    /// gRPC client settings for the ingest service.
    pub ingest: GrpcClientConfig,

    /// gRPC client settings for the query service.
    pub query: GrpcClientConfig,

    /// gRPC client settings for the retrieve service.
    pub retrieve: GrpcClientConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl DimseGatewayConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/dimse-gateway").required(false))
            .add_source(File::with_name("dimse-gateway").required(false))
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .add_source(Environment::with_prefix("RACCOON_DIMSE_GATEWAY").separator("__"))
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

    use super::DimseGatewayConfig;

    #[test]
    fn default_constructs_with_shared_component_defaults() {
        let config = DimseGatewayConfig::default();

        assert_eq!(config.app.name, "raccoon");
        assert_eq!(config.query.endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.retrieve.endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.ingest.endpoint, "http://127.0.0.1:50051");
        assert_eq!(
            config.application_entity_registry.endpoint,
            "http://127.0.0.1:50051"
        );
    }

    #[test]
    fn deserializes_top_level_components_from_example_config() {
        let config: DimseGatewayConfig = Config::builder()
            .add_source(File::from(example_config_path()))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.app.name, "raccoon-dimse-gateway");
        assert_eq!(config.query.endpoint, "http://127.0.0.1:50052");
        assert_eq!(config.retrieve.endpoint, "http://127.0.0.1:50054");
        assert_eq!(config.ingest.endpoint, "http://127.0.0.1:50053");
        assert_eq!(
            config.application_entity_registry.endpoint,
            "http://127.0.0.1:50051"
        );
    }

    #[test]
    fn environment_overrides_nested_grpc_endpoints() {
        unsafe {
            std::env::set_var(
                "RACCOON_DIMSE_GATEWAY__QUERY__ENDPOINT",
                "http://127.0.0.1:61052",
            );
            std::env::set_var(
                "RACCOON_DIMSE_GATEWAY__RETRIEVE__CONNECT_TIMEOUT_SECONDS",
                "7",
            );
        }

        let config: DimseGatewayConfig = Config::builder()
            .add_source(Environment::with_prefix("RACCOON_DIMSE_GATEWAY").separator("__"))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.query.endpoint, "http://127.0.0.1:61052");
        assert_eq!(config.retrieve.connect_timeout_seconds, Some(7));

        unsafe {
            std::env::remove_var("RACCOON_DIMSE_GATEWAY__QUERY__ENDPOINT");
            std::env::remove_var("RACCOON_DIMSE_GATEWAY__RETRIEVE__CONNECT_TIMEOUT_SECONDS");
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/dimse-gateway.example.toml")
    }
}
