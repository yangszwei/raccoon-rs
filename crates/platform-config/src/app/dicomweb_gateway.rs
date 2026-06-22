use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::database::DatabaseConfig;
use crate::component::dcmtk::DcmtkConfig;
use crate::component::dicomweb::DicomWebConfig;
use crate::component::filesystem::FilesystemConfig;
use crate::component::grpc::GrpcClientConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the DICOMweb gateway.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DicomWebGatewayConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// DCMTK external toolchain settings.
    pub dcmtk: DcmtkConfig,

    /// DICOMweb HTTP listener and provider settings.
    pub dicomweb: DicomWebConfig,

    /// Read-side database used for WADO-RS metadata lookup.
    pub database: DatabaseConfig,

    /// Local filesystem configuration for read-model metadata lookup.
    pub filesystem: FilesystemConfig,

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

impl DicomWebGatewayConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/dicomweb-gateway").required(false))
            .add_source(File::with_name("dicomweb-gateway").required(false))
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .add_source(Environment::with_prefix("RACCOON_DICOMWEB_GATEWAY").separator("__"))
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

    use super::DicomWebGatewayConfig;

    #[test]
    fn default_constructs_with_shared_component_defaults() {
        let config = DicomWebGatewayConfig::default();

        assert_eq!(config.app.name, "raccoon");
        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:8080");
        assert_eq!(config.dicomweb.base_path, "/dicom-web");
        assert_eq!(config.dcmtk.path, None);
        assert_eq!(config.query.endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.retrieve.endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.ingest.endpoint, "http://127.0.0.1:50051");
        assert!(matches!(
            config.database,
            crate::component::database::DatabaseConfig::Sqlite
        ));
        assert_eq!(config.filesystem.root.display().to_string(), "data");
    }

    #[test]
    fn deserializes_top_level_components_from_example_config() {
        let config: DicomWebGatewayConfig = Config::builder()
            .add_source(File::from(example_config_path()))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.app.name, "raccoon-dicomweb-gateway");
        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:8081");
        assert_eq!(config.dicomweb.base_path, "/dicom-web");
        assert_eq!(config.dcmtk.path, None);
        assert_eq!(config.query.endpoint, "http://127.0.0.1:50052");
        assert_eq!(config.retrieve.endpoint, "http://127.0.0.1:50054");
        assert_eq!(config.ingest.endpoint, "http://127.0.0.1:50053");
        assert_eq!(config.filesystem.root.display().to_string(), "data");
    }

    #[test]
    fn environment_overrides_nested_gateway_settings() {
        unsafe {
            std::env::set_var(
                "RACCOON_DICOMWEB_GATEWAY__DICOMWEB__BIND_ADDRESS",
                "127.0.0.1:18080",
            );
            std::env::set_var(
                "RACCOON_DICOMWEB_GATEWAY__QUERY__ENDPOINT",
                "http://127.0.0.1:61052",
            );
            std::env::set_var("RACCOON_DICOMWEB_GATEWAY__DCMTK__PATH", "/opt/dcmtk/bin");
            std::env::set_var("RACCOON_DICOMWEB_GATEWAY__DATABASE__TYPE", "postgresql");
            std::env::set_var(
                "RACCOON_DICOMWEB_GATEWAY__DATABASE__URL",
                "postgres://raccoon:raccoon@read-postgres:5432/raccoon",
            );
        }

        let config: DicomWebGatewayConfig = Config::builder()
            .add_source(Environment::with_prefix("RACCOON_DICOMWEB_GATEWAY").separator("__"))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:18080");
        assert_eq!(config.query.endpoint, "http://127.0.0.1:61052");
        assert_eq!(
            config.dcmtk.path.unwrap().display().to_string(),
            "/opt/dcmtk/bin"
        );
        let crate::component::database::DatabaseConfig::PostgreSql { url } = config.database else {
            panic!("expected postgresql database config");
        };
        assert_eq!(url, "postgres://raccoon:raccoon@read-postgres:5432/raccoon");

        unsafe {
            std::env::remove_var("RACCOON_DICOMWEB_GATEWAY__DICOMWEB__BIND_ADDRESS");
            std::env::remove_var("RACCOON_DICOMWEB_GATEWAY__QUERY__ENDPOINT");
            std::env::remove_var("RACCOON_DICOMWEB_GATEWAY__DCMTK__PATH");
            std::env::remove_var("RACCOON_DICOMWEB_GATEWAY__DATABASE__TYPE");
            std::env::remove_var("RACCOON_DICOMWEB_GATEWAY__DATABASE__URL");
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/dicomweb-gateway.example.toml")
    }
}
