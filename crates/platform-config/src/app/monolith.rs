use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::application_entities::ApplicationEntitiesConfig;
use crate::component::database::DatabaseConfig;
use crate::component::dcmtk::DcmtkConfig;
use crate::component::dicomweb::DicomWebConfig;
use crate::component::filesystem::FilesystemConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::storage::StorageConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the monolith runtime.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MonolithConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// Local and peer DICOM application entity settings.
    pub application_entities: ApplicationEntitiesConfig,

    /// Database backend configuration.
    pub database: DatabaseConfig,

    /// DCMTK external toolchain settings.
    pub dcmtk: DcmtkConfig,

    /// DICOMweb HTTP listener and provider settings.
    pub dicomweb: DicomWebConfig,

    /// Local filesystem configuration.
    pub filesystem: FilesystemConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Object storage backend configuration.
    pub storage: StorageConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl MonolithConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/raccoon").required(false))
            .add_source(File::with_name("config/application-entities").required(false))
            .add_source(File::with_name("raccoon").required(false))
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
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

    use super::MonolithConfig;

    #[test]
    fn default_constructs_with_dicomweb_components() {
        let config = MonolithConfig::default();

        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:8080");
        assert_eq!(config.dicomweb.base_path, "/dicom-web");
        assert_eq!(config.dcmtk.path, None);
    }

    #[test]
    fn deserializes_dicomweb_settings_from_example_config() {
        let config: MonolithConfig = Config::builder()
            .add_source(File::from(example_config_path()))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:8080");
        assert_eq!(config.dicomweb.base_path, "/dicom-web");
        assert_eq!(config.dcmtk.path, None);
        assert!(config.dicomweb.render_cache.enabled);
    }

    #[test]
    fn environment_overrides_dicomweb_settings() {
        unsafe {
            std::env::set_var("RACCOON__DICOMWEB__BIND_ADDRESS", "127.0.0.1:18080");
            std::env::set_var("RACCOON__DCMTK__PATH", "/opt/dcmtk/bin");
        }

        let config: MonolithConfig = Config::builder()
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.dicomweb.bind_address, "127.0.0.1:18080");
        assert_eq!(
            config.dcmtk.path.unwrap().display().to_string(),
            "/opt/dcmtk/bin"
        );

        unsafe {
            std::env::remove_var("RACCOON__DICOMWEB__BIND_ADDRESS");
            std::env::remove_var("RACCOON__DCMTK__PATH");
        }
    }

    fn example_config_path() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/raccoon.example.toml")
    }
}
