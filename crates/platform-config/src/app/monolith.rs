use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the monolith runtime.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MonolithConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl MonolithConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/raccoon").required(false))
            .add_source(File::with_name("raccoon").required(false))
            .add_source(Environment::with_prefix("RACCOON").separator("__"))
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}
