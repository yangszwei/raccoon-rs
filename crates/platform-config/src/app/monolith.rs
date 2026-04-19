use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;

/// Top-level configuration for the monolith runtime.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MonolithConfig {}

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
