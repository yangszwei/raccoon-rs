use thiserror::Error;

/// Errors produced while loading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to load or deserialize configuration sources.
    #[error("failed to load config: {0}")]
    Load(config::ConfigError),
}
