use thiserror::Error;

/// Errors produced while preparing Raccoon runtime state.
#[derive(Debug, Error)]
pub enum OrchestrationError {
    /// Configuration could not be loaded or deserialized.
    #[error(transparent)]
    Config(#[from] raccoon_platform_config::ConfigError),
}
