use serde::Deserialize;

/// Runtime lifecycle settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Graceful shutdown timeout in seconds.
    pub shutdown_timeout_seconds: u64,

    /// Whether the binary should escalate after a shutdown timeout.
    ///
    /// Runtime libraries should not call `process::exit`; this flag is intended
    /// for the outer binary/application layer to enforce.
    pub force_exit_on_timeout: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_seconds: 30,
            force_exit_on_timeout: false,
        }
    }
}
