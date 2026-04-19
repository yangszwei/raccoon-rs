/// Runtime settings.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeConfig {
    /// Graceful shutdown wait interval in seconds.
    pub shutdown_timeout_seconds: u64,

    /// Whether timeout should abort graceful shutdown.
    pub force_exit_on_timeout: bool,
}
