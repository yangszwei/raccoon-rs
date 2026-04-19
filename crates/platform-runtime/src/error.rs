use std::error::Error;

use thiserror::Error;

/// Fatal error reported by a background task.
#[derive(Debug, Error)]
#[error("{category} error in \"{component}\": {error}")]
pub struct FatalError {
    /// Component that reported the failure.
    pub component: &'static str,

    /// Stable category for logs and metrics.
    pub category: &'static str,

    /// Underlying failure.
    #[source]
    pub error: Box<dyn Error + Send + Sync + 'static>,
}

impl FatalError {
    /// Create a fatal error from a typed source error.
    pub fn new<E>(component: &'static str, category: &'static str, error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            component,
            category,
            error: Box::new(error),
        }
    }
}

/// Errors returned by runtime lifecycle operations.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Runtime start was requested twice.
    #[error("runtime is already running")]
    AlreadyRunning,

    /// Application shutdown hook failed.
    #[error("app shutdown failed: {0}")]
    AppShutdown(#[source] Box<dyn Error + Send + Sync + 'static>),

    /// A background task reported a fatal error.
    #[error("fatal error")]
    Fatal(#[from] FatalError),

    /// Graceful shutdown was force-aborted.
    #[error("graceful shutdown was force-aborted after {elapsed_seconds}s")]
    ForceShutdown {
        /// Elapsed shutdown time in seconds.
        elapsed_seconds: u64,
    },

    /// Graceful shutdown timed out.
    #[error("graceful shutdown timed out after {elapsed_seconds}s")]
    ShutdownTimedOut {
        /// Elapsed shutdown time in seconds.
        elapsed_seconds: u64,
    },
}
