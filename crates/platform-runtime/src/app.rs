use std::error::Error;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::FatalError;

/// Application hooks managed by the runtime.
pub trait App: Send + Sync {
    /// Error returned by application shutdown hooks.
    type ShutdownError: Error + Send + Sync + 'static;

    /// Start long-running work.
    ///
    /// Implementations should register spawned tasks with `task_tracker`, watch
    /// `shutdown`, and report fatal background failures through `fatal_tx`.
    fn start(
        &self,
        shutdown: CancellationToken,
        task_tracker: &TaskTracker,
        fatal_tx: mpsc::UnboundedSender<FatalError>,
    );

    /// Run application-specific shutdown hooks.
    fn shutdown(&self) -> impl Future<Output = Result<(), Self::ShutdownError>> + Send;
}
