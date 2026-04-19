use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::app::App;
use crate::config::RuntimeConfig;
use crate::error::{FatalError, RuntimeError};

/// Reason the runtime began shutdown.
#[derive(Debug)]
pub enum ShutdownReason {
    /// Shutdown was requested externally.
    Requested,

    /// A background task reported a fatal error.
    FatalError,

    /// All tracked work completed and the runtime drained naturally.
    WorkDrained,
}

/// Async runner for an application instance.
pub struct Runtime<A>
where
    A: App,
{
    /// Application lifecycle hooks.
    app: Arc<A>,

    /// Runtime policy.
    config: RuntimeConfig,

    /// Permit used to reject concurrent starts.
    run_slot: Semaphore,

    /// Cancellation signal for shutdown.
    shutdown: CancellationToken,

    /// Cancellation signal for a forced shutdown.
    force_shutdown: CancellationToken,

    /// Tracks spawned background tasks.
    task_tracker: TaskTracker,
}

impl<A> Runtime<A>
where
    A: App + 'static,
{
    /// Create a runtime around an application instance.
    pub fn new(app: A, config: RuntimeConfig) -> Self {
        Self {
            app: Arc::new(app),
            config,
            run_slot: Semaphore::new(1),
            shutdown: CancellationToken::new(),
            force_shutdown: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
        }
    }

    /// Return a clone of the runtime shutdown token.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Start the application and wait for shutdown.
    pub async fn start(&self) -> Result<ShutdownReason, RuntimeError> {
        let _permit = self.acquire_run_guard()?;
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel::<FatalError>();

        self.app
            .start(self.shutdown_token(), &self.task_tracker, fatal_tx);

        let result = tokio::select! {
            _ = self.shutdown.cancelled() => {
                info!(runtime.shutdown.reason = ?ShutdownReason::Requested, "shutdown requested");
                Ok(ShutdownReason::Requested)
            }
            message = fatal_rx.recv() => {
                match message {
                    Some(error) => {
                        error!(
                            runtime.error = ?error,
                            runtime.shutdown.reason = ?ShutdownReason::FatalError,
                            "fatal error received",
                        );
                        Err(error)
                    },
                    None => {
                        warn!(runtime.shutdown.reason = ?ShutdownReason::WorkDrained, "work drained");
                        Ok(ShutdownReason::WorkDrained)
                    }
                }
            }
        };

        self.shutdown();

        if let Err(error) = self.graceful_shutdown().await {
            warn!(runtime.shutdown.error = ?error, "failed to shut down gracefully: {error}");

            // propagates the shutdown error if result is not already error
            if result.is_ok() {
                return Err(error);
            }
        }

        result
            .inspect(|reason| info!(runtime.shutdown.reason = ?reason, "shutdown complete"))
            .map_err(Into::into)
    }

    /// Request runtime shutdown.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Force the runtime to stop waiting on shutdown.
    pub fn force_shutdown(&self) {
        self.force_shutdown.cancel();
    }

    /// Acquire the single-start permit.
    fn acquire_run_guard(&self) -> Result<SemaphorePermit<'_>, RuntimeError> {
        self.run_slot
            .try_acquire()
            .map_err(|_| RuntimeError::AlreadyRunning)
    }

    #[tracing::instrument(
        name = "raccoon.runtime.graceful_shutdown",
        level = "debug",
        skip(self)
    )]
    /// Wait for tracked work and shutdown hooks to complete.
    async fn graceful_shutdown(&self) -> Result<(), RuntimeError> {
        self.task_tracker.close();

        let started_at = Instant::now();

        let task_tracker = self.task_tracker.clone();
        let app = self.app.clone();

        let mut shutdown_task: JoinHandle<Result<(), RuntimeError>> = tokio::spawn(async move {
            task_tracker.wait().await;
            app.shutdown()
                .await
                .map_err(|err| RuntimeError::AppShutdown(Box::new(err)))
        });

        let shutdown_timeout = Duration::from_secs(self.config.shutdown_timeout_seconds);

        let sleep = sleep(shutdown_timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                task_result = &mut shutdown_task => {
                    return task_result.unwrap_or_else(|error| {
                        Err(RuntimeError::AppShutdown(Box::new(error)))
                    });
                },

                _ = &mut sleep => {
                    let elapsed_seconds = started_at.elapsed().as_secs();

                    if self.config.force_exit_on_timeout {
                        error!(?started_at, "shutdown timed out ({elapsed_seconds}s); forcing exit");

                        return Err(RuntimeError::ShutdownTimedOut { elapsed_seconds });
                    }

                    warn!(?started_at, "shutdown timed out ({elapsed_seconds}s); still waiting");

                    sleep.as_mut().reset(Instant::now() + shutdown_timeout);
                }

                _ = self.force_shutdown.cancelled() => {
                    let elapsed_seconds = started_at.elapsed().as_secs();

                    warn!(?started_at, "shutdown force-aborted ({elapsed_seconds}s)");

                    return Err(RuntimeError::ForceShutdown { elapsed_seconds });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, Ordering};

    use thiserror::Error;

    use super::*;

    #[derive(Debug, Error)]
    #[error("test error")]
    struct TestError(&'static str);

    struct IdleApp;

    impl App for IdleApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            _shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            _fatal_tx: mpsc::UnboundedSender<FatalError>,
        ) {
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Ok(())
        }
    }

    struct FatalApp;

    impl App for FatalApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            _shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalError>,
        ) {
            let _ = fatal_tx.send(FatalError::new("test", "fatal", TestError("fatal failure")));
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Ok(())
        }
    }

    struct HangingApp;

    impl App for HangingApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            _shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            _fatal_tx: mpsc::UnboundedSender<FatalError>,
        ) {
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            std::future::pending().await
        }
    }

    struct ShutdownErrorApp;

    impl App for ShutdownErrorApp {
        type ShutdownError = TestError;

        fn start(
            &self,
            _shutdown: CancellationToken,
            _task_tracker: &TaskTracker,
            _fatal_tx: mpsc::UnboundedSender<FatalError>,
        ) {
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Err(TestError("shutdown failure"))
        }
    }

    struct TrackedApp {
        task_started: Arc<AtomicBool>,
    }

    impl App for TrackedApp {
        type ShutdownError = Infallible;

        fn start(
            &self,
            shutdown: CancellationToken,
            task_tracker: &TaskTracker,
            fatal_tx: mpsc::UnboundedSender<FatalError>,
        ) {
            let task_started = self.task_started.clone();
            task_tracker.spawn(async move {
                let _fatal_tx = fatal_tx;
                task_started.store(true, Ordering::SeqCst);
                shutdown.cancelled().await;
            });
        }

        async fn shutdown(&self) -> Result<(), Self::ShutdownError> {
            Ok(())
        }
    }

    fn runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            shutdown_timeout_seconds: 1,
            force_exit_on_timeout: true,
        }
    }

    #[test]
    fn new_runtime_starts_with_active_shutdown_token() {
        let runtime = Runtime::new(IdleApp, runtime_config());

        assert!(!runtime.shutdown_token().is_cancelled());
    }

    #[test]
    fn shutdown_cancels_shutdown_token() {
        let runtime = Runtime::new(IdleApp, runtime_config());

        runtime.shutdown();

        assert!(runtime.shutdown_token().is_cancelled());
    }

    #[tokio::test]
    async fn start_returns_work_drained_when_fatal_channel_closes() {
        let runtime = Runtime::new(IdleApp, runtime_config());

        let reason = runtime.start().await.expect("runtime should drain");

        assert!(matches!(reason, ShutdownReason::WorkDrained));
    }

    #[tokio::test]
    async fn start_returns_fatal_error_when_app_reports_fatal_failure() {
        let runtime = Runtime::new(FatalApp, runtime_config());

        let error = runtime.start().await.expect_err("runtime should fail");

        assert!(matches!(error, RuntimeError::Fatal(_)));
    }

    #[tokio::test]
    async fn start_propagates_shutdown_hook_errors_after_drain() {
        let runtime = Runtime::new(ShutdownErrorApp, runtime_config());

        let error = runtime.start().await.expect_err("shutdown should fail");

        assert!(matches!(error, RuntimeError::AppShutdown(_)));
    }

    #[tokio::test]
    async fn graceful_shutdown_times_out_when_force_exit_is_enabled() {
        let runtime = Runtime::new(
            HangingApp,
            RuntimeConfig {
                shutdown_timeout_seconds: 0,
                force_exit_on_timeout: true,
            },
        );

        let error = runtime
            .graceful_shutdown()
            .await
            .expect_err("shutdown should time out");

        assert!(matches!(error, RuntimeError::ShutdownTimedOut { .. }));
    }

    #[tokio::test]
    async fn force_shutdown_interrupts_hanging_shutdown() {
        let runtime = Arc::new(Runtime::new(
            HangingApp,
            RuntimeConfig {
                shutdown_timeout_seconds: 60,
                force_exit_on_timeout: false,
            },
        ));

        let shutdown_runtime = runtime.clone();
        let shutdown_task = tokio::spawn(async move { shutdown_runtime.graceful_shutdown().await });

        tokio::task::yield_now().await;
        runtime.force_shutdown();

        let error = shutdown_task
            .await
            .expect("shutdown task should join")
            .expect_err("shutdown should be force-aborted");

        assert!(matches!(error, RuntimeError::ForceShutdown { .. }));
    }

    #[tokio::test]
    async fn concurrent_start_is_rejected() {
        let task_started = Arc::new(AtomicBool::new(false));
        let runtime = Arc::new(Runtime::new(
            TrackedApp {
                task_started: task_started.clone(),
            },
            runtime_config(),
        ));

        let first_runtime = runtime.clone();
        let first_start = tokio::spawn(async move { first_runtime.start().await });

        while !task_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        let second_start = runtime.start().await;
        runtime.shutdown();
        let first_result = first_start.await.expect("first start task should join");

        assert!(matches!(second_start, Err(RuntimeError::AlreadyRunning)));
        assert!(matches!(first_result, Ok(ShutdownReason::Requested)));
    }
}
