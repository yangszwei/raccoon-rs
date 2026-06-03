use std::sync::Arc;

use raccoon_platform_config::component::runtime as config;
use raccoon_platform_runtime::{App, Runtime, RuntimeConfig, RuntimeError};

/// Run an application with runtime configuration.
///
/// The caller provides shutdown-handler installation so binaries can choose
/// their own signal source and escalation policy.
pub async fn run_runtime<A, F>(
    app: A,
    config: &config::RuntimeConfig,
    install_shutdown_handler: F,
) -> Result<(), RuntimeError>
where
    A: App + 'static,
    F: FnOnce(Arc<Runtime<A>>),
{
    let runtime = Arc::new(Runtime::new(app, map_runtime_config(config)));

    install_shutdown_handler(runtime.clone());

    runtime.start().await.map(|_| ())
}

/// Install process signal handling for graceful shutdown escalation.
///
/// The first Ctrl-C/SIGTERM requests graceful shutdown. A second signal forces
/// the runtime to stop waiting for shutdown hooks and tracked tasks.
pub fn install_ctrl_c_handler<A>(runtime: Arc<Runtime<A>>)
where
    A: App + 'static,
{
    tokio::spawn(async move {
        let mut shutdown_requested = false;

        while wait_for_shutdown_signal().await.is_some() {
            if shutdown_requested {
                runtime.force_shutdown();
                break;
            }

            shutdown_requested = true;
            runtime.shutdown();
        }
    });
}

async fn wait_for_shutdown_signal() -> Option<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).ok()?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => result.ok(),
            _ = sigterm.recv() => Some(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok()
    }
}

/// Convert loaded runtime config into runtime initialization config.
fn map_runtime_config(config: &config::RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        shutdown_timeout_seconds: config.shutdown_timeout_seconds,
        force_exit_on_timeout: config.force_exit_on_timeout,
    }
}
