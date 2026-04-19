use thiserror::Error;

/// Errors returned while initializing telemetry.
#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("failed to build exporter: {0}")]
    ExporterBuild(#[from] opentelemetry_otlp::ExporterBuildError),

    #[error("invalid log level: {0}")]
    InvalidLogLevel(#[from] tracing_subscriber::filter::ParseError),

    #[error("failed to initialize tracing subscriber: {0}")]
    SubscriberInit(#[from] tracing_subscriber::util::TryInitError),
}
