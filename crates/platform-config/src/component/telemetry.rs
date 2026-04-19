use serde::Deserialize;

/// Log levels for the application.
#[derive(Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// Supported log output formats.
#[derive(Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Compact,
    #[default]
    Json,
}

/// Supported OTLP transport protocols.
#[derive(Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Grpc,
    HttpJson,
}

/// OTLP exporter connection settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ExporterConfig {
    /// OTLP collector endpoint.
    pub endpoint: String,

    /// OTLP transport protocol.
    pub protocol: Protocol,

    /// Export timeout in seconds.
    pub timeout_seconds: u64,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4317".to_string(),
            protocol: Protocol::default(),
            timeout_seconds: 10,
        }
    }
}

/// Tracing settings.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// Whether trace export is enabled.
    pub enabled: bool,

    /// OTLP exporter settings.
    pub exporter: ExporterConfig,
}

/// Metrics settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether metrics export is enabled.
    pub enabled: bool,

    /// Push interval in seconds.
    pub export_interval_seconds: u64,

    /// OTLP exporter settings.
    pub exporter: ExporterConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            export_interval_seconds: 30,
            exporter: ExporterConfig::default(),
        }
    }
}

/// Telemetry configuration for logs, traces, and metrics.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Minimum log level.
    pub log_level: LogLevel,

    /// Structured log output format.
    pub log_format: LogFormat,

    /// Tracing settings.
    pub tracing: TracingConfig,

    /// Metrics settings.
    pub metrics: MetricsConfig,
}
