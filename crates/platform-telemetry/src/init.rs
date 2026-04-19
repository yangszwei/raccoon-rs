use std::time::Duration;

use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{ExportConfig, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::error::TelemetryError;
use crate::guard::TelemetryGuard;

/// Application log levels.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        })
    }
}

/// Log output format for the subscriber.
#[derive(Debug, Clone, Copy)]
pub enum LogFormat {
    /// Pretty output for local development.
    Pretty,

    /// Compact single-line output.
    Compact,

    /// JSON output for aggregation.
    Json,
}

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    /// gRPC transport.
    Grpc,

    /// HTTP/JSON transport.
    HttpJson,
}

/// Shared OTLP exporter settings.
#[derive(Debug, Clone)]
pub struct ExportConfigInit {
    /// Collector endpoint.
    pub endpoint: String,

    /// Transport protocol.
    pub protocol: Protocol,

    /// Export request timeout in seconds.
    pub timeout_seconds: u64,
}

/// Tracing exporter settings.
#[derive(Debug, Clone)]
pub struct TracerProviderInit {
    /// Enables OTLP tracing export.
    pub enabled: bool,

    /// Shared OTLP exporter settings.
    pub export_config: ExportConfigInit,
}

/// Metrics exporter settings.
#[derive(Debug, Clone)]
pub struct MeterProviderInit {
    /// Enables OTLP metrics export.
    pub enabled: bool,

    /// Export interval in seconds.
    pub export_interval_seconds: u64,

    /// Shared OTLP exporter settings.
    pub export_config: ExportConfigInit,
}

/// Initializes logging, tracing, and metrics.
///
/// On success, installs the global providers and returns a guard that flushes
/// and shuts them down on drop. If subscriber setup fails, no global provider
/// is left installed.
pub fn init(
    service_name: &str,
    log_level: &LogLevel,
    log_format: &LogFormat,
    tracer_provider_init: &TracerProviderInit,
    meter_provider_init: &MeterProviderInit,
) -> Result<TelemetryGuard, TelemetryError> {
    let tracer_provider = build_tracer_provider(service_name, tracer_provider_init)?;
    let meter_provider = build_meter_provider(service_name, meter_provider_init)?;

    install_subscriber(
        service_name,
        log_level,
        log_format,
        tracer_provider.as_ref(),
    )?;

    if let Some(provider) = &tracer_provider {
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(provider.clone());
    }

    if let Some(provider) = &meter_provider {
        global::set_meter_provider(provider.clone());
    }

    Ok(TelemetryGuard::new(meter_provider, tracer_provider))
}

fn install_subscriber(
    app_name: &str,
    log_level: &LogLevel,
    log_format: &LogFormat,
    tracer_provider: Option<&SdkTracerProvider>,
) -> Result<(), TelemetryError> {
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::try_new(log_level.to_string())?)
        .with(match log_format {
            LogFormat::Pretty => tracing_subscriber::fmt::layer().pretty().boxed(),
            LogFormat::Compact => tracing_subscriber::fmt::layer().compact().boxed(),
            LogFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        });

    if let Some(tracer) = tracer_provider.map(|provider| provider.tracer(app_name.to_string())) {
        subscriber
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .map_err(Into::into)
    } else {
        subscriber.try_init().map_err(Into::into)
    }
}

fn build_tracer_provider(
    service_name: &str,
    init: &TracerProviderInit,
) -> Result<Option<SdkTracerProvider>, TelemetryError> {
    if !init.enabled {
        return Ok(None);
    }

    let exporter = match init.export_config.protocol {
        Protocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&init.export_config))
            .build()?,
        Protocol::HttpJson => SpanExporter::builder()
            .with_http()
            .with_export_config(export_config(&init.export_config))
            .build()?,
    };

    let provider = SdkTracerProvider::builder()
        .with_resource(resource(service_name))
        .with_batch_exporter(exporter)
        .build();

    Ok(Some(provider))
}

fn build_meter_provider(
    service_name: &str,
    init: &MeterProviderInit,
) -> Result<Option<SdkMeterProvider>, TelemetryError> {
    if !init.enabled {
        return Ok(None);
    }

    let exporter = match &init.export_config.protocol {
        Protocol::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_export_config(export_config(&init.export_config))
            .build()?,
        Protocol::HttpJson => MetricExporter::builder()
            .with_http()
            .with_export_config(export_config(&init.export_config))
            .build()?,
    };

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(init.export_interval_seconds))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource(service_name))
        .with_reader(reader)
        .build();

    Ok(Some(provider))
}

fn export_config(exporter: &ExportConfigInit) -> ExportConfig {
    ExportConfig {
        endpoint: exporter.endpoint.clone().into(),
        protocol: match exporter.protocol {
            Protocol::Grpc => opentelemetry_otlp::Protocol::Grpc,
            Protocol::HttpJson => opentelemetry_otlp::Protocol::HttpJson,
        },
        timeout: Some(Duration::from_secs(exporter.timeout_seconds)),
    }
}

fn resource(service_name: &str) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telemetry_init() -> (LogLevel, LogFormat, TracerProviderInit, MeterProviderInit) {
        (
            LogLevel::Info,
            LogFormat::Compact,
            TracerProviderInit {
                enabled: false,
                export_config: otlp_exporter(Protocol::Grpc),
            },
            MeterProviderInit {
                enabled: false,
                export_interval_seconds: 30,
                export_config: otlp_exporter(Protocol::Grpc),
            },
        )
    }

    fn otlp_exporter(protocol: Protocol) -> ExportConfigInit {
        ExportConfigInit {
            endpoint: "http://collector:4317".to_string(),
            protocol,
            timeout_seconds: 7,
        }
    }

    #[test]
    fn otlp_export_config_maps_grpc() {
        let exporter = otlp_exporter(Protocol::Grpc);

        let config = export_config(&exporter);

        assert_eq!(config.endpoint.as_deref(), Some("http://collector:4317"));
        assert_eq!(config.protocol, opentelemetry_otlp::Protocol::Grpc);
        assert_eq!(config.timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn otlp_export_config_maps_http_json() {
        let exporter = otlp_exporter(Protocol::HttpJson);

        let config = export_config(&exporter);

        assert_eq!(config.endpoint.as_deref(), Some("http://collector:4317"));
        assert_eq!(config.protocol, opentelemetry_otlp::Protocol::HttpJson);
        assert_eq!(config.timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn disabled_metrics_skip_provider_build() {
        let (_, _, _, meter_provider_init) = telemetry_init();

        let provider = build_meter_provider("raccoon-test", &meter_provider_init)
            .expect("disabled metrics should not build an exporter");

        assert!(provider.is_none());
    }

    #[test]
    fn disabled_traces_skip_provider_build() {
        let (_, _, tracer_provider_init, _) = telemetry_init();

        let provider = build_tracer_provider("raccoon-test", &tracer_provider_init)
            .expect("disabled traces should not build an exporter");

        assert!(provider.is_none());
    }

    #[test]
    fn log_level_formats_as_expected() {
        assert_eq!(LogLevel::Trace.to_string(), "trace");
        assert_eq!(LogLevel::Debug.to_string(), "debug");
        assert_eq!(LogLevel::Info.to_string(), "info");
        assert_eq!(LogLevel::Warn.to_string(), "warn");
        assert_eq!(LogLevel::Error.to_string(), "error");
    }
}
