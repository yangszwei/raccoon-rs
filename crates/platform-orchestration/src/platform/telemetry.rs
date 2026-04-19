use raccoon_platform_config::component as config;
use raccoon_platform_telemetry as telemetry;

use crate::error::OrchestrationError;

/// Initialize application telemetry from loaded configuration.
pub fn init_telemetry(
    app_config: &config::app::AppConfig,
    telemetry_config: &config::telemetry::TelemetryConfig,
) -> Result<telemetry::TelemetryGuard, OrchestrationError> {
    telemetry::init(
        app_config.name.as_str(),
        &map_log_level(&telemetry_config.log_level),
        &map_log_format(&telemetry_config.log_format),
        &map_tracing_config(&telemetry_config.tracing),
        &map_metrics_config(&telemetry_config.metrics),
    )
    .map_err(Into::into)
}

fn map_log_level(log_level: &config::telemetry::LogLevel) -> telemetry::LogLevel {
    match log_level {
        config::telemetry::LogLevel::Trace => telemetry::LogLevel::Trace,
        config::telemetry::LogLevel::Debug => telemetry::LogLevel::Debug,
        config::telemetry::LogLevel::Info => telemetry::LogLevel::Info,
        config::telemetry::LogLevel::Warn => telemetry::LogLevel::Warn,
        config::telemetry::LogLevel::Error => telemetry::LogLevel::Error,
    }
}

fn map_log_format(log_format: &config::telemetry::LogFormat) -> telemetry::LogFormat {
    match log_format {
        config::telemetry::LogFormat::Pretty => telemetry::LogFormat::Pretty,
        config::telemetry::LogFormat::Compact => telemetry::LogFormat::Compact,
        config::telemetry::LogFormat::Json => telemetry::LogFormat::Json,
    }
}

fn map_metrics_config(config: &config::telemetry::MetricsConfig) -> telemetry::MeterProviderInit {
    telemetry::MeterProviderInit {
        enabled: config.enabled,
        export_interval_seconds: config.export_interval_seconds,
        export_config: map_exporter_config(&config.exporter),
    }
}

fn map_tracing_config(config: &config::telemetry::TracingConfig) -> telemetry::TracerProviderInit {
    telemetry::TracerProviderInit {
        enabled: config.enabled,
        export_config: map_exporter_config(&config.exporter),
    }
}

fn map_exporter_config(config: &config::telemetry::ExporterConfig) -> telemetry::ExportConfigInit {
    telemetry::ExportConfigInit {
        endpoint: config.endpoint.clone(),
        protocol: map_protocol(&config.protocol),
        timeout_seconds: config.timeout_seconds,
    }
}

fn map_protocol(protocol: &config::telemetry::Protocol) -> telemetry::Protocol {
    match protocol {
        config::telemetry::Protocol::Grpc => telemetry::Protocol::Grpc,
        config::telemetry::Protocol::HttpJson => telemetry::Protocol::HttpJson,
    }
}
