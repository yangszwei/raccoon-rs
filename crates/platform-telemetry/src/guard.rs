use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Owns telemetry providers and shuts them down on drop.
#[derive(Debug, Default)]
pub struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    pub(crate) fn new(
        meter_provider: Option<SdkMeterProvider>,
        tracer_provider: Option<SdkTracerProvider>,
    ) -> Self {
        Self {
            meter_provider,
            tracer_provider,
        }
    }

    /// Flushes and shuts down installed telemetry providers.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }

        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_is_idempotent_for_real_providers() {
        let meter_provider = SdkMeterProvider::builder().build();
        let tracer_provider = SdkTracerProvider::builder().build();
        let mut guard = TelemetryGuard::new(Some(meter_provider), Some(tracer_provider));

        guard.shutdown();
        guard.shutdown();
    }
}
