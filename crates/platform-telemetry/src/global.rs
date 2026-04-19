use opentelemetry::global;

/// Returns a meter from the global provider.
///
/// # Example
/// ```
/// let meter = raccoon_platform_telemetry::meter("raccoon.dimse");
/// let counter = meter.u64_counter("requests_total").build();
///
/// counter.add(1, &[]);
/// ```
pub fn meter(name: &'static str) -> opentelemetry::metrics::Meter {
    global::meter(name)
}

/// Returns a tracer from the global provider.
///
/// # Example
/// ```
/// use opentelemetry::trace::Tracer;
///
/// let tracer = raccoon_platform_telemetry::tracer("raccoon.dimse");
/// let _span = tracer.start("association.accept");
/// ```
pub fn tracer(name: &'static str) -> global::BoxedTracer {
    global::tracer(name)
}
