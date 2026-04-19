//! Telemetry wiring for Raccoon binaries.
//!
//! This crate exposes compact config types, initialization helpers, and a
//! shutdown guard for logging, tracing, and metrics.

mod error;
mod global;
mod guard;
mod init;

pub use error::TelemetryError;
pub use global::{meter, tracer};
pub use guard::TelemetryGuard;
pub use init::{
    ExportConfigInit, LogFormat, LogLevel, MeterProviderInit, Protocol, TracerProviderInit, init,
};
