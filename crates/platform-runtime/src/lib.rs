//! Runtime lifecycle wiring for Raccoon binaries.
//!
//! This crate defines the application lifecycle contract, shutdown policy, and
//! async runner used to coordinate startup, graceful shutdown, and fatal
//! background task failures.

mod app;
mod config;
mod error;
mod runtime;

pub use app::App;
pub use config::RuntimeConfig;
pub use error::{FatalError, RuntimeError};
pub use runtime::{Runtime, ShutdownReason};
