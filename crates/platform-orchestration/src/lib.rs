//! Orchestration boundary for Raccoon binaries.
//!
//! This crate owns the wiring of internal components into runnable binaries.
//! Binaries depend on internal components through this crate, which keeps
//! startup, configuration, and component setup in one place. Public modules
//! follow the same naming rule as the workspace directory prefixes by their
//! internal role.

pub mod adapter;
pub mod app;
pub mod contract;
pub mod error;
pub mod platform;
pub mod protocol;
pub mod service;

pub use error::OrchestrationError;
