//! Shared configuration model for Raccoon.
//!
//! This crate contains reusable component config and app-level config for each
//! runnable binary. App-level config is composed of component config. Config
//! structs are defaultable so binaries can start without local files, and source
//! loading is centralized so TOML files and `RACCOON__...` environment
//! overrides behave consistently.

pub mod app;
pub mod component;

mod error;

pub use error::ConfigError;
