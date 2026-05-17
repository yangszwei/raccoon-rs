//! Low-level object storage contract for Raccoon.
//!
//! This crate models object storage primitives shared by filesystem, S3, GCS,
//! and future object-store adapters. Service-specific concepts such as DICOM
//! studies, ingest jobs, retrieval workflows, key layout, validation, and
//! lifecycle policy belong in the services that build on this contract.

mod error;
mod model;
mod store;

pub use bytes::Bytes;
pub use error::ObjectStoreError;
pub use futures_core::Stream;
pub use model::{ByteStream, Object, ObjectKey, ObjectKeyError, ObjectMetadata, PutResult};
pub use store::ObjectStore;

/// Convenient result alias for object store operations.
pub type Result<T, E = ObjectStoreError> = std::result::Result<T, E>;
