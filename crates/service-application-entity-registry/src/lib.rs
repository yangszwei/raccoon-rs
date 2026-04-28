//! Application Entity registry for DICOM services.
//!
//! This crate defines local and peer Application Entities and keeps their AE
//! titles unique across the process registry.

mod application_entity;
mod registry;

pub use application_entity::{
    AeTitle, AeTitleError, ApplicationEntityError, LocalApplicationEntity, PeerApplicationEntity,
};
#[cfg(any(test, feature = "test-support"))]
pub use application_entity::{local_ae, peer_ae};
pub use registry::{ApplicationEntityRegistry, ApplicationEntityRegistryError};
