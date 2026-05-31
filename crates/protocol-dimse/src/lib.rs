//! DICOM DIMSE wire protocol layer for Raccoon.
//!
//! This crate provides the DIMSE (DICOM Message Service Element) network protocol
//! layer, sitting between the UL (Upper Layer) TCP transport and the application
//! service crates. It owns:
//!
//! - Association acceptance and PDU negotiation via `dicom-ul`
//! - PDV-level command reading and dataset streaming (`DimseReader`, `DimseWriter`)
//! - Per-operation message types and status codes
//! - Service-class provider dispatch via `ServiceClassRegistry`
//! - `DimseListener` for accepting inbound DICOM associations

mod association;
mod error;
mod error_policy;
mod instrumentation;
mod listener;
mod message;
mod registry;
mod service;

pub use association::{AssociationContext, DimseAssociation};
pub use error::DimseError;
pub use error_policy::{AssociationErrorPolicy, DefaultAssociationErrorPolicy, ErrorPolicyAction};
pub use listener::DimseListener;
pub use message::{CommandField, CommandObject, DimseCommand, Priority};
pub use registry::{
    DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider, ServiceClassRegistry,
};
pub use service::{
    CEchoRequest, CEchoResponse, CFindRequest, CFindResponse, CFindStatus, CGetRequest,
    CGetResponse, CGetStatus, CMoveRequest, CMoveResponse, CMoveStatus, CStoreRequest,
    CStoreResponse, CStoreStatus, MoveDestinationError, MoveDestinationStore, MoveServiceProvider,
    MoveStoreOutcome, MoveStoreRequest, QueryServiceProvider, RegistryMoveDestinationStore,
    RegistryMoveDestinationStoreConfig, RetrieveServiceProvider, StorageServiceProvider,
    VerificationServiceProvider, verification_provider,
};
