//! Storage acceptance boundary for protocol-neutral DICOM payload intake.
//!
//! This crate owns the synchronous write-side storage decision: parse enough
//! DICOM metadata for acceptance, evaluate internal policy, store canonical
//! object bytes, and record protocol-mappable outcomes. Full
//! IOD validation, read/query projection, retrieval, and concrete parser or
//! database adapters stay outside this core boundary.

mod acceptance;
mod error;
mod identity;
mod key;
mod model;
mod repository;
mod service;
mod staging;

pub use acceptance::{
    AcceptAllStorageAcceptancePolicy, SopClassStorageAcceptancePolicy, StorageAcceptancePolicy,
};
pub(crate) use acceptance::{StorageAcceptanceDecision, StorageAcceptanceInput};
pub use error::{IngestError, IngestRepositoryError};
pub use identity::{IngestObjectId, IngestUploadId};
pub(crate) use key::IngestObjectKeyBuilder;
pub(crate) use model::ParsedDicomObject;
pub use model::{
    IngestBatchRepositoryStatus, IngestBatchResult, IngestChecksum, IngestChecksumAlgorithm,
    IngestObjectIdentity, IngestObjectOutcome, IngestObjectState, IngestPayloadRepresentation,
    IngestRequest, IngestResult, IngestSource, IngestUploadResult, ReceivedIngestObject,
};
pub use repository::IngestRepository;
pub use service::{InMemoryIngestService, IngestService, IngestServiceOptions};
