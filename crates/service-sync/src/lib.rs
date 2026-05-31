//! Polling CQRS sync service for accepted DICOM ingest objects.
//!
//! This crate owns the service-level sync workflow: workers claim accepted
//! ingest objects, parse DICOM metadata through a bounded seekable reader over
//! object-store bytes, write read-model rows, and quarantine terminal data
//! failures.
//!
//! Concrete databases implement the repository ports in this crate. In
//! particular, multi-worker correctness is enforced by adapter-level atomic
//! claims and claim tokens, not in-memory service state.

mod error;
#[cfg(feature = "grpc")]
mod grpc;
mod model;
mod parser;
mod repository;
mod service;

pub use error::{
    QuarantineError, SyncError, SyncParseError, SyncRepositoryError, SyncTerminalObjectError,
};
#[cfg(feature = "grpc")]
pub use grpc::{
    DicomSyncService, DicomSyncServiceClient, DicomSyncServiceServer, GrpcSyncServiceClient,
    SyncGrpcService,
};
pub use model::{
    ClaimedSyncObject, QuarantineCategory, QuarantineRecord, SyncBatchResult, SyncClaimToken,
    SyncInstanceRecord, SyncQuarantineKeyBuilder, SyncSeriesRecord, SyncServiceOptions,
    SyncStudyRecord, SyncWorkerId, SyncedReadModelObject,
};
pub use parser::{DicomSyncParser, ParsedSyncObject};
pub use repository::{SyncQuarantineRepository, SyncReadModelWriter, SyncSourceRepository};
pub use service::{StandardSyncService, SyncService};
