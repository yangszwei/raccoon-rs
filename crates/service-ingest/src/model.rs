use std::fmt;
use std::time::SystemTime;

use raccoon_contract_object_store::{ByteStream, ObjectKey};

use crate::{IngestObjectId, IngestUploadId};

/// Optional, unverified DICOM identity hints supplied by a protocol adapter.
///
/// These values are not trusted facts. Missing or malformed DICOM payloads can
/// still be staged and recorded by ingest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngestObjectIdentity {
    pub sop_class_uid: Option<String>,
    pub study_instance_uid: Option<String>,
    pub series_instance_uid: Option<String>,
    pub sop_instance_uid: Option<String>,
}

/// Incoming object representation as observed by the protocol adapter/parser.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IngestPayloadRepresentation {
    DicomFile,
    DicomDataSet,
    DicomWebMetadataAndBulkData,
    #[default]
    Unknown,
}

impl IngestPayloadRepresentation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DicomFile => "dicom_file",
            Self::DicomDataSet => "dicom_dataset",
            Self::DicomWebMetadataAndBulkData => "dicomweb_metadata_and_bulk_data",
            Self::Unknown => "unknown",
        }
    }
}

/// Opaque source metadata that helps adapters correlate ingest records.
///
/// Keep this deliberately narrow to avoid logging or persisting arbitrary PHI.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngestSource {
    pub request_id: Option<String>,
    pub correlation_id: Option<String>,
    pub source_ae: Option<String>,
    pub source_application: Option<String>,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
}

/// Supported checksum algorithms for byte-level intake verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestChecksumAlgorithm {
    Sha256,
}

impl IngestChecksumAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
        }
    }
}

/// Expected object checksum supplied by an adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestChecksum {
    pub algorithm: IngestChecksumAlgorithm,
    pub value: String,
}

impl IngestChecksum {
    pub fn sha256(value: impl Into<String>) -> Self {
        Self {
            algorithm: IngestChecksumAlgorithm::Sha256,
            value: value.into(),
        }
    }

    pub(crate) fn matches_value(&self, actual: &str) -> bool {
        self.value.eq_ignore_ascii_case(actual)
    }
}

impl fmt::Display for IngestChecksum {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.algorithm.as_str(), self.value)
    }
}

/// Protocol-neutral request to ingest one object payload.
#[derive(Debug)]
pub struct IngestRequest {
    pub body: ByteStream,
    pub upload_id: IngestUploadId,
    pub ingest_object_id: Option<IngestObjectId>,
    pub identity_hints: IngestObjectIdentity,
    pub source: IngestSource,
    pub payload_representation: IngestPayloadRepresentation,
    pub transfer_syntax_uid: Option<String>,
    pub expected_study_instance_uid: Option<String>,
    pub checksum: Option<IngestChecksum>,
}

impl IngestRequest {
    /// Creates a request for one object item in an upload/session.
    pub fn new(upload_id: IngestUploadId, body: impl Into<ByteStream>) -> Self {
        Self {
            body: body.into(),
            upload_id,
            ingest_object_id: None,
            identity_hints: IngestObjectIdentity::default(),
            source: IngestSource::default(),
            payload_representation: IngestPayloadRepresentation::Unknown,
            transfer_syntax_uid: None,
            expected_study_instance_uid: None,
            checksum: None,
        }
    }

    /// Carries unverified identity hints with the request.
    pub fn with_identity_hints(mut self, identity: IngestObjectIdentity) -> Self {
        self.identity_hints = identity;
        self
    }

    /// Carries source/correlation metadata with the request.
    pub fn with_source(mut self, source: IngestSource) -> Self {
        self.source = source;
        self
    }

    /// Carries the payload representation observed by the adapter.
    pub fn with_payload_representation(
        mut self,
        representation: IngestPayloadRepresentation,
    ) -> Self {
        self.payload_representation = representation;
        self
    }

    /// Carries the transfer syntax for raw DICOM data set payloads.
    pub fn with_transfer_syntax_uid(mut self, transfer_syntax_uid: impl Into<String>) -> Self {
        self.transfer_syntax_uid = Some(transfer_syntax_uid.into());
        self
    }

    /// Carries an expected Study Instance UID from request routing context.
    pub fn with_expected_study_instance_uid(
        mut self,
        study_instance_uid: impl Into<String>,
    ) -> Self {
        self.expected_study_instance_uid = Some(study_instance_uid.into());
        self
    }

    /// Requires ingest to verify the streamed bytes while staging.
    pub fn with_checksum(mut self, checksum: IngestChecksum) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

/// Ingest object state owned by this service boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestObjectState {
    Received,
    PendingSync,
    Quarantined,
}

impl IngestObjectState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::PendingSync => "pending_sync",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Per-object outcome for protocol adapters and upload aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestObjectOutcome {
    Stored,
    RejectedCannotUnderstand {
        reason: String,
    },
    RejectedUnsupportedSopClass {
        sop_class_uid: Option<String>,
        reason: String,
    },
    RejectedStudyMismatch {
        expected_study_instance_uid: String,
        actual_study_instance_uid: Option<String>,
    },
    RejectedChecksumMismatch {
        expected: String,
        actual: String,
    },
    RejectedTooLarge {
        max_content_length: u64,
    },
    ObjectStoreFailed {
        reason: String,
    },
    RepositoryFailed {
        reason: String,
    },
}

impl IngestObjectOutcome {
    pub fn is_stored(&self) -> bool {
        matches!(self, Self::Stored)
    }

    pub fn is_accepted(&self) -> bool {
        self.is_stored()
    }
}

/// Storage-critical metadata parsed from an incoming DICOM object.
#[derive(Debug)]
pub(crate) struct ParsedDicomObject {
    pub(crate) identity: IngestObjectIdentity,
    pub(crate) payload_representation: IngestPayloadRepresentation,
    pub(crate) transfer_syntax_uid: Option<String>,
}

/// Metadata prepared by the service for repository persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedIngestObject {
    pub ingest_object_id: IngestObjectId,
    pub upload_id: IngestUploadId,
    pub object_key: ObjectKey,
    pub content_length: u64,
    pub etag: Option<String>,
    pub checksum: Option<IngestChecksum>,
    pub identity: IngestObjectIdentity,
    pub payload_representation: IngestPayloadRepresentation,
    pub transfer_syntax_uid: Option<String>,
    pub source: IngestSource,
    pub state: IngestObjectState,
    pub outcome: IngestObjectOutcome,
    pub received_at: SystemTime,
    pub synced_at: Option<SystemTime>,
    pub quarantined_at: Option<SystemTime>,
    pub delete_eligible_at: Option<SystemTime>,
}

/// Per-object ingest result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestResult {
    pub ingest_object_id: IngestObjectId,
    pub upload_id: IngestUploadId,
    pub object_key: Option<ObjectKey>,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub checksum: Option<IngestChecksum>,
    pub identity: IngestObjectIdentity,
    pub payload_representation: IngestPayloadRepresentation,
    pub transfer_syntax_uid: Option<String>,
    pub state: IngestObjectState,
    pub outcome: IngestObjectOutcome,
}

impl IngestResult {
    pub fn rejected(
        ingest_object_id: IngestObjectId,
        upload_id: IngestUploadId,
        identity: IngestObjectIdentity,
        payload_representation: IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
        outcome: IngestObjectOutcome,
    ) -> Self {
        Self {
            ingest_object_id,
            upload_id,
            object_key: None,
            content_length: None,
            etag: None,
            checksum: None,
            identity,
            payload_representation,
            transfer_syntax_uid,
            state: IngestObjectState::Received,
            outcome,
        }
    }

    pub fn failed(
        ingest_object_id: IngestObjectId,
        upload_id: IngestUploadId,
        identity: IngestObjectIdentity,
        payload_representation: IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
        outcome: IngestObjectOutcome,
    ) -> Self {
        Self {
            ingest_object_id,
            upload_id,
            object_key: None,
            content_length: None,
            etag: None,
            checksum: None,
            identity,
            payload_representation,
            transfer_syntax_uid,
            state: IngestObjectState::Received,
            outcome,
        }
    }
}

impl From<ReceivedIngestObject> for IngestResult {
    fn from(record: ReceivedIngestObject) -> Self {
        Self {
            ingest_object_id: record.ingest_object_id,
            upload_id: record.upload_id,
            object_key: Some(record.object_key),
            content_length: Some(record.content_length),
            etag: record.etag,
            checksum: record.checksum,
            identity: record.identity,
            payload_representation: record.payload_representation,
            transfer_syntax_uid: record.transfer_syntax_uid,
            state: record.state,
            outcome: record.outcome,
        }
    }
}

/// Repository status for one batch ingest operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestBatchRepositoryStatus {
    Recorded,
    Failed { reason: String },
}

impl IngestBatchRepositoryStatus {
    pub fn is_recorded(&self) -> bool {
        matches!(self, Self::Recorded)
    }
}

/// Structured result for ingesting multiple objects in one upload/session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestBatchResult {
    pub upload_id: IngestUploadId,
    pub object_results: Vec<IngestResult>,
    pub repository_status: IngestBatchRepositoryStatus,
}

impl IngestBatchResult {
    pub fn new(
        upload_id: IngestUploadId,
        object_results: Vec<IngestResult>,
        repository_status: IngestBatchRepositoryStatus,
    ) -> Self {
        Self {
            upload_id,
            object_results,
            repository_status,
        }
    }

    pub fn upload_result(&self) -> IngestUploadResult {
        IngestUploadResult::from_outcomes(
            self.upload_id.clone(),
            self.object_results
                .iter()
                .map(|result| result.outcome.clone())
                .collect(),
        )
    }
}

impl From<IngestBatchResult> for IngestUploadResult {
    fn from(result: IngestBatchResult) -> Self {
        result.upload_result()
    }
}

/// Aggregate result for one logical upload/session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestUploadResult {
    pub upload_id: IngestUploadId,
    pub total_object_count: usize,
    pub accepted_count: usize,
    pub failed_count: usize,
    pub object_outcomes: Vec<IngestObjectOutcome>,
}

impl IngestUploadResult {
    /// Builds an aggregate upload result from per-object outcomes.
    pub fn from_outcomes(
        upload_id: IngestUploadId,
        object_outcomes: Vec<IngestObjectOutcome>,
    ) -> Self {
        let accepted_count = object_outcomes
            .iter()
            .filter(|outcome| outcome.is_accepted())
            .count();
        let total_object_count = object_outcomes.len();
        Self {
            upload_id,
            total_object_count,
            accepted_count,
            failed_count: total_object_count - accepted_count,
            object_outcomes,
        }
    }
}
