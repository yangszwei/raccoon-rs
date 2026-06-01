use std::fmt;
use std::time::Duration;

use raccoon_contract_dicom::{
    DicomInstanceIdentity, SeriesInstanceUid, StudyInstanceUid, TransferSyntaxUid,
};
use raccoon_contract_object_store::{ObjectKey, ObjectKeyError};
use raccoon_service_ingest::{IngestObjectId, IngestPayloadRepresentation};

/// Stable identifier for a sync worker.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SyncWorkerId(String);

impl SyncWorkerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SyncWorkerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Opaque claim token issued by a source repository.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SyncClaimToken(String);

impl SyncClaimToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SyncClaimToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One object atomically claimed by a sync worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedSyncObject {
    pub ingest_object_id: IngestObjectId,
    pub object_key: ObjectKey,
    pub content_length: u64,
    pub payload_representation: IngestPayloadRepresentation,
    pub transfer_syntax_uid: Option<String>,
    pub claim_token: SyncClaimToken,
}

/// Operational limits for polling sync workers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncServiceOptions {
    batch_size: usize,
    poll_interval: Duration,
    claim_ttl: Duration,
    max_metadata_bytes: Option<u64>,
}

impl Default for SyncServiceOptions {
    fn default() -> Self {
        Self {
            batch_size: 100,
            poll_interval: Duration::from_secs(1),
            claim_ttl: Duration::from_secs(30),
            max_metadata_bytes: None,
        }
    }
}

impl SyncServiceOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_claim_ttl(mut self, claim_ttl: Duration) -> Self {
        self.claim_ttl = claim_ttl;
        self
    }

    pub fn with_max_metadata_bytes(mut self, max_metadata_bytes: u64) -> Self {
        self.max_metadata_bytes = Some(max_metadata_bytes.max(1));
        self
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn claim_ttl(&self) -> Duration {
        self.claim_ttl
    }

    pub fn max_metadata_bytes(&self) -> Option<u64> {
        self.max_metadata_bytes
    }
}

/// Study row data projected from one parsed DICOM object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncStudyRecord {
    pub study_instance_uid: StudyInstanceUid,
    pub patient_id: Option<String>,
    pub patient_name: Option<String>,
    pub patient_birth_date: Option<String>,
    pub patient_sex: Option<String>,
    pub study_date: Option<String>,
    pub study_time: Option<String>,
    pub accession_number: Option<String>,
    pub study_id: Option<String>,
    pub study_description: Option<String>,
    pub referring_physician_name: Option<String>,
}

/// Series row data projected from one parsed DICOM object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncSeriesRecord {
    pub series_instance_uid: SeriesInstanceUid,
    pub study_instance_uid: StudyInstanceUid,
    pub modality: Option<String>,
    pub series_number: Option<i64>,
    pub series_date: Option<String>,
    pub series_time: Option<String>,
    pub series_description: Option<String>,
    pub body_part_examined: Option<String>,
}

/// Instance row data projected from one parsed DICOM object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncInstanceRecord {
    pub identity: DicomInstanceIdentity,
    pub instance_number: Option<i64>,
    pub acquisition_date_time: Option<String>,
    pub transfer_syntax_uid: Option<TransferSyntaxUid>,
    pub object_key: ObjectKey,
    pub object_size_bytes: u64,
    pub attributes_json: String,
}

/// Full read-model update for one synced object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncedReadModelObject {
    pub study: SyncStudyRecord,
    pub series: SyncSeriesRecord,
    pub instance: SyncInstanceRecord,
    pub synced_at_unix_ms: i64,
}

/// Quarantine reason category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineCategory {
    CannotUnderstand,
    Validation,
    Policy,
}

impl QuarantineCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CannotUnderstand => "cannot_understand",
            Self::Validation => "validation",
            Self::Policy => "policy",
        }
    }
}

/// Structured sync quarantine metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineRecord {
    pub ingest_object_id: IngestObjectId,
    pub claim_token: SyncClaimToken,
    pub category: QuarantineCategory,
    pub reason: String,
    pub original_object_key: ObjectKey,
    pub quarantine_object_key: ObjectKey,
    pub quarantined_at_unix_ms: i64,
}

/// Builds sync quarantine object keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncQuarantineKeyBuilder {
    prefix: String,
}

impl Default for SyncQuarantineKeyBuilder {
    fn default() -> Self {
        Self {
            prefix: "sync".to_string(),
        }
    }
}

impl SyncQuarantineKeyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn build(&self, ingest_object_id: &IngestObjectId) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::new(format!("{}/{}", self.prefix, ingest_object_id))
    }
}

/// Aggregate result from one polling batch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncBatchResult {
    pub claimed: usize,
    pub synced: usize,
    pub quarantined: usize,
    pub retryable_failures: usize,
}

impl SyncBatchResult {
    pub fn empty() -> Self {
        Self::default()
    }
}
