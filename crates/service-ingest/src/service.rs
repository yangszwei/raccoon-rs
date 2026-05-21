use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use raccoon_contract_object_store::{ObjectKey, ObjectStore, PutResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{Instrument, debug_span, instrument, warn};

use crate::acceptance::{DicomIntakeError, parse_dicom_intake_file};
use crate::staging::StagedIngestBody;
use crate::{
    AcceptAllStorageAcceptancePolicy, IngestBatchRepositoryStatus, IngestBatchResult,
    IngestChecksum, IngestError, IngestObjectId, IngestObjectIdentity, IngestObjectKeyBuilder,
    IngestObjectOutcome, IngestObjectState, IngestPayloadRepresentation, IngestRepository,
    IngestRequest, IngestResult, IngestSource, IngestUploadId, ParsedDicomObject,
    ReceivedIngestObject, StorageAcceptanceDecision, StorageAcceptanceInput,
    StorageAcceptancePolicy,
};

/// Operational limits for ingest staging and backpressure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestServiceOptions {
    filesystem_root: PathBuf,
    staging_dir: PathBuf,
    max_object_size: Option<u64>,
    max_concurrent_objects: usize,
}

impl Default for IngestServiceOptions {
    fn default() -> Self {
        Self::for_filesystem_root(std::env::temp_dir().join("raccoon"))
    }
}

impl IngestServiceOptions {
    /// Derives ingest's local layout from the filesystem storage root.
    pub fn for_filesystem_root(filesystem_root: impl Into<PathBuf>) -> Self {
        let filesystem_root = filesystem_root.into();
        Self {
            staging_dir: filesystem_root.join("ingest").join("staging"),
            filesystem_root,
            max_object_size: None,
            max_concurrent_objects: 1,
        }
    }

    /// Hard byte limit for one incoming object before it reaches object storage.
    pub fn with_max_object_size(mut self, max_object_size: u64) -> Self {
        self.max_object_size = Some(max_object_size);
        self
    }

    /// Backpressure limit shared by all ingest calls on this service instance.
    ///
    /// The worst-case staging disk use is roughly
    /// `max_object_size * max_concurrent_objects` when a size limit is set.
    pub fn with_max_concurrent_objects(mut self, max_concurrent_objects: usize) -> Self {
        self.max_concurrent_objects = max_concurrent_objects.max(1);
        self
    }

    pub fn filesystem_root(&self) -> &Path {
        &self.filesystem_root
    }

    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    pub fn max_object_size(&self) -> Option<u64> {
        self.max_object_size
    }

    pub fn max_concurrent_objects(&self) -> usize {
        self.max_concurrent_objects
    }
}

/// Protocol-neutral DICOM storage acceptance service behavior.
#[async_trait]
pub trait IngestService: Send + Sync {
    /// Convenience wrapper for single-object callers.
    ///
    /// [`Self::ingest_upload_objects`] is the canonical ingest path; this method
    /// preserves the same per-object and repository-status semantics.
    async fn ingest_upload_object(
        &self,
        request: IngestRequest,
    ) -> Result<IngestResult, IngestError>;

    /// Accepts multiple object items and records stored metadata in one repository operation.
    async fn ingest_upload_objects(&self, requests: Vec<IngestRequest>) -> IngestBatchResult;
}

/// In-memory protocol-neutral DICOM storage acceptance service.
pub struct InMemoryIngestService {
    object_store: Arc<dyn ObjectStore>,
    repository: Arc<dyn IngestRepository>,
    storage_policy: Arc<dyn StorageAcceptancePolicy>,
    key_builder: IngestObjectKeyBuilder,
    options: IngestServiceOptions,
    permits: Arc<Semaphore>,
}

impl InMemoryIngestService {
    /// Creates a storage acceptance service.
    pub fn new(object_store: Arc<dyn ObjectStore>, repository: Arc<dyn IngestRepository>) -> Self {
        Self::new_with_options(object_store, repository, IngestServiceOptions::default())
    }

    /// Creates a storage acceptance service with explicit operational limits.
    pub fn new_with_options(
        object_store: Arc<dyn ObjectStore>,
        repository: Arc<dyn IngestRepository>,
        options: IngestServiceOptions,
    ) -> Self {
        let max_concurrent_objects = options.max_concurrent_objects().max(1);
        Self {
            object_store,
            repository,
            storage_policy: Arc::new(AcceptAllStorageAcceptancePolicy),
            key_builder: IngestObjectKeyBuilder::new(),
            options: options.with_max_concurrent_objects(max_concurrent_objects),
            permits: Arc::new(Semaphore::new(max_concurrent_objects)),
        }
    }

    /// Overrides the concrete storage acceptance policy.
    pub fn with_storage_policy(
        mut self,
        storage_policy: impl StorageAcceptancePolicy + 'static,
    ) -> Self {
        self.storage_policy = Arc::new(storage_policy);
        self
    }

    /// Convenience wrapper for single-object callers.
    ///
    /// [`Self::ingest_upload_objects`] is the canonical ingest path; this method
    /// preserves the same per-object and repository-status semantics.
    #[instrument(
        skip(self, request),
        fields(
            ingest.object_id = tracing::field::Empty,
            ingest.upload_id = tracing::field::Empty,
            ingest.object_key = tracing::field::Empty,
            ingest.state = %IngestObjectState::PendingSync.as_str(),
            ingest.content_length = tracing::field::Empty,
            ingest.accepted_bytes = tracing::field::Empty,
            ingest.quarantined_bytes = tracing::field::Empty,
            ingest.payload_representation = tracing::field::Empty,
            dicom.sop_class_uid = tracing::field::Empty,
            dicom.study_instance_uid = tracing::field::Empty,
            dicom.series_instance_uid = tracing::field::Empty,
            dicom.sop_instance_uid = tracing::field::Empty,
            dicom.transfer_syntax_uid = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    pub async fn ingest_upload_object(
        &self,
        request: IngestRequest,
    ) -> Result<IngestResult, IngestError> {
        let result = self.ingest_upload_objects([request]).await;
        Ok(result.object_results.into_iter().next().unwrap_or_else(|| {
            IngestResult::failed(
                IngestObjectId::default(),
                result.upload_id,
                IngestObjectIdentity::default(),
                IngestPayloadRepresentation::Unknown,
                None,
                IngestObjectOutcome::RepositoryFailed {
                    reason: "single-object ingest produced no result".to_string(),
                },
            )
        }))
    }

    /// Accepts multiple object items and records stored metadata in one repository operation.
    #[instrument(
        skip(self, requests),
        fields(
            ingest.object_count = tracing::field::Empty,
            ingest.stored_object_count = tracing::field::Empty,
            ingest.rejected_object_count = tracing::field::Empty,
            ingest.failed_object_count = tracing::field::Empty,
            ingest.accepted_bytes = tracing::field::Empty,
            ingest.quarantined_bytes = tracing::field::Empty,
            ingest.repository_recorded = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    pub async fn ingest_upload_objects(
        &self,
        requests: impl IntoIterator<Item = IngestRequest>,
    ) -> IngestBatchResult {
        let span = tracing::Span::current();
        let requests = requests.into_iter().collect::<Vec<_>>();
        span.record("ingest.object_count", requests.len());

        let mut object_results = Vec::with_capacity(requests.len());
        let mut stored_records = Vec::new();

        for request in requests {
            match self.prepare_upload_object(request, true).await {
                Ok(prepared) => match prepared {
                    PreparedIngestObject::Stored(record) => {
                        stored_records.push(record.clone());
                        object_results.push(record.into());
                    }
                    PreparedIngestObject::Failed(result) => object_results.push(result),
                },
                Err(error) => object_results.push(failed_result_from_error(error)),
            }
        }

        let stored_object_count = object_results
            .iter()
            .filter(|result| result.outcome.is_stored())
            .count();
        let rejected_object_count = object_results
            .iter()
            .filter(|result| {
                matches!(
                    result.outcome,
                    IngestObjectOutcome::RejectedCannotUnderstand { .. }
                        | IngestObjectOutcome::RejectedUnsupportedSopClass { .. }
                        | IngestObjectOutcome::RejectedStudyMismatch { .. }
                        | IngestObjectOutcome::RejectedChecksumMismatch { .. }
                        | IngestObjectOutcome::RejectedTooLarge { .. }
                )
            })
            .count();
        let failed_object_count =
            object_results.len() - stored_object_count - rejected_object_count;
        let accepted_bytes = object_results
            .iter()
            .filter(|result| result.outcome.is_stored())
            .filter_map(|result| result.content_length)
            .sum::<u64>();
        let quarantined_bytes = object_results
            .iter()
            .filter(|result| result.state == IngestObjectState::Quarantined)
            .filter_map(|result| result.content_length)
            .sum::<u64>();

        span.record("ingest.stored_object_count", stored_object_count);
        span.record("ingest.rejected_object_count", rejected_object_count);
        span.record("ingest.failed_object_count", failed_object_count);
        span.record("ingest.accepted_bytes", accepted_bytes);
        span.record("ingest.quarantined_bytes", quarantined_bytes);

        let repository_status = if !stored_records.is_empty() {
            match self
                .repository
                .record_received_objects(&stored_records)
                .instrument(debug_span!(
                    "ingest_repository.record_received_objects",
                    ingest.object_count = stored_object_count,
                ))
                .await
            {
                Ok(_) => {
                    span.record("ingest.repository_recorded", true);
                    IngestBatchRepositoryStatus::Recorded
                }
                Err(source) => {
                    span.record("ingest.repository_recorded", false);
                    span.record("error.type", "repository");
                    IngestBatchRepositoryStatus::Failed {
                        reason: source.message().to_string(),
                    }
                }
            }
        } else {
            span.record("ingest.repository_recorded", true);
            IngestBatchRepositoryStatus::Recorded
        };

        IngestBatchResult::new(
            object_results
                .first()
                .map(|result| result.upload_id.clone())
                .unwrap_or_default(),
            object_results,
            repository_status,
        )
    }

    async fn prepare_upload_object(
        &self,
        request: IngestRequest,
        _record_internal_failures: bool,
    ) -> Result<PreparedIngestObject, IngestError> {
        let _permit = self.acquire_permit().await;
        let span = tracing::Span::current();
        let ingest_object_id = request.ingest_object_id.clone().unwrap_or_default();
        let upload_id = request.upload_id.clone();
        span.record(
            "ingest.object_id",
            tracing::field::display(&ingest_object_id),
        );
        span.record("ingest.upload_id", tracing::field::display(&upload_id));

        let checksum = request.checksum.clone();
        let source = request.source.clone();
        let identity_hints = request.identity_hints.clone();
        let payload_representation = request.payload_representation;
        let transfer_syntax_uid = request.transfer_syntax_uid.clone();
        let expected_study_instance_uid = request.expected_study_instance_uid.clone();

        let accepted_object_key = match self.key_builder.build_accepted(&ingest_object_id) {
            Ok(object_key) => object_key,
            Err(source) => {
                if _record_internal_failures {
                    let failed = IngestResult::failed(
                        ingest_object_id,
                        upload_id,
                        identity_hints,
                        payload_representation,
                        transfer_syntax_uid,
                        IngestObjectOutcome::RejectedCannotUnderstand {
                            reason: source.to_string(),
                        },
                    );
                    return Ok(PreparedIngestObject::Failed(failed));
                }
                return Err(IngestError::ObjectKey {
                    ingest_object_id,
                    source,
                });
            }
        };
        span.record(
            "ingest.object_key",
            tracing::field::display(&accepted_object_key),
        );

        let staged_body = match StagedIngestBody::write(
            ingest_object_id.clone(),
            accepted_object_key.clone(),
            request.body,
            self.options.staging_dir(),
            self.options.max_object_size(),
        )
        .await
        {
            Ok(staged_body) => staged_body,
            Err(error) => {
                if let IngestError::ContentTooLarge {
                    max_content_length, ..
                } = error
                {
                    return Ok(PreparedIngestObject::Failed(IngestResult::failed(
                        ingest_object_id,
                        upload_id,
                        identity_hints,
                        payload_representation,
                        transfer_syntax_uid,
                        IngestObjectOutcome::RejectedTooLarge { max_content_length },
                    )));
                }
                if _record_internal_failures {
                    return Ok(PreparedIngestObject::Failed(failed_result_from_error(
                        error,
                    )));
                }
                return Err(error);
            }
        };

        span.record(
            "ingest.content_length",
            tracing::field::display(staged_body.content_length),
        );

        if let Some(outcome) = checksum_rejection(checksum.as_ref(), &staged_body.sha256) {
            let record = self
                .store_quarantined_body(
                    staged_body,
                    PreparedRecord {
                        ingest_object_id,
                        upload_id,
                        checksum,
                        identity: identity_hints,
                        payload_representation,
                        transfer_syntax_uid,
                        source,
                        outcome,
                    },
                )
                .await?;
            return Ok(PreparedIngestObject::Stored(record));
        }

        let parsed = match parse_dicom_intake_file(
            staged_body.path(),
            payload_representation,
            transfer_syntax_uid,
            identity_hints.clone(),
        )
        .await
        {
            Ok(parsed) => parsed,
            Err(error) => {
                let (identity, payload_representation, transfer_syntax_uid, outcome) =
                    intake_error_outcome(error);
                record_identity_fields(&span, &identity);
                record_payload_fields(
                    &span,
                    payload_representation,
                    transfer_syntax_uid.as_deref(),
                );
                let record = self
                    .store_quarantined_body(
                        staged_body,
                        PreparedRecord {
                            ingest_object_id,
                            upload_id,
                            checksum,
                            identity,
                            payload_representation,
                            transfer_syntax_uid,
                            source,
                            outcome,
                        },
                    )
                    .await?;
                return Ok(PreparedIngestObject::Stored(record));
            }
        };

        record_identity_fields(&span, &parsed.identity);
        record_payload_fields(
            &span,
            parsed.payload_representation,
            parsed.transfer_syntax_uid.as_deref(),
        );

        if let Some(outcome) = required_identity_rejection(&parsed.identity)
            .or_else(|| study_mismatch_rejection(expected_study_instance_uid, &parsed.identity))
        {
            let ParsedDicomObject {
                identity,
                payload_representation,
                transfer_syntax_uid,
            } = parsed;
            let record = self
                .store_quarantined_body(
                    staged_body,
                    PreparedRecord {
                        ingest_object_id,
                        upload_id,
                        checksum,
                        identity,
                        payload_representation,
                        transfer_syntax_uid,
                        source,
                        outcome,
                    },
                )
                .await?;
            return Ok(PreparedIngestObject::Stored(record));
        }

        let policy_decision = self.storage_policy.evaluate(&StorageAcceptanceInput {
            identity: parsed.identity.clone(),
        });

        match policy_decision {
            StorageAcceptanceDecision::Accept => {}
            StorageAcceptanceDecision::RejectUnsupportedSopClass {
                sop_class_uid,
                reason,
            } => {
                let record = self
                    .store_quarantined_body(
                        staged_body,
                        PreparedRecord {
                            ingest_object_id,
                            upload_id,
                            checksum,
                            identity: parsed.identity,
                            payload_representation: parsed.payload_representation,
                            transfer_syntax_uid: parsed.transfer_syntax_uid,
                            source,
                            outcome: IngestObjectOutcome::RejectedUnsupportedSopClass {
                                sop_class_uid,
                                reason,
                            },
                        },
                    )
                    .await?;
                return Ok(PreparedIngestObject::Stored(record));
            }
        }

        let ParsedDicomObject {
            identity,
            payload_representation,
            transfer_syntax_uid,
        } = parsed;

        let put_result = store_staged_body(
            self.object_store.as_ref(),
            ingest_object_id.clone(),
            accepted_object_key,
            &staged_body,
        )
        .await?;
        span.record(
            "ingest.accepted_bytes",
            tracing::field::display(put_result.metadata.content_length),
        );
        let record = received_record(
            PreparedRecord {
                ingest_object_id,
                upload_id,
                checksum,
                identity,
                payload_representation,
                transfer_syntax_uid,
                source,
                outcome: IngestObjectOutcome::Stored,
            },
            put_result,
            IngestObjectState::PendingSync,
        );

        Ok(PreparedIngestObject::Stored(record))
    }

    async fn acquire_permit(&self) -> Option<OwnedSemaphorePermit> {
        match self.permits.clone().acquire_owned().await {
            Ok(permit) => Some(permit),
            Err(error) => {
                warn!(error = %error, "ingest concurrency limiter closed");
                None
            }
        }
    }

    async fn store_quarantined_body(
        &self,
        staged_body: StagedIngestBody,
        record: PreparedRecord,
    ) -> Result<ReceivedIngestObject, IngestError> {
        let object_key = self
            .key_builder
            .build_quarantine(&record.ingest_object_id)
            .map_err(|source| IngestError::ObjectKey {
                ingest_object_id: record.ingest_object_id.clone(),
                source,
            })?;
        tracing::Span::current().record("ingest.object_key", tracing::field::display(&object_key));
        let put_result = store_staged_body(
            self.object_store.as_ref(),
            record.ingest_object_id.clone(),
            object_key,
            &staged_body,
        )
        .await?;
        tracing::Span::current().record(
            "ingest.quarantined_bytes",
            tracing::field::display(put_result.metadata.content_length),
        );
        tracing::Span::current().record(
            "ingest.state",
            tracing::field::display(IngestObjectState::Quarantined.as_str()),
        );
        Ok(received_record(
            record,
            put_result,
            IngestObjectState::Quarantined,
        ))
    }
}

#[async_trait]
impl IngestService for InMemoryIngestService {
    async fn ingest_upload_object(
        &self,
        request: IngestRequest,
    ) -> Result<IngestResult, IngestError> {
        InMemoryIngestService::ingest_upload_object(self, request).await
    }

    async fn ingest_upload_objects(&self, requests: Vec<IngestRequest>) -> IngestBatchResult {
        InMemoryIngestService::ingest_upload_objects(self, requests).await
    }
}

enum PreparedIngestObject {
    Stored(ReceivedIngestObject),
    Failed(IngestResult),
}

fn failed_result_from_error(error: IngestError) -> IngestResult {
    match error {
        IngestError::ObjectKey {
            ingest_object_id, ..
        } => IngestResult::failed(
            ingest_object_id,
            IngestUploadId::default(),
            IngestObjectIdentity::default(),
            IngestPayloadRepresentation::Unknown,
            None,
            IngestObjectOutcome::RejectedCannotUnderstand {
                reason: "failed to build ingest object key".to_string(),
            },
        ),
        IngestError::ObjectStore {
            ingest_object_id, ..
        } => IngestResult::failed(
            ingest_object_id,
            IngestUploadId::default(),
            IngestObjectIdentity::default(),
            IngestPayloadRepresentation::Unknown,
            None,
            IngestObjectOutcome::ObjectStoreFailed {
                reason: "object store failed".to_string(),
            },
        ),
        IngestError::ContentTooLarge {
            ingest_object_id,
            max_content_length,
        } => IngestResult::failed(
            ingest_object_id,
            IngestUploadId::default(),
            IngestObjectIdentity::default(),
            IngestPayloadRepresentation::Unknown,
            None,
            IngestObjectOutcome::RejectedTooLarge { max_content_length },
        ),
    }
}

async fn store_staged_body(
    object_store: &dyn ObjectStore,
    ingest_object_id: IngestObjectId,
    object_key: ObjectKey,
    staged_body: &StagedIngestBody,
) -> Result<PutResult, IngestError> {
    let body = staged_body
        .body_stream(&object_key)
        .await
        .map_err(|source| IngestError::ObjectStore {
            ingest_object_id: ingest_object_id.clone(),
            object_key: object_key.clone(),
            source,
        })?;

    object_store
        .put(object_key.clone(), body)
        .instrument(debug_span!(
            "object_store.put",
            ingest.object_id = %ingest_object_id,
            ingest.object_key = %object_key,
        ))
        .await
        .map_err(|source| IngestError::ObjectStore {
            ingest_object_id,
            object_key,
            source,
        })
}

struct PreparedRecord {
    ingest_object_id: IngestObjectId,
    upload_id: IngestUploadId,
    checksum: Option<IngestChecksum>,
    identity: IngestObjectIdentity,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
    source: IngestSource,
    outcome: IngestObjectOutcome,
}

fn received_record(
    record: PreparedRecord,
    put_result: PutResult,
    state: IngestObjectState,
) -> ReceivedIngestObject {
    let now = SystemTime::now();
    ReceivedIngestObject {
        ingest_object_id: record.ingest_object_id,
        upload_id: record.upload_id,
        object_key: put_result.metadata.key,
        content_length: put_result.metadata.content_length,
        etag: put_result.metadata.etag,
        checksum: record.checksum,
        identity: record.identity,
        payload_representation: record.payload_representation,
        transfer_syntax_uid: record.transfer_syntax_uid,
        source: record.source,
        state,
        outcome: record.outcome,
        received_at: now,
        synced_at: None,
        quarantined_at: (state == IngestObjectState::Quarantined).then_some(now),
        delete_eligible_at: None,
    }
}

fn checksum_rejection(
    expected: Option<&IngestChecksum>,
    actual_sha256: &str,
) -> Option<IngestObjectOutcome> {
    let expected = expected?;
    if expected.matches_value(actual_sha256) {
        return None;
    }
    Some(IngestObjectOutcome::RejectedChecksumMismatch {
        expected: expected.to_string(),
        actual: format!("{}:{actual_sha256}", expected.algorithm.as_str()),
    })
}

fn intake_error_outcome(
    error: DicomIntakeError,
) -> (
    IngestObjectIdentity,
    IngestPayloadRepresentation,
    Option<String>,
    IngestObjectOutcome,
) {
    match error {
        DicomIntakeError::CannotUnderstand {
            reason,
            identity,
            payload_representation,
            transfer_syntax_uid,
        } => (
            *identity,
            payload_representation,
            transfer_syntax_uid,
            IngestObjectOutcome::RejectedCannotUnderstand { reason },
        ),
    }
}

fn required_identity_rejection(identity: &IngestObjectIdentity) -> Option<IngestObjectOutcome> {
    if identity.sop_class_uid.is_none() {
        return Some(IngestObjectOutcome::RejectedCannotUnderstand {
            reason: "missing SOP Class UID".to_string(),
        });
    }
    if identity.sop_instance_uid.is_none() {
        return Some(IngestObjectOutcome::RejectedCannotUnderstand {
            reason: "missing SOP Instance UID".to_string(),
        });
    }
    if identity.study_instance_uid.is_none() {
        return Some(IngestObjectOutcome::RejectedCannotUnderstand {
            reason: "missing Study Instance UID".to_string(),
        });
    }
    if identity.series_instance_uid.is_none() {
        return Some(IngestObjectOutcome::RejectedCannotUnderstand {
            reason: "missing Series Instance UID".to_string(),
        });
    }
    None
}

fn study_mismatch_rejection(
    expected_study_instance_uid: Option<String>,
    identity: &IngestObjectIdentity,
) -> Option<IngestObjectOutcome> {
    let expected_study_instance_uid = expected_study_instance_uid?;
    if identity.study_instance_uid.as_deref() != Some(expected_study_instance_uid.as_str()) {
        return Some(IngestObjectOutcome::RejectedStudyMismatch {
            expected_study_instance_uid,
            actual_study_instance_uid: identity.study_instance_uid.clone(),
        });
    }
    None
}

fn record_identity_fields(span: &tracing::Span, identity: &IngestObjectIdentity) {
    if let Some(sop_class_uid) = &identity.sop_class_uid {
        span.record(
            "dicom.sop_class_uid",
            tracing::field::display(sop_class_uid),
        );
    }
    if let Some(study_uid) = &identity.study_instance_uid {
        span.record(
            "dicom.study_instance_uid",
            tracing::field::display(study_uid),
        );
    }
    if let Some(series_uid) = &identity.series_instance_uid {
        span.record(
            "dicom.series_instance_uid",
            tracing::field::display(series_uid),
        );
    }
    if let Some(sop_uid) = &identity.sop_instance_uid {
        span.record("dicom.sop_instance_uid", tracing::field::display(sop_uid));
    }
}

fn record_payload_fields(
    span: &tracing::Span,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<&str>,
) {
    span.record(
        "ingest.payload_representation",
        tracing::field::display(payload_representation.as_str()),
    );
    if let Some(transfer_syntax_uid) = transfer_syntax_uid {
        span.record(
            "dicom.transfer_syntax_uid",
            tracing::field::display(transfer_syntax_uid),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures_util::TryStreamExt;
    use raccoon_contract_object_store::{
        ByteStream, Bytes, Object, ObjectKey, ObjectMetadata, ObjectStore, ObjectStoreError,
        PutResult,
    };
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::staging::hex_lower;
    use crate::{
        IngestObjectId, IngestRepositoryError, IngestSource, IngestUploadId, IngestUploadResult,
        SopClassStorageAcceptancePolicy,
    };

    #[derive(Default)]
    struct FakeObjectStore {
        puts: Mutex<Vec<ObjectKey>>,
        deletes: Mutex<Vec<ObjectKey>>,
        bodies: Mutex<HashMap<ObjectKey, Bytes>>,
        fail_next: Mutex<Option<ObjectStoreError>>,
    }

    #[async_trait]
    impl ObjectStore for FakeObjectStore {
        async fn put(
            &self,
            key: ObjectKey,
            body: ByteStream,
        ) -> raccoon_contract_object_store::Result<PutResult> {
            if let Some(error) = self.fail_next.lock().unwrap().take() {
                return Err(error);
            }
            let bytes = collect_body_bytes(body).await?;
            self.puts.lock().unwrap().push(key.clone());
            self.bodies
                .lock()
                .unwrap()
                .insert(key.clone(), bytes.clone());
            Ok(PutResult::new(
                ObjectMetadata::new(key, bytes.len() as u64).with_etag("test-etag"),
            ))
        }

        async fn get(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<Object> {
            let body = self
                .bodies
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(Object::new(
                ObjectMetadata::new(key.clone(), body.len() as u64),
                ByteStream::once(body),
            ))
        }

        async fn head(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<ObjectMetadata> {
            let body = self
                .bodies
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(ObjectMetadata::new(key.clone(), body.len() as u64))
        }

        async fn delete(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<()> {
            self.deletes.lock().unwrap().push(key.clone());
            self.bodies.lock().unwrap().remove(key);
            Ok(())
        }
    }

    async fn collect_body_bytes(body: ByteStream) -> raccoon_contract_object_store::Result<Bytes> {
        let mut stream = body.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    }

    #[derive(Default)]
    struct FakeRepository {
        records: Mutex<Vec<ReceivedIngestObject>>,
        calls: Mutex<usize>,
        failures: Mutex<VecDeque<IngestRepositoryError>>,
    }

    #[async_trait]
    impl IngestRepository for FakeRepository {
        async fn record_received_objects(
            &self,
            records: &[ReceivedIngestObject],
        ) -> Result<(), IngestRepositoryError> {
            *self.calls.lock().unwrap() += 1;
            if let Some(error) = self.failures.lock().unwrap().pop_front() {
                return Err(error);
            }
            self.records.lock().unwrap().extend(records.iter().cloned());
            Ok(())
        }
    }

    fn identity() -> IngestObjectIdentity {
        IngestObjectIdentity {
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.1.2".to_string()),
            study_instance_uid: Some("1.2.3".to_string()),
            series_instance_uid: Some("1.2.3.4".to_string()),
            sop_instance_uid: Some("1.2.3.4.5".to_string()),
        }
    }

    fn service_with_fakes() -> (
        InMemoryIngestService,
        Arc<FakeObjectStore>,
        Arc<FakeRepository>,
    ) {
        let object_store = Arc::new(FakeObjectStore::default());
        let repository = Arc::new(FakeRepository::default());
        (
            InMemoryIngestService::new(object_store.clone(), repository.clone()),
            object_store,
            repository,
        )
    }

    fn service_with_options(
        options: IngestServiceOptions,
    ) -> (
        InMemoryIngestService,
        Arc<FakeObjectStore>,
        Arc<FakeRepository>,
    ) {
        let object_store = Arc::new(FakeObjectStore::default());
        let repository = Arc::new(FakeRepository::default());
        (
            InMemoryIngestService::new_with_options(
                object_store.clone(),
                repository.clone(),
                options,
            ),
            object_store,
            repository,
        )
    }

    fn request(upload_id: IngestUploadId) -> IngestRequest {
        IngestRequest::new(upload_id, explicit_vr_dataset(&identity()))
            .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
            .with_transfer_syntax_uid("1.2.840.10008.1.2.1")
            .with_source(IngestSource {
                request_id: Some("request-1".to_string()),
                protocol: Some("dicomweb".to_string()),
                ..IngestSource::default()
            })
    }

    fn malformed_request(upload_id: IngestUploadId) -> IngestRequest {
        IngestRequest::new(upload_id, "payload")
            .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
            .with_transfer_syntax_uid("1.2.840.10008.1.2.1")
    }

    fn explicit_vr_dataset(identity: &IngestObjectIdentity) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_ui(
            &mut bytes,
            0x0008,
            0x0016,
            identity.sop_class_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0020,
            0x000D,
            identity.study_instance_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0020,
            0x000E,
            identity.series_instance_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0008,
            0x0018,
            identity.sop_instance_uid.as_deref().unwrap(),
        );
        bytes
    }

    fn sha256_checksum(bytes: &[u8]) -> IngestChecksum {
        IngestChecksum::sha256(hex_lower(&Sha256::digest(bytes)))
    }

    async fn staging_dir_is_empty(path: &std::path::Path) -> bool {
        let mut entries = tokio::fs::read_dir(path).await.expect("read staging dir");
        entries
            .next_entry()
            .await
            .expect("read staging entry")
            .is_none()
    }

    fn push_ui(bytes: &mut Vec<u8>, group: u16, element: u16, value: &str) {
        bytes.extend_from_slice(&group.to_le_bytes());
        bytes.extend_from_slice(&element.to_le_bytes());
        bytes.extend_from_slice(b"UI");

        let mut value = value.as_bytes().to_vec();
        if value.len() % 2 == 1 {
            value.push(0);
        }

        bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&value);
    }

    #[test]
    fn object_key_layout_uses_generated_id_without_file_suffix() {
        let object_id = IngestObjectId::new();
        let key = IngestObjectKeyBuilder::new()
            .build(&object_id)
            .expect("valid object key");

        assert_eq!(key.as_str(), format!("ingest/{object_id}"));
    }

    #[tokio::test]
    async fn successful_storage_uses_batch_repository_and_preserves_protocol_fields() {
        let upload_id = IngestUploadId::new();
        let parsed_identity = identity();
        let (service, object_store, repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(
                IngestRequest::new(upload_id.clone(), explicit_vr_dataset(&parsed_identity))
                    .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
                    .with_transfer_syntax_uid("1.2.840.10008.1.2.1"),
            )
            .await
            .expect("storage should succeed");

        assert_eq!(result.upload_id, upload_id);
        assert_eq!(result.identity, parsed_identity);
        assert_eq!(
            result.payload_representation,
            IngestPayloadRepresentation::DicomDataSet
        );
        assert_eq!(
            result.transfer_syntax_uid,
            Some("1.2.840.10008.1.2.1".to_string())
        );
        assert_eq!(result.outcome, IngestObjectOutcome::Stored);
        assert_eq!(
            result.content_length,
            Some(explicit_vr_dataset(&parsed_identity).len() as u64)
        );
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn custom_staging_dir_is_cleaned_after_success() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let options =
            IngestServiceOptions::for_filesystem_root(filesystem_root.path().to_path_buf());
        let staging_dir = options.staging_dir().to_path_buf();
        let (service, _object_store, _repository) = service_with_options(options);

        let result = service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("storage should succeed");

        assert_eq!(result.outcome, IngestObjectOutcome::Stored);
        assert!(staging_dir_is_empty(&staging_dir).await);
    }

    #[tokio::test]
    async fn staging_file_is_removed_when_size_limit_rejects_object() {
        let filesystem_root = tempfile::tempdir().expect("filesystem root");
        let options =
            IngestServiceOptions::for_filesystem_root(filesystem_root.path().to_path_buf())
                .with_max_object_size(4);
        let staging_dir = options.staging_dir().to_path_buf();
        let (service, object_store, repository) = service_with_options(options);

        let result = service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("size rejection should be a service result");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::RejectedTooLarge {
                max_content_length: 4
            }
        ));
        assert_eq!(object_store.puts.lock().unwrap().len(), 0);
        assert_eq!(*repository.calls.lock().unwrap(), 0);
        assert!(staging_dir_is_empty(&staging_dir).await);
    }

    #[tokio::test]
    async fn checksum_mismatch_writes_quarantine() {
        let bytes = explicit_vr_dataset(&identity());
        let (service, object_store, repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(
                IngestRequest::new(IngestUploadId::new(), bytes)
                    .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
                    .with_transfer_syntax_uid("1.2.840.10008.1.2.1")
                    .with_checksum(IngestChecksum::sha256("deadbeef")),
            )
            .await
            .expect("checksum mismatch should be retained");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::RejectedChecksumMismatch { .. }
        ));
        assert_eq!(result.state, IngestObjectState::Quarantined);
        assert!(
            result
                .object_key
                .unwrap()
                .as_str()
                .starts_with("ingest/quarantine/")
        );
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checksum_match_allows_normal_storage() {
        let bytes = explicit_vr_dataset(&identity());
        let checksum = sha256_checksum(&bytes);
        let (service, _object_store, _repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(
                IngestRequest::new(IngestUploadId::new(), bytes)
                    .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
                    .with_transfer_syntax_uid("1.2.840.10008.1.2.1")
                    .with_checksum(checksum),
            )
            .await
            .expect("checksum match should store");

        assert_eq!(result.outcome, IngestObjectOutcome::Stored);
        assert_eq!(result.state, IngestObjectState::PendingSync);
    }

    #[tokio::test]
    async fn cannot_understand_rejection_writes_quarantine_bytes_and_metadata() {
        let (service, object_store, repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(malformed_request(IngestUploadId::new()))
            .await
            .expect("semantic rejection is a service result");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::RejectedCannotUnderstand { .. }
        ));
        let object_key = result.object_key.expect("quarantined object key");
        assert!(object_key.as_str().starts_with("ingest/quarantine/"));
        assert_eq!(result.state, IngestObjectState::Quarantined);
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 0);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        let records = repository.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, IngestObjectState::Quarantined);
        assert!(records[0].quarantined_at.is_some());
    }

    #[tokio::test]
    async fn unsupported_sop_class_rejection_writes_quarantine() {
        let unsupported_identity = IngestObjectIdentity {
            sop_class_uid: Some("1.2.3.unsupported".to_string()),
            ..identity()
        };
        let (service, object_store, repository) = service_with_fakes();
        let service =
            service.with_storage_policy(SopClassStorageAcceptancePolicy::supported_sop_classes([
                identity().sop_class_uid.unwrap(),
            ]));

        let result = service
            .ingest_upload_object(
                IngestRequest::new(
                    IngestUploadId::new(),
                    explicit_vr_dataset(&unsupported_identity),
                )
                .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
                .with_transfer_syntax_uid("1.2.840.10008.1.2.1"),
            )
            .await
            .expect("policy rejection is a service result");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::RejectedUnsupportedSopClass { .. }
        ));
        assert!(
            result
                .object_key
                .unwrap()
                .as_str()
                .starts_with("ingest/quarantine/")
        );
        assert_eq!(result.state, IngestObjectState::Quarantined);
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 0);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn study_mismatch_rejection_writes_quarantine() {
        let (service, object_store, repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(
                request(IngestUploadId::new()).with_expected_study_instance_uid("9.9.9"),
            )
            .await
            .expect("study mismatch is a service result");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::RejectedStudyMismatch { .. }
        ));
        assert!(
            result
                .object_key
                .unwrap()
                .as_str()
                .starts_with("ingest/quarantine/")
        );
        assert_eq!(result.state, IngestObjectState::Quarantined);
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 0);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn single_object_wrapper_uses_batch_repository_status() {
        let (service, object_store, repository) = service_with_fakes();
        repository
            .failures
            .lock()
            .unwrap()
            .push_back(IngestRepositoryError::new("db down"));

        let result = service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("single-object wrapper returns per-object result");

        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(
            result.content_length,
            Some(explicit_vr_dataset(&identity()).len() as u64)
        );
        assert_eq!(result.outcome, IngestObjectOutcome::Stored);
    }

    #[tokio::test]
    async fn object_store_failure_maps_to_protocol_neutral_outcome() {
        let (service, object_store, repository) = service_with_fakes();
        *object_store.fail_next.lock().unwrap() = Some(ObjectStoreError::backend("write failed"));

        let result = service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("single-object wrapper returns per-object result");

        assert!(matches!(
            result.outcome,
            IngestObjectOutcome::ObjectStoreFailed { .. }
        ));
        assert_eq!(*repository.calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn multi_object_upload_records_stored_objects_in_one_repository_call() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();

        let result = service
            .ingest_upload_objects([
                request(upload_id.clone()),
                malformed_request(upload_id.clone()),
                request(upload_id.clone()),
            ])
            .await;

        assert_eq!(result.object_results.len(), 3);
        assert!(result.object_results[0].outcome.is_stored());
        assert!(matches!(
            result.object_results[1].outcome,
            IngestObjectOutcome::RejectedCannotUnderstand { .. }
        ));
        assert!(result.object_results[2].outcome.is_stored());
        assert_eq!(object_store.puts.lock().unwrap().len(), 3);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 0);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 3);
        assert!(result.repository_status.is_recorded());
        assert_eq!(result.upload_result().total_object_count, 3);
    }

    #[tokio::test]
    async fn chunked_dataset_storage_replays_body_for_storage() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();
        let parsed_identity = identity();
        let bytes = explicit_vr_dataset(&parsed_identity);
        let request = IngestRequest::new(
            upload_id,
            ByteStream::from_chunks([
                Bytes::from(bytes[..12].to_vec()),
                Bytes::from(bytes[12..].to_vec()),
            ]),
        )
        .with_payload_representation(IngestPayloadRepresentation::DicomDataSet)
        .with_transfer_syntax_uid("1.2.840.10008.1.2.1");

        let result = service
            .ingest_upload_object(request)
            .await
            .expect("chunked storage should succeed");

        assert!(result.outcome.is_stored());
        assert_eq!(result.identity, parsed_identity);
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn multi_object_repository_failure_keeps_all_stored_context() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();
        repository
            .failures
            .lock()
            .unwrap()
            .push_back(IngestRepositoryError::new("db down"));

        let result = service
            .ingest_upload_objects([request(upload_id.clone()), request(upload_id)])
            .await;

        assert_eq!(object_store.puts.lock().unwrap().len(), 2);
        assert!(!result.repository_status.is_recorded());
        assert!(matches!(
            result.repository_status,
            IngestBatchRepositoryStatus::Failed { .. }
        ));
        assert_eq!(result.object_results.len(), 2);
        assert!(
            result
                .object_results
                .iter()
                .all(|record| record.outcome == IngestObjectOutcome::Stored)
        );
    }

    #[test]
    fn large_upload_aggregation_supports_mixed_storage_outcomes() {
        let upload_id = IngestUploadId::new();
        let outcomes = vec![
            IngestObjectOutcome::Stored,
            IngestObjectOutcome::RejectedCannotUnderstand {
                reason: "bad dataset".to_string(),
            },
            IngestObjectOutcome::ObjectStoreFailed {
                reason: "store unavailable".to_string(),
            },
        ];

        let result = IngestUploadResult::from_outcomes(upload_id.clone(), outcomes);

        assert_eq!(result.upload_id, upload_id);
        assert_eq!(result.total_object_count, 3);
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.failed_count, 2);
    }

    #[tokio::test]
    async fn dataset_storage_does_not_add_file_suffix() {
        let (service, _object_store, _repository) = service_with_fakes();

        let result = service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("storage should succeed");

        assert_eq!(
            result.payload_representation,
            IngestPayloadRepresentation::DicomDataSet
        );
        assert_eq!(result.outcome, IngestObjectOutcome::Stored);
        assert!(!result.object_key.unwrap().as_str().ends_with(".dcm"));
    }

    #[tokio::test]
    async fn instrumentation_does_not_require_payload_debug_output() {
        let (service, _object_store, _repository) = service_with_fakes();

        service
            .ingest_upload_object(request(IngestUploadId::new()))
            .await
            .expect("instrumented ingest should not inspect payload debug");
    }
}
