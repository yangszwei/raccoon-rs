use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use raccoon_contract_object_store::{ByteStream, ObjectKey, ObjectStore};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, warn};

use crate::error::{QuarantineError, SyncError, SyncParseError, SyncTerminalObjectError};
use crate::model::{
    ClaimedSyncObject, QuarantineCategory, QuarantineRecord, SyncBatchResult,
    SyncQuarantineKeyBuilder, SyncServiceOptions, SyncWorkerId, SyncedReadModelObject,
};
use crate::parser::{DicomSyncParser, ParsedSyncObject};
use crate::repository::{SyncQuarantineRepository, SyncReadModelWriter, SyncSourceRepository};

/// Polling sync service behavior.
#[async_trait]
pub trait SyncService: Send + Sync {
    async fn sync_once(&self, worker_id: SyncWorkerId) -> Result<SyncBatchResult, SyncError>;

    async fn run_until_shutdown(
        &self,
        worker_id: SyncWorkerId,
        shutdown: CancellationToken,
    ) -> Result<(), SyncError>;
}

/// Standard polling implementation of [`SyncService`].
pub struct StandardSyncService {
    source_repository: Arc<dyn SyncSourceRepository>,
    read_model_writer: Arc<dyn SyncReadModelWriter>,
    quarantine_repository: Arc<dyn SyncQuarantineRepository>,
    object_store: Arc<dyn ObjectStore>,
    parser: Arc<dyn SyncObjectParser>,
    key_builder: SyncQuarantineKeyBuilder,
    options: SyncServiceOptions,
}

impl StandardSyncService {
    pub fn new(
        source_repository: Arc<dyn SyncSourceRepository>,
        read_model_writer: Arc<dyn SyncReadModelWriter>,
        quarantine_repository: Arc<dyn SyncQuarantineRepository>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::new_with_options(
            source_repository,
            read_model_writer,
            quarantine_repository,
            object_store,
            SyncServiceOptions::default(),
        )
    }

    pub fn new_with_options(
        source_repository: Arc<dyn SyncSourceRepository>,
        read_model_writer: Arc<dyn SyncReadModelWriter>,
        quarantine_repository: Arc<dyn SyncQuarantineRepository>,
        object_store: Arc<dyn ObjectStore>,
        options: SyncServiceOptions,
    ) -> Self {
        Self {
            source_repository,
            read_model_writer,
            quarantine_repository,
            object_store,
            parser: Arc::new(DicomSyncParser::new()),
            key_builder: SyncQuarantineKeyBuilder::new(),
            options,
        }
    }

    pub fn with_quarantine_key_builder(mut self, key_builder: SyncQuarantineKeyBuilder) -> Self {
        self.key_builder = key_builder;
        self
    }

    #[cfg(test)]
    fn with_parser(mut self, parser: Arc<dyn SyncObjectParser>) -> Self {
        self.parser = parser;
        self
    }
}

#[async_trait]
trait SyncObjectParser: Send + Sync {
    async fn parse(
        &self,
        body: ByteStream,
        object_key: ObjectKey,
        object_size_bytes: u64,
        payload_representation: raccoon_service_ingest::IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
        max_metadata_bytes: Option<u64>,
    ) -> Result<ParsedSyncObject, SyncParseError>;
}

#[async_trait]
impl SyncObjectParser for DicomSyncParser {
    async fn parse(
        &self,
        body: ByteStream,
        object_key: ObjectKey,
        object_size_bytes: u64,
        payload_representation: raccoon_service_ingest::IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
        max_metadata_bytes: Option<u64>,
    ) -> Result<ParsedSyncObject, SyncParseError> {
        DicomSyncParser::parse(
            self,
            body,
            object_key,
            object_size_bytes,
            payload_representation,
            transfer_syntax_uid,
            max_metadata_bytes,
        )
        .await
    }
}

#[async_trait]
impl SyncService for StandardSyncService {
    #[instrument(skip(self), fields(sync.worker_id = %worker_id))]
    async fn sync_once(&self, worker_id: SyncWorkerId) -> Result<SyncBatchResult, SyncError> {
        let claims = self
            .source_repository
            .claim_pending_objects(
                &worker_id,
                self.options.batch_size(),
                self.options.claim_ttl(),
            )
            .await
            .map_err(SyncError::SourceRepository)?;

        let mut result = SyncBatchResult {
            claimed: claims.len(),
            ..SyncBatchResult::empty()
        };

        for claim in claims {
            match self.process_claim(claim).await {
                ClaimOutcome::Synced => result.synced += 1,
                ClaimOutcome::Quarantined => result.quarantined += 1,
                ClaimOutcome::Retryable => result.retryable_failures += 1,
            }
        }

        Ok(result)
    }

    async fn run_until_shutdown(
        &self,
        worker_id: SyncWorkerId,
        shutdown: CancellationToken,
    ) -> Result<(), SyncError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = self.sync_once(worker_id.clone()) => {
                    result?;
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.options.poll_interval()) => {}
            }
        }
    }
}

enum ClaimOutcome {
    Synced,
    Quarantined,
    Retryable,
}

impl StandardSyncService {
    async fn process_claim(&self, claim: ClaimedSyncObject) -> ClaimOutcome {
        let object = match self.object_store.get(&claim.object_key).await {
            Ok(object) => object,
            Err(error) => {
                warn!(error = %error, "object store read failed; claim will be retried");
                self.release_retryable(&claim).await;
                return ClaimOutcome::Retryable;
            }
        };

        let parsed = match self
            .parser
            .parse(
                object.body,
                claim.object_key.clone(),
                object.metadata.content_length,
                claim.payload_representation,
                claim.transfer_syntax_uid.clone(),
                self.options.max_metadata_bytes(),
            )
            .await
        {
            Ok(parsed) => parsed,
            Err(SyncParseError::ParserTask(ref msg)) => {
                warn!(error = %msg, "DICOM parser task failed; claim will be retried");
                self.release_retryable(&claim).await;
                return ClaimOutcome::Retryable;
            }
            Err(SyncParseError::MetadataTooLarge { max_metadata_bytes }) => {
                let error = SyncTerminalObjectError::Policy {
                    reason: format!(
                        "DICOM metadata exceeded configured maximum of {max_metadata_bytes} bytes; \
                         object_content_length={}",
                        claim.content_length
                    ),
                };
                return match self.quarantine_terminal_failure(&claim, error).await {
                    Ok(()) => ClaimOutcome::Quarantined,
                    Err(error) => {
                        warn!(error = %error, "sync quarantine failed; claim will be retried");
                        self.release_retryable(&claim).await;
                        ClaimOutcome::Retryable
                    }
                };
            }
            Err(error) => {
                return match self.quarantine_terminal_failure(&claim, error.into()).await {
                    Ok(()) => ClaimOutcome::Quarantined,
                    Err(error) => {
                        warn!(error = %error, "sync quarantine failed; claim will be retried");
                        self.release_retryable(&claim).await;
                        ClaimOutcome::Retryable
                    }
                };
            }
        };

        let read_model = read_model_object(parsed, now_unix_ms());

        if let Err(error) = self
            .read_model_writer
            .upsert_synced_object(&read_model)
            .await
        {
            warn!(error = %error, "read model write failed; claim will be retried");
            self.release_retryable(&claim).await;
            return ClaimOutcome::Retryable;
        }

        if let Err(error) = self.source_repository.mark_synced(&claim.claim_token).await {
            warn!(error = %error, "failed to mark synced; claim will be retried");
            self.release_retryable(&claim).await;
            return ClaimOutcome::Retryable;
        }

        ClaimOutcome::Synced
    }

    async fn quarantine_terminal_failure(
        &self,
        claim: &ClaimedSyncObject,
        error: SyncTerminalObjectError,
    ) -> Result<(), QuarantineError> {
        let quarantine_key = self.key_builder.build(&claim.ingest_object_id)?;
        move_object(
            self.object_store.as_ref(),
            &claim.object_key,
            quarantine_key.clone(),
        )
        .await?;

        let record = QuarantineRecord {
            ingest_object_id: claim.ingest_object_id.clone(),
            claim_token: claim.claim_token.clone(),
            category: quarantine_category(&error),
            reason: error.reason(),
            original_object_key: claim.object_key.clone(),
            quarantine_object_key: quarantine_key,
            quarantined_at_unix_ms: now_unix_ms(),
        };

        self.quarantine_repository.mark_quarantined(&record).await?;

        if let Err(source) = self.object_store.delete(&claim.object_key).await {
            warn!(
                error = %source,
                object_key = %claim.object_key,
                "failed to delete original object after quarantine"
            );
        }

        Ok(())
    }

    async fn release_retryable(&self, claim: &ClaimedSyncObject) {
        if let Err(error) = self
            .source_repository
            .release_claim(&claim.claim_token)
            .await
        {
            warn!(error = %error, claim_token = %claim.claim_token, "failed to release sync claim");
        }
    }
}

async fn move_object(
    object_store: &dyn ObjectStore,
    original_key: &ObjectKey,
    quarantine_key: ObjectKey,
) -> Result<(), QuarantineError> {
    let object =
        object_store
            .get(original_key)
            .await
            .map_err(|source| QuarantineError::ObjectStore {
                object_key: original_key.clone(),
                source,
            })?;

    object_store
        .put(quarantine_key.clone(), object.body)
        .await
        .map_err(|source| QuarantineError::ObjectStore {
            object_key: quarantine_key,
            source,
        })?;

    Ok(())
}

fn read_model_object(parsed: ParsedSyncObject, synced_at_unix_ms: i64) -> SyncedReadModelObject {
    SyncedReadModelObject {
        study: parsed.study,
        series: parsed.series,
        instance: parsed.instance,
        synced_at_unix_ms,
    }
}

fn quarantine_category(error: &SyncTerminalObjectError) -> QuarantineCategory {
    match error {
        SyncTerminalObjectError::CannotUnderstand { .. } => QuarantineCategory::CannotUnderstand,
        SyncTerminalObjectError::Validation { .. } => QuarantineCategory::Validation,
        SyncTerminalObjectError::Policy { .. } => QuarantineCategory::Policy,
    }
}

fn now_unix_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures_util::StreamExt;
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    };
    use raccoon_contract_object_store::{
        ByteStream, Bytes, Object, ObjectKey, ObjectMetadata, ObjectStore, ObjectStoreError,
        PutResult,
    };
    use raccoon_service_ingest::IngestObjectId;

    use super::*;
    use crate::error::SyncRepositoryError;
    use crate::model::{SyncClaimToken, SyncInstanceRecord, SyncSeriesRecord, SyncStudyRecord};

    #[derive(Default)]
    struct FakeSourceRepository {
        pending: Mutex<VecDeque<ClaimedSyncObject>>,
        marked_synced: Mutex<Vec<SyncClaimToken>>,
        released: Mutex<Vec<SyncClaimToken>>,
    }

    #[async_trait]
    impl SyncSourceRepository for FakeSourceRepository {
        async fn claim_pending_objects(
            &self,
            _worker_id: &SyncWorkerId,
            batch_size: usize,
            _claim_ttl: std::time::Duration,
        ) -> Result<Vec<ClaimedSyncObject>, SyncRepositoryError> {
            let mut pending = self.pending.lock().unwrap();
            Ok((0..batch_size)
                .filter_map(|_| pending.pop_front())
                .collect())
        }

        async fn mark_synced(
            &self,
            claim_token: &SyncClaimToken,
        ) -> Result<(), SyncRepositoryError> {
            self.marked_synced.lock().unwrap().push(claim_token.clone());
            Ok(())
        }

        async fn release_claim(
            &self,
            claim_token: &SyncClaimToken,
        ) -> Result<(), SyncRepositoryError> {
            self.released.lock().unwrap().push(claim_token.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeWriter {
        objects: Mutex<Vec<SyncedReadModelObject>>,
        fail: Mutex<bool>,
    }

    #[async_trait]
    impl SyncReadModelWriter for FakeWriter {
        async fn upsert_synced_object(
            &self,
            object: &SyncedReadModelObject,
        ) -> Result<(), SyncRepositoryError> {
            if *self.fail.lock().unwrap() {
                return Err(SyncRepositoryError::new("writer offline"));
            }
            self.objects.lock().unwrap().push(object.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeQuarantineRepository {
        records: Mutex<Vec<QuarantineRecord>>,
    }

    #[async_trait]
    impl SyncQuarantineRepository for FakeQuarantineRepository {
        async fn mark_quarantined(
            &self,
            record: &QuarantineRecord,
        ) -> Result<(), SyncRepositoryError> {
            self.records.lock().unwrap().push(record.clone());
            Ok(())
        }
    }

    enum ParserMode {
        Ok,
        Invalid,
        MetadataTooLarge,
    }

    struct FakeParser {
        mode: Mutex<ParserMode>,
    }

    #[async_trait]
    impl SyncObjectParser for FakeParser {
        async fn parse(
            &self,
            _body: ByteStream,
            object_key: ObjectKey,
            _object_size_bytes: u64,
            _payload_representation: raccoon_service_ingest::IngestPayloadRepresentation,
            _transfer_syntax_uid: Option<String>,
            _max_metadata_bytes: Option<u64>,
        ) -> Result<ParsedSyncObject, SyncParseError> {
            match *self.mode.lock().unwrap() {
                ParserMode::Ok => Ok(parsed_object(object_key)),
                ParserMode::Invalid => Err(SyncParseError::validation("missing SOPInstanceUID")),
                ParserMode::MetadataTooLarge => Err(SyncParseError::MetadataTooLarge {
                    max_metadata_bytes: 5,
                }),
            }
        }
    }

    #[derive(Default)]
    struct FakeObjectStore {
        objects: Mutex<HashMap<ObjectKey, Bytes>>,
        deleted: Mutex<Vec<ObjectKey>>,
    }

    #[async_trait]
    impl ObjectStore for FakeObjectStore {
        async fn put(
            &self,
            key: ObjectKey,
            body: ByteStream,
        ) -> raccoon_contract_object_store::Result<PutResult> {
            let bytes = collect_body(body).await?;
            let len = bytes.len() as u64;
            self.objects.lock().unwrap().insert(key.clone(), bytes);
            Ok(PutResult::new(ObjectMetadata::new(key, len)))
        }

        async fn get(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<Object> {
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(Object::new(
                ObjectMetadata::new(key.clone(), bytes.len() as u64),
                ByteStream::once(bytes),
            ))
        }

        async fn head(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<ObjectMetadata> {
            let len = self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?
                .len() as u64;
            Ok(ObjectMetadata::new(key.clone(), len))
        }

        async fn delete(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<()> {
            self.objects.lock().unwrap().remove(key);
            self.deleted.lock().unwrap().push(key.clone());
            Ok(())
        }
    }

    async fn collect_body(body: ByteStream) -> raccoon_contract_object_store::Result<Bytes> {
        let mut stream = body.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Bytes::from(bytes))
    }

    fn claim() -> ClaimedSyncObject {
        ClaimedSyncObject {
            ingest_object_id: IngestObjectId::default(),
            object_key: ObjectKey::new("ingest/object-1").unwrap(),
            content_length: 10,
            payload_representation: raccoon_service_ingest::IngestPayloadRepresentation::DicomFile,
            transfer_syntax_uid: None,
            claim_token: SyncClaimToken::new("claim-1"),
        }
    }

    fn parsed_object(object_key: ObjectKey) -> ParsedSyncObject {
        let study_uid = StudyInstanceUid::new("1.2.3").unwrap();
        let series_uid = SeriesInstanceUid::new("1.2.3.4").unwrap();
        let sop_uid = SopInstanceUid::new("1.2.3.4.5").unwrap();
        let sop_class_uid = SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap();
        ParsedSyncObject {
            study: SyncStudyRecord {
                study_instance_uid: study_uid.clone(),
                patient_id: Some("P1".to_string()),
                patient_name: None,
                patient_birth_date: None,
                patient_sex: None,
                study_date: None,
                study_time: None,
                accession_number: None,
                study_id: None,
                study_description: None,
                referring_physician_name: None,
            },
            series: SyncSeriesRecord {
                series_instance_uid: series_uid.clone(),
                study_instance_uid: study_uid.clone(),
                modality: Some("CT".to_string()),
                series_number: None,
                series_date: None,
                series_time: None,
                series_description: None,
                body_part_examined: None,
            },
            instance: SyncInstanceRecord {
                identity: DicomInstanceIdentity::new(study_uid, series_uid, sop_uid, sop_class_uid),
                instance_number: None,
                acquisition_date_time: None,
                transfer_syntax_uid: None,
                object_key,
                object_size_bytes: 10,
                attributes_json: "{}".to_string(),
            },
        }
    }

    fn service_parts(
        parser: ParserMode,
    ) -> (
        StandardSyncService,
        Arc<FakeSourceRepository>,
        Arc<FakeWriter>,
        Arc<FakeQuarantineRepository>,
        Arc<FakeObjectStore>,
    ) {
        let source = Arc::new(FakeSourceRepository::default());
        source.pending.lock().unwrap().push_back(claim());
        let writer = Arc::new(FakeWriter::default());
        let quarantine = Arc::new(FakeQuarantineRepository::default());
        let object_store = Arc::new(FakeObjectStore::default());
        object_store.objects.lock().unwrap().insert(
            ObjectKey::new("ingest/object-1").unwrap(),
            Bytes::from_static(b"not a dicom file, but retained"),
        );
        let fake_parser = Arc::new(FakeParser {
            mode: Mutex::new(parser),
        });
        let service = StandardSyncService::new(
            source.clone(),
            writer.clone(),
            quarantine.clone(),
            object_store.clone(),
        )
        .with_parser(fake_parser);
        (service, source, writer, quarantine, object_store)
    }

    fn service_parts_without_object(
        parser: ParserMode,
    ) -> (
        StandardSyncService,
        Arc<FakeSourceRepository>,
        Arc<FakeWriter>,
        Arc<FakeQuarantineRepository>,
        Arc<FakeObjectStore>,
    ) {
        let source = Arc::new(FakeSourceRepository::default());
        source.pending.lock().unwrap().push_back(claim());
        let writer = Arc::new(FakeWriter::default());
        let quarantine = Arc::new(FakeQuarantineRepository::default());
        let object_store = Arc::new(FakeObjectStore::default());
        let fake_parser = Arc::new(FakeParser {
            mode: Mutex::new(parser),
        });
        let service = StandardSyncService::new(
            source.clone(),
            writer.clone(),
            quarantine.clone(),
            object_store.clone(),
        )
        .with_parser(fake_parser);
        (service, source, writer, quarantine, object_store)
    }

    #[tokio::test]
    async fn successful_sync_writes_read_model_and_marks_claim_synced() {
        let (service, source, writer, quarantine, _store) = service_parts(ParserMode::Ok);

        let result = service
            .sync_once(SyncWorkerId::new("worker-1"))
            .await
            .expect("sync succeeds");

        assert_eq!(result.claimed, 1);
        assert_eq!(result.synced, 1);
        assert_eq!(writer.objects.lock().unwrap().len(), 1);
        assert_eq!(source.marked_synced.lock().unwrap().len(), 1);
        assert!(quarantine.records.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn validation_failure_moves_object_to_quarantine_and_marks_terminal() {
        let (service, source, writer, quarantine, store) = service_parts(ParserMode::Invalid);

        let result = service
            .sync_once(SyncWorkerId::new("worker-1"))
            .await
            .expect("sync succeeds");

        assert_eq!(result.quarantined, 1);
        assert!(writer.objects.lock().unwrap().is_empty());
        assert!(source.marked_synced.lock().unwrap().is_empty());
        let records = quarantine.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].category, QuarantineCategory::Validation);
        assert!(
            records[0]
                .quarantine_object_key
                .as_str()
                .starts_with("sync/quarantine/")
        );
        assert!(
            !store
                .objects
                .lock()
                .unwrap()
                .contains_key(&ObjectKey::new("ingest/object-1").unwrap())
        );
    }

    #[tokio::test]
    async fn writer_failure_releases_claim_for_retry() {
        let (service, source, writer, quarantine, _store) = service_parts(ParserMode::Ok);
        *writer.fail.lock().unwrap() = true;

        let result = service
            .sync_once(SyncWorkerId::new("worker-1"))
            .await
            .expect("sync succeeds with retryable object failure");

        assert_eq!(result.retryable_failures, 1);
        assert_eq!(source.released.lock().unwrap().len(), 1);
        assert!(source.marked_synced.lock().unwrap().is_empty());
        assert!(quarantine.records.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_metadata_is_policy_quarantined_without_retry_release() {
        let (service, source, writer, quarantine, store) =
            service_parts(ParserMode::MetadataTooLarge);

        let result = service
            .sync_once(SyncWorkerId::new("worker-1"))
            .await
            .expect("sync succeeds");

        assert_eq!(result.quarantined, 1);
        assert_eq!(result.retryable_failures, 0);
        assert!(writer.objects.lock().unwrap().is_empty());
        assert!(source.released.lock().unwrap().is_empty());
        assert!(source.marked_synced.lock().unwrap().is_empty());
        let records = quarantine.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].category, QuarantineCategory::Policy);
        assert!(records[0].reason.contains("metadata exceeded"));
        assert!(records[0].reason.contains("maximum of 5 bytes"));
        assert!(records[0].reason.contains("object_content_length=10"));
        assert!(
            !store
                .objects
                .lock()
                .unwrap()
                .contains_key(&ObjectKey::new("ingest/object-1").unwrap())
        );
    }

    #[tokio::test]
    async fn object_store_read_failure_releases_claim_for_retry() {
        let (service, source, writer, quarantine, _store) =
            service_parts_without_object(ParserMode::Ok);

        let result = service
            .sync_once(SyncWorkerId::new("worker-1"))
            .await
            .expect("sync succeeds with retryable object-store failure");

        assert_eq!(result.retryable_failures, 1);
        assert_eq!(source.released.lock().unwrap().len(), 1);
        assert!(writer.objects.lock().unwrap().is_empty());
        assert!(quarantine.records.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fake_source_claims_each_object_once_across_workers() {
        let source = FakeSourceRepository::default();
        source.pending.lock().unwrap().push_back(claim());

        let first = source
            .claim_pending_objects(
                &SyncWorkerId::new("a"),
                1,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        let second = source
            .claim_pending_objects(
                &SyncWorkerId::new("b"),
                1,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }
}
