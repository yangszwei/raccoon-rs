use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_ingest::{
    IngestObjectId, IngestObjectOutcome, IngestPayloadRepresentation, IngestRepository,
    IngestRepositoryError, ReceivedIngestObject,
};
use raccoon_service_sync::{
    ClaimedSyncObject, QuarantineRecord, SyncClaimToken, SyncQuarantineRepository,
    SyncRepositoryError, SyncSourceRepository, SyncWorkerId,
};
use sqlx::{
    PgPool, Row,
    postgres::{PgPoolOptions, PgRow},
};
use tracing::{Instrument, Span, info_span, instrument};
use uuid::Uuid;

use crate::error::{PostgresError, PostgresIngestRepositoryError, error_kind};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
const INSERT_CHUNK_SIZE: usize = 1024;

/// Postgres-backed implementation of [`IngestRepository`].
#[derive(Clone, Debug)]
pub struct PostgresIngestRepository {
    pool: PgPool,
}

impl PostgresIngestRepository {
    /// Creates a repository from an existing connection pool.
    ///
    /// The pool is assumed to have already been migrated. Prefer [`open`][Self::open]
    /// for production use; use this constructor to inject a pre-migrated pool in tests.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Opens a connection to Postgres and runs all pending migrations before
    /// returning the repository.
    pub async fn open(url: &str) -> Result<Self, PostgresIngestRepositoryError> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(PostgresIngestRepositoryError::Connect)?;

        MIGRATOR
            .run(&pool)
            .await
            .map_err(PostgresIngestRepositoryError::Migrate)?;

        Ok(Self::new(pool))
    }

    /// Consumes the repository and returns the underlying connection pool.
    pub fn into_pool(self) -> PgPool {
        self.pool
    }
}

#[async_trait]
impl IngestRepository for PostgresIngestRepository {
    #[instrument(
        skip_all,
        fields(
            ingest.object_count = records.len(),
            db.system = "postgresql",
            error.type = tracing::field::Empty,
        )
    )]
    async fn record_received_objects(
        &self,
        records: &[ReceivedIngestObject],
    ) -> Result<(), IngestRepositoryError> {
        if records.is_empty() {
            return Ok(());
        }

        self.try_record_received_objects(records)
            .await
            .map_err(|err| {
                Span::current().record("error.type", error_kind(&err));
                IngestRepositoryError::with_source("failed to record received ingest objects", err)
            })
    }
}

#[async_trait]
impl SyncSourceRepository for PostgresIngestRepository {
    #[instrument(
        skip_all,
        fields(
            sync.worker_id = %worker_id,
            sync.batch_size = batch_size,
            db.system = "postgresql",
            error.type = tracing::field::Empty,
        )
    )]
    async fn claim_pending_objects(
        &self,
        worker_id: &SyncWorkerId,
        batch_size: usize,
        claim_ttl: Duration,
    ) -> Result<Vec<ClaimedSyncObject>, SyncRepositoryError> {
        self.try_claim_pending_objects(worker_id, batch_size, claim_ttl)
            .await
            .map_err(sync_repository_error(
                "failed to claim pending sync objects",
            ))
    }

    #[instrument(
        skip_all,
        fields(
            sync.claim_token = %claim_token,
            db.system = "postgresql",
            error.type = tracing::field::Empty,
        )
    )]
    async fn mark_synced(&self, claim_token: &SyncClaimToken) -> Result<(), SyncRepositoryError> {
        self.try_mark_synced(claim_token)
            .await
            .map_err(sync_repository_error("failed to mark sync claim as synced"))
    }

    #[instrument(
        skip_all,
        fields(
            sync.claim_token = %claim_token,
            db.system = "postgresql",
            error.type = tracing::field::Empty,
        )
    )]
    async fn release_claim(&self, claim_token: &SyncClaimToken) -> Result<(), SyncRepositoryError> {
        self.try_release_claim(claim_token)
            .await
            .map_err(sync_repository_error("failed to release sync claim"))
    }
}

#[async_trait]
impl SyncQuarantineRepository for PostgresIngestRepository {
    #[instrument(
        skip_all,
        fields(
            sync.ingest_object_id = %record.ingest_object_id,
            sync.claim_token = %record.claim_token,
            sync.quarantine_category = record.category.as_str(),
            db.system = "postgresql",
            error.type = tracing::field::Empty,
        )
    )]
    async fn mark_quarantined(&self, record: &QuarantineRecord) -> Result<(), SyncRepositoryError> {
        self.try_mark_quarantined(record)
            .await
            .map_err(sync_repository_error(
                "failed to mark sync claim as quarantined",
            ))
    }
}

impl PostgresIngestRepository {
    async fn try_record_received_objects(
        &self,
        records: &[ReceivedIngestObject],
    ) -> Result<(), PostgresError> {
        let chunks = records
            .chunks(INSERT_CHUNK_SIZE)
            .map(InsertBatch::try_from_records)
            .collect::<Result<Vec<_>, _>>()?;

        let mut tx = self
            .pool
            .begin()
            .instrument(info_span!("postgres.ingest.begin_transaction"))
            .await
            .map_err(PostgresError::Sqlx)?;

        async {
            for chunk in &chunks {
                insert_batch(&mut tx, chunk).await?;
            }
            Ok::<(), PostgresError>(())
        }
        .instrument(info_span!(
            "postgres.ingest.insert_records",
            ingest.object_count = records.len()
        ))
        .await?;

        tx.commit()
            .instrument(info_span!("postgres.ingest.commit_transaction"))
            .await
            .map_err(PostgresError::Sqlx)?;
        Ok(())
    }

    async fn try_claim_pending_objects(
        &self,
        worker_id: &SyncWorkerId,
        batch_size: usize,
        claim_ttl: Duration,
    ) -> Result<Vec<ClaimedSyncObject>, PostgresError> {
        let now_ms = current_unix_ms()?;
        let ttl_ms = duration_to_i64("claim_ttl", claim_ttl)?;
        let expires_at = now_ms
            .checked_add(ttl_ms)
            .ok_or(PostgresError::TimeOutOfRange {
                field: "sync_claim_expires_at",
            })?;
        let limit = usize_to_i64("batch_size", batch_size)?;

        let rows = sqlx::query(
            r#"
            WITH candidates AS (
                SELECT s.ingest_object_id
                FROM ingest_object_sync_states s
                WHERE s.sync_state = 'pending'
                  AND (
                      s.sync_claim_token IS NULL
                      OR s.sync_claim_expires_at_unix_ms IS NULL
                      OR s.sync_claim_expires_at_unix_ms <= $3
                  )
                ORDER BY s.received_at_unix_ms, s.ingest_object_id
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            ),
            claimed AS (
                UPDATE ingest_object_sync_states s
                SET
                    sync_claim_token = gen_random_uuid()::text,
                    sync_claimed_by = $1,
                    sync_claim_expires_at_unix_ms = $2
                FROM candidates
                WHERE s.ingest_object_id = candidates.ingest_object_id
                RETURNING
                    s.ingest_object_id,
                    s.sync_claim_token,
                    s.received_at_unix_ms
            )
            SELECT
                i.ingest_object_id,
                i.object_key,
                i.content_length,
                i.payload_representation,
                i.transfer_syntax_uid,
                claimed.sync_claim_token
            FROM claimed
            JOIN ingest_objects i ON i.ingest_object_id = claimed.ingest_object_id
            ORDER BY claimed.received_at_unix_ms, i.ingest_object_id
            "#,
        )
        .bind(worker_id.as_str())
        .bind(expires_at)
        .bind(now_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(PostgresError::Sqlx)?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let ingest_object_id = IngestObjectId::from_uuid(
                row.try_get::<Uuid, _>("ingest_object_id")
                    .map_err(PostgresError::Sqlx)?,
            );
            let object_key = parse_object_key(&row, "object_key")?;
            let content_length = parse_content_length(&row, "content_length")?;
            let payload_representation = parse_payload_representation(&row)?;
            let transfer_syntax_uid = row
                .try_get::<Option<String>, _>("transfer_syntax_uid")
                .map_err(PostgresError::Sqlx)?;
            let claim_token = SyncClaimToken::new(read_string(&row, "sync_claim_token")?);

            claims.push(ClaimedSyncObject {
                ingest_object_id,
                object_key,
                content_length,
                payload_representation,
                transfer_syntax_uid,
                claim_token,
            });
        }

        Ok(claims)
    }

    async fn try_mark_synced(&self, claim_token: &SyncClaimToken) -> Result<(), PostgresError> {
        let synced_at = current_unix_ms()?;
        let result = sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_state = 'synced',
                synced_at_unix_ms = $1,
                terminal_at_unix_ms = $1,
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND sync_claim_token = $2
            "#,
        )
        .bind(synced_at)
        .bind(claim_token.as_str())
        .execute(&self.pool)
        .await
        .map_err(PostgresError::Sqlx)?;

        if result.rows_affected() == 0 {
            return Err(PostgresError::StaleSyncClaim);
        }

        Ok(())
    }

    async fn try_release_claim(&self, claim_token: &SyncClaimToken) -> Result<(), PostgresError> {
        sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND sync_claim_token = $1
            "#,
        )
        .bind(claim_token.as_str())
        .execute(&self.pool)
        .await
        .map_err(PostgresError::Sqlx)?;

        Ok(())
    }

    async fn try_mark_quarantined(&self, record: &QuarantineRecord) -> Result<(), PostgresError> {
        let mut tx = self.pool.begin().await.map_err(PostgresError::Sqlx)?;
        let ingest_object_id = record.ingest_object_id.as_uuid();

        let result = sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_state = 'quarantined',
                terminal_at_unix_ms = $1,
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND ingest_object_id = $2
              AND sync_claim_token = $3
            "#,
        )
        .bind(record.quarantined_at_unix_ms)
        .bind(ingest_object_id)
        .bind(record.claim_token.as_str())
        .execute(&mut *tx)
        .await
        .map_err(PostgresError::Sqlx)?;

        if result.rows_affected() == 0 {
            return Err(PostgresError::StaleSyncClaim);
        }

        sqlx::query(
            r#"
            INSERT INTO ingest_object_quarantines (
                ingest_object_id,
                category,
                reason,
                original_object_key,
                quarantine_object_key,
                quarantined_at_unix_ms
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(ingest_object_id)
        .bind(record.category.as_str())
        .bind(record.reason.as_str())
        .bind(record.original_object_key.as_str())
        .bind(record.quarantine_object_key.as_str())
        .bind(record.quarantined_at_unix_ms)
        .execute(&mut *tx)
        .await
        .map_err(PostgresError::Sqlx)?;

        sqlx::query("UPDATE ingest_objects SET object_key = $1 WHERE ingest_object_id = $2")
            .bind(record.quarantine_object_key.as_str())
            .bind(ingest_object_id)
            .execute(&mut *tx)
            .await
            .map_err(PostgresError::Sqlx)?;

        tx.commit().await.map_err(PostgresError::Sqlx)?;

        Ok(())
    }
}

struct InsertBatch {
    ingest_object_ids: Vec<Uuid>,
    upload_ids: Vec<Uuid>,
    object_keys: Vec<String>,
    content_lengths: Vec<i64>,
    etags: Vec<Option<String>>,
    checksum_algorithms: Vec<Option<String>>,
    checksum_values: Vec<Option<String>>,
    sop_class_uids: Vec<Option<String>>,
    study_instance_uids: Vec<Option<String>>,
    series_instance_uids: Vec<Option<String>>,
    sop_instance_uids: Vec<Option<String>>,
    payload_representations: Vec<String>,
    transfer_syntax_uids: Vec<Option<String>>,
    source_aes: Vec<Option<String>>,
    outcome_kinds: Vec<String>,
    outcome_reasons: Vec<Option<String>>,
    received_at_unix_ms: Vec<i64>,
}

impl InsertBatch {
    fn try_from_records(records: &[ReceivedIngestObject]) -> Result<Self, PostgresError> {
        let mut batch = Self {
            ingest_object_ids: Vec::with_capacity(records.len()),
            upload_ids: Vec::with_capacity(records.len()),
            object_keys: Vec::with_capacity(records.len()),
            content_lengths: Vec::with_capacity(records.len()),
            etags: Vec::with_capacity(records.len()),
            checksum_algorithms: Vec::with_capacity(records.len()),
            checksum_values: Vec::with_capacity(records.len()),
            sop_class_uids: Vec::with_capacity(records.len()),
            study_instance_uids: Vec::with_capacity(records.len()),
            series_instance_uids: Vec::with_capacity(records.len()),
            sop_instance_uids: Vec::with_capacity(records.len()),
            payload_representations: Vec::with_capacity(records.len()),
            transfer_syntax_uids: Vec::with_capacity(records.len()),
            source_aes: Vec::with_capacity(records.len()),
            outcome_kinds: Vec::with_capacity(records.len()),
            outcome_reasons: Vec::with_capacity(records.len()),
            received_at_unix_ms: Vec::with_capacity(records.len()),
        };

        for record in records {
            let outcome = OutcomeColumns::from(&record.outcome);
            batch
                .ingest_object_ids
                .push(record.ingest_object_id.as_uuid());
            batch.upload_ids.push(record.upload_id.as_uuid());
            batch
                .object_keys
                .push(record.object_key.as_str().to_owned());
            batch
                .content_lengths
                .push(to_i64("content_length", record.content_length)?);
            batch.etags.push(record.etag.clone());
            batch.checksum_algorithms.push(
                record
                    .checksum
                    .as_ref()
                    .map(|c| c.algorithm.as_str().to_owned()),
            );
            batch
                .checksum_values
                .push(record.checksum.as_ref().map(|c| c.value.clone()));
            batch
                .sop_class_uids
                .push(record.identity.sop_class_uid.clone());
            batch
                .study_instance_uids
                .push(record.identity.study_instance_uid.clone());
            batch
                .series_instance_uids
                .push(record.identity.series_instance_uid.clone());
            batch
                .sop_instance_uids
                .push(record.identity.sop_instance_uid.clone());
            batch
                .payload_representations
                .push(record.payload_representation.as_str().to_owned());
            batch
                .transfer_syntax_uids
                .push(record.transfer_syntax_uid.clone());
            batch.source_aes.push(record.source.source_ae.clone());
            batch.outcome_kinds.push(outcome.kind.to_owned());
            batch.outcome_reasons.push(outcome.reason);
            batch
                .received_at_unix_ms
                .push(to_unix_ms("received_at", record.received_at)?);
        }

        Ok(batch)
    }
}

async fn insert_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &InsertBatch,
) -> Result<(), PostgresError> {
    sqlx::query(
        r#"
        WITH input AS (
            SELECT *
            FROM unnest(
                $1::uuid[],
                $2::uuid[],
                $3::text[],
                $4::bigint[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::text[],
                $9::text[],
                $10::text[],
                $11::text[],
                $12::text[],
                $13::text[],
                $14::text[],
                $15::text[],
                $16::text[],
                $17::bigint[]
            ) AS records (
                ingest_object_id,
                upload_id,
                object_key,
                content_length,
                etag,
                checksum_algorithm,
                checksum_value,
                sop_class_uid,
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
                payload_representation,
                transfer_syntax_uid,
                source_ae,
                outcome_kind,
                outcome_reason,
                received_at_unix_ms
            )
        ),
        inserted AS (
            INSERT INTO ingest_objects (
                ingest_object_id,
                upload_id,
                object_key,
                content_length,
                etag,
                checksum_algorithm,
                checksum_value,
                sop_class_uid,
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
                payload_representation,
                transfer_syntax_uid,
                source_ae,
                outcome_kind,
                outcome_reason,
                received_at_unix_ms
            )
            SELECT
                ingest_object_id,
                upload_id,
                object_key,
                content_length,
                etag,
                checksum_algorithm,
                checksum_value,
                sop_class_uid,
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
                payload_representation,
                transfer_syntax_uid,
                source_ae,
                outcome_kind,
                outcome_reason,
                received_at_unix_ms
            FROM input
            ON CONFLICT (sop_instance_uid) DO NOTHING
            RETURNING ingest_object_id, outcome_kind, received_at_unix_ms
        )
        INSERT INTO ingest_object_sync_states (
            ingest_object_id,
            sync_state,
            received_at_unix_ms
        )
        SELECT
            ingest_object_id,
            'pending',
            received_at_unix_ms
        FROM inserted
        WHERE outcome_kind = 'stored'
        "#,
    )
    .bind(&batch.ingest_object_ids)
    .bind(&batch.upload_ids)
    .bind(&batch.object_keys)
    .bind(&batch.content_lengths)
    .bind(&batch.etags)
    .bind(&batch.checksum_algorithms)
    .bind(&batch.checksum_values)
    .bind(&batch.sop_class_uids)
    .bind(&batch.study_instance_uids)
    .bind(&batch.series_instance_uids)
    .bind(&batch.sop_instance_uids)
    .bind(&batch.payload_representations)
    .bind(&batch.transfer_syntax_uids)
    .bind(&batch.source_aes)
    .bind(&batch.outcome_kinds)
    .bind(&batch.outcome_reasons)
    .bind(&batch.received_at_unix_ms)
    .execute(&mut **tx)
    .await
    .map_err(PostgresError::Sqlx)?;

    Ok(())
}

struct OutcomeColumns {
    kind: &'static str,
    /// Human-readable support/debug text only. Do not parse this as a
    /// structured persistence contract.
    reason: Option<String>,
}

impl OutcomeColumns {
    fn new(kind: &'static str) -> Self {
        Self { kind, reason: None }
    }
}

impl From<&IngestObjectOutcome> for OutcomeColumns {
    fn from(outcome: &IngestObjectOutcome) -> Self {
        match outcome {
            IngestObjectOutcome::Stored => Self::new("stored"),
            IngestObjectOutcome::RejectedCannotUnderstand { reason } => Self {
                kind: "rejected_cannot_understand",
                reason: Some(reason.clone()),
            },
            IngestObjectOutcome::RejectedUnsupportedSopClass {
                sop_class_uid,
                reason,
            } => Self {
                kind: "rejected_unsupported_sop_class",
                reason: Some(match sop_class_uid {
                    Some(uid) => format!("{reason}; sop_class_uid={uid}"),
                    None => reason.clone(),
                }),
            },
            IngestObjectOutcome::RejectedStudyMismatch {
                expected_study_instance_uid,
                actual_study_instance_uid,
            } => Self {
                kind: "rejected_study_mismatch",
                reason: Some(format!(
                    "expected_study_instance_uid={expected_study_instance_uid}; \
                     actual_study_instance_uid={}",
                    actual_study_instance_uid.as_deref().unwrap_or("<missing>")
                )),
            },
            IngestObjectOutcome::RejectedChecksumMismatch { expected, actual } => Self {
                kind: "rejected_checksum_mismatch",
                reason: Some(format!(
                    "expected_checksum={expected}; actual_checksum={actual}"
                )),
            },
            IngestObjectOutcome::RejectedTooLarge { max_content_length } => Self {
                kind: "rejected_too_large",
                reason: Some(format!("max_content_length={max_content_length}")),
            },
            IngestObjectOutcome::ObjectStoreFailed { reason } => Self {
                kind: "object_store_failed",
                reason: Some(reason.clone()),
            },
            IngestObjectOutcome::RepositoryFailed { reason } => Self {
                kind: "repository_failed",
                reason: Some(reason.clone()),
            },
        }
    }
}

fn to_i64(field: &'static str, value: u64) -> Result<i64, PostgresError> {
    i64::try_from(value).map_err(|_| PostgresError::IntegerOutOfRange { field, value })
}

fn to_unix_ms(field: &'static str, time: SystemTime) -> Result<i64, PostgresError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresError::TimeBeforeUnixEpoch { field })?;

    i64::try_from(duration.as_millis()).map_err(|_| PostgresError::TimeOutOfRange { field })
}

fn current_unix_ms() -> Result<i64, PostgresError> {
    to_unix_ms("current_time", SystemTime::now())
}

fn duration_to_i64(field: &'static str, duration: Duration) -> Result<i64, PostgresError> {
    i64::try_from(duration.as_millis()).map_err(|_| PostgresError::DurationOutOfRange { field })
}

fn usize_to_i64(field: &'static str, value: usize) -> Result<i64, PostgresError> {
    i64::try_from(value).map_err(|_| PostgresError::IntegerOutOfRange {
        field,
        value: value as u64,
    })
}

fn parse_object_key(row: &PgRow, column: &str) -> Result<ObjectKey, PostgresError> {
    let value = read_string(row, column)?;
    ObjectKey::new(value).map_err(|e| invalid_sync_metadata(column, e))
}

fn parse_content_length(row: &PgRow, column: &str) -> Result<u64, PostgresError> {
    let value = row.try_get::<i64, _>(column).map_err(PostgresError::Sqlx)?;

    u64::try_from(value).map_err(|_| PostgresError::InvalidStoredSyncMetadata {
        column: column.to_owned(),
        reason: "must not be negative".to_owned(),
    })
}

fn read_string(row: &PgRow, column: &str) -> Result<String, PostgresError> {
    row.try_get::<String, _>(column)
        .map_err(PostgresError::Sqlx)
}

fn parse_payload_representation(row: &PgRow) -> Result<IngestPayloadRepresentation, PostgresError> {
    let value = read_string(row, "payload_representation")?;
    match value.as_str() {
        "dicom_file" => Ok(IngestPayloadRepresentation::DicomFile),
        "dicom_dataset" => Ok(IngestPayloadRepresentation::DicomDataSet),
        "dicomweb_metadata_and_bulk_data" => {
            Ok(IngestPayloadRepresentation::DicomWebMetadataAndBulkData)
        }
        "unknown" => Ok(IngestPayloadRepresentation::Unknown),
        other => Err(PostgresError::InvalidStoredSyncMetadata {
            column: "payload_representation".to_owned(),
            reason: format!("unsupported payload representation {other:?}"),
        }),
    }
}

fn invalid_sync_metadata(column: &str, err: impl std::fmt::Display) -> PostgresError {
    PostgresError::InvalidStoredSyncMetadata {
        column: column.to_owned(),
        reason: err.to_string(),
    }
}

fn sync_repository_error(
    message: &'static str,
) -> impl FnOnce(PostgresError) -> SyncRepositoryError {
    move |err| {
        Span::current().record("error.type", error_kind(&err));
        SyncRepositoryError::with_source(message, err)
    }
}
