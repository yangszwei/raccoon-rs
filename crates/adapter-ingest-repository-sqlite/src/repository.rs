use std::str::FromStr;
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
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tracing::{Instrument, Span, info_span, instrument};

use crate::error::{SqliteError, SqliteIngestRepositoryError, error_kind};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// SQLite-backed implementation of [`IngestRepository`].
#[derive(Clone, Debug)]
pub struct SqliteIngestRepository {
    pool: SqlitePool,
}

impl SqliteIngestRepository {
    /// Creates a repository from an existing connection pool.
    ///
    /// The pool is assumed to have already been migrated. Prefer [`open`][Self::open]
    /// for production use; use this constructor to inject a pre-migrated pool in tests.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Opens a connection to the SQLite database at `url` and runs all pending
    /// migrations before returning the repository.
    ///
    /// The database file is created if it does not already exist. The pool is
    /// configured with a single connection: SQLite serializes all writes, so a
    /// larger pool does not improve write throughput, and a single connection
    /// ensures in-memory databases (used in tests) share the migrated schema
    /// across all pool checkouts.
    pub async fn open(url: &str) -> Result<Self, SqliteIngestRepositoryError> {
        let mut options = SqliteConnectOptions::from_str(url)
            .map_err(SqliteIngestRepositoryError::Connect)?
            .create_if_missing(true);

        if !is_in_memory_url(url) {
            options = options
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal);
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(SqliteIngestRepositoryError::Connect)?;

        MIGRATOR
            .run(&pool)
            .await
            .map_err(SqliteIngestRepositoryError::Migrate)?;

        Ok(Self::new(pool))
    }

    /// Consumes the repository and returns the underlying connection pool.
    ///
    /// Useful in tests that need to run verification queries directly against
    /// the database after operating through the repository.
    pub fn into_pool(self) -> SqlitePool {
        self.pool
    }
}

fn is_in_memory_url(url: &str) -> bool {
    url.contains("mode=memory") || url.contains(":memory:")
}

#[async_trait]
impl IngestRepository for SqliteIngestRepository {
    #[instrument(
        skip_all,
        fields(
            ingest.object_count = records.len(),
            db.system = "sqlite",
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
impl SyncSourceRepository for SqliteIngestRepository {
    #[instrument(
        skip_all,
        fields(
            sync.worker_id = %worker_id,
            sync.batch_size = batch_size,
            db.system = "sqlite",
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
            db.system = "sqlite",
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
            db.system = "sqlite",
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
impl SyncQuarantineRepository for SqliteIngestRepository {
    #[instrument(
        skip_all,
        fields(
            sync.ingest_object_id = %record.ingest_object_id,
            sync.claim_token = %record.claim_token,
            sync.quarantine_category = record.category.as_str(),
            db.system = "sqlite",
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

impl SqliteIngestRepository {
    async fn try_record_received_objects(
        &self,
        records: &[ReceivedIngestObject],
    ) -> Result<(), SqliteError> {
        let mut tx = self
            .pool
            .begin()
            .instrument(info_span!("sqlite.ingest.begin_transaction"))
            .await
            .map_err(SqliteError::Sqlx)?;

        async {
            for record in records {
                insert_record(&mut tx, record).await?;
            }
            Ok::<(), SqliteError>(())
        }
        .instrument(info_span!(
            "sqlite.ingest.insert_records",
            ingest.object_count = records.len()
        ))
        .await?;

        tx.commit()
            .instrument(info_span!("sqlite.ingest.commit_transaction"))
            .await
            .map_err(SqliteError::Sqlx)?;
        Ok(())
    }

    async fn try_claim_pending_objects(
        &self,
        worker_id: &SyncWorkerId,
        batch_size: usize,
        claim_ttl: Duration,
    ) -> Result<Vec<ClaimedSyncObject>, SqliteError> {
        let now_ms = current_unix_ms()?;
        let ttl_ms = duration_to_i64("claim_ttl", claim_ttl)?;
        let expires_at = now_ms
            .checked_add(ttl_ms)
            .ok_or(SqliteError::TimeOutOfRange {
                field: "sync_claim_expires_at",
            })?;
        let limit = usize_to_i64("batch_size", batch_size)?;

        let claimed_rows = sqlx::query(
            r#"
            INSERT INTO ingest_object_sync_states (
                ingest_object_id,
                sync_state,
                sync_claim_token,
                sync_claimed_by,
                sync_claim_expires_at_unix_ms
            )
            SELECT
                ingest_objects.ingest_object_id,
                'pending',
                lower(hex(randomblob(16))),
                ?,
                ?
            FROM ingest_objects
            LEFT JOIN ingest_object_sync_states
                ON ingest_object_sync_states.ingest_object_id = ingest_objects.ingest_object_id
            WHERE ingest_objects.outcome_kind = 'stored'
              AND (
                  ingest_object_sync_states.ingest_object_id IS NULL
                  OR (
                      ingest_object_sync_states.sync_state = 'pending'
                      AND (
                      ingest_object_sync_states.sync_claim_token IS NULL
                      OR ingest_object_sync_states.sync_claim_expires_at_unix_ms IS NULL
                      OR ingest_object_sync_states.sync_claim_expires_at_unix_ms <= ?
                      )
                  )
              )
            ORDER BY ingest_objects.received_at_unix_ms, ingest_objects.ingest_object_id
            LIMIT ?
            ON CONFLICT(ingest_object_id) DO UPDATE SET
                sync_claim_token = excluded.sync_claim_token,
                sync_claimed_by = excluded.sync_claimed_by,
                sync_claim_expires_at_unix_ms = excluded.sync_claim_expires_at_unix_ms
            WHERE ingest_object_sync_states.sync_state = 'pending'
              AND (
                  ingest_object_sync_states.sync_claim_token IS NULL
                  OR ingest_object_sync_states.sync_claim_expires_at_unix_ms IS NULL
                  OR ingest_object_sync_states.sync_claim_expires_at_unix_ms <= ?
              )
            RETURNING
                ingest_object_id,
                sync_claim_token
            "#,
        )
        .bind(worker_id.as_str())
        .bind(expires_at)
        .bind(now_ms)
        .bind(limit)
        .bind(now_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(SqliteError::Sqlx)?;

        // Collect (id → token) from the RETURNING rows.
        let mut token_by_id: std::collections::HashMap<String, SyncClaimToken> =
            std::collections::HashMap::with_capacity(claimed_rows.len());
        for row in &claimed_rows {
            let id = read_string(row, "ingest_object_id")?;
            let token = SyncClaimToken::new(read_string(row, "sync_claim_token")?);
            token_by_id.insert(id, token);
        }

        if token_by_id.is_empty() {
            return Ok(vec![]);
        }

        // Single batch fetch for all object metadata.
        let placeholders = token_by_id
            .keys()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT ingest_object_id, object_key, content_length, payload_representation, \
                    transfer_syntax_uid, received_at_unix_ms \
             FROM ingest_objects WHERE ingest_object_id IN ({placeholders})"
        );
        let mut stmt = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for id in token_by_id.keys() {
            stmt = stmt.bind(id);
        }
        let metadata_rows = stmt
            .fetch_all(&self.pool)
            .await
            .map_err(SqliteError::Sqlx)?;

        let mut claims: Vec<OrderedClaimedSyncObject> = Vec::with_capacity(metadata_rows.len());
        for row in metadata_rows {
            let id_str = read_string(&row, "ingest_object_id")?;
            let ingest_object_id = id_str
                .parse::<IngestObjectId>()
                .map_err(|e| invalid_sync_metadata("ingest_object_id", e))?;
            let claim_token =
                token_by_id
                    .remove(&id_str)
                    .ok_or(SqliteError::InvalidStoredSyncMetadata {
                        column: "ingest_object_id".to_owned(),
                        reason: format!("returned id {id_str} was not in RETURNING set"),
                    })?;
            let object_key = parse_object_key(&row, "object_key")?;
            let content_length = parse_content_length(&row, "content_length")?;
            let payload_representation = parse_payload_representation(&row)?;
            let transfer_syntax_uid = row
                .try_get::<Option<String>, _>("transfer_syntax_uid")
                .map_err(SqliteError::Sqlx)?;
            let received_at_unix_ms = row
                .try_get::<i64, _>("received_at_unix_ms")
                .map_err(SqliteError::Sqlx)?;
            claims.push(OrderedClaimedSyncObject {
                received_at_unix_ms,
                object: ClaimedSyncObject {
                    ingest_object_id,
                    object_key,
                    content_length,
                    payload_representation,
                    transfer_syntax_uid,
                    claim_token,
                },
            });
        }

        claims.sort_by(|left, right| {
            left.received_at_unix_ms
                .cmp(&right.received_at_unix_ms)
                .then_with(|| {
                    left.object
                        .ingest_object_id
                        .cmp(&right.object.ingest_object_id)
                })
        });

        Ok(claims.into_iter().map(|claim| claim.object).collect())
    }

    async fn try_mark_synced(&self, claim_token: &SyncClaimToken) -> Result<(), SqliteError> {
        let synced_at = current_unix_ms()?;
        let result = sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_state = 'synced',
                synced_at_unix_ms = ?,
                terminal_at_unix_ms = ?,
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND sync_claim_token = ?
            "#,
        )
        .bind(synced_at)
        .bind(synced_at)
        .bind(claim_token.as_str())
        .execute(&self.pool)
        .await
        .map_err(SqliteError::Sqlx)?;

        if result.rows_affected() == 0 {
            return Err(SqliteError::StaleSyncClaim);
        }

        Ok(())
    }

    async fn try_release_claim(&self, claim_token: &SyncClaimToken) -> Result<(), SqliteError> {
        sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND sync_claim_token = ?
            "#,
        )
        .bind(claim_token.as_str())
        .execute(&self.pool)
        .await
        .map_err(SqliteError::Sqlx)?;

        Ok(())
    }

    async fn try_mark_quarantined(&self, record: &QuarantineRecord) -> Result<(), SqliteError> {
        let mut tx = self.pool.begin().await.map_err(SqliteError::Sqlx)?;

        let result = sqlx::query(
            r#"
            UPDATE ingest_object_sync_states
            SET
                sync_state = 'quarantined',
                terminal_at_unix_ms = ?,
                sync_claim_token = NULL,
                sync_claimed_by = NULL,
                sync_claim_expires_at_unix_ms = NULL
            WHERE sync_state = 'pending'
              AND ingest_object_id = ?
              AND sync_claim_token = ?
            "#,
        )
        .bind(record.quarantined_at_unix_ms)
        .bind(record.ingest_object_id.to_string())
        .bind(record.claim_token.as_str())
        .execute(&mut *tx)
        .await
        .map_err(SqliteError::Sqlx)?;

        if result.rows_affected() == 0 {
            return Err(SqliteError::StaleSyncClaim);
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
            ) VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(record.ingest_object_id.to_string())
        .bind(record.category.as_str())
        .bind(record.reason.as_str())
        .bind(record.original_object_key.as_str())
        .bind(record.quarantine_object_key.as_str())
        .bind(record.quarantined_at_unix_ms)
        .execute(&mut *tx)
        .await
        .map_err(SqliteError::Sqlx)?;

        sqlx::query("UPDATE ingest_objects SET object_key = ? WHERE ingest_object_id = ?")
            .bind(record.quarantine_object_key.as_str())
            .bind(record.ingest_object_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(SqliteError::Sqlx)?;

        tx.commit().await.map_err(SqliteError::Sqlx)?;

        Ok(())
    }
}

async fn insert_record(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    record: &ReceivedIngestObject,
) -> Result<(), SqliteError> {
    let content_length = to_i64("content_length", record.content_length)?;
    let outcome = OutcomeColumns::from(&record.outcome);
    let received_at = to_unix_ms("received_at", record.received_at)?;

    sqlx::query(
        r#"
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sop_instance_uid) DO NOTHING
        "#,
    )
    .bind(record.ingest_object_id.to_string())
    .bind(record.upload_id.to_string())
    .bind(record.object_key.as_str())
    .bind(content_length)
    .bind(record.etag.as_deref())
    .bind(record.checksum.as_ref().map(|c| c.algorithm.as_str()))
    .bind(record.checksum.as_ref().map(|c| c.value.as_str()))
    .bind(record.identity.sop_class_uid.as_deref())
    .bind(record.identity.study_instance_uid.as_deref())
    .bind(record.identity.series_instance_uid.as_deref())
    .bind(record.identity.sop_instance_uid.as_deref())
    .bind(record.payload_representation.as_str())
    .bind(record.transfer_syntax_uid.as_deref())
    .bind(record.source.source_ae.as_deref())
    .bind(outcome.kind)
    .bind(outcome.reason)
    .bind(received_at)
    .execute(&mut **tx)
    .await
    .map_err(SqliteError::Sqlx)?;

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

fn to_i64(field: &'static str, value: u64) -> Result<i64, SqliteError> {
    i64::try_from(value).map_err(|_| SqliteError::IntegerOutOfRange { field, value })
}

fn to_unix_ms(field: &'static str, time: SystemTime) -> Result<i64, SqliteError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SqliteError::TimeBeforeUnixEpoch { field })?;

    i64::try_from(duration.as_millis()).map_err(|_| SqliteError::TimeOutOfRange { field })
}

fn current_unix_ms() -> Result<i64, SqliteError> {
    to_unix_ms("current_time", SystemTime::now())
}

fn duration_to_i64(field: &'static str, duration: Duration) -> Result<i64, SqliteError> {
    i64::try_from(duration.as_millis()).map_err(|_| SqliteError::DurationOutOfRange { field })
}

fn usize_to_i64(field: &'static str, value: usize) -> Result<i64, SqliteError> {
    i64::try_from(value).map_err(|_| SqliteError::IntegerOutOfRange {
        field,
        value: value as u64,
    })
}

struct OrderedClaimedSyncObject {
    received_at_unix_ms: i64,
    object: ClaimedSyncObject,
}

fn parse_object_key(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<ObjectKey, SqliteError> {
    let value = read_string(row, column)?;
    ObjectKey::new(value).map_err(|e| invalid_sync_metadata(column, e))
}

fn parse_content_length(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<u64, SqliteError> {
    let value = row.try_get::<i64, _>(column).map_err(SqliteError::Sqlx)?;

    u64::try_from(value).map_err(|_| SqliteError::InvalidStoredSyncMetadata {
        column: column.to_owned(),
        reason: "must not be negative".to_owned(),
    })
}

fn read_string(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<String, SqliteError> {
    row.try_get::<String, _>(column).map_err(SqliteError::Sqlx)
}

fn parse_payload_representation(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<IngestPayloadRepresentation, SqliteError> {
    let value = read_string(row, "payload_representation")?;
    match value.as_str() {
        "dicom_file" => Ok(IngestPayloadRepresentation::DicomFile),
        "dicom_dataset" => Ok(IngestPayloadRepresentation::DicomDataSet),
        "dicomweb_metadata_and_bulk_data" => {
            Ok(IngestPayloadRepresentation::DicomWebMetadataAndBulkData)
        }
        "unknown" => Ok(IngestPayloadRepresentation::Unknown),
        other => Err(SqliteError::InvalidStoredSyncMetadata {
            column: "payload_representation".to_owned(),
            reason: format!("unsupported payload representation {other:?}"),
        }),
    }
}

fn invalid_sync_metadata(column: &str, err: impl std::fmt::Display) -> SqliteError {
    SqliteError::InvalidStoredSyncMetadata {
        column: column.to_owned(),
        reason: err.to_string(),
    }
}

fn sync_repository_error(message: &'static str) -> impl FnOnce(SqliteError) -> SyncRepositoryError {
    move |err| {
        Span::current().record("error.type", error_kind(&err));
        SyncRepositoryError::with_source(message, err)
    }
}
