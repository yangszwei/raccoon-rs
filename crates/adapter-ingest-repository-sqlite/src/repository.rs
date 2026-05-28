use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use raccoon_service_ingest::{
    IngestObjectOutcome, IngestRepository, IngestRepositoryError, ReceivedIngestObject,
};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tracing::{Span, instrument};

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

impl SqliteIngestRepository {
    async fn try_record_received_objects(
        &self,
        records: &[ReceivedIngestObject],
    ) -> Result<(), SqliteError> {
        let mut tx = self.pool.begin().await.map_err(SqliteError::Sqlx)?;

        for record in records {
            insert_record(&mut tx, record).await?;
        }

        tx.commit().await.map_err(SqliteError::Sqlx)
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
