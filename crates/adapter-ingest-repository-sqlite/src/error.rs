use thiserror::Error;

/// Error returned by [`SqliteIngestRepository::open`].
#[derive(Debug, Error)]
pub enum SqliteIngestRepositoryError {
    #[error("failed to connect to SQLite ingest repository")]
    Connect(#[source] sqlx::Error),

    #[error("failed to run SQLite ingest repository migrations")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

/// Internal error used during repository operations.
///
/// Never crosses the public boundary — it is mapped to
/// [`IngestRepositoryError`][raccoon_service_ingest::IngestRepositoryError]
/// before returning from the trait method.
#[derive(Debug, Error)]
pub(crate) enum SqliteError {
    #[error("sqlite operation failed")]
    Sqlx(#[source] sqlx::Error),

    #[error("{field} value {value} is outside SQLite INTEGER range")]
    IntegerOutOfRange { field: &'static str, value: u64 },

    #[error("{field} is before the Unix epoch")]
    TimeBeforeUnixEpoch { field: &'static str },

    #[error("{field} Unix epoch milliseconds are outside SQLite INTEGER range")]
    TimeOutOfRange { field: &'static str },

    #[error("{field} duration milliseconds are outside SQLite INTEGER range")]
    DurationOutOfRange { field: &'static str },

    #[error("invalid stored sync metadata in column '{column}': {reason}")]
    InvalidStoredSyncMetadata { column: String, reason: String },

    #[error("sync claim token no longer owns a pending object")]
    StaleSyncClaim,
}

/// Returns the `error.type` attribute value for the given internal error.
///
/// Used to populate the OpenTelemetry `error.type` span attribute without
/// allocating a string for the happy path.
pub(crate) fn error_kind(err: &SqliteError) -> &'static str {
    match err {
        SqliteError::Sqlx(_) => "sqlx::Error",
        SqliteError::IntegerOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_sqlite::IntegerOutOfRange"
        }
        SqliteError::TimeBeforeUnixEpoch { .. } => {
            "raccoon_adapter_ingest_repository_sqlite::TimeBeforeUnixEpoch"
        }
        SqliteError::TimeOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_sqlite::TimeOutOfRange"
        }
        SqliteError::DurationOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_sqlite::DurationOutOfRange"
        }
        SqliteError::InvalidStoredSyncMetadata { .. } => {
            "raccoon_adapter_ingest_repository_sqlite::InvalidStoredSyncMetadata"
        }
        SqliteError::StaleSyncClaim => "raccoon_adapter_ingest_repository_sqlite::StaleSyncClaim",
    }
}
