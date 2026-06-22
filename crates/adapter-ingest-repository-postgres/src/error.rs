use thiserror::Error;

/// Error returned by [`PostgresIngestRepository::open`].
#[derive(Debug, Error)]
pub enum PostgresIngestRepositoryError {
    #[error("failed to connect to Postgres ingest repository")]
    Connect(#[source] sqlx::Error),

    #[error("failed to run Postgres ingest repository migrations")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

/// Internal error used during repository operations.
///
/// Never crosses the public boundary. It is mapped to the service repository
/// error type before returning from trait methods.
#[derive(Debug, Error)]
pub(crate) enum PostgresError {
    #[error("postgres operation failed")]
    Sqlx(#[source] sqlx::Error),

    #[error("{field} value {value} is outside Postgres BIGINT range")]
    IntegerOutOfRange { field: &'static str, value: u64 },

    #[error("{field} is before the Unix epoch")]
    TimeBeforeUnixEpoch { field: &'static str },

    #[error("{field} Unix epoch milliseconds are outside Postgres BIGINT range")]
    TimeOutOfRange { field: &'static str },

    #[error("{field} duration milliseconds are outside Postgres BIGINT range")]
    DurationOutOfRange { field: &'static str },

    #[error("invalid stored sync metadata in column '{column}': {reason}")]
    InvalidStoredSyncMetadata { column: String, reason: String },

    #[error("sync claim token no longer owns a pending object")]
    StaleSyncClaim,
}

/// Returns the `error.type` attribute value for the given internal error.
pub(crate) fn error_kind(err: &PostgresError) -> &'static str {
    match err {
        PostgresError::Sqlx(_) => "sqlx::Error",
        PostgresError::IntegerOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_postgres::IntegerOutOfRange"
        }
        PostgresError::TimeBeforeUnixEpoch { .. } => {
            "raccoon_adapter_ingest_repository_postgres::TimeBeforeUnixEpoch"
        }
        PostgresError::TimeOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_postgres::TimeOutOfRange"
        }
        PostgresError::DurationOutOfRange { .. } => {
            "raccoon_adapter_ingest_repository_postgres::DurationOutOfRange"
        }
        PostgresError::InvalidStoredSyncMetadata { .. } => {
            "raccoon_adapter_ingest_repository_postgres::InvalidStoredSyncMetadata"
        }
        PostgresError::StaleSyncClaim => {
            "raccoon_adapter_ingest_repository_postgres::StaleSyncClaim"
        }
    }
}
