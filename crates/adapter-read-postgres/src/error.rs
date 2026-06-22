use raccoon_service_query::QueryRepositoryError;
use raccoon_service_retrieve::RetrieveRepositoryError;
use thiserror::Error;

/// Errors produced by [`crate::PostgresReadRepository`].
#[derive(Debug, Error)]
pub enum PostgresReadRepositoryError {
    #[error("failed to connect to read database: {0}")]
    Connect(#[source] sqlx::Error),

    #[error("failed to run read database migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("query execution failed: {0}")]
    Query(#[source] sqlx::Error),

    #[error("failed to read column '{0}' from result row: {1}")]
    RowRead(String, #[source] sqlx::Error),

    #[error("invalid predicate: {0}")]
    InvalidPredicate(String),

    #[error("invalid stored retrieve metadata in column '{column}': {reason}")]
    InvalidStoredRetrieveMetadata { column: String, reason: String },

    #[error("{field} value {value} is outside Postgres BIGINT range")]
    IntegerOutOfRange { field: &'static str, value: u64 },

    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<PostgresReadRepositoryError> for QueryRepositoryError {
    fn from(err: PostgresReadRepositoryError) -> Self {
        QueryRepositoryError::new(err.to_string())
    }
}

impl From<PostgresReadRepositoryError> for RetrieveRepositoryError {
    fn from(err: PostgresReadRepositoryError) -> Self {
        let message = err.to_string();
        RetrieveRepositoryError::with_source(message, err)
    }
}
