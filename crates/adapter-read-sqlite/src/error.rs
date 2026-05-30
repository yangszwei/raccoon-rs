use raccoon_service_query::QueryRepositoryError;
use thiserror::Error;

/// Errors produced by [`crate::SqliteReadRepository`].
#[derive(Debug, Error)]
pub enum SqliteReadRepositoryError {
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

    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<SqliteReadRepositoryError> for QueryRepositoryError {
    fn from(err: SqliteReadRepositoryError) -> Self {
        QueryRepositoryError::new(err.to_string())
    }
}
