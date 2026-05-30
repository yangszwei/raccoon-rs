use thiserror::Error;

/// Repository-layer failure abstracted from concrete database adapters.
#[derive(Debug, Error)]
#[error("query repository error: {message}")]
pub struct QueryRepositoryError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl QueryRepositoryError {
    /// Creates a repository error without a source.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a repository error with a causal source.
    pub fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Query service failure.
#[derive(Debug, Error)]
pub enum QueryError {
    /// The query request was structurally invalid.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// The repository returned an error.
    #[error(transparent)]
    Repository(#[from] QueryRepositoryError),
}

impl QueryError {
    pub fn invalid_query(message: impl Into<String>) -> Self {
        Self::InvalidQuery(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_error_exposes_message() {
        let error = QueryRepositoryError::new("database offline");

        assert_eq!(error.message(), "database offline");
        assert_eq!(
            error.to_string(),
            "query repository error: database offline"
        );
    }

    #[test]
    fn repository_error_with_source_chains_cause() {
        use std::error::Error;

        let cause = std::io::Error::other("connection refused");
        let error = QueryRepositoryError::with_source("database offline", cause);

        assert!(error.source().is_some());
    }

    #[test]
    fn query_error_invalid_query_formats_message() {
        let error = QueryError::invalid_query("projection must not be empty");

        assert_eq!(
            error.to_string(),
            "invalid query: projection must not be empty"
        );
    }

    #[test]
    fn query_error_converts_from_repository_error() {
        let repo_error = QueryRepositoryError::new("db down");
        let error: QueryError = repo_error.into();

        assert!(matches!(error, QueryError::Repository(_)));
    }
}
