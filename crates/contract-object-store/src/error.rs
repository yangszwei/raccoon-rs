use thiserror::Error;

use crate::ObjectKey;

/// Object storage contract error.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// The requested object does not exist.
    #[error("object not found: {key}")]
    NotFound {
        /// Missing object key.
        key: ObjectKey,
    },
    /// The request is not valid for the object store.
    #[error("invalid object store request: {message}")]
    InvalidRequest {
        /// Human-readable validation message.
        message: String,
    },
    /// The backing store denied access.
    #[error("object store permission denied: {message}")]
    PermissionDenied {
        /// Human-readable permission error message.
        message: String,
        /// Optional source error from the adapter.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    /// The backing store is unavailable or out of capacity.
    #[error("object store unavailable: {message}")]
    Unavailable {
        /// Whether retrying the operation may succeed.
        transient: bool,
        /// Human-readable availability error message.
        message: String,
        /// Optional source error from the adapter.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    /// The backing store contains an unexpected or unsafe state.
    #[error("object store integrity error: {message}")]
    Integrity {
        /// Human-readable integrity error message.
        message: String,
    },
    /// The backing store failed.
    #[error("object store backend error: {message}")]
    Backend {
        /// Human-readable backend error message.
        message: String,
        /// Optional source error from the adapter.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

impl ObjectStoreError {
    /// Creates a not-found error for a key.
    pub fn not_found(key: ObjectKey) -> Self {
        Self::NotFound { key }
    }

    /// Creates an invalid request error.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    /// Creates a permission-denied error with a source.
    pub fn permission_denied_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::PermissionDenied {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Creates an unavailable error with a source.
    pub fn unavailable_with_source(
        message: impl Into<String>,
        transient: bool,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Unavailable {
            transient,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Creates an integrity error.
    pub fn integrity(message: impl Into<String>) -> Self {
        Self::Integrity {
            message: message.into(),
        }
    }

    /// Creates a backend error without a source.
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a backend error with a source.
    pub fn backend_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Backend {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}
