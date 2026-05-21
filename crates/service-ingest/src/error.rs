use raccoon_contract_object_store::{ObjectKey, ObjectKeyError, ObjectStoreError};
use thiserror::Error;

use crate::{IngestObjectId, IngestObjectOutcome};

/// Repository-layer failure abstracted from concrete database adapters.
#[derive(Debug, Error)]
#[error("ingest repository error: {message}")]
pub struct IngestRepositoryError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl IngestRepositoryError {
    /// Creates a repository error without a source.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Creates a repository error with a source.
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

/// Ingest service failure.
#[derive(Debug, Error)]
pub enum IngestError {
    #[error("failed to build ingest object key for {ingest_object_id}: {source}")]
    ObjectKey {
        ingest_object_id: IngestObjectId,
        #[source]
        source: ObjectKeyError,
    },

    #[error("object store failed for {ingest_object_id} at {object_key}: {source}")]
    ObjectStore {
        ingest_object_id: IngestObjectId,
        object_key: ObjectKey,
        #[source]
        source: ObjectStoreError,
    },

    #[error(
        "ingest object {ingest_object_id} exceeds configured maximum of {max_content_length} bytes"
    )]
    ContentTooLarge {
        ingest_object_id: IngestObjectId,
        max_content_length: u64,
    },
}

impl IngestError {
    /// Converts the error into a protocol-neutral per-object outcome.
    pub fn outcome(&self) -> IngestObjectOutcome {
        match self {
            Self::ObjectKey { source, .. } => IngestObjectOutcome::RejectedCannotUnderstand {
                reason: source.to_string(),
            },
            Self::ObjectStore { source, .. } => IngestObjectOutcome::ObjectStoreFailed {
                reason: source.to_string(),
            },
            Self::ContentTooLarge {
                max_content_length, ..
            } => IngestObjectOutcome::RejectedTooLarge {
                max_content_length: *max_content_length,
            },
        }
    }
}
