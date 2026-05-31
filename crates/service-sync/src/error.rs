use raccoon_contract_object_store::{ObjectKey, ObjectKeyError, ObjectStoreError};
use thiserror::Error;

use crate::model::{SyncClaimToken, SyncWorkerId};

/// Repository-layer failure abstracted from concrete sync adapters.
#[derive(Debug, Error)]
#[error("sync repository error: {message}")]
pub struct SyncRepositoryError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl SyncRepositoryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

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

/// Terminal DICOM parse or validation failure.
#[derive(Debug, Error)]
pub enum SyncParseError {
    #[error("cannot understand DICOM object: {reason}")]
    CannotUnderstand { reason: String },

    #[error("DICOM object failed sync validation: {reason}")]
    Validation { reason: String },

    #[error("DICOM parser task failed: {0}")]
    ParserTask(String),

    #[error("DICOM metadata exceeded configured maximum of {max_metadata_bytes} bytes")]
    MetadataTooLarge { max_metadata_bytes: u64 },
}

impl SyncParseError {
    pub fn cannot_understand(reason: impl Into<String>) -> Self {
        Self::CannotUnderstand {
            reason: reason.into(),
        }
    }

    pub fn validation(reason: impl Into<String>) -> Self {
        Self::Validation {
            reason: reason.into(),
        }
    }

    pub fn reason(&self) -> String {
        match self {
            Self::CannotUnderstand { reason } | Self::Validation { reason } => reason.clone(),
            Self::ParserTask(reason) => reason.clone(),
            Self::MetadataTooLarge { max_metadata_bytes } => {
                format!("DICOM metadata exceeded configured maximum of {max_metadata_bytes} bytes")
            }
        }
    }
}

/// Terminal object failure that should be quarantined instead of retried.
#[derive(Debug, Error)]
pub enum SyncTerminalObjectError {
    #[error("cannot understand DICOM object: {reason}")]
    CannotUnderstand { reason: String },

    #[error("DICOM object failed sync validation: {reason}")]
    Validation { reason: String },

    #[error("object violates sync policy: {reason}")]
    Policy { reason: String },
}

impl SyncTerminalObjectError {
    pub fn reason(&self) -> String {
        match self {
            Self::CannotUnderstand { reason }
            | Self::Validation { reason }
            | Self::Policy { reason } => reason.clone(),
        }
    }
}

impl From<SyncParseError> for SyncTerminalObjectError {
    fn from(error: SyncParseError) -> Self {
        match error {
            SyncParseError::CannotUnderstand { reason } => Self::CannotUnderstand { reason },
            SyncParseError::Validation { reason } => Self::Validation { reason },
            SyncParseError::ParserTask(reason) => Self::CannotUnderstand { reason },
            SyncParseError::MetadataTooLarge { max_metadata_bytes } => Self::Policy {
                reason: format!(
                    "DICOM metadata exceeded configured maximum of {max_metadata_bytes} bytes"
                ),
            },
        }
    }
}

/// Failure while moving an object into sync quarantine.
#[derive(Debug, Error)]
pub enum QuarantineError {
    #[error("failed to build quarantine key: {0}")]
    ObjectKey(#[from] ObjectKeyError),

    #[error("object store failed while quarantining {object_key}: {source}")]
    ObjectStore {
        object_key: ObjectKey,
        #[source]
        source: ObjectStoreError,
    },

    #[error("quarantine repository failed: {0}")]
    Repository(#[from] SyncRepositoryError),
}

/// Sync service failure.
#[derive(Debug, Error)]
pub enum SyncError {
    #[error("source repository failed: {0}")]
    SourceRepository(#[source] SyncRepositoryError),

    #[error("read model writer failed: {0}")]
    ReadModel(#[source] SyncRepositoryError),

    #[error("failed to mark synced claim {claim_token} for worker {worker_id}: {source}")]
    MarkSynced {
        worker_id: SyncWorkerId,
        claim_token: SyncClaimToken,
        #[source]
        source: SyncRepositoryError,
    },

    #[error("quarantine failed: {0}")]
    Quarantine(#[from] QuarantineError),
}
