use raccoon_contract_dicom::{PatientIdError, SopInstanceUid};
use raccoon_contract_object_store::ObjectStoreError;
use thiserror::Error;

/// Repository-layer failure abstracted from the concrete database adapter.
#[derive(Debug, Error)]
#[error("retrieve repository error: {message}")]
pub struct RetrieveRepositoryError {
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl RetrieveRepositoryError {
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

/// Retrieve service failure.
///
/// [`Repository`] surfaces from [`RetrieveService::retrieve`] before the
/// stream is returned (scope resolution failed). [`ObjectStore`] surfaces as
/// an item within the [`RetrieveStream`], allowing the bridge to report
/// per-instance failure counts without aborting the session.
///
/// [`InvalidRequest`] is produced by bridge code **before** calling
/// [`RetrieveService::retrieve`] — typically when constructing a [`PatientId`]
/// from a wire-frame value that is blank. Use `PatientIdError`'s `From` impl
/// to convert it: `PatientId::new(raw)?` in a function returning
/// `Result<_, RetrieveError>` will map `PatientIdError::Blank` here
/// automatically.
///
/// [`InvalidRequest`]: RetrieveError::InvalidRequest
/// [`Repository`]: RetrieveError::Repository
/// [`ObjectStore`]: RetrieveError::ObjectStore
/// [`RetrieveService::retrieve`]: crate::RetrieveService::retrieve
/// [`RetrieveStream`]: crate::RetrieveStream
/// [`PatientId`]: raccoon_contract_dicom::PatientId
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RetrieveError {
    /// The retrieve request was structurally invalid.
    ///
    /// Produced when a wire-frame patient ID is blank — see the enum-level
    /// doc for the intended usage pattern.
    #[error("invalid retrieve request: {0}")]
    InvalidRequest(String),

    #[error(transparent)]
    Repository(#[from] RetrieveRepositoryError),

    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    #[error("object store error for SOP Instance UID {sop_instance_uid}: {source}")]
    ObjectStoreForInstance {
        sop_instance_uid: SopInstanceUid,
        #[source]
        source: ObjectStoreError,
    },
}

impl From<PatientIdError> for RetrieveError {
    fn from(e: PatientIdError) -> Self {
        RetrieveError::InvalidRequest(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_error_exposes_message() {
        let error = RetrieveRepositoryError::new("database offline");

        assert_eq!(error.message(), "database offline");
        assert_eq!(
            error.to_string(),
            "retrieve repository error: database offline"
        );
    }

    #[test]
    fn repository_error_with_source_chains_cause() {
        use std::error::Error;

        let cause = std::io::Error::other("connection refused");
        let error = RetrieveRepositoryError::with_source("database offline", cause);

        assert!(error.source().is_some());
    }

    #[test]
    fn retrieve_error_converts_from_repository_error() {
        let repo_error = RetrieveRepositoryError::new("db down");
        let error: RetrieveError = repo_error.into();

        assert!(matches!(error, RetrieveError::Repository(_)));
    }

    #[test]
    fn retrieve_error_converts_from_object_store_error() {
        let store_error = ObjectStoreError::backend("read failed");
        let error: RetrieveError = store_error.into();

        assert!(matches!(error, RetrieveError::ObjectStore(_)));
    }

    #[test]
    fn retrieve_error_converts_from_patient_id_error() {
        let patient_id_error = PatientIdError::Blank;
        let error: RetrieveError = patient_id_error.into();

        assert!(matches!(error, RetrieveError::InvalidRequest(_)));
        assert!(error.to_string().contains("patient ID must not be blank"));
    }
}
