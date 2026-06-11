use async_trait::async_trait;
use raccoon_contract_dicom::{PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

use crate::error::RetrieveRepositoryError;
use crate::model::InstanceRef;

/// Read-side repository contract for DICOM instance location lookup.
///
/// Implementors resolve a retrieve scope to the set of [`InstanceRef`] records
/// that describe where each matching instance is stored. The service then
/// fetches each body from the object store using the returned keys.
///
/// All methods return an empty `Vec` when the scope matches no stored
/// instances. A missing study, series, or instance is **not an error** at
/// this layer — implementations must return `Ok(vec![])`, never `Err`, for a
/// resource that simply does not exist. Returning `Err` for a missing resource
/// is a contract violation: the service propagates repository errors as
/// [`RetrieveError::Repository`], causing the bridge to report a protocol-level
/// Failure status instead of the correct Success-with-zero-sub-operations
/// (PS3.4 C.4.1.2.1). Reserve `Err` for genuine infrastructure failures
/// (connection lost, query timeout, schema mismatch, etc.).
///
/// [`RetrieveError::Repository`]: crate::RetrieveError
#[async_trait]
pub trait RetrieveRepository: Send + Sync {
    /// Returns all instances belonging to the given patient.
    ///
    /// Used for Patient Root C-MOVE/C-GET at the PATIENT level (PS3.4
    /// Table C.4.1-1). Study Root and WADO-RS do not use patient-level
    /// retrieval.
    async fn find_instances_for_patient(
        &self,
        patient_id: &PatientId,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError>;

    /// Returns all instances belonging to the given study.
    async fn find_instances_for_study(
        &self,
        uid: &StudyInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError>;

    /// Returns all instances belonging to the given series.
    async fn find_instances_for_series(
        &self,
        uid: &SeriesInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError>;

    /// Returns all instances belonging to the given series within the given
    /// study.
    async fn find_instances_for_study_series(
        &self,
        study_uid: &StudyInstanceUid,
        series_uid: &SeriesInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        Ok(self
            .find_instances_for_series(series_uid)
            .await?
            .into_iter()
            .filter(|ref_| &ref_.identity.study_instance_uid == study_uid)
            .collect())
    }

    /// Returns the instance with the given SOP Instance UID, or `None` if it
    /// is not stored.
    async fn find_instance(
        &self,
        uid: &SopInstanceUid,
    ) -> Result<Option<InstanceRef>, RetrieveRepositoryError>;

    /// Returns the instance with the given SOP Instance UID constrained to its
    /// parent study and series when supplied.
    async fn find_instance_in_scope(
        &self,
        study_uid: Option<&StudyInstanceUid>,
        series_uid: Option<&SeriesInstanceUid>,
        sop_uid: &SopInstanceUid,
    ) -> Result<Option<InstanceRef>, RetrieveRepositoryError> {
        Ok(self.find_instance(sop_uid).await?.filter(|ref_| {
            study_uid.is_none_or(|uid| ref_.identity.study_instance_uid == *uid)
                && series_uid.is_none_or(|uid| ref_.identity.series_instance_uid == *uid)
        }))
    }
}
