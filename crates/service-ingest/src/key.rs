use raccoon_contract_object_store::{ObjectKey, ObjectKeyError};

use crate::IngestObjectId;

/// Builds stable staging object keys for ingest objects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IngestObjectKeyBuilder {
    prefix: String,
}

impl Default for IngestObjectKeyBuilder {
    fn default() -> Self {
        Self {
            prefix: "ingest".to_string(),
        }
    }
}

impl IngestObjectKeyBuilder {
    /// Creates a key builder that stores staged DICOM objects under `ingest/`.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Builds `ingest/{ingest_object_id}` for accepted objects.
    pub(crate) fn build_accepted(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::new(format!("{}/{}", self.prefix, ingest_object_id))
    }

    #[cfg(test)]
    pub(crate) fn build(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        self.build_accepted(ingest_object_id)
    }

    /// Builds `ingest/quarantine/{ingest_object_id}` for retained rejection bytes.
    pub(crate) fn build_quarantine(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::new(format!("{}/quarantine/{}", self.prefix, ingest_object_id))
    }
}
