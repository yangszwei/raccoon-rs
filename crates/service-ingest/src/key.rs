use raccoon_contract_object_store::{ObjectKey, ObjectKeyError};

use crate::IngestObjectId;

/// Builds stable staging object keys for ingest objects.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct IngestObjectKeyBuilder;

impl IngestObjectKeyBuilder {
    /// Creates a key builder for object-store-relative ingest keys.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Builds `{ingest_object_id}` for accepted objects.
    pub(crate) fn build_accepted(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::new(ingest_object_id.to_string())
    }

    #[cfg(test)]
    pub(crate) fn build(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        self.build_accepted(ingest_object_id)
    }

    /// Builds `{ingest_object_id}` for retained rejection bytes.
    pub(crate) fn build_quarantine(
        &self,
        ingest_object_id: &IngestObjectId,
    ) -> Result<ObjectKey, ObjectKeyError> {
        ObjectKey::new(ingest_object_id.to_string())
    }
}
