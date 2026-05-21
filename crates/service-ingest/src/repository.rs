use async_trait::async_trait;

use crate::{IngestRepositoryError, ReceivedIngestObject};

/// Write-side repository contract for persisted ingest metadata.
///
/// Adapters must persist every returned [`ReceivedIngestObject`], including
/// quarantined records. Quarantined records point at retained bytes under the
/// quarantine object key and carry the rejection outcome that withheld the object
/// from normal sync.
#[async_trait]
pub trait IngestRepository: Send + Sync {
    /// Records received objects in one repository operation.
    async fn record_received_objects(
        &self,
        records: &[ReceivedIngestObject],
    ) -> Result<(), IngestRepositoryError>;

    /// Records one received object.
    async fn record_received_object(
        &self,
        record: &ReceivedIngestObject,
    ) -> Result<(), IngestRepositoryError> {
        self.record_received_objects(std::slice::from_ref(record))
            .await
    }
}
