use std::sync::Arc;

use raccoon_service_sync::{
    StandardSyncService, SyncQuarantineRepository, SyncReadModelWriter, SyncService,
    SyncSourceRepository,
};

use crate::contract::object_store::ObjectStoreHandle;

/// Build a polling sync service from the ingest and read-side repositories.
pub fn build_sync_service(
    source_repository: Arc<dyn SyncSourceRepository>,
    read_model_writer: Arc<dyn SyncReadModelWriter>,
    quarantine_repository: Arc<dyn SyncQuarantineRepository>,
    object_store: ObjectStoreHandle,
    quarantine_object_store: ObjectStoreHandle,
) -> Arc<dyn SyncService> {
    Arc::new(StandardSyncService::new(
        source_repository,
        read_model_writer,
        quarantine_repository,
        object_store,
        quarantine_object_store,
    ))
}
