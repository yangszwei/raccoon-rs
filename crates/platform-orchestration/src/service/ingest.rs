use std::sync::Arc;

use raccoon_service_ingest::{InMemoryIngestService, IngestRepository, IngestService};

use crate::contract::object_store::ObjectStoreHandle;

/// Build an in-memory ingest service from a pre-built object store and repository.
pub fn build_ingest_service(
    object_store: ObjectStoreHandle,
    repository: Arc<dyn IngestRepository>,
) -> Arc<dyn IngestService> {
    Arc::new(InMemoryIngestService::new(object_store, repository))
}
