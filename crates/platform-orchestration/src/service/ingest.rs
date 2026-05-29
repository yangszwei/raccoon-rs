use std::sync::Arc;

use raccoon_adapter_ingest_repository_sqlite::SqliteIngestRepository;
use raccoon_contract_object_store::ObjectStore;
use raccoon_service_ingest::{InMemoryIngestService, IngestService};

/// Build an in-memory ingest service from a pre-built object store and repository.
pub fn build_ingest_service(
    object_store: Arc<dyn ObjectStore>,
    repository: SqliteIngestRepository,
) -> Arc<dyn IngestService> {
    Arc::new(InMemoryIngestService::new(
        object_store,
        Arc::new(repository),
    ))
}
