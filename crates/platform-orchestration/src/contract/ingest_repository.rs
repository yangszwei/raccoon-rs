use std::sync::Arc;

use raccoon_platform_config::component::filesystem::FilesystemConfig;
use raccoon_service_ingest::IngestRepository;
use raccoon_service_sync::{SyncQuarantineRepository, SyncSourceRepository};

use crate::adapter::ingest_repository_sqlite::build_sqlite_ingest_repository;
use crate::error::OrchestrationError;

/// Shared ingest write-side repository handle.
pub type IngestRepositoryHandle = Arc<dyn IngestRepository>;

/// Shared sync source repository handle.
pub type SyncSourceRepositoryHandle = Arc<dyn SyncSourceRepository>;

/// Shared sync quarantine repository handle.
pub type SyncQuarantineRepositoryHandle = Arc<dyn SyncQuarantineRepository>;

/// Contract-facing handles backed by the configured ingest repository.
pub struct IngestRepositoryHandles {
    /// Write-side ingest metadata repository.
    pub ingest_repository: IngestRepositoryHandle,

    /// Source repository used by sync workers.
    pub sync_source_repository: SyncSourceRepositoryHandle,

    /// Quarantine repository used by sync workers.
    pub sync_quarantine_repository: SyncQuarantineRepositoryHandle,
}

/// Build ingest-side repository contract handles from loaded configuration.
pub async fn build_ingest_repository_handles(
    filesystem: &FilesystemConfig,
) -> Result<IngestRepositoryHandles, OrchestrationError> {
    let repository = Arc::new(build_sqlite_ingest_repository(filesystem).await?);

    Ok(IngestRepositoryHandles {
        ingest_repository: repository.clone(),
        sync_source_repository: repository.clone(),
        sync_quarantine_repository: repository,
    })
}
