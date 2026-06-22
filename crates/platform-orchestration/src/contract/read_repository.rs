use std::sync::Arc;

use raccoon_platform_config::component::database::DatabaseConfig;
use raccoon_platform_config::component::filesystem::FilesystemConfig;
use raccoon_service_query::QueryRepository;
use raccoon_service_retrieve::{MetadataRepository, RetrieveRepository};
use raccoon_service_sync::SyncReadModelWriter;

use crate::adapter::read_postgres::build_postgres_read_repository;
use crate::adapter::read_sqlite::build_sqlite_read_repository;
use crate::error::OrchestrationError;

/// Shared query repository handle.
pub type QueryRepositoryHandle = Arc<dyn QueryRepository>;

/// Shared retrieve repository handle.
pub type RetrieveRepositoryHandle = Arc<dyn RetrieveRepository>;

/// Shared metadata repository handle.
pub type MetadataRepositoryHandle = Arc<dyn MetadataRepository>;

/// Shared sync read-model writer handle.
pub type SyncReadModelWriterHandle = Arc<dyn SyncReadModelWriter>;

/// Contract-facing handles backed by the configured read repository.
pub struct ReadRepositoryHandles {
    /// Read-side query repository.
    pub query_repository: QueryRepositoryHandle,

    /// Read-side retrieve repository.
    pub retrieve_repository: RetrieveRepositoryHandle,

    /// Read-side metadata repository.
    pub metadata_repository: MetadataRepositoryHandle,

    /// Writer used by sync workers to update the read model.
    pub sync_read_model_writer: SyncReadModelWriterHandle,
}

/// Build read-side repository contract handles from loaded configuration.
pub async fn build_read_repository_handles(
    database: &DatabaseConfig,
    filesystem: &FilesystemConfig,
) -> Result<ReadRepositoryHandles, OrchestrationError> {
    match database {
        DatabaseConfig::Sqlite => {
            let repository = Arc::new(build_sqlite_read_repository(filesystem).await?);
            Ok(ReadRepositoryHandles {
                query_repository: repository.clone(),
                retrieve_repository: repository.clone(),
                metadata_repository: repository.clone(),
                sync_read_model_writer: repository,
            })
        }
        DatabaseConfig::PostgreSql { url } => {
            let repository = Arc::new(build_postgres_read_repository(url).await?);
            Ok(ReadRepositoryHandles {
                query_repository: repository.clone(),
                retrieve_repository: repository.clone(),
                metadata_repository: repository.clone(),
                sync_read_model_writer: repository,
            })
        }
    }
}
