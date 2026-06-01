use raccoon_adapter_ingest_repository_sqlite::SqliteIngestRepository;
use raccoon_platform_config::component::filesystem::FilesystemConfig;

use crate::component::database::ingest_database_path;
use crate::error::OrchestrationError;

/// Build a [`SqliteIngestRepository`] from the filesystem configuration.
///
/// The SQLite file is placed at `{filesystem.root}/ingest/ingest.db`.
pub async fn build_sqlite_ingest_repository(
    filesystem: &FilesystemConfig,
) -> Result<SqliteIngestRepository, OrchestrationError> {
    let path = ingest_database_path(filesystem);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let url = format!("sqlite:{}", path.display());
    SqliteIngestRepository::open(&url).await.map_err(Into::into)
}
