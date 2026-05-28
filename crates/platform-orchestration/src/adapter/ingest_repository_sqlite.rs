use raccoon_adapter_ingest_repository_sqlite::SqliteIngestRepository;
use raccoon_platform_config::component::filesystem::FilesystemConfig;

use crate::error::OrchestrationError;

/// Build a [`SqliteIngestRepository`] from the filesystem configuration.
///
/// The SQLite file is placed at `{filesystem.root}/ingest.db`.
pub async fn build_sqlite_ingest_repository(
    filesystem: &FilesystemConfig,
) -> Result<SqliteIngestRepository, OrchestrationError> {
    tokio::fs::create_dir_all(&filesystem.root).await?;
    let path = filesystem.root.join("ingest.db");
    let url = format!("sqlite:{}", path.display());
    SqliteIngestRepository::open(&url).await.map_err(Into::into)
}
