use raccoon_adapter_read_sqlite::SqliteReadRepository;
use raccoon_platform_config::component::filesystem::FilesystemConfig;

use crate::error::OrchestrationError;

/// Build a [`SqliteReadRepository`] from the filesystem configuration.
///
/// The SQLite file is placed at `{filesystem.root}/read.db`.
pub async fn build_sqlite_read_repository(
    filesystem: &FilesystemConfig,
) -> Result<SqliteReadRepository, OrchestrationError> {
    tokio::fs::create_dir_all(&filesystem.root).await?;
    let path = filesystem.root.join("read.db");
    let url = format!("sqlite:{}", path.display());
    SqliteReadRepository::open(&url).await.map_err(Into::into)
}
