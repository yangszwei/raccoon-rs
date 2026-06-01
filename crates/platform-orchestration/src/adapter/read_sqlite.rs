use raccoon_adapter_read_sqlite::SqliteReadRepository;
use raccoon_platform_config::component::filesystem::FilesystemConfig;

use crate::component::database::read_database_path;
use crate::error::OrchestrationError;

/// Build a [`SqliteReadRepository`] from the filesystem configuration.
///
/// The SQLite file is placed at `{filesystem.root}/read/read.db`.
pub async fn build_sqlite_read_repository(
    filesystem: &FilesystemConfig,
) -> Result<SqliteReadRepository, OrchestrationError> {
    let path = read_database_path(filesystem);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let url = format!("sqlite:{}", path.display());
    SqliteReadRepository::open(&url).await.map_err(Into::into)
}
