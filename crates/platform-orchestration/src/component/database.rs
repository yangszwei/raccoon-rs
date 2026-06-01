use std::path::PathBuf;

use raccoon_platform_config::component::filesystem::FilesystemConfig;

/// Returns the SQLite database path for ingest write-side metadata.
pub fn ingest_database_path(filesystem: &FilesystemConfig) -> PathBuf {
    filesystem.root.join("ingest").join("ingest.db")
}

/// Returns the SQLite database path for read-side query/retrieve metadata.
pub fn read_database_path(filesystem: &FilesystemConfig) -> PathBuf {
    filesystem.root.join("read").join("read.db")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn database_paths_are_scoped_by_model() {
        let filesystem = FilesystemConfig {
            root: PathBuf::from("data"),
        };

        assert_eq!(
            ingest_database_path(&filesystem),
            PathBuf::from("data/ingest/ingest.db")
        );
        assert_eq!(
            read_database_path(&filesystem),
            PathBuf::from("data/read/read.db")
        );
    }
}
