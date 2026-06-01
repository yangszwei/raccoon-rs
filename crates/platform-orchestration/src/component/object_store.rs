use std::path::PathBuf;

use raccoon_platform_config::component::filesystem::FilesystemConfig;

/// Returns the object-store root for accepted ingest bytes.
pub fn ingest_object_store_root(filesystem: &FilesystemConfig) -> PathBuf {
    filesystem.root.join("objects").join("ingest")
}

/// Returns the object-store root for retained quarantine bytes.
pub fn quarantine_object_store_root(filesystem: &FilesystemConfig) -> PathBuf {
    filesystem.root.join("objects").join("quarantine")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn object_store_roots_are_scoped_under_objects() {
        let filesystem = FilesystemConfig {
            root: PathBuf::from("data"),
        };

        assert_eq!(
            ingest_object_store_root(&filesystem),
            PathBuf::from("data/objects/ingest")
        );
        assert_eq!(
            quarantine_object_store_root(&filesystem),
            PathBuf::from("data/objects/quarantine")
        );
    }
}
