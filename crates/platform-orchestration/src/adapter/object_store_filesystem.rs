use raccoon_adapter_object_store_filesystem::FsObjectStore;
use raccoon_platform_config::component::filesystem::FilesystemConfig;

/// Build a filesystem-backed object store from loaded configuration.
pub fn build_filesystem_object_store(config: &FilesystemConfig) -> FsObjectStore {
    FsObjectStore::new(config.root.clone())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn build_filesystem_object_store_uses_configured_root() {
        let config = FilesystemConfig {
            root: PathBuf::from("objects"),
        };

        let store = build_filesystem_object_store(&config);

        assert_eq!(store.root(), PathBuf::from("objects"));
    }
}
