use std::path::PathBuf;

use raccoon_adapter_object_store_filesystem::FsObjectStore;

/// Build a filesystem-backed object store rooted at `root`.
pub fn build_filesystem_object_store(root: impl Into<PathBuf>) -> FsObjectStore {
    FsObjectStore::new(root)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn build_filesystem_object_store_uses_root() {
        let root = PathBuf::from("objects/ingest");

        let store = build_filesystem_object_store(root.clone());

        assert_eq!(store.root(), root);
    }
}
