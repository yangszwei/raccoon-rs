use std::path::PathBuf;
use std::sync::Arc;

use raccoon_contract_object_store::ObjectStore;
use raccoon_platform_config::component::storage::{StorageBackend, StorageConfig};

use crate::adapter::object_store_filesystem::build_filesystem_object_store;

/// Shared object store handle used by service wiring.
pub type ObjectStoreHandle = Arc<dyn ObjectStore>;

/// Build the configured object store implementation.
pub fn build_object_store(storage: &StorageConfig, root: impl Into<PathBuf>) -> ObjectStoreHandle {
    match storage.backend {
        StorageBackend::Filesystem => Arc::new(build_filesystem_object_store(root)),
    }
}
