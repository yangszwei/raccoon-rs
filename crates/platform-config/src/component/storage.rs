use serde::Deserialize;

/// Object storage backend selection.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Object storage backend.
    pub backend: StorageBackend,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Filesystem,
        }
    }
}

/// Supported object storage backends.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBackend {
    /// Local filesystem object storage.
    #[default]
    Filesystem,
}

#[cfg(test)]
mod tests {
    use super::{StorageBackend, StorageConfig};

    #[test]
    fn default_storage_uses_filesystem_backend() {
        let config = StorageConfig::default();

        assert_eq!(config.backend, StorageBackend::Filesystem);
    }
}
