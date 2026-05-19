use std::path::PathBuf;

use serde::Deserialize;

const DEFAULT_ROOT: &str = "data";

/// Local filesystem storage settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct FilesystemConfig {
    /// Root directory used for local object storage.
    pub root: PathBuf,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from(DEFAULT_ROOT),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{DEFAULT_ROOT, FilesystemConfig};

    #[test]
    fn default_root_matches_monolith_config_example() {
        let config = FilesystemConfig::default();

        assert_eq!(config.root, PathBuf::from(DEFAULT_ROOT));
    }
}
