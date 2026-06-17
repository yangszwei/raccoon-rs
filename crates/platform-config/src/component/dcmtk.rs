//! DCMTK external toolchain configuration.

use std::path::PathBuf;

use serde::Deserialize;

/// DCMTK command-line tool configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DcmtkConfig {
    /// Directory containing DCMTK executables such as `dcm2img`.
    pub path: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use config::Config;

    use super::DcmtkConfig;

    #[test]
    fn default_path_is_unset() {
        let config = DcmtkConfig::default();

        assert_eq!(config.path, None);
    }

    #[test]
    fn deserializes_bin_directory_path() {
        let config: DcmtkConfig = Config::builder()
            .add_source(config::File::from_str(
                r#"
                path = "/usr/local/bin"
                "#,
                config::FileFormat::Toml,
            ))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.path, Some(PathBuf::from("/usr/local/bin")));
    }
}
