//! Reusable DICOMweb HTTP listener configuration.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

const DEFAULT_DICOMWEB_BIND_ADDRESS: &str = "127.0.0.1:8080";
const DEFAULT_DICOMWEB_BASE_PATH: &str = "/dicom-web";
const DEFAULT_RENDER_CACHE_TTL_SECONDS: u64 = 86_400;
const DEFAULT_RENDER_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_RENDERED_MEDIA_TYPE: &str = "image/jpeg";

/// DICOMweb HTTP server and provider settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DicomWebConfig {
    /// Socket address where the DICOMweb HTTP server accepts requests.
    pub bind_address: String,

    /// Base path where QIDO-RS, WADO-RS, STOW-RS, and WADO-URI routes mount.
    pub base_path: String,

    /// Rendered WADO-RS cache and default media settings.
    pub render_cache: RenderCacheConfig,
}

impl Default for DicomWebConfig {
    fn default() -> Self {
        Self {
            bind_address: DEFAULT_DICOMWEB_BIND_ADDRESS.to_string(),
            base_path: DEFAULT_DICOMWEB_BASE_PATH.to_string(),
            render_cache: RenderCacheConfig::default(),
        }
    }
}

/// Filesystem cache settings for rendered WADO-RS responses.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct RenderCacheConfig {
    /// Enables filesystem caching for rendered image responses.
    pub enabled: bool,

    /// Cache directory. Relative paths are resolved by the process cwd.
    pub directory: PathBuf,

    /// Optional time-to-live in seconds for cache entries.
    pub ttl_seconds: Option<u64>,

    /// Optional maximum total cache size in bytes.
    pub max_bytes: Option<u64>,

    /// Default rendered media type when clients do not constrain `Accept`.
    pub default_rendered_media_type: String,
}

impl RenderCacheConfig {
    /// Return the configured cache TTL.
    pub fn ttl(&self) -> Option<Duration> {
        self.ttl_seconds.map(Duration::from_secs)
    }
}

impl Default for RenderCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            directory: PathBuf::from("data/dicomweb-render-cache"),
            ttl_seconds: Some(DEFAULT_RENDER_CACHE_TTL_SECONDS),
            max_bytes: Some(DEFAULT_RENDER_CACHE_MAX_BYTES),
            default_rendered_media_type: DEFAULT_RENDERED_MEDIA_TYPE.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use config::Config;

    use super::DicomWebConfig;

    #[test]
    fn defaults_match_dicomweb_listener_and_render_cache() {
        let config = DicomWebConfig::default();

        assert_eq!(config.bind_address, "127.0.0.1:8080");
        assert_eq!(config.base_path, "/dicom-web");
        assert!(config.render_cache.enabled);
        assert_eq!(
            config.render_cache.directory,
            PathBuf::from("data/dicomweb-render-cache")
        );
        assert_eq!(config.render_cache.ttl(), Some(Duration::from_secs(86_400)));
        assert_eq!(config.render_cache.max_bytes, Some(512 * 1024 * 1024));
        assert_eq!(
            config.render_cache.default_rendered_media_type,
            "image/jpeg"
        );
    }

    #[test]
    fn deserializes_nested_render_cache_settings() {
        let config: DicomWebConfig = Config::builder()
            .add_source(config::File::from_str(
                r#"
                bind_address = "127.0.0.1:18080"
                base_path = "/pacs/dicom-web"

                [render_cache]
                enabled = false
                directory = "/var/cache/raccoon/rendered"
                ttl_seconds = 60
                max_bytes = 2048
                default_rendered_media_type = "image/png"
                "#,
                config::FileFormat::Toml,
            ))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        assert_eq!(config.bind_address, "127.0.0.1:18080");
        assert_eq!(config.base_path, "/pacs/dicom-web");
        assert!(!config.render_cache.enabled);
        assert_eq!(
            config.render_cache.directory,
            PathBuf::from("/var/cache/raccoon/rendered")
        );
        assert_eq!(config.render_cache.ttl(), Some(Duration::from_secs(60)));
        assert_eq!(config.render_cache.max_bytes, Some(2048));
        assert_eq!(config.render_cache.default_rendered_media_type, "image/png");
    }
}
