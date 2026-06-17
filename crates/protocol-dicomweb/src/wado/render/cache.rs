use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use raccoon_contract_object_store::Bytes;
use sha2::{Digest, Sha256};
use tokio::fs;

use super::{RenderError, RenderInput, RenderedImage};
use crate::media;

#[async_trait]
pub trait RenderCache: Send + Sync {
    async fn get(&self, key: &RenderCacheKey) -> Result<RenderCacheResult, RenderError>;
    async fn put(&self, key: &RenderCacheKey, image: &RenderedImage) -> Result<(), RenderError>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RenderCacheLookup {
    Hit,
    Miss,
    Bypass,
}

#[derive(Debug, Clone)]
pub struct RenderCacheResult {
    pub lookup: RenderCacheLookup,
    pub image: Option<RenderedImage>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenderCacheKey {
    value: String,
}

impl RenderCacheKey {
    pub fn new(input: &RenderInput, backend: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"raccoon-render-cache-v2;");
        hash_str(
            &mut hasher,
            "study",
            input.identity.study_instance_uid.as_str(),
        );
        hash_str(
            &mut hasher,
            "series",
            input.identity.series_instance_uid.as_str(),
        );
        hash_str(
            &mut hasher,
            "instance",
            input.identity.sop_instance_uid.as_str(),
        );
        hash_str(&mut hasher, "media_type", &input.media_type);
        hash_str(&mut hasher, "backend", backend);
        hash_str(&mut hasher, "thumbnail", &input.thumbnail.to_string());
        hash_frames(&mut hasher, input.frames.as_deref());
        hash_optional_str(
            &mut hasher,
            "transfer_syntax",
            input.transfer_syntax_uid.as_ref().map(|uid| uid.as_str()),
        );
        hasher.update(b"params={");
        input.params.hash_into(&mut hasher);
        hasher.update(b"};");
        let digest = hasher.finalize();
        Self {
            value: hex_lower(&digest),
        }
    }

    fn path(&self, directory: &Path, media_type: &str) -> PathBuf {
        let extension = match media_type {
            media::IMAGE_PNG => "png",
            _ => "jpg",
        };
        directory.join(format!("{}.{}", self.value, extension))
    }
}

fn hash_str(hasher: &mut Sha256, name: &str, value: &str) {
    hasher.update(name.as_bytes());
    hasher.update(b"=");
    hasher.update(value.len().to_string().as_bytes());
    hasher.update(b":");
    hasher.update(value.as_bytes());
    hasher.update(b";");
}

fn hash_optional_str(hasher: &mut Sha256, name: &str, value: Option<&str>) {
    match value {
        Some(value) => hash_str(hasher, name, value),
        None => {
            hasher.update(name.as_bytes());
            hasher.update(b"=-;");
        }
    }
}

fn hash_frames(hasher: &mut Sha256, frames: Option<&[u32]>) {
    hasher.update(b"frames=");
    if let Some(frames) = frames {
        for frame in frames {
            hasher.update(frame.to_string().as_bytes());
            hasher.update(b",");
        }
    } else {
        hasher.update(b"-");
    }
    hasher.update(b";");
}

impl RenderCacheKey {
    #[cfg(test)]
    fn as_str(&self) -> &str {
        &self.value
    }
}

#[cfg(test)]
fn legacy_unlabeled_cache_key(input: &RenderInput, backend: &str) -> RenderCacheKey {
    let mut hasher = Sha256::new();
    hasher.update(input.identity.study_instance_uid.as_str());
    hasher.update(b"|");
    hasher.update(input.identity.series_instance_uid.as_str());
    hasher.update(b"|");
    hasher.update(input.identity.sop_instance_uid.as_str());
    hasher.update(b"|");
    hasher.update(input.media_type.as_bytes());
    hasher.update(b"|");
    hasher.update(backend.as_bytes());
    hasher.update(b"|");
    hasher.update(input.thumbnail.to_string().as_bytes());
    hasher.update(b"|");
    for frame in input.frames.iter().flatten() {
        hasher.update(frame.to_string().as_bytes());
        hasher.update(b",");
    }
    hasher.update(b"|");
    if let Some(transfer_syntax_uid) = &input.transfer_syntax_uid {
        hasher.update(transfer_syntax_uid.as_str());
    }
    hasher.update(b"|");
    input.params.hash_into(&mut hasher);
    let digest = hasher.finalize();
    RenderCacheKey {
        value: hex_lower(&digest),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[derive(Debug, Clone)]
pub struct RenderCacheConfig {
    pub directory: PathBuf,
    pub ttl: Option<Duration>,
    pub max_bytes: Option<u64>,
}

pub struct FilesystemRenderCache {
    config: RenderCacheConfig,
}

impl FilesystemRenderCache {
    pub fn new(config: RenderCacheConfig) -> Self {
        Self { config }
    }

    async fn cleanup(&self) -> Result<(), RenderError> {
        fs::create_dir_all(&self.config.directory)
            .await
            .map_err(|error| RenderError::Failed(format!("render cache setup failed: {error}")))?;

        let mut entries = Vec::new();
        let mut reader = fs::read_dir(&self.config.directory)
            .await
            .map_err(|error| RenderError::Failed(format!("render cache scan failed: {error}")))?;
        while let Some(entry) = reader.next_entry().await.map_err(|error| {
            RenderError::Failed(format!("render cache entry scan failed: {error}"))
        })? {
            let metadata = entry.metadata().await.map_err(|error| {
                RenderError::Failed(format!("render cache metadata failed: {error}"))
            })?;
            if !metadata.is_file() {
                continue;
            }
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if self
                .config
                .ttl
                .is_some_and(|ttl| modified.elapsed().is_ok_and(|age| age > ttl))
            {
                let _ = fs::remove_file(entry.path()).await;
                continue;
            }
            entries.push((entry.path(), metadata.len(), modified));
        }

        if let Some(max_bytes) = self.config.max_bytes {
            let mut total = entries.iter().map(|(_, size, _)| *size).sum::<u64>();
            if total > max_bytes {
                entries.sort_by_key(|(_, _, modified)| *modified);
                for (path, size, _) in entries {
                    if total <= max_bytes {
                        break;
                    }
                    if fs::remove_file(path).await.is_ok() {
                        total = total.saturating_sub(size);
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RenderCache for FilesystemRenderCache {
    async fn get(&self, key: &RenderCacheKey) -> Result<RenderCacheResult, RenderError> {
        self.cleanup().await?;
        for media_type in [media::IMAGE_JPEG, media::IMAGE_PNG] {
            let path = key.path(&self.config.directory, media_type);
            if path.exists() {
                let bytes = fs::read(path).await.map_err(|error| {
                    RenderError::Failed(format!("render cache read failed: {error}"))
                })?;
                return Ok(RenderCacheResult {
                    lookup: RenderCacheLookup::Hit,
                    image: Some(RenderedImage {
                        media_type: media_type.to_string(),
                        bytes: Bytes::from(bytes),
                    }),
                });
            }
        }
        Ok(RenderCacheResult {
            lookup: RenderCacheLookup::Miss,
            image: None,
        })
    }

    async fn put(&self, key: &RenderCacheKey, image: &RenderedImage) -> Result<(), RenderError> {
        self.cleanup().await?;
        fs::write(
            key.path(&self.config.directory, &image.media_type),
            &image.bytes,
        )
        .await
        .map_err(|error| RenderError::Failed(format!("render cache write failed: {error}")))?;
        self.cleanup().await
    }
}

#[derive(Default)]
pub struct NullRenderCache;

#[async_trait]
impl RenderCache for NullRenderCache {
    async fn get(&self, _key: &RenderCacheKey) -> Result<RenderCacheResult, RenderError> {
        Ok(RenderCacheResult {
            lookup: RenderCacheLookup::Bypass,
            image: None,
        })
    }

    async fn put(&self, _key: &RenderCacheKey, _image: &RenderedImage) -> Result<(), RenderError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    };

    use super::*;
    use crate::wado::render::RenderParams;

    #[test]
    fn render_cache_key_distinguishes_quality() {
        let mut low_quality = render_input();
        low_quality.params.quality = Some(1);
        let mut high_quality = render_input();
        high_quality.params.quality = Some(100);

        assert_ne!(
            RenderCacheKey::new(&low_quality, "dcmtk"),
            RenderCacheKey::new(&high_quality, "dcmtk")
        );
    }

    #[test]
    fn render_cache_key_uses_labeled_parameter_format() {
        let input = render_input();
        let key = RenderCacheKey::new(&input, "dcmtk");
        let legacy_key = legacy_unlabeled_cache_key(&input, "dcmtk");

        assert_ne!(key, legacy_key);
        assert_eq!(key.as_str().len(), 64);
    }

    #[test]
    fn render_cache_key_distinguishes_viewport() {
        let mut original = render_input();
        original.params.viewport = Some("128,128".to_string());
        let mut resized = render_input();
        resized.params.viewport = Some("256,256".to_string());

        assert_ne!(
            RenderCacheKey::new(&original, "dcmtk"),
            RenderCacheKey::new(&resized, "dcmtk")
        );
    }

    fn render_input() -> RenderInput {
        RenderInput {
            identity: DicomInstanceIdentity::new(
                StudyInstanceUid::new("1.2.3").expect("valid UID"),
                SeriesInstanceUid::new("1.2.3.4").expect("valid UID"),
                SopInstanceUid::new("1.2.3.4.5").expect("valid UID"),
                SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").expect("valid UID"),
            ),
            transfer_syntax_uid: None,
            dicom: Bytes::new(),
            frames: None,
            media_type: media::IMAGE_JPEG.to_string(),
            params: RenderParams::default(),
            thumbnail: false,
        }
    }
}
