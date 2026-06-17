use std::path::PathBuf;

use async_trait::async_trait;
use raccoon_contract_dicom::{DicomInstanceIdentity, TransferSyntaxUid};
use raccoon_contract_object_store::Bytes;
use raccoon_service_retrieve::RetrieveScope;
use sha2::{Digest, Sha256};

use crate::media;

#[derive(Debug, Clone)]
pub struct RenderRequest {
    pub scope: RetrieveScope,
    pub frames: Option<Vec<u32>>,
    pub media_type: String,
    pub params: RenderParams,
    pub thumbnail: bool,
    pub single: bool,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RenderParams {
    pub viewport: Option<String>,
    pub window: Option<String>,
    pub quality: Option<u8>,
    pub annotation: Option<String>,
    pub iccprofile: Option<String>,
    pub presentation_state: Option<String>,
}

impl RenderParams {
    pub(crate) fn hash_into(&self, hasher: &mut Sha256) {
        for value in [
            self.viewport.as_deref(),
            self.window.as_deref(),
            self.annotation.as_deref(),
            self.iccprofile.as_deref(),
            self.presentation_state.as_deref(),
        ] {
            if let Some(value) = value {
                hasher.update(value.as_bytes());
            }
            hasher.update(b"|");
        }
        if let Some(quality) = self.quality {
            hasher.update(quality.to_string().as_bytes());
        }
    }
}

#[derive(Debug, Clone)]
pub struct RenderedImage {
    pub media_type: String,
    pub bytes: Bytes,
}

#[derive(Debug, Clone)]
pub struct RenderResponse {
    pub images: Vec<RenderedImage>,
}

#[async_trait]
pub trait RenderService: Send + Sync {
    async fn render(&self, request: RenderRequest) -> Result<RenderResponse, RenderError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("no matching DICOM instances")]
    NotFound,
    #[error("not acceptable: {0}")]
    NotAcceptable(String),
    #[error("rendering failed: {0}")]
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct RenderInput {
    pub identity: DicomInstanceIdentity,
    pub transfer_syntax_uid: Option<TransferSyntaxUid>,
    pub dicom: Bytes,
    pub frames: Option<Vec<u32>>,
    pub media_type: String,
    pub params: RenderParams,
    pub thumbnail: bool,
}

#[derive(Debug, Clone)]
pub struct WadoRenderOptions {
    pub dcmtk_path: Option<PathBuf>,
    pub cache: Option<super::RenderCacheConfig>,
    pub default_media_type: String,
}

impl Default for WadoRenderOptions {
    fn default() -> Self {
        Self {
            dcmtk_path: None,
            cache: None,
            default_media_type: media::IMAGE_JPEG.to_string(),
        }
    }
}
