use std::sync::Arc;

use async_trait::async_trait;
use raccoon_service_retrieve::RetrieveService;
use tracing::Span;

use super::backend::{BackendRenderError, DcmtkRenderer, NativeRenderer, RendererBackend};
use super::cache::{NullRenderCache, RenderCacheKey, RenderCacheLookup};
use super::{FilesystemRenderCache, RenderCache};
use super::{
    RenderError, RenderInput, RenderRequest, RenderResponse, RenderedImage, WadoRenderOptions,
};
use crate::DicomWebError;
use crate::wado::collect_instances;

pub struct RetrieveRenderService {
    retrieve: Arc<dyn RetrieveService>,
    backends: Vec<Arc<dyn RendererBackend>>,
    cache: Arc<dyn RenderCache>,
}

impl RetrieveRenderService {
    pub fn new(retrieve: Arc<dyn RetrieveService>, options: WadoRenderOptions) -> Self {
        let mut backends: Vec<Arc<dyn RendererBackend>> = vec![Arc::new(NativeRenderer)];
        if let Some(path) = options.dcmtk_path {
            backends.push(Arc::new(DcmtkRenderer { path }));
        }
        let cache: Arc<dyn RenderCache> = options
            .cache
            .map(|config| Arc::new(FilesystemRenderCache::new(config)) as Arc<dyn RenderCache>)
            .unwrap_or_else(|| Arc::new(NullRenderCache));
        Self {
            retrieve,
            backends,
            cache,
        }
    }

    pub fn with_backends(
        retrieve: Arc<dyn RetrieveService>,
        backends: Vec<Arc<dyn RendererBackend>>,
        cache: Arc<dyn RenderCache>,
    ) -> Self {
        Self {
            retrieve,
            backends,
            cache,
        }
    }
}

#[async_trait]
impl super::RenderService for RetrieveRenderService {
    async fn render(&self, request: RenderRequest) -> Result<RenderResponse, RenderError> {
        let instances = collect_instances(self.retrieve.as_ref(), request.scope)
            .await
            .map_err(|error| match error {
                DicomWebError::NotFound(_) => RenderError::NotFound,
                other => RenderError::Failed(other.to_string()),
            })?;
        let mut images = Vec::new();
        let mut last_unsupported = None;
        for instance in instances {
            let frames = request
                .frames
                .clone()
                .unwrap_or_else(|| vec![1])
                .into_iter()
                .map(|frame| Some(vec![frame]))
                .collect::<Vec<_>>();
            for frames in frames {
                let input = RenderInput {
                    identity: instance.identity.clone(),
                    transfer_syntax_uid: instance.transfer_syntax_uid.clone(),
                    dicom: instance.body.clone(),
                    frames,
                    media_type: request.media_type.clone(),
                    params: request.params.clone(),
                    thumbnail: request.thumbnail,
                };
                match self.render_instance(input).await {
                    Ok(image) => {
                        images.push(image);
                        if request.thumbnail || request.single {
                            return Ok(RenderResponse { images });
                        }
                    }
                    Err(BackendRenderError::Unsupported(message)) => {
                        last_unsupported = Some(message);
                    }
                    Err(BackendRenderError::Failed(message)) => {
                        return Err(RenderError::Failed(message));
                    }
                }
            }
        }
        if images.is_empty() {
            return Err(RenderError::NotAcceptable(
                last_unsupported.unwrap_or_else(|| "no renderable DICOM instances".to_string()),
            ));
        }
        Ok(RenderResponse { images })
    }
}

impl RetrieveRenderService {
    async fn render_instance(
        &self,
        input: RenderInput,
    ) -> Result<RenderedImage, BackendRenderError> {
        let mut last_unsupported = None;
        for backend in &self.backends {
            Span::current().record("dicomweb.renderer_backend", backend.name());
            let key = RenderCacheKey::new(&input, backend.name());
            let cached = self
                .cache
                .get(&key)
                .await
                .map_err(|error| BackendRenderError::Failed(error.to_string()))?;
            record_cache_lookup(cached.lookup);
            if let Some(image) = cached.image {
                return Ok(image);
            }
            match backend.render(&input).await {
                Ok(image) => {
                    self.cache
                        .put(&key, &image)
                        .await
                        .map_err(|error| BackendRenderError::Failed(error.to_string()))?;
                    if cached.lookup != RenderCacheLookup::Bypass {
                        Span::current().record("dicomweb.render_cache_result", "store");
                    }
                    return Ok(image);
                }
                Err(BackendRenderError::Unsupported(message)) => {
                    last_unsupported = Some(message);
                }
                Err(error @ BackendRenderError::Failed(_)) => return Err(error),
            }
        }
        Err(BackendRenderError::Unsupported(
            last_unsupported.unwrap_or_else(|| "no renderer accepted the instance".to_string()),
        ))
    }
}

fn record_cache_lookup(lookup: RenderCacheLookup) {
    let value = match lookup {
        RenderCacheLookup::Hit => "hit",
        RenderCacheLookup::Miss => "miss",
        RenderCacheLookup::Bypass => "bypass",
    };
    Span::current().record("dicomweb.render_cache_result", value);
}
