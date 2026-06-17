use std::sync::Arc;

use raccoon_service_retrieve::RetrieveService;

use super::{RenderService, RetrieveRenderService, WadoRenderOptions, routes};
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// WADO-RS rendered and thumbnail provider.
pub struct RenderedWadoRsProvider {
    render: Arc<dyn RenderService>,
    default_media_type: String,
}

impl RenderedWadoRsProvider {
    pub fn new(retrieve: Arc<dyn RetrieveService>) -> Self {
        Self::with_options(retrieve, WadoRenderOptions::default())
    }

    pub fn with_options(retrieve: Arc<dyn RetrieveService>, options: WadoRenderOptions) -> Self {
        let default_media_type = options.default_media_type.clone();
        Self {
            render: Arc::new(RetrieveRenderService::new(retrieve, options)),
            default_media_type,
        }
    }

    pub fn with_service(
        render: Arc<dyn RenderService>,
        default_media_type: impl Into<String>,
    ) -> Self {
        Self {
            render,
            default_media_type: default_media_type.into(),
        }
    }
}

impl DicomWebProvider for RenderedWadoRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_wado_rs();
        registry.feature_set_mut().enable_rendered();
        registry.feature_set_mut().enable_thumbnail();
        registry.state_mut().render = Some(self.render.clone());
        registry.state_mut().render_default_media_type = Some(self.default_media_type.clone());
        routes::register(registry);
    }
}
