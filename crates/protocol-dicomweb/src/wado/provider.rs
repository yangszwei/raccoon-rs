use std::sync::Arc;

use raccoon_service_retrieve::{MetadataRepository, RetrieveService};

use super::routes;
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// WADO-RS DICOM object retrieve provider backed by protocol-neutral retrieve.
pub struct WadoRsProvider {
    retrieve: Arc<dyn RetrieveService>,
    metadata: Option<Arc<dyn MetadataRepository>>,
}

impl WadoRsProvider {
    pub fn new(retrieve: Arc<dyn RetrieveService>) -> Self {
        Self {
            retrieve,
            metadata: None,
        }
    }

    pub fn with_metadata_repository(mut self, metadata: Arc<dyn MetadataRepository>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

impl DicomWebProvider for WadoRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_wado_rs();
        registry.state_mut().retrieve = Some(self.retrieve.clone());
        if let Some(metadata) = &self.metadata {
            registry.feature_set_mut().enable_wado_rs_metadata();
            registry.state_mut().metadata = Some(metadata.clone());
        }
        routes::register(registry);
    }
}
