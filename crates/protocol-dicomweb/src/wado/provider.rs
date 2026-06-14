use std::sync::Arc;

use raccoon_service_retrieve::{MetadataRepository, RetrieveService};

use super::routes;
use super::transcoding::TransferSyntaxPolicy;
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// WADO-RS DICOM object retrieve provider backed by protocol-neutral retrieve.
pub struct WadoRsProvider {
    retrieve: Arc<dyn RetrieveService>,
    metadata: Option<Arc<dyn MetadataRepository>>,
    transfer_syntax_policy: TransferSyntaxPolicy,
}

impl WadoRsProvider {
    pub fn new(retrieve: Arc<dyn RetrieveService>) -> Self {
        Self {
            retrieve,
            metadata: None,
            transfer_syntax_policy: TransferSyntaxPolicy::native_little_endian(),
        }
    }

    pub fn with_metadata_repository(mut self, metadata: Arc<dyn MetadataRepository>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_transfer_syntax_policy(mut self, policy: TransferSyntaxPolicy) -> Self {
        self.transfer_syntax_policy = policy;
        self
    }
}

impl DicomWebProvider for WadoRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_wado_rs();
        registry.feature_set_mut().set_wado_rs_transfer_syntaxes(
            self.transfer_syntax_policy.advertised_transfer_syntaxes(),
        );
        registry.state_mut().retrieve = Some(self.retrieve.clone());
        registry.state_mut().wado_rs_transfer_syntax_policy =
            Some(self.transfer_syntax_policy.clone());
        if let Some(metadata) = &self.metadata {
            registry.feature_set_mut().enable_wado_rs_metadata();
            registry.state_mut().metadata = Some(metadata.clone());
        }
        routes::register(registry);
    }
}
