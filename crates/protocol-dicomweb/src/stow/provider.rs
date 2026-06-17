use std::sync::Arc;

use raccoon_service_ingest::IngestService;

use super::routes;
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// Operational limits for STOW-RS HTTP spooling.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct StowRsProviderOptions {
    max_part_size_bytes: Option<u64>,
    max_part_count: Option<usize>,
}

impl StowRsProviderOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_part_size_bytes(mut self, max_part_size_bytes: u64) -> Self {
        self.max_part_size_bytes = Some(max_part_size_bytes.max(1));
        self
    }

    pub fn without_max_part_size_bytes(mut self) -> Self {
        self.max_part_size_bytes = None;
        self
    }

    pub fn with_max_part_count(mut self, max_part_count: usize) -> Self {
        self.max_part_count = Some(max_part_count.max(1));
        self
    }

    pub fn without_max_part_count(mut self) -> Self {
        self.max_part_count = None;
        self
    }

    pub(crate) fn max_part_size_bytes(&self) -> Option<u64> {
        self.max_part_size_bytes
    }

    pub(crate) fn max_part_count(&self) -> Option<usize> {
        self.max_part_count
    }
}

/// STOW-RS PS3.10 binary DICOM provider backed by protocol-neutral ingest.
pub struct StowRsProvider {
    ingest: Arc<dyn IngestService>,
    options: StowRsProviderOptions,
}

impl StowRsProvider {
    pub fn new(ingest: Arc<dyn IngestService>) -> Self {
        Self::with_options(ingest, StowRsProviderOptions::default())
    }

    pub fn with_options(ingest: Arc<dyn IngestService>, options: StowRsProviderOptions) -> Self {
        Self { ingest, options }
    }
}

impl DicomWebProvider for StowRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_stow_rs();
        if let Some(stow_rs) = registry.feature_set_mut().stow_rs.as_mut() {
            stow_rs.max_upload_size_bytes = self.options.max_part_size_bytes();
        }
        registry.state_mut().ingest = Some(self.ingest.clone());
        registry.state_mut().stow = Some(self.options.clone());
        routes::register(registry);
    }
}
