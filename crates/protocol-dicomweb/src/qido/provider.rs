use std::sync::Arc;
use std::time::Duration;

use raccoon_service_query::QueryService;

use super::cache::QidoJsonCache;
use super::routes;
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// QIDO-RS JSON provider backed by a protocol-neutral query service.
pub struct QidoRsProvider {
    query: Arc<dyn QueryService>,
    cache_revision_check_interval: Duration,
}

impl QidoRsProvider {
    pub fn new(query: Arc<dyn QueryService>) -> Self {
        Self {
            query,
            cache_revision_check_interval: Duration::from_secs(1),
        }
    }

    pub fn with_cache_revision_check_interval(mut self, interval: Duration) -> Self {
        self.cache_revision_check_interval = interval;
        self
    }
}

impl DicomWebProvider for QidoRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_qido_rs();
        registry.state_mut().query = Some(self.query.clone());
        registry.state_mut().qido_json_cache =
            QidoJsonCache::with_revision_check_interval(self.cache_revision_check_interval);
        routes::register(registry);
    }
}
