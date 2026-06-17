use std::sync::Arc;

use raccoon_service_query::QueryService;

use super::routes;
use crate::{DicomWebProvider, DicomWebRouteRegistry};

/// QIDO-RS JSON provider backed by a protocol-neutral query service.
pub struct QidoRsProvider {
    query: Arc<dyn QueryService>,
}

impl QidoRsProvider {
    pub fn new(query: Arc<dyn QueryService>) -> Self {
        Self { query }
    }
}

impl DicomWebProvider for QidoRsProvider {
    fn register(&self, registry: &mut DicomWebRouteRegistry) {
        registry.feature_set_mut().enable_qido_rs();
        registry.state_mut().query = Some(self.query.clone());
        routes::register(registry);
    }
}
