use std::sync::Arc;

use raccoon_service_query::{QueryRepository, QueryService, StandardQueryService};

/// Build a repository-backed query service.
pub fn build_query_service(repository: Arc<dyn QueryRepository>) -> Arc<dyn QueryService> {
    Arc::new(StandardQueryService::new(repository))
}
