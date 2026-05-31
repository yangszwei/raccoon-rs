use std::sync::Arc;

use raccoon_service_retrieve::{RetrieveRepository, RetrieveService, StandardRetrieveService};

use crate::contract::object_store::ObjectStoreHandle;

/// Build an object-store-backed retrieve service.
pub fn build_retrieve_service(
    repository: Arc<dyn RetrieveRepository>,
    object_store: ObjectStoreHandle,
) -> Arc<dyn RetrieveService> {
    Arc::new(StandardRetrieveService::new(repository, object_store))
}
