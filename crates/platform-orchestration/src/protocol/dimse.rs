use std::sync::Arc;

use raccoon_protocol_dimse::{
    DimseListener, ServiceClassRegistry, StorageServiceProvider, verification_provider,
};
use raccoon_service_application_entity_registry::LocalApplicationEntity;
use raccoon_service_ingest::IngestService;

use crate::error::OrchestrationError;

/// Build a `ServiceClassRegistry` with C-ECHO and C-STORE providers.
pub fn build_dimse_service_registry(
    ingest_service: Arc<dyn IngestService>,
) -> ServiceClassRegistry {
    let mut registry = ServiceClassRegistry::new();

    let echo = verification_provider();
    registry.register_described(echo);

    let store = StorageServiceProvider::with_default_storage_sop_classes(ingest_service);
    registry.register_described(Arc::new(store));

    registry
}

/// Bind a `DimseListener` to `local_ae` and configure it with the registry's abstract syntaxes.
pub async fn bind_dimse_listener(
    local_ae: &LocalApplicationEntity,
    registry: &ServiceClassRegistry,
) -> Result<DimseListener, OrchestrationError> {
    let listener = DimseListener::bind(local_ae)
        .await?
        .with_registry_syntaxes(registry);
    Ok(listener)
}
