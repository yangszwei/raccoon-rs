use std::sync::Arc;

use raccoon_protocol_dimse::{
    DimseListener, MoveServiceProvider, QueryServiceProvider, RegistryMoveDestinationStore,
    RetrieveServiceProvider, ServiceClassRegistry, StorageServiceProvider, verification_provider,
};
use raccoon_service_application_entity_registry::{
    ApplicationEntityRegistry, LocalApplicationEntity,
};
use raccoon_service_ingest::IngestService;
use raccoon_service_query::QueryService;
use raccoon_service_retrieve::RetrieveService;

use crate::error::OrchestrationError;

/// Build a `ServiceClassRegistry` with all default DIMSE providers.
pub fn build_dimse_service_registry(
    ingest_service: Arc<dyn IngestService>,
    query_service: Arc<dyn QueryService>,
    retrieve_service: Arc<dyn RetrieveService>,
    application_entity_registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
    local_ae: LocalApplicationEntity,
) -> ServiceClassRegistry {
    let mut registry = ServiceClassRegistry::new();

    let echo = verification_provider();
    registry.register_described(echo);

    registry.register_described(Arc::new(build_storage_service_provider(ingest_service)));
    registry.register_described(Arc::new(build_query_service_provider(query_service)));
    registry.register_described(Arc::new(build_retrieve_service_provider(Arc::clone(
        &retrieve_service,
    ))));
    registry.register_described(Arc::new(build_move_service_provider(
        retrieve_service,
        application_entity_registry,
        local_ae,
    )));

    registry
}

/// Build a C-STORE service provider with the default storage SOP classes.
pub fn build_storage_service_provider(
    ingest_service: Arc<dyn IngestService>,
) -> StorageServiceProvider {
    StorageServiceProvider::with_default_storage_sop_classes(ingest_service)
}

/// Build a C-FIND service provider with the default query SOP classes.
pub fn build_query_service_provider(query_service: Arc<dyn QueryService>) -> QueryServiceProvider {
    QueryServiceProvider::with_default_find_sop_classes(query_service)
}

/// Build a C-GET service provider with the default retrieve SOP classes.
pub fn build_retrieve_service_provider(
    retrieve_service: Arc<dyn RetrieveService>,
) -> RetrieveServiceProvider {
    RetrieveServiceProvider::with_default_get_sop_classes(retrieve_service)
}

/// Build a C-MOVE service provider with the default move SOP classes.
pub fn build_move_service_provider(
    retrieve_service: Arc<dyn RetrieveService>,
    application_entity_registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
    local_ae: LocalApplicationEntity,
) -> MoveServiceProvider {
    let destination_store = Arc::new(RegistryMoveDestinationStore::new(
        application_entity_registry,
        local_ae,
    ));
    MoveServiceProvider::with_default_move_sop_classes(retrieve_service, destination_store)
}

/// Bind a `DimseListener` to `local_ae` and configure it with the registry's abstract syntaxes.
pub async fn bind_dimse_listener(
    local_ae: &LocalApplicationEntity,
    registry: &ServiceClassRegistry,
) -> Result<DimseListener, OrchestrationError> {
    let supported_abstract_syntax_count = registry.supported_abstract_syntax_uids().len();
    let listener = DimseListener::bind(local_ae)
        .await?
        .with_registry_syntaxes(registry);
    let local_addr = listener.local_addr()?;

    tracing::info!(
        ae.title = %local_ae.title(),
        server.address = %local_addr,
        dimse.max_concurrent_associations = local_ae.max_concurrent_associations(),
        dimse.supported_abstract_syntax_count = supported_abstract_syntax_count,
        "DIMSE server listening"
    );

    Ok(listener)
}
