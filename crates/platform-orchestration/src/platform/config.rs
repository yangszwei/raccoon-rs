use raccoon_platform_config::app::{
    ApplicationEntityRegistryServiceConfig, DimseGatewayConfig, IngestServiceConfig,
    MonolithConfig, QueryServiceConfig, RetrieveServiceConfig, SyncServiceConfig,
};

use crate::error::OrchestrationError;

/// Load configuration for the monolith binary.
pub fn load_monolith_config() -> Result<MonolithConfig, OrchestrationError> {
    MonolithConfig::load().map_err(Into::into)
}

/// Load configuration for the ingest service binary.
pub fn load_ingest_config() -> Result<IngestServiceConfig, OrchestrationError> {
    IngestServiceConfig::load().map_err(Into::into)
}

/// Load configuration for the DIMSE gateway binary.
pub fn load_dimse_gateway_config() -> Result<DimseGatewayConfig, OrchestrationError> {
    DimseGatewayConfig::load().map_err(Into::into)
}

/// Load configuration for the query service binary.
pub fn load_query_config() -> Result<QueryServiceConfig, OrchestrationError> {
    QueryServiceConfig::load().map_err(Into::into)
}

/// Load configuration for the retrieve service binary.
pub fn load_retrieve_config() -> Result<RetrieveServiceConfig, OrchestrationError> {
    RetrieveServiceConfig::load().map_err(Into::into)
}

/// Load configuration for the sync service binary.
pub fn load_sync_config() -> Result<SyncServiceConfig, OrchestrationError> {
    SyncServiceConfig::load().map_err(Into::into)
}

/// Load configuration for the Application Entity registry service binary.
pub fn load_application_entity_registry_config()
-> Result<ApplicationEntityRegistryServiceConfig, OrchestrationError> {
    ApplicationEntityRegistryServiceConfig::load().map_err(Into::into)
}
