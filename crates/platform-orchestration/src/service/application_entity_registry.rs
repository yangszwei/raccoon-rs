use raccoon_platform_config::component::application_entities::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, PeerApplicationEntityConfig,
};
use raccoon_service_application_entity_registry::{
    ApplicationEntityError, ApplicationEntityRegistry, ApplicationEntityRegistryError,
    LocalApplicationEntity, PeerApplicationEntity,
};

use crate::error::OrchestrationError;

/// Build the application entity registry from loaded configuration.
pub fn build_application_entity_registry(
    config: ApplicationEntitiesConfig,
) -> Result<ApplicationEntityRegistry, OrchestrationError> {
    ApplicationEntityRegistry::try_new(
        map_application_entities(&config.local, map_local_application_entity)?,
        map_application_entities(&config.peer, map_peer_application_entity)?,
    )
    .map_err(OrchestrationError::from)
}

fn map_application_entities<T, U, E>(
    configs: &[T],
    map: impl Fn(&T) -> Result<U, E>,
) -> Result<Vec<U>, ApplicationEntityRegistryError>
where
    ApplicationEntityRegistryError: From<E>,
{
    configs
        .iter()
        .map(map)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn map_local_application_entity(
    config: &LocalApplicationEntityConfig,
) -> Result<LocalApplicationEntity, ApplicationEntityError> {
    LocalApplicationEntity::try_new(
        config.title.as_str(),
        config.bind_address.as_str(),
        config.max_concurrent_associations,
        config.read_timeout_seconds,
        config.write_timeout_seconds,
        config.max_pdu_length,
    )
}

fn map_peer_application_entity(
    config: &PeerApplicationEntityConfig,
) -> Result<PeerApplicationEntity, ApplicationEntityError> {
    PeerApplicationEntity::try_new(
        config.title.as_str(),
        config.address.as_str(),
        config.connect_timeout_seconds,
        config.read_timeout_seconds,
        config.write_timeout_seconds,
        config.max_pdu_length,
    )
}
