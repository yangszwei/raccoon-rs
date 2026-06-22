#[cfg(feature = "grpc")]
use std::net::SocketAddr;

use raccoon_platform_config::component::application_entities::{
    ApplicationEntitiesConfig, LocalApplicationEntityConfig, PeerApplicationEntityConfig,
};
#[cfg(feature = "grpc")]
use raccoon_platform_config::component::grpc::{GrpcClientConfig, GrpcServerConfig};
use raccoon_service_application_entity_registry::{
    ApplicationEntityError, ApplicationEntityRegistryError, InMemoryApplicationEntityRegistry,
    LocalApplicationEntity, PeerApplicationEntity,
};
#[cfg(feature = "grpc")]
use raccoon_service_application_entity_registry::{
    ApplicationEntityRegistry, ApplicationEntityRegistryGrpcService,
    ApplicationEntityRegistryServiceClient, ApplicationEntityRegistryServiceServer,
    GrpcApplicationEntityRegistryClient,
};

#[cfg(feature = "grpc")]
use crate::app::grpc::grpc_client_endpoint;
use crate::error::OrchestrationError;

/// Build the application entity registry from loaded configuration.
pub fn build_application_entity_registry(
    config: &ApplicationEntitiesConfig,
) -> Result<InMemoryApplicationEntityRegistry, OrchestrationError> {
    InMemoryApplicationEntityRegistry::try_new(
        map_application_entities(&config.local, map_local_application_entity)?,
        map_application_entities(&config.peer, map_peer_application_entity)?,
    )
    .map_err(OrchestrationError::from)
}

/// Build a gRPC-backed application entity registry client.
#[cfg(feature = "grpc")]
pub async fn build_grpc_application_entity_registry_client(
    config: &GrpcClientConfig,
) -> Result<GrpcApplicationEntityRegistryClient, OrchestrationError> {
    let endpoint = grpc_client_endpoint(config)?;
    let server_address = endpoint.uri().to_string();
    let inner = ApplicationEntityRegistryServiceClient::connect(endpoint).await?;

    Ok(GrpcApplicationEntityRegistryClient::with_server_address(
        inner,
        server_address,
    ))
}

/// Build a gRPC application entity registry server.
#[cfg(feature = "grpc")]
pub fn build_grpc_application_entity_registry_server<R>(
    config: &GrpcServerConfig,
    registry: R,
) -> Result<
    (
        SocketAddr,
        ApplicationEntityRegistryServiceServer<ApplicationEntityRegistryGrpcService<R>>,
    ),
    OrchestrationError,
>
where
    R: ApplicationEntityRegistry + Send + Sync + 'static,
{
    let bind_address = config.bind_address.parse()?;
    let server = ApplicationEntityRegistryGrpcService::new(registry).into_server();

    Ok((bind_address, server))
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
