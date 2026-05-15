use thiserror::Error;

/// Errors produced while preparing Raccoon runtime state.
#[derive(Debug, Error)]
pub enum OrchestrationError {
    /// Application entity registry could not be built.
    #[error(transparent)]
    ApplicationEntityRegistry(
        #[from] raccoon_service_application_entity_registry::ApplicationEntityRegistryError,
    ),

    /// Configuration could not be loaded or deserialized.
    #[error(transparent)]
    Config(#[from] raccoon_platform_config::ConfigError),

    /// Configured network address could not be parsed.
    #[cfg(feature = "grpc")]
    #[error(transparent)]
    NetworkAddress(#[from] std::net::AddrParseError),

    /// Telemetry could not be initialized.
    #[error(transparent)]
    Telemetry(#[from] raccoon_platform_telemetry::TelemetryError),

    /// gRPC transport could not be prepared.
    #[cfg(feature = "grpc")]
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
}
