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

    /// SQLite ingest repository could not be opened or migrated.
    #[error(transparent)]
    SqliteIngestRepository(
        #[from] raccoon_adapter_ingest_repository_sqlite::SqliteIngestRepositoryError,
    ),

    /// SQLite read repository could not be opened or migrated.
    #[error(transparent)]
    SqliteReadRepository(#[from] raccoon_adapter_read_sqlite::SqliteReadRepositoryError),

    /// Configured network address could not be parsed.
    #[cfg(feature = "grpc")]
    #[error(transparent)]
    NetworkAddress(#[from] std::net::AddrParseError),

    /// Telemetry could not be initialized.
    #[error(transparent)]
    Telemetry(#[from] raccoon_platform_telemetry::TelemetryError),

    /// Filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// DIMSE listener could not be bound.
    #[error(transparent)]
    Dimse(#[from] raccoon_protocol_dimse::DimseError),

    /// gRPC transport could not be prepared.
    #[cfg(feature = "grpc")]
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
}
