use config::{Config, Environment, File};
use serde::Deserialize;

use crate::ConfigError;
use crate::component::app::AppConfig;
use crate::component::application_entities::ApplicationEntitiesConfig;
use crate::component::grpc::GrpcServerConfig;
use crate::component::runtime::RuntimeConfig;
use crate::component::telemetry::TelemetryConfig;

/// Top-level configuration for the Application Entity registry gRPC service.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ApplicationEntityRegistryServiceConfig {
    /// Application-level identity and process settings.
    pub app: AppConfig,

    /// gRPC server listener settings for the registry service.
    pub grpc: GrpcServerConfig,

    /// Initial local and peer DICOM application entity settings.
    pub application_entities: ApplicationEntitiesConfig,

    /// Runtime lifecycle configuration.
    pub runtime: RuntimeConfig,

    /// Telemetry configuration, including logs, traces, and metrics.
    pub telemetry: TelemetryConfig,
}

impl ApplicationEntityRegistryServiceConfig {
    /// Load configuration from configured sources.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name("config/application-entity-registry").required(false))
            .add_source(File::with_name("config/application-entities").required(false))
            .add_source(File::with_name("application-entity-registry").required(false))
            .add_source(Environment::with_prefix("RACCOON_AE_REGISTRY").separator("__"))
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}
