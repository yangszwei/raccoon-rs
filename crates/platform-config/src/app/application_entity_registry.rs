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

    /// Initial local and peer DICOM application entity settings.
    pub application_entities: ApplicationEntitiesConfig,

    /// gRPC server listener settings for the registry service.
    pub grpc: GrpcServerConfig,

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
            .add_source(
                Environment::with_prefix("RACCOON")
                    .separator("__")
                    .try_parsing(true),
            )
            .add_source(
                Environment::with_prefix("RACCOON_AE_REGISTRY")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)
    }
}

#[cfg(test)]
mod tests {
    use config::{Config, Environment};

    use super::ApplicationEntityRegistryServiceConfig;

    #[test]
    fn environment_overrides_local_application_entity_array() {
        unsafe {
            std::env::set_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__TITLE",
                "RACCOON",
            );
            std::env::set_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__BIND_ADDRESS",
                "0.0.0.0:11112",
            );
            std::env::set_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__MAX_CONCURRENT_ASSOCIATIONS",
                "64",
            );
            std::env::set_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__MAX_PDU_LENGTH",
                "65536",
            );
        }

        let config: ApplicationEntityRegistryServiceConfig = Config::builder()
            .add_source(
                Environment::with_prefix("RACCOON_AE_REGISTRY")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("config deserializes");

        let local = config
            .application_entities
            .local
            .first()
            .expect("local AE is configured");
        assert_eq!(local.title, "RACCOON");
        assert_eq!(local.bind_address, "0.0.0.0:11112");
        assert_eq!(local.max_concurrent_associations, 64);
        assert_eq!(local.max_pdu_length, 65_536);

        unsafe {
            std::env::remove_var("RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__TITLE");
            std::env::remove_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__BIND_ADDRESS",
            );
            std::env::remove_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__MAX_CONCURRENT_ASSOCIATIONS",
            );
            std::env::remove_var(
                "RACCOON_AE_REGISTRY__APPLICATION_ENTITIES__LOCAL__0__MAX_PDU_LENGTH",
            );
        }
    }
}
