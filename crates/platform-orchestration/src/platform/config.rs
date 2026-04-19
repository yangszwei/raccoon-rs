use raccoon_platform_config::app::MonolithConfig;

use crate::error::OrchestrationError;

/// Load configuration for the monolith binary.
pub fn load_monolith_config() -> Result<MonolithConfig, OrchestrationError> {
    MonolithConfig::load().map_err(Into::into)
}
