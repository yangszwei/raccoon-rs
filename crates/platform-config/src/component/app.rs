use serde::Deserialize;

/// Application-level identity and process settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    /// Canonical service name used across subsystems.
    pub name: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: "raccoon".to_string(),
        }
    }
}
