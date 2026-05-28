use serde::Deserialize;

/// Database backend configuration.
///
/// Serialised as a tagged enum: set `type = "sqlite"` (or a future variant) in
/// the `[database]` section.  Each variant can carry backend-specific fields
/// without breaking the shared deserialization path.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DatabaseConfig {
    /// SQLite embedded database.
    ///
    /// No extra fields are required: the database file path is derived from
    /// [`FilesystemConfig::root`][crate::component::filesystem::FilesystemConfig].
    #[default]
    Sqlite,
}

#[cfg(test)]
mod tests {
    use super::DatabaseConfig;

    #[test]
    fn default_database_is_sqlite() {
        assert!(matches!(DatabaseConfig::default(), DatabaseConfig::Sqlite));
    }
}
