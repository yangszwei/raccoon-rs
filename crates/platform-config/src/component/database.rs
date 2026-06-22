use serde::Deserialize;

/// Database backend configuration.
///
/// Serialised as a tagged enum: set `type = "sqlite"` or
/// `type = "postgresql"` in the `[database]` section.  Each variant can carry
/// backend-specific fields without breaking the shared deserialization path.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DatabaseConfig {
    /// SQLite embedded database.
    ///
    /// No extra fields are required: the database file path is derived from
    /// [`FilesystemConfig::root`][crate::component::filesystem::FilesystemConfig].
    #[default]
    Sqlite,

    /// PostgreSQL database addressed by a connection URL.
    #[serde(rename = "postgresql", alias = "postgres")]
    PostgreSql {
        /// Postgres connection URL.
        url: String,
    },
}

#[cfg(test)]
mod tests {
    use config::{Config, FileFormat};

    use super::DatabaseConfig;

    #[test]
    fn default_database_is_sqlite() {
        assert!(matches!(DatabaseConfig::default(), DatabaseConfig::Sqlite));
    }

    #[test]
    fn deserializes_postgresql_database_config() {
        let config: DatabaseConfig = Config::builder()
            .add_source(config::File::from_str(
                r#"
                type = "postgresql"
                url = "postgres://raccoon:raccoon@db:5432/raccoon"
                "#,
                FileFormat::Toml,
            ))
            .build()
            .expect("config builds")
            .try_deserialize()
            .expect("database config deserializes");

        let DatabaseConfig::PostgreSql { url } = config else {
            panic!("expected postgresql database config");
        };

        assert_eq!(url, "postgres://raccoon:raccoon@db:5432/raccoon");
    }
}
