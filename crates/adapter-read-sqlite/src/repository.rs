use std::str::FromStr;

use async_trait::async_trait;
use raccoon_service_query::{DicomQuery, QueryPage, QueryRepository, QueryRepositoryError};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tracing::{Span, instrument};

use crate::compile::{BindValue, compile};
use crate::error::SqliteReadRepositoryError;
use crate::project::materialize_page;
use crate::schema::AttributeRegistry;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// SQLite-backed implementation of [`QueryRepository`].
///
/// Wraps a single-connection [`SqlitePool`] (SQLite serialises all writes, and
/// a single connection ensures in-memory databases used in tests share the
/// migrated schema across pool checkouts).
#[derive(Clone, Debug)]
pub struct SqliteReadRepository {
    pool: SqlitePool,
    registry: std::sync::Arc<AttributeRegistry>,
}

impl SqliteReadRepository {
    /// Creates a repository from an existing migrated pool.
    ///
    /// The pool must have already had all migrations applied.  Prefer
    /// [`open`][Self::open] for production; use this in tests to inject a
    /// pre-migrated pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            registry: std::sync::Arc::new(AttributeRegistry::new()),
        }
    }

    /// Opens a connection to the SQLite database at `url`, runs all pending
    /// migrations, and returns the repository.
    ///
    /// Creates the file if it does not exist.  WAL mode and `SYNCHRONOUS=NORMAL`
    /// are applied for production databases; in-memory URLs (`":memory:"` /
    /// `"mode=memory"`) keep the default journal mode.
    pub async fn open(url: &str) -> Result<Self, SqliteReadRepositoryError> {
        let mut options = SqliteConnectOptions::from_str(url)
            .map_err(SqliteReadRepositoryError::Connect)?
            .create_if_missing(true);

        if !is_in_memory_url(url) {
            options = options
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal);
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(SqliteReadRepositoryError::Connect)?;

        MIGRATOR
            .run(&pool)
            .await
            .map_err(SqliteReadRepositoryError::Migrate)?;

        Ok(Self::new(pool))
    }

    /// Consumes the repository and returns the underlying connection pool.
    ///
    /// Useful in tests that need to run verification queries or insert test
    /// data directly after operating through the repository.
    pub fn into_pool(self) -> SqlitePool {
        self.pool
    }
}

#[async_trait]
impl QueryRepository for SqliteReadRepository {
    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            query.scope = ?query.scope(),
            query.has_predicate = query.predicate().is_some(),
            query.has_paging = query.paging().is_some(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn execute(&self, query: &DicomQuery) -> Result<QueryPage, QueryRepositoryError> {
        let scope = query.scope();
        let projection = query.projection().clone();

        let compiled = compile(query, &self.registry).map_err(|e| {
            Span::current().record("error.type", "compile");
            QueryRepositoryError::new(e.to_string())
        })?;

        let mut stmt = sqlx::query(sqlx::AssertSqlSafe(compiled.sql.as_str()));
        for bind in &compiled.binds {
            stmt = match bind {
                BindValue::Text(v) => stmt.bind(v),
                BindValue::Int(v) => stmt.bind(*v),
            };
        }

        let rows = stmt.fetch_all(&self.pool).await.map_err(|e| {
            Span::current().record("error.type", "sqlx");
            QueryRepositoryError::new(SqliteReadRepositoryError::Query(e).to_string())
        })?;

        materialize_page(rows, &compiled, &projection, scope, &self.registry).map_err(|e| {
            Span::current().record("error.type", "materialize");
            QueryRepositoryError::new(e.to_string())
        })
    }
}

fn is_in_memory_url(url: &str) -> bool {
    url.contains(":memory:") || url.contains("mode=memory")
}
