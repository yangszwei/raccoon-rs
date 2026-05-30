use std::str::FromStr;

use async_trait::async_trait;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid,
};
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_query::{DicomQuery, QueryPage, QueryRepository, QueryRepositoryError};
use raccoon_service_retrieve::{InstanceRef, RetrieveRepository, RetrieveRepositoryError};
use sqlx::{
    Row, SqlitePool,
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

#[async_trait]
impl RetrieveRepository for SqliteReadRepository {
    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = "patient",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instances_for_patient(
        &self,
        patient_id: &PatientId,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        self.fetch_instance_refs(
            "SELECT i.study_instance_uid, i.series_instance_uid, i.sop_instance_uid, \
                    i.sop_class_uid, i.transfer_syntax_uid, i.object_key, i.object_size_bytes \
             FROM instances i \
             INNER JOIN studies st ON st.study_instance_uid = i.study_instance_uid \
             WHERE st.patient_id = ? AND i.object_key IS NOT NULL \
             ORDER BY i.study_instance_uid, i.series_instance_uid, i.sop_instance_uid",
            patient_id.as_str(),
        )
        .await
    }

    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = "study",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instances_for_study(
        &self,
        uid: &StudyInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        self.fetch_instance_refs(
            "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                    transfer_syntax_uid, object_key, object_size_bytes \
             FROM instances \
             WHERE study_instance_uid = ? AND object_key IS NOT NULL \
             ORDER BY series_instance_uid, sop_instance_uid",
            uid.as_str(),
        )
        .await
    }

    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = "series",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instances_for_series(
        &self,
        uid: &SeriesInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        self.fetch_instance_refs(
            "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                    transfer_syntax_uid, object_key, object_size_bytes \
             FROM instances \
             WHERE series_instance_uid = ? AND object_key IS NOT NULL \
             ORDER BY sop_instance_uid",
            uid.as_str(),
        )
        .await
    }

    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = "instance",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instance(
        &self,
        uid: &SopInstanceUid,
    ) -> Result<Option<InstanceRef>, RetrieveRepositoryError> {
        let row = sqlx::query(
            "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                    transfer_syntax_uid, object_key, object_size_bytes \
             FROM instances \
             WHERE sop_instance_uid = ? AND object_key IS NOT NULL",
        )
        .bind(uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(retrieve_query_error)?;

        row.map(materialize_instance_ref)
            .transpose()
            .map_err(Into::into)
    }
}

impl SqliteReadRepository {
    async fn fetch_instance_refs(
        &self,
        sql: &'static str,
        bind: &str,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        let rows = sqlx::query(sql)
            .bind(bind)
            .fetch_all(&self.pool)
            .await
            .map_err(retrieve_query_error)?;

        rows.into_iter()
            .map(materialize_instance_ref)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

fn retrieve_query_error(err: sqlx::Error) -> RetrieveRepositoryError {
    Span::current().record("error.type", "sqlx");
    SqliteReadRepositoryError::Query(err).into()
}

fn materialize_instance_ref(
    row: sqlx::sqlite::SqliteRow,
) -> Result<InstanceRef, SqliteReadRepositoryError> {
    let identity = DicomInstanceIdentity::new(
        parse_uid(&row, "study_instance_uid")?,
        parse_uid(&row, "series_instance_uid")?,
        parse_uid(&row, "sop_instance_uid")?,
        parse_uid(&row, "sop_class_uid")?,
    );
    let object_key = parse_object_key(&row, "object_key")?;
    let transfer_syntax_uid = parse_optional_uid(&row, "transfer_syntax_uid")?;
    let content_length = parse_content_length(&row, "object_size_bytes")?;

    Ok(InstanceRef {
        identity,
        transfer_syntax_uid,
        object_key,
        content_length,
    })
}

fn parse_uid<T>(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<T, SqliteReadRepositoryError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = row
        .try_get::<String, _>(column)
        .map_err(|e| SqliteReadRepositoryError::RowRead(column.to_owned(), e))?;

    value.parse::<T>().map_err(|e| invalid_metadata(column, e))
}

fn parse_optional_uid<T>(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<T>, SqliteReadRepositoryError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    row.try_get::<Option<String>, _>(column)
        .map_err(|e| SqliteReadRepositoryError::RowRead(column.to_owned(), e))?
        .map(|value| value.parse::<T>().map_err(|e| invalid_metadata(column, e)))
        .transpose()
}

fn parse_object_key(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<ObjectKey, SqliteReadRepositoryError> {
    let value = row
        .try_get::<String, _>(column)
        .map_err(|e| SqliteReadRepositoryError::RowRead(column.to_owned(), e))?;

    ObjectKey::new(value).map_err(|e| invalid_metadata(column, e))
}

fn parse_content_length(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<u64>, SqliteReadRepositoryError> {
    row.try_get::<Option<i64>, _>(column)
        .map_err(|e| SqliteReadRepositoryError::RowRead(column.to_owned(), e))?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                SqliteReadRepositoryError::InvalidStoredRetrieveMetadata {
                    column: column.to_owned(),
                    reason: "must not be negative".to_owned(),
                }
            })
        })
        .transpose()
}

fn invalid_metadata(column: &str, err: impl std::fmt::Display) -> SqliteReadRepositoryError {
    SqliteReadRepositoryError::InvalidStoredRetrieveMetadata {
        column: column.to_owned(),
        reason: err.to_string(),
    }
}

fn is_in_memory_url(url: &str) -> bool {
    url.contains(":memory:") || url.contains("mode=memory")
}
