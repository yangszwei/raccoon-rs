use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid,
};
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_query::{DicomQuery, QueryPage, QueryRepository, QueryRepositoryError};
use raccoon_service_retrieve::{
    InstanceMetadata, InstanceRef, MetadataRepository, RetrieveRepository, RetrieveRepositoryError,
    RetrieveScope,
};
use raccoon_service_sync::{SyncReadModelWriter, SyncRepositoryError, SyncedReadModelObject};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tracing::{Instrument, Span, info_span, instrument};

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
            query.row_count = tracing::field::Empty,
            query.result_count = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn execute(&self, query: &DicomQuery) -> Result<QueryPage, QueryRepositoryError> {
        let scope = query.scope();
        let projection = query.projection().clone();

        let compiled = {
            let span = info_span!("sqlite.query.compile");
            let _guard = span.enter();
            compile(query, &self.registry).map_err(|e| {
                Span::current().record("error.type", "compile");
                QueryRepositoryError::new(e.to_string())
            })?
        };

        let mut stmt = sqlx::query(sqlx::AssertSqlSafe(compiled.sql.as_str()));
        for bind in &compiled.binds {
            stmt = match bind {
                BindValue::Text(v) => stmt.bind(v),
                BindValue::Int(v) => stmt.bind(*v),
            };
        }

        let rows = stmt
            .fetch_all(&self.pool)
            .instrument(info_span!("sqlite.query.execute"))
            .await
            .map_err(|e| {
                Span::current().record("error.type", "sqlx");
                QueryRepositoryError::new(SqliteReadRepositoryError::Query(e).to_string())
            })?;
        let row_count = rows.len();
        Span::current().record("query.row_count", row_count);

        let materialize_span = info_span!(
            "sqlite.query.materialize",
            query.row_count = row_count,
            query.selected_column_count = compiled.selected_mappings.len(),
            query.includes_attributes = compiled.includes_attributes,
        );
        let _guard = materialize_span.enter();
        let page =
            materialize_page(rows, &compiled, &projection, scope, &self.registry).map_err(|e| {
                Span::current().record("error.type", "materialize");
                QueryRepositoryError::new(e.to_string())
            })?;
        Span::current().record("query.result_count", page.items.len());
        Ok(page)
    }

    async fn read_model_revision(&self) -> Result<u64, QueryRepositoryError> {
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM read_model_state WHERE id = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    QueryRepositoryError::with_source(
                        "failed to read read model revision",
                        SqliteReadRepositoryError::Query(e),
                    )
                })?;

        u64::try_from(revision)
            .map_err(|e| QueryRepositoryError::with_source("read model revision is negative", e))
    }
}

fn current_unix_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    i64::try_from(millis).unwrap_or(i64::MAX)
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
            retrieve.scope = "series",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instances_for_study_series(
        &self,
        study_uid: &StudyInstanceUid,
        series_uid: &SeriesInstanceUid,
    ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
        let rows = sqlx::query(
            "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                    transfer_syntax_uid, object_key, object_size_bytes \
             FROM instances \
             WHERE study_instance_uid = ? AND series_instance_uid = ? AND object_key IS NOT NULL \
             ORDER BY sop_instance_uid",
        )
        .bind(study_uid.as_str())
        .bind(series_uid.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(retrieve_query_error)?;

        rows.into_iter()
            .map(materialize_instance_ref)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
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

    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = "instance",
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_instance_in_scope(
        &self,
        study_uid: Option<&StudyInstanceUid>,
        series_uid: Option<&SeriesInstanceUid>,
        sop_uid: &SopInstanceUid,
    ) -> Result<Option<InstanceRef>, RetrieveRepositoryError> {
        let row = match (study_uid, series_uid) {
            (Some(study_uid), Some(series_uid)) => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                            transfer_syntax_uid, object_key, object_size_bytes \
                     FROM instances \
                     WHERE study_instance_uid = ? AND series_instance_uid = ? AND sop_instance_uid = ? \
                       AND object_key IS NOT NULL",
                )
                .bind(study_uid.as_str())
                .bind(series_uid.as_str())
                .bind(sop_uid.as_str())
                .fetch_optional(&self.pool)
                .await
            }
            (Some(study_uid), None) => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                            transfer_syntax_uid, object_key, object_size_bytes \
                     FROM instances \
                     WHERE study_instance_uid = ? AND sop_instance_uid = ? AND object_key IS NOT NULL",
                )
                .bind(study_uid.as_str())
                .bind(sop_uid.as_str())
                .fetch_optional(&self.pool)
                .await
            }
            (None, Some(series_uid)) => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                            transfer_syntax_uid, object_key, object_size_bytes \
                     FROM instances \
                     WHERE series_instance_uid = ? AND sop_instance_uid = ? AND object_key IS NOT NULL",
                )
                .bind(series_uid.as_str())
                .bind(sop_uid.as_str())
                .fetch_optional(&self.pool)
                .await
            }
            (None, None) => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, sop_class_uid, \
                            transfer_syntax_uid, object_key, object_size_bytes \
                     FROM instances \
                     WHERE sop_instance_uid = ? AND object_key IS NOT NULL",
                )
                .bind(sop_uid.as_str())
                .fetch_optional(&self.pool)
                .await
            }
        }
        .map_err(retrieve_query_error)?;

        row.map(materialize_instance_ref)
            .transpose()
            .map_err(Into::into)
    }
}

#[async_trait]
impl MetadataRepository for SqliteReadRepository {
    #[instrument(
        skip_all,
        fields(
            db.system = "sqlite",
            retrieve.scope = scope.label(),
            metadata.row_count = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn find_metadata(
        &self,
        scope: &RetrieveScope,
    ) -> Result<Vec<InstanceMetadata>, RetrieveRepositoryError> {
        let rows = match scope {
            RetrieveScope::Patient { patient_id } => {
                sqlx::query(
                    "SELECT i.study_instance_uid, i.series_instance_uid, i.sop_instance_uid, \
                            i.sop_class_uid, i.attributes \
                     FROM instances i \
                     INNER JOIN studies st ON st.study_instance_uid = i.study_instance_uid \
                     WHERE st.patient_id = ? \
                     ORDER BY i.study_instance_uid, i.series_instance_uid, i.sop_instance_uid",
                )
                .bind(patient_id.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Study { study_instance_uid } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE study_instance_uid = ? \
                     ORDER BY series_instance_uid, sop_instance_uid",
                )
                .bind(study_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Series {
                study_instance_uid: Some(study_uid),
                series_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE study_instance_uid = ? AND series_instance_uid = ? \
                     ORDER BY sop_instance_uid",
                )
                .bind(study_uid.as_str())
                .bind(series_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Series {
                study_instance_uid: None,
                series_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE series_instance_uid = ? \
                     ORDER BY sop_instance_uid",
                )
                .bind(series_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Instance {
                study_instance_uid: Some(study_uid),
                series_instance_uid: Some(series_uid),
                sop_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE study_instance_uid = ? AND series_instance_uid = ? \
                       AND sop_instance_uid = ?",
                )
                .bind(study_uid.as_str())
                .bind(series_uid.as_str())
                .bind(sop_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Instance {
                study_instance_uid: Some(study_uid),
                series_instance_uid: None,
                sop_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE study_instance_uid = ? AND sop_instance_uid = ?",
                )
                .bind(study_uid.as_str())
                .bind(sop_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: Some(series_uid),
                sop_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE series_instance_uid = ? AND sop_instance_uid = ?",
                )
                .bind(series_uid.as_str())
                .bind(sop_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
            RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid,
            } => {
                sqlx::query(
                    "SELECT study_instance_uid, series_instance_uid, sop_instance_uid, \
                            sop_class_uid, attributes \
                     FROM instances \
                     WHERE sop_instance_uid = ?",
                )
                .bind(sop_instance_uid.as_str())
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(retrieve_query_error)?;

        Span::current().record("metadata.row_count", rows.len());

        rows.into_iter()
            .map(materialize_instance_metadata)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[async_trait]
impl SyncReadModelWriter for SqliteReadRepository {
    #[instrument(
        skip_all,
        fields(
            sync.study_instance_uid = %object.study.study_instance_uid,
            sync.series_instance_uid = %object.series.series_instance_uid,
            sync.sop_instance_uid = %object.instance.identity.sop_instance_uid,
            db.system = "sqlite",
            error.type = tracing::field::Empty,
        )
    )]
    async fn upsert_synced_object(
        &self,
        object: &SyncedReadModelObject,
    ) -> Result<(), SyncRepositoryError> {
        self.try_upsert_synced_object(object).await.map_err(|err| {
            Span::current().record("error.type", read_error_kind(&err));
            SyncRepositoryError::with_source("failed to upsert synced read model object", err)
        })
    }
}

impl SqliteReadRepository {
    async fn try_upsert_synced_object(
        &self,
        object: &SyncedReadModelObject,
    ) -> Result<(), SqliteReadRepositoryError> {
        let object_size_bytes = to_i64("object_size_bytes", object.instance.object_size_bytes)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(SqliteReadRepositoryError::Query)?;

        sqlx::query(
            r#"
            INSERT INTO studies (
                study_instance_uid,
                patient_id,
                patient_name,
                patient_birth_date,
                patient_sex,
                study_date,
                study_time,
                accession_number,
                study_id,
                study_description,
                referring_physician_name,
                synced_at_unix_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(study_instance_uid) DO UPDATE SET
                patient_id = COALESCE(excluded.patient_id, studies.patient_id),
                patient_name = COALESCE(excluded.patient_name, studies.patient_name),
                patient_birth_date = COALESCE(excluded.patient_birth_date, studies.patient_birth_date),
                patient_sex = COALESCE(excluded.patient_sex, studies.patient_sex),
                study_date = COALESCE(excluded.study_date, studies.study_date),
                study_time = COALESCE(excluded.study_time, studies.study_time),
                accession_number = COALESCE(excluded.accession_number, studies.accession_number),
                study_id = COALESCE(excluded.study_id, studies.study_id),
                study_description = COALESCE(excluded.study_description, studies.study_description),
                referring_physician_name = COALESCE(excluded.referring_physician_name, studies.referring_physician_name),
                synced_at_unix_ms = excluded.synced_at_unix_ms
            "#,
        )
        .bind(object.study.study_instance_uid.as_str())
        .bind(object.study.patient_id.as_deref())
        .bind(object.study.patient_name.as_deref())
        .bind(object.study.patient_birth_date.as_deref())
        .bind(object.study.patient_sex.as_deref())
        .bind(object.study.study_date.as_deref())
        .bind(object.study.study_time.as_deref())
        .bind(object.study.accession_number.as_deref())
        .bind(object.study.study_id.as_deref())
        .bind(object.study.study_description.as_deref())
        .bind(object.study.referring_physician_name.as_deref())
        .bind(object.synced_at_unix_ms)
        .execute(&mut *tx)
        .await
        .map_err(SqliteReadRepositoryError::Query)?;

        sqlx::query(
            r#"
            INSERT INTO series (
                series_instance_uid,
                study_instance_uid,
                modality,
                series_number,
                series_date,
                series_time,
                series_description,
                body_part_examined,
                synced_at_unix_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(series_instance_uid) DO UPDATE SET
                study_instance_uid = excluded.study_instance_uid,
                modality = COALESCE(excluded.modality, series.modality),
                series_number = COALESCE(excluded.series_number, series.series_number),
                series_date = COALESCE(excluded.series_date, series.series_date),
                series_time = COALESCE(excluded.series_time, series.series_time),
                series_description = COALESCE(excluded.series_description, series.series_description),
                body_part_examined = COALESCE(excluded.body_part_examined, series.body_part_examined),
                synced_at_unix_ms = excluded.synced_at_unix_ms
            "#,
        )
        .bind(object.series.series_instance_uid.as_str())
        .bind(object.series.study_instance_uid.as_str())
        .bind(object.series.modality.as_deref())
        .bind(object.series.series_number)
        .bind(object.series.series_date.as_deref())
        .bind(object.series.series_time.as_deref())
        .bind(object.series.series_description.as_deref())
        .bind(object.series.body_part_examined.as_deref())
        .bind(object.synced_at_unix_ms)
        .execute(&mut *tx)
        .await
        .map_err(SqliteReadRepositoryError::Query)?;

        sqlx::query(
            r#"
            INSERT INTO instances (
                sop_instance_uid,
                sop_class_uid,
                series_instance_uid,
                study_instance_uid,
                instance_number,
                acquisition_date_time,
                transfer_syntax_uid,
                object_key,
                object_size_bytes,
                attributes,
                synced_at_unix_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sop_instance_uid) DO UPDATE SET
                sop_class_uid = excluded.sop_class_uid,
                series_instance_uid = excluded.series_instance_uid,
                study_instance_uid = excluded.study_instance_uid,
                instance_number = excluded.instance_number,
                acquisition_date_time = excluded.acquisition_date_time,
                transfer_syntax_uid = excluded.transfer_syntax_uid,
                object_key = excluded.object_key,
                object_size_bytes = excluded.object_size_bytes,
                attributes = excluded.attributes,
                synced_at_unix_ms = excluded.synced_at_unix_ms
            "#,
        )
        .bind(object.instance.identity.sop_instance_uid.as_str())
        .bind(object.instance.identity.sop_class_uid.as_str())
        .bind(object.instance.identity.series_instance_uid.as_str())
        .bind(object.instance.identity.study_instance_uid.as_str())
        .bind(object.instance.instance_number)
        .bind(object.instance.acquisition_date_time.as_deref())
        .bind(
            object
                .instance
                .transfer_syntax_uid
                .as_ref()
                .map(|uid| uid.as_str()),
        )
        .bind(object.instance.object_key.as_str())
        .bind(object_size_bytes)
        .bind(object.instance.attributes_json.as_str())
        .bind(object.synced_at_unix_ms)
        .execute(&mut *tx)
        .await
        .map_err(SqliteReadRepositoryError::Query)?;

        let revision_update = sqlx::query(
            "UPDATE read_model_state \
             SET revision = revision + 1, updated_at_unix_ms = ? \
             WHERE id = 1",
        )
        .bind(current_unix_ms())
        .execute(&mut *tx)
        .await
        .map_err(SqliteReadRepositoryError::Query)?;
        if revision_update.rows_affected() != 1 {
            return Err(SqliteReadRepositoryError::InternalError(
                "read model state row is missing".to_string(),
            ));
        }

        tx.commit()
            .await
            .map_err(SqliteReadRepositoryError::Query)?;

        Ok(())
    }

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

fn materialize_instance_metadata(
    row: sqlx::sqlite::SqliteRow,
) -> Result<InstanceMetadata, SqliteReadRepositoryError> {
    let identity = DicomInstanceIdentity::new(
        parse_uid(&row, "study_instance_uid")?,
        parse_uid(&row, "series_instance_uid")?,
        parse_uid(&row, "sop_instance_uid")?,
        parse_uid(&row, "sop_class_uid")?,
    );
    let attributes_json = row
        .try_get::<String, _>("attributes")
        .map_err(|e| SqliteReadRepositoryError::RowRead("attributes".to_owned(), e))?;

    Ok(InstanceMetadata::new(identity, attributes_json))
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

fn to_i64(field: &'static str, value: u64) -> Result<i64, SqliteReadRepositoryError> {
    i64::try_from(value).map_err(|_| SqliteReadRepositoryError::IntegerOutOfRange { field, value })
}

fn read_error_kind(err: &SqliteReadRepositoryError) -> &'static str {
    match err {
        SqliteReadRepositoryError::Connect(_) => "raccoon_adapter_read_sqlite::Connect",
        SqliteReadRepositoryError::Migrate(_) => "raccoon_adapter_read_sqlite::Migrate",
        SqliteReadRepositoryError::Query(_) => "sqlx::Error",
        SqliteReadRepositoryError::RowRead(_, _) => "raccoon_adapter_read_sqlite::RowRead",
        SqliteReadRepositoryError::InvalidPredicate(_) => {
            "raccoon_adapter_read_sqlite::InvalidPredicate"
        }
        SqliteReadRepositoryError::InvalidStoredRetrieveMetadata { .. } => {
            "raccoon_adapter_read_sqlite::InvalidStoredRetrieveMetadata"
        }
        SqliteReadRepositoryError::IntegerOutOfRange { .. } => {
            "raccoon_adapter_read_sqlite::IntegerOutOfRange"
        }
        SqliteReadRepositoryError::InternalError(_) => "raccoon_adapter_read_sqlite::InternalError",
    }
}

fn is_in_memory_url(url: &str) -> bool {
    url.contains(":memory:") || url.contains("mode=memory")
}
