use std::str::FromStr;
use std::time::{Duration, UNIX_EPOCH};

use raccoon_adapter_ingest_repository_postgres::PostgresIngestRepository;
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_ingest::{
    IngestChecksum, IngestObjectId, IngestObjectIdentity, IngestObjectOutcome, IngestObjectState,
    IngestPayloadRepresentation, IngestRepository, IngestSource, IngestUploadId,
    ReceivedIngestObject,
};
use raccoon_service_sync::{
    QuarantineCategory, QuarantineRecord, SyncClaimToken, SyncQuarantineRepository,
    SyncSourceRepository, SyncWorkerId,
};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

async fn migrated_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("RACCOON_POSTGRES_TEST_URL") else {
        eprintln!("skipping postgres ingest repository test: RACCOON_POSTGRES_TEST_URL is unset");
        return None;
    };

    let schema = format!("raccoon_ingest_test_{}", Uuid::now_v7().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect to postgres test database");
    let create_schema = format!(r#"CREATE SCHEMA "{schema}""#);
    sqlx::query(sqlx::AssertSqlSafe(create_schema.as_str()))
        .execute(&admin_pool)
        .await
        .expect("create test schema");
    admin_pool.close().await;

    let options = PgConnectOptions::from_str(&url)
        .expect("parse postgres test url")
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .expect("connect to isolated test schema");

    MIGRATOR.run(&pool).await.expect("run migrations");
    Some(pool)
}

fn ingest_object_id(value: u128) -> IngestObjectId {
    IngestObjectId::from_uuid(Uuid::from_u128(value))
}

fn upload_id(value: u128) -> IngestUploadId {
    IngestUploadId::from_uuid(Uuid::from_u128(value))
}

fn object_key(value: &str) -> ObjectKey {
    ObjectKey::new(value).expect("valid object key")
}

fn record(id: u128, key: &str) -> ReceivedIngestObject {
    ReceivedIngestObject {
        ingest_object_id: ingest_object_id(id),
        upload_id: upload_id(100),
        object_key: object_key(key),
        content_length: 123,
        etag: Some("etag-1".to_string()),
        checksum: Some(IngestChecksum::sha256("abc123")),
        identity: IngestObjectIdentity {
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.1.2".to_string()),
            study_instance_uid: Some("1.2.3".to_string()),
            series_instance_uid: Some("1.2.3.4".to_string()),
            sop_instance_uid: Some(format!("1.2.3.4.5.{id}")),
        },
        payload_representation: IngestPayloadRepresentation::DicomFile,
        transfer_syntax_uid: Some("1.2.840.10008.1.2.1".to_string()),
        source: IngestSource {
            request_id: Some("request-1".to_string()),
            correlation_id: Some("correlation-1".to_string()),
            source_ae: Some("SRC_AE".to_string()),
            source_application: Some("storescu".to_string()),
            protocol: Some("dicom-c-store".to_string()),
            content_type: Some("application/dicom".to_string()),
        },
        state: IngestObjectState::PendingSync,
        outcome: IngestObjectOutcome::Stored,
        received_at: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
    }
}

async fn object_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects")
        .fetch_one(pool)
        .await
        .expect("count ingest objects")
}

#[tokio::test]
async fn migrations_create_ingest_objects_table() {
    let Some(pool) = migrated_pool().await else {
        return;
    };

    let table_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name IN (
              'ingest_objects',
              'ingest_object_sync_states',
              'ingest_object_quarantines'
          )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("query tables");

    let index_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname IN (
              'idx_ingest_objects_upload_id',
              'idx_ingest_objects_study_series_sop',
              'idx_ingest_object_sync_states_active_claim_token',
              'idx_ingest_object_sync_states_pending_order',
              'idx_ingest_object_sync_states_pending_expiry',
              'idx_ingest_object_sync_states_terminal',
              'idx_ingest_object_quarantines_category'
          )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("query indexes");

    let ingest_columns = table_columns(&pool, "ingest_objects").await;
    let sync_columns = table_columns(&pool, "ingest_object_sync_states").await;
    let quarantine_columns = table_columns(&pool, "ingest_object_quarantines").await;

    assert_eq!(table_count, 3);
    assert_eq!(index_count, 7);
    assert_eq!(
        ingest_columns,
        [
            "ingest_object_id",
            "upload_id",
            "object_key",
            "content_length",
            "etag",
            "checksum_algorithm",
            "checksum_value",
            "sop_class_uid",
            "study_instance_uid",
            "series_instance_uid",
            "sop_instance_uid",
            "payload_representation",
            "transfer_syntax_uid",
            "source_ae",
            "outcome_kind",
            "outcome_reason",
            "received_at_unix_ms",
        ]
    );
    assert_eq!(
        sync_columns,
        [
            "ingest_object_id",
            "sync_state",
            "received_at_unix_ms",
            "sync_claim_token",
            "sync_claimed_by",
            "sync_claim_expires_at_unix_ms",
            "synced_at_unix_ms",
            "terminal_at_unix_ms",
        ]
    );
    assert_eq!(
        quarantine_columns,
        [
            "ingest_object_id",
            "category",
            "reason",
            "original_object_key",
            "quarantine_object_key",
            "quarantined_at_unix_ms",
        ]
    );
}

async fn table_columns(pool: &PgPool, table_name: &str) -> Vec<String> {
    sqlx::query(
        r#"
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
        ORDER BY ordinal_position
        "#,
    )
    .bind(table_name)
    .fetch_all(pool)
    .await
    .expect("query columns")
    .into_iter()
    .map(|row| row.get::<String, _>("column_name"))
    .collect()
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[])
        .await
        .expect("empty batch succeeds");

    assert_eq!(object_count(&pool).await, 0);
}

#[tokio::test]
async fn multiple_records_persist_atomically() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[record(1, "upload/one.dcm"), record(2, "upload/two.dcm")])
        .await
        .expect("batch insert succeeds");

    assert_eq!(object_count(&pool).await, 2);
}

#[tokio::test]
async fn duplicate_ingest_object_id_fails_and_rolls_back_batch() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    store
        .record_received_object(&record(1, "upload/existing.dcm"))
        .await
        .expect("seed record");
    let mut duplicate_id = record(1, "upload/duplicate-id.dcm");
    duplicate_id.identity.sop_instance_uid = Some("1.2.3.4.5.999".to_string());

    let result = store
        .record_received_objects(&[record(2, "upload/new.dcm"), duplicate_id])
        .await;

    assert!(result.is_err());
    assert_eq!(object_count(&pool).await, 1);
    let rolled_back: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = $1")
            .bind("upload/new.dcm")
            .fetch_one(&pool)
            .await
            .expect("count rolled-back record");
    assert_eq!(rolled_back, 0);
}

#[tokio::test]
async fn duplicate_object_key_fails_and_rolls_back_batch() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    store
        .record_received_object(&record(1, "upload/existing.dcm"))
        .await
        .expect("seed record");

    let result = store
        .record_received_objects(&[
            record(2, "upload/new.dcm"),
            record(3, "upload/existing.dcm"),
        ])
        .await;

    assert!(result.is_err());
    assert_eq!(object_count(&pool).await, 1);
    let rolled_back: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = $1")
            .bind("upload/new.dcm")
            .fetch_one(&pool)
            .await
            .expect("count rolled-back record");
    assert_eq!(rolled_back, 0);
}

#[tokio::test]
async fn duplicate_sop_instance_uid_is_ignored_without_rolling_back_batch() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    store
        .record_received_object(&record(1, "upload/existing.dcm"))
        .await
        .expect("seed record");
    let mut duplicate = record(1_000, "upload/duplicate-sop.dcm");
    duplicate.identity.sop_instance_uid = Some("1.2.3.4.5.1".to_string());

    store
        .record_received_objects(&[record(2, "upload/new.dcm"), duplicate])
        .await
        .expect("duplicate SOP Instance UID is ignored");

    assert_eq!(object_count(&pool).await, 2);
    let duplicate_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = $1")
            .bind("upload/duplicate-sop.dcm")
            .fetch_one(&pool)
            .await
            .expect("count duplicate record");
    let new_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = $1")
            .bind("upload/new.dcm")
            .fetch_one(&pool)
            .await
            .expect("count new record");
    let sync_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ingest_object_sync_states")
        .fetch_one(&pool)
        .await
        .expect("count sync states");

    assert_eq!(duplicate_count, 0);
    assert_eq!(new_count, 1);
    assert_eq!(sync_count, 2);
}

#[tokio::test]
async fn all_fields_round_trip() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let record = record(1, "upload/one.dcm");

    store
        .record_received_object(&record)
        .await
        .expect("insert record");

    let row = sqlx::query("SELECT * FROM ingest_objects")
        .fetch_one(&pool)
        .await
        .expect("fetch record");

    assert_eq!(
        row.get::<Uuid, _>("ingest_object_id"),
        record.ingest_object_id.as_uuid()
    );
    assert_eq!(row.get::<Uuid, _>("upload_id"), record.upload_id.as_uuid());
    assert_eq!(row.get::<String, _>("object_key"), "upload/one.dcm");
    assert_eq!(row.get::<i64, _>("content_length"), 123);
    assert_eq!(row.get::<String, _>("etag"), "etag-1");
    assert_eq!(row.get::<String, _>("checksum_algorithm"), "sha256");
    assert_eq!(row.get::<String, _>("checksum_value"), "abc123");
    assert_eq!(
        row.get::<String, _>("sop_class_uid"),
        "1.2.840.10008.5.1.4.1.1.2"
    );
    assert_eq!(row.get::<String, _>("study_instance_uid"), "1.2.3");
    assert_eq!(row.get::<String, _>("series_instance_uid"), "1.2.3.4");
    assert_eq!(
        row.get::<String, _>("sop_instance_uid"),
        record.identity.sop_instance_uid.as_deref().unwrap()
    );
    assert_eq!(row.get::<String, _>("payload_representation"), "dicom_file");
    assert_eq!(
        row.get::<String, _>("transfer_syntax_uid"),
        "1.2.840.10008.1.2.1"
    );
    assert_eq!(row.get::<String, _>("source_ae"), "SRC_AE");
    assert_eq!(row.get::<String, _>("outcome_kind"), "stored");
    assert_eq!(row.get::<Option<String>, _>("outcome_reason"), None);
    assert_eq!(row.get::<i64, _>("received_at_unix_ms"), 1_700_000_000_123);
}

#[tokio::test]
async fn nullable_fields_persist_as_null() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut record = record(1, "nullable/object.dcm");
    record.etag = None;
    record.checksum = None;
    record.identity = IngestObjectIdentity::default();
    record.transfer_syntax_uid = None;
    record.source = IngestSource::default();

    store
        .record_received_object(&record)
        .await
        .expect("insert nullable record");

    let row = sqlx::query("SELECT * FROM ingest_objects")
        .fetch_one(&pool)
        .await
        .expect("fetch nullable record");

    assert_eq!(row.get::<Option<String>, _>("etag"), None);
    assert_eq!(row.get::<Option<String>, _>("checksum_algorithm"), None);
    assert_eq!(row.get::<Option<String>, _>("checksum_value"), None);
    assert_eq!(row.get::<Option<String>, _>("sop_class_uid"), None);
    assert_eq!(row.get::<Option<String>, _>("study_instance_uid"), None);
    assert_eq!(row.get::<Option<String>, _>("series_instance_uid"), None);
    assert_eq!(row.get::<Option<String>, _>("sop_instance_uid"), None);
    assert_eq!(row.get::<Option<String>, _>("transfer_syntax_uid"), None);
    assert_eq!(row.get::<Option<String>, _>("source_ae"), None);
}

#[tokio::test]
async fn all_outcome_variants_map_to_expected_columns() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut records = vec![
        record(1, "outcomes/stored.dcm"),
        record(2, "outcomes/cannot-understand.dcm"),
        record(3, "outcomes/unsupported-sop.dcm"),
        record(4, "outcomes/study-mismatch.dcm"),
        record(5, "outcomes/checksum-mismatch.dcm"),
        record(6, "outcomes/too-large.dcm"),
        record(7, "outcomes/object-store-failed.dcm"),
        record(8, "outcomes/repository-failed.dcm"),
    ];
    records[1].outcome = IngestObjectOutcome::RejectedCannotUnderstand {
        reason: "bad dicom".to_string(),
    };
    records[2].outcome = IngestObjectOutcome::RejectedUnsupportedSopClass {
        sop_class_uid: Some("1.2.unsupported".to_string()),
        reason: "unsupported".to_string(),
    };
    records[3].outcome = IngestObjectOutcome::RejectedStudyMismatch {
        expected_study_instance_uid: "1.expected".to_string(),
        actual_study_instance_uid: Some("1.actual".to_string()),
    };
    records[4].outcome = IngestObjectOutcome::RejectedChecksumMismatch {
        expected: "expected-checksum".to_string(),
        actual: "actual-checksum".to_string(),
    };
    records[5].outcome = IngestObjectOutcome::RejectedTooLarge {
        max_content_length: 99,
    };
    records[6].outcome = IngestObjectOutcome::ObjectStoreFailed {
        reason: "object store down".to_string(),
    };
    records[7].outcome = IngestObjectOutcome::RepositoryFailed {
        reason: "repository down".to_string(),
    };

    store
        .record_received_objects(&records)
        .await
        .expect("insert outcomes");

    let rows =
        sqlx::query("SELECT outcome_kind, outcome_reason FROM ingest_objects ORDER BY object_key")
            .fetch_all(&pool)
            .await
            .expect("fetch outcomes");

    let actual: Vec<(String, Option<String>)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("outcome_kind"),
                row.get::<Option<String>, _>("outcome_reason"),
            )
        })
        .collect();

    assert_eq!(
        actual,
        [
            (
                "rejected_cannot_understand".into(),
                Some("bad dicom".into()),
            ),
            (
                "rejected_checksum_mismatch".into(),
                Some("expected_checksum=expected-checksum; actual_checksum=actual-checksum".into()),
            ),
            (
                "object_store_failed".into(),
                Some("object store down".into())
            ),
            ("repository_failed".into(), Some("repository down".into())),
            ("stored".into(), None),
            (
                "rejected_study_mismatch".into(),
                Some(
                    "expected_study_instance_uid=1.expected; \
                     actual_study_instance_uid=1.actual"
                        .into(),
                ),
            ),
            (
                "rejected_too_large".into(),
                Some("max_content_length=99".into())
            ),
            (
                "rejected_unsupported_sop_class".into(),
                Some("unsupported; sop_class_uid=1.2.unsupported".into()),
            ),
        ]
    );
}

#[tokio::test]
async fn content_length_outside_postgres_range_fails_before_insert() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut record = record(1, "upload/too-large.dcm");
    record.content_length = i64::MAX as u64 + 1;

    assert!(store.record_received_object(&record).await.is_err());
    assert_eq!(object_count(&pool).await, 0);
}

#[tokio::test]
async fn pre_epoch_received_at_fails_before_insert() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut record = record(1, "upload/pre-epoch.dcm");
    record.received_at = UNIX_EPOCH - Duration::from_millis(1);

    assert!(store.record_received_object(&record).await.is_err());
    assert_eq!(object_count(&pool).await, 0);
}

#[tokio::test]
async fn claim_pending_objects_claims_only_stored_pending_rows() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut rejected = record(2, "sync/rejected.dcm");
    rejected.outcome = IngestObjectOutcome::RejectedCannotUnderstand {
        reason: "bad dicom".to_string(),
    };

    store
        .record_received_objects(&[record(1, "sync/stored.dcm"), rejected])
        .await
        .expect("insert records");

    let claims = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 10, Duration::from_secs(30))
        .await
        .expect("claim pending");

    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].object_key, object_key("sync/stored.dcm"));

    let row = sqlx::query(
        "SELECT sync_state, sync_claim_token, sync_claimed_by \
         FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(claims[0].ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch claimed row");

    assert_eq!(row.get::<String, _>("sync_state"), "pending");
    assert_eq!(
        row.get::<Option<String>, _>("sync_claimed_by"),
        Some("worker-1".to_string())
    );
    assert!(row.get::<Option<String>, _>("sync_claim_token").is_some());

    let rejected_sync_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) \
         FROM ingest_object_sync_states s \
         JOIN ingest_objects i ON i.ingest_object_id = s.ingest_object_id \
         WHERE i.object_key = $1",
    )
    .bind("sync/rejected.dcm")
    .fetch_one(&pool)
    .await
    .expect("count rejected sync rows");
    assert_eq!(rejected_sync_count, 0);
}

#[tokio::test]
async fn active_claims_are_excluded_and_expired_claims_reclaim() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[record(1, "sync/one.dcm"), record(2, "sync/two.dcm")])
        .await
        .expect("insert records");

    let first = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 1, Duration::from_secs(30))
        .await
        .expect("first claim");
    assert_eq!(first.len(), 1);

    let second = store
        .claim_pending_objects(&SyncWorkerId::new("worker-2"), 10, Duration::from_secs(30))
        .await
        .expect("second claim");
    assert_eq!(second.len(), 1);
    assert_ne!(second[0].ingest_object_id, first[0].ingest_object_id);

    sqlx::query(
        "UPDATE ingest_object_sync_states SET sync_claim_expires_at_unix_ms = 0 \
         WHERE ingest_object_id = $1",
    )
    .bind(first[0].ingest_object_id.as_uuid())
    .execute(&pool)
    .await
    .expect("expire first claim");

    let reclaimed = store
        .claim_pending_objects(&SyncWorkerId::new("worker-3"), 10, Duration::from_secs(30))
        .await
        .expect("reclaim expired");

    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].ingest_object_id, first[0].ingest_object_id);
    assert_ne!(reclaimed[0].claim_token, first[0].claim_token);
}

#[tokio::test]
async fn claim_order_is_received_at_then_ingest_object_id() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());
    let mut later = record(2, "sync/later.dcm");
    later.received_at = UNIX_EPOCH + Duration::from_millis(20);
    let mut earlier_high_id = record(3, "sync/earlier-high.dcm");
    earlier_high_id.received_at = UNIX_EPOCH + Duration::from_millis(10);
    let mut earlier_low_id = record(1, "sync/earlier-low.dcm");
    earlier_low_id.received_at = UNIX_EPOCH + Duration::from_millis(10);

    store
        .record_received_objects(&[later, earlier_high_id, earlier_low_id])
        .await
        .expect("insert records");

    let claims = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 3, Duration::from_secs(30))
        .await
        .expect("claim pending");

    let keys = claims
        .iter()
        .map(|claim| claim.object_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "sync/earlier-low.dcm",
            "sync/earlier-high.dcm",
            "sync/later.dcm"
        ]
    );
}

#[tokio::test]
async fn mark_synced_requires_active_claim_token() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_object(&record(1, "sync/synced.dcm"))
        .await
        .expect("insert record");
    let claim = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 1, Duration::from_secs(30))
        .await
        .expect("claim pending")
        .pop()
        .expect("one claim");

    assert!(
        store
            .mark_synced(&SyncClaimToken::new("stale-token"))
            .await
            .is_err()
    );
    store
        .mark_synced(&claim.claim_token)
        .await
        .expect("mark synced");

    let row = sqlx::query(
        "SELECT sync_state, sync_claim_token, synced_at_unix_ms, terminal_at_unix_ms \
         FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(claim.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch synced row");

    assert_eq!(row.get::<String, _>("sync_state"), "synced");
    assert!(row.get::<Option<String>, _>("sync_claim_token").is_none());
    assert!(row.get::<Option<i64>, _>("synced_at_unix_ms").is_some());
    assert!(row.get::<Option<i64>, _>("terminal_at_unix_ms").is_some());
}

#[tokio::test]
async fn release_claim_clears_only_pending_claim() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[record(1, "sync/retry.dcm"), record(2, "sync/done.dcm")])
        .await
        .expect("insert records");
    let mut claims = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 2, Duration::from_secs(30))
        .await
        .expect("claim pending");
    let done = claims.pop().expect("done claim");
    let retry = claims.pop().expect("retry claim");

    store
        .mark_synced(&done.claim_token)
        .await
        .expect("mark synced");
    store
        .release_claim(&done.claim_token)
        .await
        .expect("stale terminal release is noop");
    store
        .release_claim(&retry.claim_token)
        .await
        .expect("release retry");

    let retry_token: Option<String> = sqlx::query_scalar(
        "SELECT sync_claim_token FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(retry.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch retry token");
    let done_state: String = sqlx::query_scalar(
        "SELECT sync_state FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(done.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch done state");

    assert!(retry_token.is_none());
    assert_eq!(done_state, "synced");
}

#[tokio::test]
async fn mark_quarantined_requires_claim_and_records_metadata() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_object(&record(1, "sync/original.dcm"))
        .await
        .expect("insert record");
    let claim = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 1, Duration::from_secs(30))
        .await
        .expect("claim pending")
        .pop()
        .expect("one claim");

    let stale_record = QuarantineRecord {
        ingest_object_id: claim.ingest_object_id.clone(),
        claim_token: SyncClaimToken::new("stale-token"),
        category: QuarantineCategory::Validation,
        reason: "missing uid".to_string(),
        original_object_key: claim.object_key.clone(),
        quarantine_object_key: object_key("sync/quarantine/stale"),
        quarantined_at_unix_ms: 123,
    };
    assert!(store.mark_quarantined(&stale_record).await.is_err());

    let quarantine_record = QuarantineRecord {
        ingest_object_id: claim.ingest_object_id.clone(),
        claim_token: claim.claim_token,
        category: QuarantineCategory::Policy,
        reason: "metadata limit exceeded".to_string(),
        original_object_key: claim.object_key,
        quarantine_object_key: object_key("sync/quarantine/1"),
        quarantined_at_unix_ms: 456,
    };
    store
        .mark_quarantined(&quarantine_record)
        .await
        .expect("mark quarantined");

    let sync_row = sqlx::query(
        "SELECT sync_state, terminal_at_unix_ms, sync_claim_token \
         FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(quarantine_record.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch quarantine sync row");
    let quarantine_row = sqlx::query(
        "SELECT category, reason, original_object_key, quarantine_object_key, \
                quarantined_at_unix_ms \
         FROM ingest_object_quarantines WHERE ingest_object_id = $1",
    )
    .bind(quarantine_record.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch quarantine row");
    let object_key: String =
        sqlx::query_scalar("SELECT object_key FROM ingest_objects WHERE ingest_object_id = $1")
            .bind(quarantine_record.ingest_object_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("fetch current object key");

    assert_eq!(sync_row.get::<String, _>("sync_state"), "quarantined");
    assert_eq!(sync_row.get::<i64, _>("terminal_at_unix_ms"), 456);
    assert!(
        sync_row
            .get::<Option<String>, _>("sync_claim_token")
            .is_none()
    );
    assert_eq!(object_key, "sync/quarantine/1");
    assert_eq!(quarantine_row.get::<String, _>("category"), "policy");
    assert_eq!(
        quarantine_row.get::<String, _>("reason"),
        "metadata limit exceeded"
    );
    assert_eq!(
        quarantine_row.get::<String, _>("original_object_key"),
        "sync/original.dcm"
    );
    assert_eq!(
        quarantine_row.get::<String, _>("quarantine_object_key"),
        "sync/quarantine/1"
    );
    assert_eq!(quarantine_row.get::<i64, _>("quarantined_at_unix_ms"), 456);

    let reclaimed = store
        .claim_pending_objects(&SyncWorkerId::new("worker-2"), 10, Duration::from_secs(30))
        .await
        .expect("claim after quarantine");
    assert!(reclaimed.is_empty());
}

#[tokio::test]
async fn duplicate_quarantine_key_rolls_back_terminal_update() {
    let Some(pool) = migrated_pool().await else {
        return;
    };
    let store = PostgresIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[
            record(1, "sync/original-one.dcm"),
            record(2, "sync/original-two.dcm"),
        ])
        .await
        .expect("insert records");
    let claims = store
        .claim_pending_objects(&SyncWorkerId::new("worker-1"), 2, Duration::from_secs(30))
        .await
        .expect("claim pending");
    let first = claims
        .iter()
        .find(|claim| claim.object_key.as_str() == "sync/original-one.dcm")
        .expect("first claim");
    let second = claims
        .iter()
        .find(|claim| claim.object_key.as_str() == "sync/original-two.dcm")
        .expect("second claim");

    let first_record = QuarantineRecord {
        ingest_object_id: first.ingest_object_id.clone(),
        claim_token: first.claim_token.clone(),
        category: QuarantineCategory::Validation,
        reason: "missing uid".to_string(),
        original_object_key: first.object_key.clone(),
        quarantine_object_key: object_key("sync/quarantine/duplicate"),
        quarantined_at_unix_ms: 100,
    };
    store
        .mark_quarantined(&first_record)
        .await
        .expect("mark first quarantined");

    let second_record = QuarantineRecord {
        ingest_object_id: second.ingest_object_id.clone(),
        claim_token: second.claim_token.clone(),
        category: QuarantineCategory::Policy,
        reason: "policy".to_string(),
        original_object_key: second.object_key.clone(),
        quarantine_object_key: object_key("sync/quarantine/duplicate"),
        quarantined_at_unix_ms: 200,
    };
    assert!(store.mark_quarantined(&second_record).await.is_err());

    let second_sync = sqlx::query(
        "SELECT sync_state, sync_claim_token, terminal_at_unix_ms \
         FROM ingest_object_sync_states WHERE ingest_object_id = $1",
    )
    .bind(second.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("fetch second sync row");
    let second_object_key: String =
        sqlx::query_scalar("SELECT object_key FROM ingest_objects WHERE ingest_object_id = $1")
            .bind(second.ingest_object_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("fetch second object key");
    let second_quarantine_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM ingest_object_quarantines WHERE ingest_object_id = $1",
    )
    .bind(second.ingest_object_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("count second quarantine records");

    assert_eq!(second_sync.get::<String, _>("sync_state"), "pending");
    assert_eq!(
        second_sync.get::<Option<String>, _>("sync_claim_token"),
        Some(second.claim_token.to_string())
    );
    assert!(
        second_sync
            .get::<Option<i64>, _>("terminal_at_unix_ms")
            .is_none()
    );
    assert_eq!(second_object_key, "sync/original-two.dcm");
    assert_eq!(second_quarantine_count, 0);
}
