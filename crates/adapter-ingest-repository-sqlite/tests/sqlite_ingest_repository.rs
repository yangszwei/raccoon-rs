use std::time::{Duration, UNIX_EPOCH};

use raccoon_adapter_ingest_repository_sqlite::SqliteIngestRepository;
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_ingest::{
    IngestChecksum, IngestObjectId, IngestObjectIdentity, IngestObjectOutcome, IngestObjectState,
    IngestPayloadRepresentation, IngestRepository, IngestSource, IngestUploadId,
    ReceivedIngestObject,
};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

/// Opens an in-memory SQLite store and returns its pool for verification queries.
///
/// [`SqliteIngestRepository::open`] uses `max_connections(1)`, so all pool checkouts —
/// including the raw verification queries below — share the single in-memory
/// connection where migrations were run.
async fn migrated_pool() -> SqlitePool {
    SqliteIngestRepository::open("sqlite::memory:")
        .await
        .expect("open in-memory ingest store")
        .into_pool()
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

async fn object_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects")
        .fetch_one(pool)
        .await
        .expect("count ingest objects")
}

#[tokio::test]
async fn migrations_create_ingest_objects_table() {
    let pool = migrated_pool().await;

    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ingest_objects'",
    )
    .fetch_one(&pool)
    .await
    .expect("query table");

    let index_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'index'
          AND name IN (
              'idx_ingest_objects_upload_id',
              'idx_ingest_objects_received_at',
              'idx_ingest_objects_study_series_sop'
          )
        "#,
    )
    .fetch_one(&pool)
    .await
    .expect("query indexes");

    let columns = sqlx::query("PRAGMA table_info(ingest_objects)")
        .fetch_all(&pool)
        .await
        .expect("query columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();

    assert_eq!(table_count, 1);
    assert_eq!(index_count, 3);
    assert_eq!(
        columns,
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
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[])
        .await
        .expect("empty batch succeeds");

    assert_eq!(object_count(&pool).await, 0);
}

#[tokio::test]
async fn multiple_records_persist_atomically() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());

    store
        .record_received_objects(&[record(1, "upload/one.dcm"), record(2, "upload/two.dcm")])
        .await
        .expect("batch insert succeeds");

    assert_eq!(object_count(&pool).await, 2);
}

#[tokio::test]
async fn duplicate_ingest_object_id_fails_and_rolls_back_batch() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
    store
        .record_received_object(&record(1, "upload/existing.dcm"))
        .await
        .expect("seed record");

    let result = store
        .record_received_objects(&[
            record(2, "upload/new.dcm"),
            record(1, "upload/duplicate-id.dcm"),
        ])
        .await;

    assert!(result.is_err());
    assert_eq!(object_count(&pool).await, 1);
    let rolled_back: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = ?")
            .bind("upload/new.dcm")
            .fetch_one(&pool)
            .await
            .expect("count rolled-back record");
    assert_eq!(rolled_back, 0);
}

#[tokio::test]
async fn duplicate_object_key_fails_and_rolls_back_batch() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
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
        sqlx::query_scalar("SELECT COUNT(*) FROM ingest_objects WHERE object_key = ?")
            .bind("upload/new.dcm")
            .fetch_one(&pool)
            .await
            .expect("count rolled-back record");
    assert_eq!(rolled_back, 0);
}

#[tokio::test]
async fn all_fields_round_trip() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
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
        row.get::<String, _>("ingest_object_id"),
        record.ingest_object_id.to_string()
    );
    assert_eq!(
        row.get::<String, _>("upload_id"),
        record.upload_id.to_string()
    );
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
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
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
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
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
async fn content_length_outside_sqlite_range_fails_before_insert() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
    let mut record = record(1, "upload/too-large.dcm");
    record.content_length = i64::MAX as u64 + 1;

    assert!(store.record_received_object(&record).await.is_err());
    assert_eq!(object_count(&pool).await, 0);
}

#[tokio::test]
async fn pre_epoch_received_at_fails_before_insert() {
    let pool = migrated_pool().await;
    let store = SqliteIngestRepository::new(pool.clone());
    let mut record = record(1, "upload/pre-epoch.dcm");
    record.received_at = UNIX_EPOCH - Duration::from_millis(1);

    assert!(store.record_received_object(&record).await.is_err());
    assert_eq!(object_count(&pool).await, 0);
}
