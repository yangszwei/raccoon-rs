use raccoon_adapter_read_sqlite::SqliteReadRepository;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    TransferSyntaxUid,
};
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_query::{
    AttributePath, AttributeValue, DicomQuery, Projection, QueryRepository, QueryScope,
    ResponseValue, StudyRootQueryRetrieveLevel,
};
use raccoon_service_retrieve::RetrieveRepository;
use raccoon_service_sync::{
    SyncInstanceRecord, SyncReadModelWriter, SyncSeriesRecord, SyncStudyRecord,
    SyncedReadModelObject,
};
use sqlx::{Row, SqlitePool};

async fn open_repo() -> (SqliteReadRepository, SqlitePool) {
    let repo = SqliteReadRepository::open("sqlite::memory:")
        .await
        .expect("open in-memory repo");
    let pool = repo.into_pool();
    let repo = SqliteReadRepository::new(pool.clone());
    (repo, pool)
}

fn uid<T>(value: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .unwrap_or_else(|err| panic!("valid uid {value}: {err}"))
}

fn object_key(value: &str) -> ObjectKey {
    ObjectKey::new(value).expect("valid object key")
}

fn synced_object(
    suffix: &str,
    object_key_value: &str,
    synced_at_unix_ms: i64,
) -> SyncedReadModelObject {
    let study_uid = uid::<StudyInstanceUid>("1.2.840.1");
    let series_uid = uid::<SeriesInstanceUid>("1.2.840.1.1");
    let sop_uid = uid::<SopInstanceUid>(&format!("1.2.840.1.1.{suffix}"));
    let sop_class_uid = uid::<SopClassUid>("1.2.840.10008.5.1.4.1.1.2");

    SyncedReadModelObject {
        study: SyncStudyRecord {
            study_instance_uid: study_uid.clone(),
            patient_id: Some("PAT-001".to_string()),
            patient_name: Some("DOE^JANE".to_string()),
            patient_birth_date: Some("19700101".to_string()),
            patient_sex: Some("F".to_string()),
            study_date: Some("20260531".to_string()),
            study_time: Some("120000".to_string()),
            accession_number: Some("ACC-001".to_string()),
            study_id: Some("STUDY-001".to_string()),
            study_description: Some("Head CT".to_string()),
            referring_physician_name: Some("REF^DOC".to_string()),
        },
        series: SyncSeriesRecord {
            series_instance_uid: series_uid.clone(),
            study_instance_uid: study_uid.clone(),
            modality: Some("CT".to_string()),
            series_number: Some(7),
            series_date: Some("20260531".to_string()),
            series_time: Some("120100".to_string()),
            series_description: Some("Axial".to_string()),
            body_part_examined: Some("HEAD".to_string()),
        },
        instance: SyncInstanceRecord {
            identity: DicomInstanceIdentity::new(study_uid, series_uid, sop_uid, sop_class_uid),
            instance_number: Some(3),
            acquisition_date_time: Some("20260531120102".to_string()),
            transfer_syntax_uid: Some(uid::<TransferSyntaxUid>("1.2.840.10008.1.2.1")),
            object_key: object_key(object_key_value),
            object_size_bytes: 42,
            attributes_json: r#"{"00080016":{"vr":"UI","Value":["1.2.840.10008.5.1.4.1.1.2"]}}"#
                .to_string(),
        },
        synced_at_unix_ms,
    }
}

#[tokio::test]
async fn upsert_synced_object_creates_read_model_rows() {
    let (repo, pool) = open_repo().await;
    let object = synced_object("1", "instances/one.dcm", 100);

    repo.upsert_synced_object(&object)
        .await
        .expect("upsert synced object");

    let study_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM studies")
        .fetch_one(&pool)
        .await
        .expect("count studies");
    let series_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM series")
        .fetch_one(&pool)
        .await
        .expect("count series");
    let instance = sqlx::query(
        "SELECT object_key, object_size_bytes, attributes, synced_at_unix_ms \
         FROM instances WHERE sop_instance_uid = ?",
    )
    .bind("1.2.840.1.1.1")
    .fetch_one(&pool)
    .await
    .expect("fetch instance");

    assert_eq!(study_count, 1);
    assert_eq!(series_count, 1);
    assert_eq!(instance.get::<String, _>("object_key"), "instances/one.dcm");
    assert_eq!(instance.get::<i64, _>("object_size_bytes"), 42);
    assert!(instance.get::<String, _>("attributes").contains("00080016"));
    assert_eq!(instance.get::<i64, _>("synced_at_unix_ms"), 100);
}

#[tokio::test]
async fn upsert_synced_object_is_idempotent_by_sop_instance_uid() {
    let (repo, pool) = open_repo().await;
    let mut object = synced_object("1", "instances/one.dcm", 100);
    repo.upsert_synced_object(&object)
        .await
        .expect("initial upsert");

    object.study.patient_name = Some("DOE^UPDATED".to_string());
    object.series.modality = Some("MR".to_string());
    object.instance.object_key = object_key("instances/updated.dcm");
    object.instance.object_size_bytes = 77;
    object.instance.attributes_json =
        r#"{"00100010":{"vr":"PN","Value":[{"Alphabetic":"DOE^UPDATED"}]}}"#.to_string();
    object.synced_at_unix_ms = 200;
    repo.upsert_synced_object(&object)
        .await
        .expect("second upsert");

    let counts: (i64, i64, i64) = (
        sqlx::query_scalar("SELECT COUNT(*) FROM studies")
            .fetch_one(&pool)
            .await
            .expect("count studies"),
        sqlx::query_scalar("SELECT COUNT(*) FROM series")
            .fetch_one(&pool)
            .await
            .expect("count series"),
        sqlx::query_scalar("SELECT COUNT(*) FROM instances")
            .fetch_one(&pool)
            .await
            .expect("count instances"),
    );
    let row = sqlx::query(
        "SELECT st.patient_name, se.modality, i.object_key, i.object_size_bytes, \
                i.attributes, i.synced_at_unix_ms \
         FROM instances i \
         JOIN series se ON se.series_instance_uid = i.series_instance_uid \
         JOIN studies st ON st.study_instance_uid = i.study_instance_uid \
         WHERE i.sop_instance_uid = ?",
    )
    .bind("1.2.840.1.1.1")
    .fetch_one(&pool)
    .await
    .expect("fetch updated row");

    assert_eq!(counts, (1, 1, 1));
    assert_eq!(row.get::<String, _>("patient_name"), "DOE^UPDATED");
    assert_eq!(row.get::<String, _>("modality"), "MR");
    assert_eq!(row.get::<String, _>("object_key"), "instances/updated.dcm");
    assert_eq!(row.get::<i64, _>("object_size_bytes"), 77);
    assert!(row.get::<String, _>("attributes").contains("DOE^UPDATED"));
    assert_eq!(row.get::<i64, _>("synced_at_unix_ms"), 200);
}

#[tokio::test]
async fn retrieve_repository_finds_synced_instance() {
    let (repo, _pool) = open_repo().await;
    let object = synced_object("1", "instances/retrieve.dcm", 100);
    repo.upsert_synced_object(&object)
        .await
        .expect("upsert synced object");

    let found = repo
        .find_instance(&uid::<SopInstanceUid>("1.2.840.1.1.1"))
        .await
        .expect("find instance")
        .expect("instance exists");

    assert_eq!(found.object_key, object_key("instances/retrieve.dcm"));
    assert_eq!(found.content_length, Some(42));
}

#[tokio::test]
async fn query_repository_finds_synced_fields() {
    let (repo, _pool) = open_repo().await;
    let object = synced_object("1", "instances/query.dcm", 100);
    repo.upsert_synced_object(&object)
        .await
        .expect("upsert synced object");

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query");
    let page = repo.execute(&query).await.expect("execute query");

    let patient_id = page.items[0]
        .attributes()
        .iter()
        .find(|attr| attr.path == AttributePath::from_tag(dicom_dictionary_std::tags::PATIENT_ID))
        .expect("patient id attribute");

    assert_eq!(
        patient_id.value,
        ResponseValue::Present(AttributeValue::Text("PAT-001".to_string()))
    );
}

#[tokio::test]
async fn oversized_object_size_fails_before_partial_write() {
    let (repo, pool) = open_repo().await;
    let mut object = synced_object("1", "instances/too-large.dcm", 100);
    object.instance.object_size_bytes = i64::MAX as u64 + 1;

    assert!(repo.upsert_synced_object(&object).await.is_err());

    let instance_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instances")
        .fetch_one(&pool)
        .await
        .expect("count instances");
    assert_eq!(instance_count, 0);
}
