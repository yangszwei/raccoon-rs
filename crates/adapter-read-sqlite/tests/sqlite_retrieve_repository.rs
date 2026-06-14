use raccoon_adapter_read_sqlite::SqliteReadRepository;
use raccoon_contract_dicom::{
    PatientId, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid, TransferSyntaxUid,
};
use raccoon_contract_object_store::ObjectKey;
use raccoon_service_retrieve::{MetadataRepository, RetrieveRepository, RetrieveScope};
use sqlx::SqlitePool;

async fn open_repo() -> (SqliteReadRepository, SqlitePool) {
    let repo = SqliteReadRepository::open("sqlite::memory:")
        .await
        .expect("open in-memory repo");
    let pool = repo.into_pool();
    let repo = SqliteReadRepository::new(pool.clone());
    (repo, pool)
}

#[allow(clippy::too_many_arguments)]
async fn insert_instance(
    pool: &SqlitePool,
    study_uid: &str,
    series_uid: &str,
    sop_uid: &str,
    patient_id: &str,
    object_key: Option<&str>,
    transfer_syntax_uid: Option<&str>,
    object_size_bytes: Option<i64>,
) {
    sqlx::query("INSERT OR IGNORE INTO studies (study_instance_uid, patient_id) VALUES (?, ?)")
        .bind(study_uid)
        .bind(patient_id)
        .execute(pool)
        .await
        .expect("insert study");

    sqlx::query(
        "INSERT OR IGNORE INTO series (series_instance_uid, study_instance_uid) VALUES (?, ?)",
    )
    .bind(series_uid)
    .bind(study_uid)
    .execute(pool)
    .await
    .expect("insert series");

    sqlx::query(
        "INSERT INTO instances \
         (sop_instance_uid, sop_class_uid, series_instance_uid, study_instance_uid, \
          transfer_syntax_uid, object_key, object_size_bytes) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(sop_uid)
    .bind("1.2.840.10008.5.1.4.1.1.2")
    .bind(series_uid)
    .bind(study_uid)
    .bind(transfer_syntax_uid)
    .bind(object_key)
    .bind(object_size_bytes)
    .execute(pool)
    .await
    .expect("insert instance");
}

#[tokio::test]
async fn patient_scope_returns_retrievable_instances() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        Some("instances/one.dcm"),
        Some("1.2.840.10008.1.2.1"),
        Some(42),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.4",
        "1.2.4.1",
        "1.2.4.1.1",
        "PAT-002",
        Some("instances/two.dcm"),
        None,
        None,
    )
    .await;

    let refs = repo
        .find_instances_for_patient(&PatientId::new("PAT-001").unwrap())
        .await
        .expect("find patient instances");

    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].identity.study_instance_uid,
        StudyInstanceUid::new("1.2.3").unwrap()
    );
    assert_eq!(
        refs[0].identity.series_instance_uid,
        SeriesInstanceUid::new("1.2.3.1").unwrap()
    );
    assert_eq!(
        refs[0].identity.sop_instance_uid,
        SopInstanceUid::new("1.2.3.1.1").unwrap()
    );
    assert_eq!(
        refs[0].identity.sop_class_uid,
        SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap()
    );
    assert_eq!(
        refs[0].transfer_syntax_uid,
        Some(TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap())
    );
    assert_eq!(
        refs[0].object_key,
        ObjectKey::new("instances/one.dcm").unwrap()
    );
    assert_eq!(refs[0].content_length, Some(42));
}

#[tokio::test]
async fn study_and_series_scopes_filter_instances() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        Some("instances/one.dcm"),
        None,
        None,
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.2",
        "1.2.3.2.1",
        "PAT-001",
        Some("instances/two.dcm"),
        None,
        None,
    )
    .await;

    let study_refs = repo
        .find_instances_for_study(&StudyInstanceUid::new("1.2.3").unwrap())
        .await
        .expect("find study instances");
    let series_refs = repo
        .find_instances_for_series(&SeriesInstanceUid::new("1.2.3.2").unwrap())
        .await
        .expect("find series instances");

    assert_eq!(study_refs.len(), 2);
    assert_eq!(series_refs.len(), 1);
    assert_eq!(
        series_refs[0].identity.sop_instance_uid,
        SopInstanceUid::new("1.2.3.2.1").unwrap()
    );
}

#[tokio::test]
async fn scoped_series_filters_by_study_and_series() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.10",
        "1.2.10.1",
        "PAT-001",
        Some("instances/one.dcm"),
        None,
        None,
    )
    .await;
    insert_instance(
        &pool,
        "1.2.4",
        "1.2.10",
        "1.2.10.2",
        "PAT-002",
        Some("instances/two.dcm"),
        None,
        None,
    )
    .await;

    let refs = repo
        .find_instances_for_study_series(
            &StudyInstanceUid::new("1.2.3").unwrap(),
            &SeriesInstanceUid::new("1.2.10").unwrap(),
        )
        .await
        .expect("find scoped series instances");

    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].identity.sop_instance_uid,
        SopInstanceUid::new("1.2.10.1").unwrap()
    );
}

#[tokio::test]
async fn scoped_instance_filters_by_parent_uids() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        Some("instances/one.dcm"),
        None,
        None,
    )
    .await;

    let matched = repo
        .find_instance_in_scope(
            Some(&StudyInstanceUid::new("1.2.3").unwrap()),
            Some(&SeriesInstanceUid::new("1.2.3.1").unwrap()),
            &SopInstanceUid::new("1.2.3.1.1").unwrap(),
        )
        .await
        .expect("find scoped instance");
    let wrong_study = repo
        .find_instance_in_scope(
            Some(&StudyInstanceUid::new("1.2.4").unwrap()),
            Some(&SeriesInstanceUid::new("1.2.3.1").unwrap()),
            &SopInstanceUid::new("1.2.3.1.1").unwrap(),
        )
        .await
        .expect("find scoped instance with wrong study");
    let wrong_series = repo
        .find_instance_in_scope(
            Some(&StudyInstanceUid::new("1.2.3").unwrap()),
            Some(&SeriesInstanceUid::new("1.2.3.2").unwrap()),
            &SopInstanceUid::new("1.2.3.1.1").unwrap(),
        )
        .await
        .expect("find scoped instance with wrong series");

    assert!(matched.is_some());
    assert!(wrong_study.is_none());
    assert!(wrong_series.is_none());
}

#[tokio::test]
async fn instance_scope_returns_none_for_missing_or_unavailable_object() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        None,
        None,
        None,
    )
    .await;

    let unavailable = repo
        .find_instance(&SopInstanceUid::new("1.2.3.1.1").unwrap())
        .await
        .expect("find unavailable instance");
    let missing = repo
        .find_instance(&SopInstanceUid::new("1.2.3.1.2").unwrap())
        .await
        .expect("find missing instance");

    assert!(unavailable.is_none());
    assert!(missing.is_none());
}

#[tokio::test]
async fn invalid_stored_metadata_returns_repository_error() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        Some("../invalid.dcm"),
        None,
        None,
    )
    .await;

    let err = repo
        .find_instances_for_study(&StudyInstanceUid::new("1.2.3").unwrap())
        .await
        .expect_err("invalid object key should fail");

    assert!(err.to_string().contains("invalid stored retrieve metadata"));
}

#[tokio::test]
async fn metadata_lookup_supports_study_series_and_instance_scopes() {
    let (repo, pool) = open_repo().await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "PAT-001",
        None,
        None,
        None,
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.2",
        "1.2.3.2.1",
        "PAT-001",
        None,
        None,
        None,
    )
    .await;
    sqlx::query("UPDATE instances SET attributes = ? WHERE sop_instance_uid = ?")
        .bind(r#"{"00080018":{"vr":"UI","Value":["1.2.3.1.1"]}}"#)
        .bind("1.2.3.1.1")
        .execute(&pool)
        .await
        .expect("update first attributes");
    sqlx::query("UPDATE instances SET attributes = ? WHERE sop_instance_uid = ?")
        .bind(r#"{"00080018":{"vr":"UI","Value":["1.2.3.2.1"]}}"#)
        .bind("1.2.3.2.1")
        .execute(&pool)
        .await
        .expect("update second attributes");

    let study_rows = repo
        .find_metadata(&RetrieveScope::Study {
            study_instance_uid: StudyInstanceUid::new("1.2.3").unwrap(),
        })
        .await
        .expect("find study metadata");
    let series_rows = repo
        .find_metadata(&RetrieveScope::Series {
            study_instance_uid: Some(StudyInstanceUid::new("1.2.3").unwrap()),
            series_instance_uid: SeriesInstanceUid::new("1.2.3.2").unwrap(),
        })
        .await
        .expect("find series metadata");
    let instance_rows = repo
        .find_metadata(&RetrieveScope::Instance {
            study_instance_uid: Some(StudyInstanceUid::new("1.2.3").unwrap()),
            series_instance_uid: Some(SeriesInstanceUid::new("1.2.3.1").unwrap()),
            sop_instance_uid: SopInstanceUid::new("1.2.3.1.1").unwrap(),
        })
        .await
        .expect("find instance metadata");

    assert_eq!(study_rows.len(), 2);
    assert_eq!(series_rows.len(), 1);
    assert_eq!(
        series_rows[0].identity.sop_instance_uid,
        SopInstanceUid::new("1.2.3.2.1").unwrap()
    );
    assert_eq!(instance_rows.len(), 1);
    assert!(instance_rows[0].attributes_json.contains("1.2.3.1.1"));
}
