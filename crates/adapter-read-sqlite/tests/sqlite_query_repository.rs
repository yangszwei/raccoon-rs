//! Integration tests for [`SqliteReadRepository`].
//!
//! Each test opens an in-memory SQLite database, inserts rows directly via the
//! underlying pool, then drives the repository through [`QueryRepository::execute`]
//! and asserts on the returned [`QueryPage`] and [`QueryMatch`] values.

use dicom_core::{DataElement, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use raccoon_adapter_read_sqlite::SqliteReadRepository;
use raccoon_service_query::DicomQuery;
use raccoon_service_query::{
    AttributePath, AttributeValue, MatchingRule, PatientRootQueryRetrieveLevel, Predicate,
    Projection, QueryPaging, QueryRepository, QueryScope, RangeMatching, ResponseValue,
    SequenceMatching, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};
use sqlx::SqlitePool;

async fn open_repo() -> (SqliteReadRepository, SqlitePool) {
    let repo = SqliteReadRepository::open("sqlite::memory:")
        .await
        .expect("open in-memory repo");
    let pool = repo.into_pool();
    let repo2 = SqliteReadRepository::new(pool.clone());
    (repo2, pool)
}

/// Serialises an [`InMemDicomObject`] to the DICOM JSON string stored in
/// `instances.attributes`.
fn to_attributes_json(obj: &InMemDicomObject) -> String {
    serde_json::to_string(&dicom_json::DicomJson::from(obj)).expect("serialise attributes")
}

/// Inserts one complete PACS hierarchy (study → series → instance).
#[allow(clippy::too_many_arguments)]
async fn insert_instance(
    pool: &SqlitePool,
    study_uid: &str,
    series_uid: &str,
    sop_uid: &str,
    sop_class_uid: &str,
    patient_id: &str,
    patient_name: &str,
    study_date: &str,
    modality: &str,
    instance_number: Option<i64>,
    extra_attributes: &InMemDicomObject,
) {
    sqlx::query(
        "INSERT OR IGNORE INTO studies \
         (study_instance_uid, patient_id, patient_name, study_date, synced_at_unix_ms) \
         VALUES (?, ?, ?, ?, 0)",
    )
    .bind(study_uid)
    .bind(patient_id)
    .bind(patient_name)
    .bind(study_date)
    .execute(pool)
    .await
    .expect("insert study");

    sqlx::query(
        "INSERT OR IGNORE INTO series \
         (series_instance_uid, study_instance_uid, modality) \
         VALUES (?, ?, ?)",
    )
    .bind(series_uid)
    .bind(study_uid)
    .bind(modality)
    .execute(pool)
    .await
    .expect("insert series");

    let attributes_json = to_attributes_json(extra_attributes);
    sqlx::query(
        "INSERT OR REPLACE INTO instances \
         (sop_instance_uid, sop_class_uid, series_instance_uid, study_instance_uid, \
          instance_number, attributes) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(sop_uid)
    .bind(sop_class_uid)
    .bind(series_uid)
    .bind(study_uid)
    .bind(instance_number)
    .bind(attributes_json)
    .execute(pool)
    .await
    .expect("insert instance");
}

fn empty_attrs() -> InMemDicomObject {
    InMemDicomObject::new_empty()
}

fn find_attr(
    page: &raccoon_service_query::QueryPage,
    row: usize,
    tag: dicom_core::Tag,
) -> Option<&ResponseValue> {
    page.items.get(row).and_then(|m| {
        m.attributes()
            .iter()
            .find(|a| a.path == AttributePath::from_tag(tag))
            .map(|a| &a.value)
    })
}

fn present_text(value: &ResponseValue) -> &str {
    match value {
        ResponseValue::Present(AttributeValue::Text(s)) => s,
        other => panic!("expected Present(Text), got {other:?}"),
    }
}

#[tokio::test]
async fn study_scope_returns_one_row_per_study_uid() {
    let (repo, pool) = open_repo().await;

    // Two instances in the same study → should collapse to one result row.
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE^JOHN",
        "20260101",
        "MR",
        Some(1),
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.2",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE^JOHN",
        "20260101",
        "MR",
        Some(2),
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");

    assert_eq!(
        page.items.len(),
        1,
        "two instances in one study → one study result"
    );
    let study_uid = find_attr(&page, 0, tags::STUDY_INSTANCE_UID).expect("study uid");
    assert_eq!(present_text(study_uid), "1.2.3");
}

#[tokio::test]
async fn study_scope_projects_derived_counts_and_modalities() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "MR",
        Some(1),
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.2",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "MR",
        Some(2),
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.2",
        "1.2.3.2.1",
        "1.2.840.10008.5.1.4.1.1.2",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        Some(1),
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Fields(vec![
            AttributePath::from_tag(tags::MODALITIES_IN_STUDY),
            AttributePath::from_tag(tags::NUMBER_OF_STUDY_RELATED_SERIES),
            AttributePath::from_tag(tags::NUMBER_OF_STUDY_RELATED_INSTANCES),
        ]),
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");

    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::MODALITIES_IN_STUDY).unwrap()),
        "CT\\MR"
    );
    assert_eq!(
        present_text(find_attr(&page, 0, tags::NUMBER_OF_STUDY_RELATED_SERIES).unwrap()),
        "2"
    );
    assert_eq!(
        present_text(find_attr(&page, 0, tags::NUMBER_OF_STUDY_RELATED_INSTANCES).unwrap()),
        "3"
    );
}

#[tokio::test]
async fn series_scope_projects_derived_instance_count() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "MR",
        Some(1),
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.2",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "MR",
        Some(2),
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
        Projection::Fields(vec![AttributePath::from_tag(
            tags::NUMBER_OF_SERIES_RELATED_INSTANCES,
        )]),
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");

    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::NUMBER_OF_SERIES_RELATED_INSTANCES).unwrap()),
        "2"
    );
}

#[tokio::test]
async fn series_scope_returns_one_row_per_series() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        Some(1),
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.2",
        "1.2.3.2.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "MR",
        Some(1),
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
        Projection::Default,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 2);
}

#[tokio::test]
async fn image_scope_returns_every_instance() {
    let (repo, pool) = open_repo().await;

    for i in 1u32..=3 {
        insert_instance(
            &pool,
            "1.2.3",
            "1.2.3.1",
            &format!("1.2.3.1.{i}"),
            "1.2.840.10008.5.1.4.1.1.4",
            "PAT-001",
            "DOE",
            "20260101",
            "MR",
            Some(i as i64),
            &empty_attrs(),
        )
        .await;
    }

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::Default,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 3);
}

#[tokio::test]
async fn single_value_filter_on_patient_id() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.4",
        "1.2.4.1",
        "1.2.4.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-002",
        "SMITH",
        "20260102",
        "MR",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::PATIENT_ID),
        MatchingRule::SingleValue("PAT-001".to_string()),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::PATIENT_ID).unwrap()),
        "PAT-001"
    );
}

#[tokio::test]
async fn wildcard_filter_on_patient_name() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE^JOHN",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.4",
        "1.2.4.1",
        "1.2.4.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-002",
        "SMITH^JANE",
        "20260101",
        "MR",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::PATIENT_NAME),
        MatchingRule::Wildcard("DOE*".to_string()),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::PATIENT_NAME).unwrap()),
        "DOE^JOHN"
    );
}

#[tokio::test]
async fn date_range_filter_on_study_date() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.1",
        "1.2.1.1",
        "1.2.1.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "P1",
        "A",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.2",
        "1.2.2.1",
        "1.2.2.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "P2",
        "B",
        "20260601",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "P3",
        "C",
        "20261201",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::STUDY_DATE),
        MatchingRule::Range(RangeMatching::closed("20260201", "20261101")),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::STUDY_DATE).unwrap()),
        "20260601"
    );
}

#[tokio::test]
async fn uid_list_filter_returns_matching_studies() {
    let (repo, pool) = open_repo().await;

    for (uid, patient) in [("1.2.1", "P1"), ("1.2.2", "P2"), ("1.2.3", "P3")] {
        insert_instance(
            &pool,
            uid,
            &format!("{uid}.1"),
            &format!("{uid}.1.1"),
            "1.2.840.10008.5.1.4.1.1.4",
            patient,
            patient,
            "20260101",
            "CT",
            None,
            &empty_attrs(),
        )
        .await;
    }

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
        MatchingRule::UidList(vec!["1.2.1".to_string(), "1.2.3".to_string()]),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 2);
}

#[tokio::test]
async fn fields_projection_returns_only_requested_attributes() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE^JOHN",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Fields(vec![
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            AttributePath::from_tag(tags::PATIENT_ID),
        ]),
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    let attrs = page.items[0].attributes();

    // Only the two requested tags should be present.
    assert_eq!(attrs.len(), 2);
    assert!(
        attrs
            .iter()
            .any(|a| a.path == AttributePath::from_tag(tags::STUDY_INSTANCE_UID))
    );
    assert!(
        attrs
            .iter()
            .any(|a| a.path == AttributePath::from_tag(tags::PATIENT_ID))
    );
    // Patient name was not requested.
    assert!(
        !attrs
            .iter()
            .any(|a| a.path == AttributePath::from_tag(tags::PATIENT_NAME))
    );
}

#[tokio::test]
async fn blob_attribute_returned_for_all_projection() {
    let (repo, pool) = open_repo().await;

    // Store a non-indexed attribute (Manufacturer) in the blob.
    let mut extra = InMemDicomObject::new_empty();
    extra.put(DataElement::new(tags::MANUFACTURER, VR::LO, "ACME"));

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        None,
        &extra,
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::All,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    let manufacturer = find_attr(&page, 0, tags::MANUFACTURER).expect("manufacturer present");
    assert_eq!(present_text(manufacturer), "ACME");
}

#[tokio::test]
async fn absent_blob_attribute_returns_absent_for_fields_projection() {
    let (repo, pool) = open_repo().await;

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    // Request a non-indexed, non-stored attribute.
    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::Fields(vec![
            AttributePath::from_tag(tags::SOP_INSTANCE_UID),
            AttributePath::from_tag(tags::MANUFACTURER), // not in blob
        ]),
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    let manufacturer = find_attr(&page, 0, tags::MANUFACTURER).expect("manufacturer attr");
    assert!(
        matches!(manufacturer, ResponseValue::Absent),
        "non-stored blob attribute must be Absent"
    );
}

#[tokio::test]
async fn null_indexed_column_returns_zero_length_for_fields_projection() {
    let (repo, pool) = open_repo().await;

    // Accession number is indexed but we don't insert it → NULL column.
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Fields(vec![
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            AttributePath::from_tag(tags::ACCESSION_NUMBER), // stored but NULL
        ]),
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    let accession = find_attr(&page, 0, tags::ACCESSION_NUMBER).expect("accession attr");
    assert!(
        matches!(accession, ResponseValue::ZeroLength),
        "NULL indexed column must be ZeroLength, not Absent"
    );
}

#[tokio::test]
async fn sequence_predicate_matches_instance_with_matching_item() {
    let (repo, pool) = open_repo().await;

    // Build an attributes blob with RequestAttributesSequence containing a step ID.
    let mut item = InMemDicomObject::new_empty();
    item.put(DataElement::new(
        tags::SCHEDULED_PROCEDURE_STEP_ID,
        VR::SH,
        "STEP-1",
    ));
    let mut attrs = InMemDicomObject::new_empty();
    attrs.put(DataElement::new(
        tags::REQUEST_ATTRIBUTES_SEQUENCE,
        VR::SQ,
        dicom_core::value::Value::new_sequence(vec![item], dicom_core::Length::UNDEFINED),
    ));

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        Some(1),
        &attrs,
    )
    .await;

    // Insert a second instance without the sequence (should not match).
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.2",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        Some(2),
        &empty_attrs(),
    )
    .await;

    let inner = Predicate::Attribute(
        AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
        MatchingRule::SingleValue("STEP-1".to_string()),
    );
    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::Default,
    )
    .expect("valid query")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
        MatchingRule::Sequence(SequenceMatching {
            predicate: Box::new(inner),
        }),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::SOP_INSTANCE_UID).unwrap()),
        "1.2.3.1.1"
    );
}

#[tokio::test]
async fn paging_returns_correct_page_and_total_count() {
    let (repo, pool) = open_repo().await;

    // Insert 5 studies.
    for i in 1u32..=5 {
        let uid = format!("1.2.{i}");
        let suid = format!("1.2.{i}.1");
        let iuid = format!("1.2.{i}.1.1");
        insert_instance(
            &pool,
            &uid,
            &suid,
            &iuid,
            "1.2.840.10008.5.1.4.1.1.4",
            &format!("P{i}"),
            &format!("PAT{i}"),
            "20260101",
            "CT",
            None,
            &empty_attrs(),
        )
        .await;
    }

    let paging = QueryPaging::new(2, 2).expect("valid paging"); // skip 2, take 2
    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_paging(paging);

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.total, Some(5), "total should reflect all 5 studies");
    assert_eq!(page.offset, 2);
    assert_eq!(page.limit, 2);
}

#[tokio::test]
async fn empty_result_with_paging_reports_zero_total() {
    let (repo, _pool) = open_repo().await;

    let paging = QueryPaging::new(0, 10).expect("valid paging");
    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_paging(paging);

    let page = repo.execute(&query).await.expect("execute");
    assert!(page.items.is_empty());
    assert_eq!(page.total, Some(0));
}

#[tokio::test]
async fn patient_scope_returns_one_row_per_patient() {
    let (repo, pool) = open_repo().await;

    // Same patient, two studies.
    insert_instance(
        &pool,
        "1.2.1",
        "1.2.1.1",
        "1.2.1.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    insert_instance(
        &pool,
        "1.2.2",
        "1.2.2.1",
        "1.2.2.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260201",
        "MR",
        None,
        &empty_attrs(),
    )
    .await;
    // Different patient.
    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-002",
        "SMITH",
        "20260301",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
        Projection::Default,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    assert_eq!(page.items.len(), 2, "two distinct patients");
}

#[tokio::test]
async fn sequence_attribute_in_blob_materialises_as_sequence_value() {
    let (repo, pool) = open_repo().await;

    let mut item = InMemDicomObject::new_empty();
    item.put(DataElement::new(
        tags::SCHEDULED_PROCEDURE_STEP_ID,
        VR::SH,
        "STEP-1",
    ));
    let mut attrs = InMemDicomObject::new_empty();
    attrs.put(DataElement::new(
        tags::REQUEST_ATTRIBUTES_SEQUENCE,
        VR::SQ,
        dicom_core::value::Value::new_sequence(vec![item], dicom_core::Length::UNDEFINED),
    ));

    insert_instance(
        &pool,
        "1.2.3",
        "1.2.3.1",
        "1.2.3.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "DOE",
        "20260101",
        "CT",
        Some(1),
        &attrs,
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::All,
    )
    .expect("valid query");

    let page = repo.execute(&query).await.expect("execute");
    let seq_value =
        find_attr(&page, 0, tags::REQUEST_ATTRIBUTES_SEQUENCE).expect("sequence attribute present");

    match seq_value {
        ResponseValue::Present(AttributeValue::Sequence(items)) => {
            assert_eq!(items.len(), 1, "one sequence item");
            let step_id = items[0]
                .attributes()
                .iter()
                .find(|a| a.path == AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID))
                .expect("step ID inside sequence item");
            assert_eq!(present_text(&step_id.value), "STEP-1");
        }
        other => panic!("expected Sequence, got {other:?}"),
    }
}

#[tokio::test]
async fn sort_key_orders_results_by_study_date_descending() {
    let (repo, pool) = open_repo().await;

    // Insert three studies with different dates and different synced_at so the
    // default order (newest-synced first) would NOT be date order.
    for (uid, date, synced_at) in [
        ("1.2.1", "20260301", 1000i64),
        ("1.2.2", "20260101", 3000i64), // newest synced → first without sort
        ("1.2.3", "20260201", 2000i64),
    ] {
        sqlx::query(
            "INSERT OR IGNORE INTO studies \
             (study_instance_uid, patient_id, study_date, synced_at_unix_ms) \
             VALUES (?, 'P', ?, ?)",
        )
        .bind(uid)
        .bind(date)
        .bind(synced_at)
        .execute(&pool)
        .await
        .expect("insert study");

        sqlx::query(
            "INSERT OR IGNORE INTO series (series_instance_uid, study_instance_uid) VALUES (?, ?)",
        )
        .bind(format!("{uid}.1"))
        .bind(uid)
        .execute(&pool)
        .await
        .expect("insert series");

        let attrs = to_attributes_json(&empty_attrs());
        sqlx::query(
            "INSERT OR IGNORE INTO instances \
             (sop_instance_uid, sop_class_uid, series_instance_uid, study_instance_uid, attributes) \
             VALUES (?, '1.2.840.10008.5.1.4.1.1.4', ?, ?, ?)",
        )
        .bind(format!("{uid}.1.1"))
        .bind(format!("{uid}.1"))
        .bind(uid)
        .bind(attrs)
        .execute(&pool)
        .await
        .expect("insert instance");
    }

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_sort_keys(vec![SortKey {
        path: AttributePath::from_tag(tags::STUDY_DATE),
        direction: SortDirection::Descending,
    }])
    .expect("valid sort keys");

    let page = repo.execute(&query).await.expect("execute");

    assert_eq!(page.items.len(), 3);
    // Expect descending date order: 20260301, 20260201, 20260101
    let dates: Vec<&str> = page
        .items
        .iter()
        .map(|m| {
            m.attributes()
                .iter()
                .find(|a| a.path == AttributePath::from_tag(tags::STUDY_DATE))
                .and_then(|a| {
                    if let ResponseValue::Present(AttributeValue::Text(s)) = &a.value {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or("")
        })
        .collect();
    assert_eq!(dates, vec!["20260301", "20260201", "20260101"]);
}

#[tokio::test]
async fn fuzzy_matching_finds_phonetically_similar_patient_name() {
    let (repo, pool) = open_repo().await;

    // "SMITH" and "SMYTH" are phonetically equivalent under SOUNDEX (both → S530).
    // We store just the family-name component so the SOUNDEX comparison operates
    // on the same granularity as the query value.  In production the sync process
    // stores the full DICOM PN value; callers that need family-name matching should
    // pass only the family-name component as the query value.
    insert_instance(
        &pool,
        "1.2.1",
        "1.2.1.1",
        "1.2.1.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-001",
        "SMYTH",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;
    // "JONES" → J520, phonetically distinct from "SMITH" → S530.
    insert_instance(
        &pool,
        "1.2.2",
        "1.2.2.1",
        "1.2.2.1.1",
        "1.2.840.10008.5.1.4.1.1.4",
        "PAT-002",
        "JONES",
        "20260101",
        "CT",
        None,
        &empty_attrs(),
    )
    .await;

    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
        Projection::Default,
    )
    .expect("valid query")
    .with_fuzzy_matching()
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::PATIENT_NAME),
        MatchingRule::SingleValue("SMITH".to_string()),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");

    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::PATIENT_NAME).unwrap()),
        "SMYTH"
    );
}

#[tokio::test]
async fn timezone_adjustment_shifts_dt_range_for_acquisition_time() {
    let (repo, pool) = open_repo().await;

    // Store two instances.  We store acquisition_date_time in UTC (no offset).
    // Instance A: acquired at UTC 20260101 020000 (= local midnight UTC+0200).
    // Instance B: acquired at UTC 20260101 120000 (= local noon UTC+0200).
    for (uid, acq_dt, idx) in [
        ("1.2.1.1.1", "20260101020000", 1i64),
        ("1.2.1.1.2", "20260101120000", 2i64),
    ] {
        sqlx::query(
            "INSERT OR IGNORE INTO studies \
             (study_instance_uid, patient_id, synced_at_unix_ms) VALUES ('1.2.1', 'P', 0)",
        )
        .execute(&pool)
        .await
        .expect("study");
        sqlx::query(
            "INSERT OR IGNORE INTO series \
             (series_instance_uid, study_instance_uid) VALUES ('1.2.1.1', '1.2.1')",
        )
        .execute(&pool)
        .await
        .expect("series");
        sqlx::query(
            "INSERT OR REPLACE INTO instances \
             (sop_instance_uid, sop_class_uid, series_instance_uid, study_instance_uid, \
              instance_number, acquisition_date_time, attributes) \
             VALUES (?, '1.2.840.10008.5.1.4.1.1.4', '1.2.1.1', '1.2.1', ?, ?, '{}')",
        )
        .bind(uid)
        .bind(idx)
        .bind(acq_dt)
        .execute(&pool)
        .await
        .expect("instance");
    }

    // SCU is at UTC+0200 and queries for instances acquired after 06:00 local
    // time (= 04:00 UTC).  Only the 12:00 UTC instance should match.
    let query = DicomQuery::new(
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
        Projection::Default,
    )
    .expect("valid query")
    .with_timezone_offset("+0200")
    .expect("valid offset")
    .with_predicate(Predicate::Attribute(
        AttributePath::from_tag(tags::ACQUISITION_DATE_TIME),
        // SCU says "after 06:00 in my +0200 timezone"
        MatchingRule::DateTimeRange(RangeMatching::from_start("20260101060000")),
    ))
    .expect("valid predicate");

    let page = repo.execute(&query).await.expect("execute");

    // Only the 12:00 UTC instance (06:00+0200 == 04:00 UTC < 12:00 UTC) matches.
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        present_text(find_attr(&page, 0, tags::SOP_INSTANCE_UID).unwrap()),
        "1.2.1.1.2"
    );
}
