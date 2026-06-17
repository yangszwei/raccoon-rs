use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use dicom_dictionary_std::tags;
use raccoon_protocol_dicomweb::{DicomWebRouter, QidoRsProvider};
use raccoon_service_query::{
    AttributePath, AttributeValue, DicomQuery, MatchingRule, Predicate, ProjectedAttribute,
    Projection, QueryError, QueryMatch, QueryPage, QueryScope, QueryService, ResponseValue,
    StudyRootQueryRetrieveLevel,
};
use serde_json::Value;
use tower::ServiceExt;

#[derive(Default)]
struct FakeQueryService {
    requests: Mutex<Vec<DicomQuery>>,
}

#[async_trait]
impl QueryService for FakeQueryService {
    async fn query(&self, request: DicomQuery) -> Result<QueryPage, QueryError> {
        self.requests.lock().unwrap().push(request);
        Ok(QueryPage::new(vec![query_match()], 0, 100, Some(1)))
    }
}

#[tokio::test]
async fn qido_supports_all_json_search_endpoints() {
    let cases = [
        (
            "/studies",
            StudyRootQueryRetrieveLevel::Study,
            "http://pacs.example.test/studies/1.2.3",
        ),
        (
            "/studies/1.2.3/series",
            StudyRootQueryRetrieveLevel::Series,
            "http://pacs.example.test/studies/1.2.3/series/1.2.3.4",
        ),
        (
            "/studies/1.2.3/instances",
            StudyRootQueryRetrieveLevel::Image,
            "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        ),
        (
            "/studies/1.2.3/series/1.2.3.4/instances",
            StudyRootQueryRetrieveLevel::Image,
            "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        ),
        (
            "/series",
            StudyRootQueryRetrieveLevel::Series,
            "http://pacs.example.test/studies/1.2.3/series/1.2.3.4",
        ),
        (
            "/instances",
            StudyRootQueryRetrieveLevel::Image,
            "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        ),
    ];

    for (path, expected_level, expected_retrieve_url) in cases {
        let query = Arc::new(FakeQueryService::default());
        let response = request(router(query.clone()), path).await;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/dicom+json",
            "{path}"
        );
        let json = response_json(response).await;
        assert_eq!(json[0]["00081190"]["Value"][0], expected_retrieve_url);
        assert_eq!(json[0]["00100010"]["Value"][0]["Alphabetic"], "DOE^JOHN");
        assert_eq!(json[0]["00080050"]["vr"], "SH");
        assert!(json[0]["00080050"].get("Value").is_none());

        let requests = query.requests.lock().unwrap();
        assert!(matches!(
            requests.last().unwrap().scope(),
            QueryScope::StudyRoot(level) if level == expected_level
        ));
    }
}

#[tokio::test]
async fn qido_supports_xml_search_endpoints() {
    for path in ["/studies", "/series", "/instances"] {
        let response = request_with_accept(
            router(Arc::new(FakeQueryService::default())),
            path,
            "multipart/related; type=\"application/dicom+xml\"",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
        assert!(content_type.contains("multipart/related"), "{content_type}");
        assert!(
            content_type.contains("type=\"application/dicom+xml\""),
            "{content_type}"
        );
        let body = response_text(response).await;
        assert!(
            body.contains("Content-Type: application/dicom+xml"),
            "{body}"
        );
        assert!(
            body.contains(
                r#"<NativeDicomModel xmlns="http://dicom.nema.org/PS3.19/models/NativeDICOM">"#
            ),
            "{body}"
        );
        assert!(
            body.contains(r#"<DicomAttribute tag="00100010" vr="PN" keyword="PatientName"><PersonName number="1"><Alphabetic>DOE^JOHN</Alphabetic></PersonName></DicomAttribute>"#),
            "{body}"
        );
        assert!(body.contains(r#"<DicomAttribute tag="00081190" vr="UR" keyword="RetrieveURL"><Value number="1">http://pacs.example.test/studies/1.2.3"#), "{body}");
    }
}

#[tokio::test]
async fn qido_xml_retrieve_url_preserves_mounted_and_proxy_prefixes() {
    let query = Arc::new(FakeQueryService::default());
    let router = Router::new().nest("/proxy/dicomweb", router(query));
    let response = request_with_accept(
        router,
        "/proxy/dicomweb/instances",
        "multipart/related; type=\"application/dicom+xml\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(
        body.contains("http://pacs.example.test/proxy/dicomweb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"),
        "{body}"
    );
}

#[tokio::test]
async fn qido_q_values_choose_json_or_xml() {
    let response = request_with_accept(
        router(Arc::new(FakeQueryService::default())),
        "/studies",
        "application/dicom+json;q=0.1, multipart/related; type=\"application/dicom+xml\";q=0.9",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .contains("type=\"application/dicom+xml\"")
    );

    let response = request_with_accept(
        router(Arc::new(FakeQueryService::default())),
        "/studies",
        "application/dicom+json;q=0.9, multipart/related; type=\"application/dicom+xml\";q=0.1",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom+json"
    );
}

#[tokio::test]
async fn qido_builds_query_controls_and_predicates() {
    let query = Arc::new(FakeQueryService::default());
    let response = request(
        router(query.clone()),
        "/studies?includefield=00100020,PatientName&limit=5&offset=2&fuzzymatching=true&timezoneoffset=%2B0800&StudyDate=20260101-20260201&Modality=CT\\MR&PatientID=",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let requests = query.requests.lock().unwrap();
    let request = requests.last().unwrap();

    assert!(matches!(request.projection(), Projection::Fields(fields) if fields.len() == 2));
    assert_eq!(
        request
            .paging()
            .map(|paging| (paging.offset(), paging.limit())),
        Some((2, 5))
    );
    assert!(request.fuzzy_matching());
    assert_eq!(request.timezone_offset_from_utc(), Some("+0800"));

    let Predicate::All(predicates) = request.predicate().unwrap() else {
        panic!("expected conjunction");
    };
    assert!(
        predicates
            .iter()
            .any(|predicate| matches!(predicate, Predicate::Attribute(_, MatchingRule::Range(_))))
    );
    assert!(predicates.iter().any(|predicate| matches!(
        predicate,
        Predicate::Attribute(_, MatchingRule::MultipleValues(values)) if values == &["CT", "MR"]
    )));
    assert!(
        predicates
            .iter()
            .any(|predicate| matches!(predicate, Predicate::Attribute(_, MatchingRule::Universal)))
    );
}

#[tokio::test]
async fn qido_supports_includefield_all_and_uid_lists() {
    let query = Arc::new(FakeQueryService::default());
    let response = request(
        router(query.clone()),
        "/instances?includefield=all&SOPInstanceUID=1.2.3.4.5\\1.2.3.4.6",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let requests = query.requests.lock().unwrap();
    let request = requests.last().unwrap();
    assert!(request.projection().is_all());
    let Predicate::All(predicates) = request.predicate().unwrap() else {
        panic!("expected conjunction");
    };
    assert!(predicates.iter().any(|predicate| matches!(
        predicate,
        Predicate::Attribute(_, MatchingRule::UidList(values)) if values.len() == 2
    )));
}

#[tokio::test]
async fn qido_retrieve_url_preserves_mounted_and_proxy_prefixes() {
    let query = Arc::new(FakeQueryService::default());
    let mounted = Router::new().nest("/dicomweb", router(query.clone()));
    let response = request(mounted, "/dicomweb/series").await;
    let json = response_json(response).await;
    assert_eq!(
        json[0]["00081190"]["Value"][0],
        "http://pacs.example.test/dicomweb/studies/1.2.3/series/1.2.3.4"
    );

    let proxied = Router::new().nest("/proxy", Router::new().nest("/dicomweb", router(query)));
    let response = request(proxied, "/proxy/dicomweb/instances").await;
    let json = response_json(response).await;
    assert_eq!(
        json[0]["00081190"]["Value"][0],
        "http://pacs.example.test/proxy/dicomweb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
    );
}

#[tokio::test]
async fn qido_rejects_malformed_path_uid_and_sequence_matching() {
    let response = request(
        router(Arc::new(FakeQueryService::default())),
        "/studies/1..2/series",
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = request(
        router(Arc::new(FakeQueryService::default())),
        "/series?RequestAttributesSequence=STEP-1",
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn qido_capabilities_advertise_json_and_supported_params() {
    let payload = response_json(
        router(Arc::new(FakeQueryService::default()))
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/")
                    .header(header::HOST, "pacs.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;

    let text = payload.to_string();
    assert!(text.contains("application/dicom+json"));
    assert!(text.contains("timezoneoffset"));
    assert!(text.contains("multipart/related; type=\\\"application/dicom+xml\\\""));
}

fn router(query: Arc<FakeQueryService>) -> Router {
    DicomWebRouter::new()
        .register(QidoRsProvider::new(query))
        .into_router()
}

async fn request(router: Router, uri: &str) -> axum::response::Response {
    request_with_accept(router, uri, "application/dicom+json").await
}

async fn request_with_accept(router: Router, uri: &str, accept: &str) -> axum::response::Response {
    router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header(header::ACCEPT, accept)
                .header(header::HOST, "pacs.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn response_text(response: axum::response::Response) -> String {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn query_match() -> QueryMatch {
    QueryMatch::new(vec![
        attr(tags::STUDY_DATE, "20260101"),
        attr(tags::STUDY_TIME, "120000"),
        zero(tags::ACCESSION_NUMBER),
        attr(tags::MODALITIES_IN_STUDY, "CT"),
        attr(tags::REFERRING_PHYSICIAN_NAME, "REF^DOC"),
        attr(tags::PATIENT_NAME, "DOE^JOHN"),
        attr(tags::PATIENT_ID, "PAT-001"),
        attr(tags::PATIENT_BIRTH_DATE, "19700101"),
        attr(tags::PATIENT_SEX, "O"),
        attr(tags::STUDY_INSTANCE_UID, "1.2.3"),
        attr(tags::STUDY_ID, "STUDY-001"),
        attr(tags::NUMBER_OF_STUDY_RELATED_SERIES, "1"),
        attr(tags::NUMBER_OF_STUDY_RELATED_INSTANCES, "1"),
        attr(tags::MODALITY, "CT"),
        attr(tags::SERIES_DESCRIPTION, "Head CT"),
        attr(tags::SERIES_INSTANCE_UID, "1.2.3.4"),
        attr(tags::SERIES_NUMBER, "7"),
        attr(tags::NUMBER_OF_SERIES_RELATED_INSTANCES, "1"),
        attr(tags::SOP_CLASS_UID, "1.2.840.10008.5.1.4.1.1.2"),
        attr(tags::SOP_INSTANCE_UID, "1.2.3.4.5"),
        attr(tags::INSTANCE_NUMBER, "9"),
        attr(tags::ROWS, "512"),
        attr(tags::COLUMNS, "512"),
        attr(tags::BITS_ALLOCATED, "16"),
        attr(tags::NUMBER_OF_FRAMES, "1"),
    ])
    .unwrap()
}

fn attr(tag: dicom_core::Tag, value: &str) -> ProjectedAttribute {
    ProjectedAttribute {
        path: AttributePath::from_tag(tag),
        value: ResponseValue::Present(AttributeValue::Text(value.to_string())),
    }
}

fn zero(tag: dicom_core::Tag) -> ProjectedAttribute {
    ProjectedAttribute {
        path: AttributePath::from_tag(tag),
        value: ResponseValue::ZeroLength,
    }
}
