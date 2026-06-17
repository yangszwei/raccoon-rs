use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use futures_util::StreamExt;
use raccoon_contract_object_store::ObjectKey;
use raccoon_protocol_dicomweb::{DicomWebRouter, StowRsProvider, StowRsProviderOptions};
use raccoon_service_ingest::{
    IngestBatchRepositoryStatus, IngestBatchResult, IngestError, IngestObjectId,
    IngestObjectIdentity, IngestObjectOutcome, IngestObjectState, IngestPayloadRepresentation,
    IngestRequest, IngestResult, IngestService,
};
use serde_json::Value;
use tokio::time::sleep;
use tower::ServiceExt;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Default)]
struct FakeIngest {
    requests: Mutex<Vec<RecordedIngestRequest>>,
    outcome: Mutex<Option<IngestObjectOutcome>>,
    repository_failed: bool,
}

#[derive(Debug)]
struct RecordedIngestRequest {
    expected_study_instance_uid: Option<String>,
    body_len: usize,
    payload_representation: IngestPayloadRepresentation,
}

#[async_trait]
impl IngestService for FakeIngest {
    async fn ingest_upload_object(
        &self,
        request: IngestRequest,
    ) -> Result<IngestResult, IngestError> {
        Ok(self
            .ingest_upload_objects(vec![request])
            .await
            .object_results
            .remove(0))
    }

    async fn ingest_upload_objects(&self, requests: Vec<IngestRequest>) -> IngestBatchResult {
        let mut results = Vec::new();
        for mut request in requests {
            let mut body_len = 0;
            while let Some(chunk) = request.body.next().await {
                body_len += chunk.expect("read request body").len();
            }
            self.requests.lock().unwrap().push(RecordedIngestRequest {
                expected_study_instance_uid: request.expected_study_instance_uid.clone(),
                body_len,
                payload_representation: request.payload_representation,
            });
            let outcome = self
                .outcome
                .lock()
                .unwrap()
                .clone()
                .unwrap_or(IngestObjectOutcome::Stored);
            results.push(result(request, outcome));
        }
        let repository_status = if self.repository_failed {
            IngestBatchRepositoryStatus::Failed {
                reason: "repository unavailable".to_string(),
            }
        } else {
            IngestBatchRepositoryStatus::Recorded
        };
        IngestBatchResult::new(
            results
                .first()
                .map(|result| result.upload_id.clone())
                .unwrap_or_default(),
            results,
            repository_status,
        )
    }
}

#[tokio::test]
async fn stow_accepts_multipart_dicom_instances() {
    let ingest = Arc::new(FakeIngest::default());
    let response = request(router(ingest.clone()), "/studies", dicom_body("DICOMDATA")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom+json"
    );
    let json = response_json(response).await;
    assert_eq!(
        json["00081199"]["Value"][0]["00081155"]["Value"][0],
        "1.2.3.4.5"
    );
    assert_eq!(
        json["00081199"]["Value"][0]["00081190"]["Value"][0],
        "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
    );

    let requests = ingest.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body_len, 9);
    assert_eq!(
        requests[0].payload_representation,
        IngestPayloadRepresentation::DicomFile
    );
}

#[tokio::test]
async fn stow_passes_expected_study_uid_for_study_route() {
    let ingest = Arc::new(FakeIngest::default());
    let response = request(
        router(ingest.clone()),
        "/studies/1.2.3",
        dicom_body("DICOMDATA"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let requests = ingest.requests.lock().unwrap();
    assert_eq!(
        requests[0].expected_study_instance_uid.as_deref(),
        Some("1.2.3")
    );
}

#[tokio::test]
async fn stow_study_uid_mismatch_returns_conflict() {
    let ingest = Arc::new(FakeIngest::default());
    *ingest.outcome.lock().unwrap() = Some(IngestObjectOutcome::RejectedStudyMismatch {
        expected_study_instance_uid: "1.2.9".to_string(),
        actual_study_instance_uid: Some("1.2.3".to_string()),
    });
    let response = request(router(ingest), "/studies/1.2.9", dicom_body("DICOMDATA")).await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = response_json(response).await;
    assert_eq!(json["00081198"]["Value"][0]["00081197"]["Value"][0], 0xC000);
}

#[tokio::test]
async fn stow_accepts_json_metadata_plus_bulkdata() {
    let ingest = Arc::new(FakeIngest::default());
    let response = metadata_request(
        router(ingest.clone()),
        "/studies",
        APPLICATION_DICOM_JSON_MULTIPART,
        json_metadata_body("bulk/pixel"),
        "application/octet-stream",
        "bulk/pixel",
        b"\0\x01\x02\x03",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(
        json["00081199"]["Value"][0]["00081155"]["Value"][0],
        "1.2.3.4.5"
    );
    assert_eq!(
        json["00081199"]["Value"][0]["00081190"]["Value"][0],
        "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
    );

    let requests = ingest.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].body_len > 132);
    assert_eq!(
        requests[0].payload_representation,
        IngestPayloadRepresentation::DicomFile
    );
}

#[tokio::test]
async fn stow_accepts_xml_metadata_plus_bulkdata() {
    let ingest = Arc::new(FakeIngest::default());
    let response = metadata_request(
        router(ingest.clone()),
        "/studies",
        APPLICATION_DICOM_XML_MULTIPART,
        xml_metadata_body("bulk/pixel"),
        "application/octet-stream",
        "bulk/pixel",
        b"\0\x01\x02\x03",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(ingest.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn stow_missing_bulk_part_returns_failed_sop_item() {
    let ingest = Arc::new(FakeIngest::default());
    let response = router(ingest.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/studies")
                .header(header::HOST, "pacs.example.test")
                .header(header::CONTENT_TYPE, APPLICATION_DICOM_JSON_MULTIPART)
                .body(Body::from(format!(
                    "--BOUNDARY\r\nContent-Type: application/dicom+json\r\n\r\n{}\r\n--BOUNDARY--\r\n",
                    json_metadata_body("missing/pixel")
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_json(response).await;
    assert_eq!(
        json["00081198"]["Value"][0]["00081155"]["Value"][0],
        "1.2.3.4.5"
    );
    assert_eq!(json["00081198"]["Value"][0]["00081197"]["Value"][0], 0xC000);
    assert!(ingest.requests.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn stow_metadata_plus_bulkdata_records_safe_span_counts() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        records: records.clone(),
    });
    let _ = tracing::subscriber::set_global_default(subscriber);

    let response = router(Arc::new(FakeIngest::default()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/studies")
                .header(header::HOST, "pacs.example.test")
                .header(header::CONTENT_TYPE, APPLICATION_DICOM_JSON_MULTIPART)
                .body(Body::from(format!(
                    "--BOUNDARY\r\nContent-Type: application/dicom+json\r\n\r\n{}\r\n--BOUNDARY--\r\n",
                    json_metadata_body("missing/pixel")
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let records = records.lock().unwrap().join("\n");
    assert!(
        records.contains("dicomweb.store.metadata_media_type=JSON"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.store.bulk_part_count=0"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.store.missing_bulk_part_count=1"),
        "{records}"
    );
    assert!(!records.contains("missing/pixel"), "{records}");
}

#[tokio::test]
async fn stow_metadata_study_uid_mismatch_returns_conflict() {
    let ingest = Arc::new(FakeIngest::default());
    let response = metadata_request(
        router(ingest.clone()),
        "/studies/1.2.9",
        APPLICATION_DICOM_JSON_MULTIPART,
        json_metadata_body("bulk/pixel"),
        "application/octet-stream",
        "bulk/pixel",
        b"\0\x01\x02\x03",
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(ingest.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn stow_rejects_compressed_bulk_media() {
    let response = router(Arc::new(FakeIngest::default()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/studies")
                .header(header::HOST, "pacs.example.test")
                .header(header::CONTENT_TYPE, APPLICATION_DICOM_JSON_MULTIPART)
                .body(Body::from(format!(
                    "--BOUNDARY\r\nContent-Type: application/dicom+json\r\n\r\n{}\r\n\
                     --BOUNDARY\r\nContent-Type: image/jpeg\r\nContent-Location: bulk/pixel\r\n\r\njpeg\r\n\
                     --BOUNDARY--\r\n",
                    json_metadata_body("bulk/pixel")
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn stow_rejects_part_before_spool_limit_is_exceeded() {
    let response = request(
        limited_router(
            Arc::new(FakeIngest::default()),
            StowRsProviderOptions::new().with_max_part_size_bytes(4),
        ),
        "/studies",
        dicom_body("DICOMDATA"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn stow_rejects_too_many_parts() {
    let response = request(
        limited_router(
            Arc::new(FakeIngest::default()),
            StowRsProviderOptions::new().with_max_part_count(1),
        ),
        "/studies",
        two_part_dicom_body(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn stow_deletes_spool_file_when_ingest_fails() {
    let before = stow_spool_file_count().await;
    let ingest = Arc::new(FakeIngest {
        repository_failed: true,
        ..FakeIngest::default()
    });
    let response = router(ingest)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/studies")
                .header(header::HOST, "pacs.example.test")
                .header(
                    header::CONTENT_TYPE,
                    "multipart/related; type=\"application/dicom\"; boundary=BOUNDARY",
                )
                .body(Body::from(dicom_body("DICOMDATA")))
                .unwrap(),
        )
        .await;

    assert_eq!(
        response.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    for _ in 0..20 {
        if stow_spool_file_count().await <= before {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(stow_spool_file_count().await, before);
}

#[tokio::test]
async fn stow_capabilities_advertise_metadata_plus_bulkdata() {
    let payload = response_json(
        router(Arc::new(FakeIngest::default()))
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

    assert!(text.contains("StoreInstances"));
    assert!(text.contains("StoreStudyInstances"));
    assert!(text.contains("application/dicom"));
    assert!(text.contains("application/dicom+json"));
    assert!(text.contains("application/dicom+xml"));
    assert!(!text.contains("maxUploadSizeBytes"));
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: tracing::Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = CaptureVisitor {
            records: Vec::new(),
        };
        attrs.record(&mut visitor);
        self.records.lock().unwrap().extend(visitor.records);
    }

    fn on_record(
        &self,
        _id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = CaptureVisitor {
            records: Vec::new(),
        };
        values.record(&mut visitor);
        self.records.lock().unwrap().extend(visitor.records);
    }
}

struct CaptureVisitor {
    records: Vec<String>,
}

impl Visit for CaptureVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.records.push(format!("{}={value}", field.name()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.records.push(format!("{}={value}", field.name()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.records.push(format!("{}={value}", field.name()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.records.push(format!("{}={value}", field.name()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.records.push(format!("{}={value:?}", field.name()));
    }
}

const APPLICATION_DICOM_JSON_MULTIPART: &str =
    "multipart/related; type=\"application/dicom+json\"; boundary=BOUNDARY";
const APPLICATION_DICOM_XML_MULTIPART: &str =
    "multipart/related; type=\"application/dicom+xml\"; boundary=BOUNDARY";

fn router(ingest: Arc<FakeIngest>) -> Router {
    limited_router(ingest, StowRsProviderOptions::new())
}

fn limited_router(ingest: Arc<FakeIngest>, options: StowRsProviderOptions) -> Router {
    DicomWebRouter::new()
        .register(StowRsProvider::with_options(ingest, options))
        .into_router()
}

async fn request(router: Router, uri: &str, body: String) -> axum::response::Response {
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::HOST, "pacs.example.test")
                .header(
                    header::CONTENT_TYPE,
                    "multipart/related; type=\"application/dicom\"; boundary=BOUNDARY",
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn metadata_request(
    router: Router,
    uri: &str,
    request_content_type: &str,
    metadata: String,
    bulk_content_type: &str,
    bulk_location: &str,
    bulk: &[u8],
) -> axum::response::Response {
    let mut body = Vec::new();
    body.extend_from_slice(b"--BOUNDARY\r\nContent-Type: ");
    if request_content_type.contains("application/dicom+xml") {
        body.extend_from_slice(b"application/dicom+xml");
    } else {
        body.extend_from_slice(b"application/dicom+json");
    }
    body.extend_from_slice(b"\r\n\r\n");
    body.extend_from_slice(metadata.as_bytes());
    body.extend_from_slice(b"\r\n--BOUNDARY\r\nContent-Type: ");
    body.extend_from_slice(bulk_content_type.as_bytes());
    body.extend_from_slice(b"\r\nContent-Location: ");
    body.extend_from_slice(bulk_location.as_bytes());
    body.extend_from_slice(b"\r\n\r\n");
    body.extend_from_slice(bulk);
    body.extend_from_slice(b"\r\n--BOUNDARY--\r\n");
    router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::HOST, "pacs.example.test")
                .header(header::CONTENT_TYPE, request_content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn json_metadata_body(bulk_uri: &str) -> String {
    format!(
        r#"[{{
            "00080016": {{"vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"]}},
            "00080018": {{"vr": "UI", "Value": ["1.2.3.4.5"]}},
            "0020000D": {{"vr": "UI", "Value": ["1.2.3"]}},
            "0020000E": {{"vr": "UI", "Value": ["1.2.3.4"]}},
            "7FE00010": {{"vr": "OB", "BulkDataURI": "{bulk_uri}"}}
        }}]"#
    )
}

fn xml_metadata_body(bulk_uri: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <NativeDicomModel xmlns="http://dicom.nema.org/PS3.19/models/NativeDICOM">
            <DicomAttribute tag="00080016" vr="UI"><Value number="1">1.2.840.10008.5.1.4.1.1.2</Value></DicomAttribute>
            <DicomAttribute tag="00080018" vr="UI"><Value number="1">1.2.3.4.5</Value></DicomAttribute>
            <DicomAttribute tag="0020000D" vr="UI"><Value number="1">1.2.3</Value></DicomAttribute>
            <DicomAttribute tag="0020000E" vr="UI"><Value number="1">1.2.3.4</Value></DicomAttribute>
            <DicomAttribute tag="7FE00010" vr="OB"><BulkData uri="{bulk_uri}"/></DicomAttribute>
        </NativeDicomModel>"#
    )
}

fn dicom_body(data: &str) -> String {
    format!("--BOUNDARY\r\nContent-Type: application/dicom\r\n\r\n{data}\r\n--BOUNDARY--\r\n")
}

fn two_part_dicom_body() -> String {
    concat!(
        "--BOUNDARY\r\nContent-Type: application/dicom\r\n\r\nONE\r\n",
        "--BOUNDARY\r\nContent-Type: application/dicom\r\n\r\nTWO\r\n",
        "--BOUNDARY--\r\n"
    )
    .to_string()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn result(request: IngestRequest, outcome: IngestObjectOutcome) -> IngestResult {
    IngestResult {
        ingest_object_id: IngestObjectId::new(),
        upload_id: request.upload_id,
        object_key: Some(ObjectKey::new("instances/1").unwrap()),
        content_length: Some(9),
        etag: None,
        checksum: None,
        identity: IngestObjectIdentity {
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.1.2".to_string()),
            study_instance_uid: Some("1.2.3".to_string()),
            series_instance_uid: Some("1.2.3.4".to_string()),
            sop_instance_uid: Some("1.2.3.4.5".to_string()),
        },
        payload_representation: IngestPayloadRepresentation::DicomFile,
        transfer_syntax_uid: None,
        state: IngestObjectState::PendingSync,
        outcome,
    }
}

async fn stow_spool_file_count() -> usize {
    let Ok(mut entries) = tokio::fs::read_dir(std::env::temp_dir()).await else {
        return 0;
    };
    let mut count = 0;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("raccoon-dicomweb-stow-")
        {
            count += 1;
        }
    }
    count
}
