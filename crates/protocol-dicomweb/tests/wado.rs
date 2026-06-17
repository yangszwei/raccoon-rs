use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use dicom_core::value::Value as DicomValue;
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use futures_util::stream;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    TransferSyntaxUid,
};
use raccoon_contract_object_store::ByteStream;
use raccoon_protocol_dicomweb::{
    DicomWebRouter, RenderCacheConfig, RenderedWadoRsProvider, WadoRenderOptions, WadoRsProvider,
    WadoUriProvider,
};
use raccoon_service_retrieve::{
    InstanceMetadata, MetadataRepository, RetrieveError, RetrieveRepositoryError, RetrieveRequest,
    RetrieveResult, RetrieveScope, RetrieveService, RetrievedInstance,
};
use serde_json::Value;
use tower::ServiceExt;
use tracing::Metadata;
use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const NATIVE_TS: &str = "1.2.840.10008.1.2.1";
const OTHER_TS: &str = "1.2.840.10008.1.2.4.50";
static TRACING_CAPTURE_LOCK: Mutex<()> = Mutex::new(());
static TRACING_RECORDS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

#[derive(Default)]
struct FakeRetrieve {
    requests: Mutex<Vec<RetrieveScope>>,
    instances: Vec<FakeInstance>,
}

#[derive(Clone)]
struct FakeInstance {
    sop_instance_uid: &'static str,
    transfer_syntax_uid: &'static str,
    body: Vec<u8>,
}

#[derive(Default)]
struct FakeMetadata {
    requests: Mutex<Vec<RetrieveScope>>,
    rows: Vec<InstanceMetadata>,
}

#[async_trait]
impl RetrieveService for FakeRetrieve {
    async fn retrieve(&self, request: RetrieveRequest) -> Result<RetrieveResult, RetrieveError> {
        self.requests.lock().unwrap().push(request.scope);
        let instances = self
            .instances
            .iter()
            .map(|instance| {
                Ok(RetrievedInstance {
                    identity: identity(instance.sop_instance_uid),
                    transfer_syntax_uid: Some(
                        TransferSyntaxUid::new(instance.transfer_syntax_uid).unwrap(),
                    ),
                    content_length: instance.body.len() as u64,
                    body: ByteStream::once(instance.body.clone()),
                })
            })
            .collect::<Vec<_>>();
        Ok(RetrieveResult {
            instance_count: instances.len(),
            total_content_length: None,
            stream: Box::pin(stream::iter(instances)),
        })
    }
}

#[async_trait]
impl MetadataRepository for FakeMetadata {
    async fn find_metadata(
        &self,
        scope: &RetrieveScope,
    ) -> Result<Vec<InstanceMetadata>, RetrieveRepositoryError> {
        self.requests.lock().unwrap().push(scope.clone());
        Ok(self
            .rows
            .iter()
            .filter(|row| scope_matches(scope, row))
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn wado_study_retrieve_returns_multipart_dicom() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![
            fake_instance("1.2.3.4.5", "ONE"),
            fake_instance("1.2.3.4.6", "TWO"),
        ],
        ..FakeRetrieve::default()
    });
    let response = request(
        router(retrieve.clone()),
        "/studies/1.2.3",
        "multipart/related; type=\"application/dicom\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(content_type.contains("multipart/related"));
    assert!(content_type.contains("type=\"application/dicom\""));
    let body = response_text(response).await;
    assert!(
        body.contains("Content-Type: application/dicom; transfer-syntax=\"1.2.840.10008.1.2.1\"")
    );
    assert!(body.contains("Content-Location: http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"));
    assert!(body.contains("ONE"));
    assert!(body.contains("TWO"));
    assert_eq!(body.matches("Content-Type: application/dicom").count(), 2);

    let requests = retrieve.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Study { study_instance_uid } if study_instance_uid.as_str() == "1.2.3"
    ));
}

#[tokio::test]
async fn wado_retrieve_wildcard_accept_selects_multipart_dicom() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![fake_instance("1.2.3.4.5", "ONE")],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(content_type.contains("multipart/related"), "{content_type}");
    assert!(
        content_type.contains("type=\"application/dicom\""),
        "{content_type}"
    );
}

#[tokio::test]
async fn wado_series_retrieve_includes_parent_study_scope() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![fake_instance("1.2.3.4.5", "ONE")],
        ..FakeRetrieve::default()
    });
    let response = request(
        router(retrieve.clone()),
        "/studies/1.2.3/series/1.2.3.4",
        "multipart/related; type=\"application/dicom\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let requests = retrieve.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Series {
            study_instance_uid: Some(study),
            series_instance_uid,
        } if study.as_str() == "1.2.3" && series_instance_uid.as_str() == "1.2.3.4"
    ));
}

#[tokio::test]
async fn wado_instance_retrieve_supports_single_application_dicom() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![fake_instance("1.2.3.4.5", "ONE")],
        ..FakeRetrieve::default()
    });
    let response = request(
        router(retrieve.clone()),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        "application/dicom",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom; transfer-syntax=\"1.2.840.10008.1.2.1\""
    );
    assert_eq!(response_text(response).await, "ONE");
    let requests = retrieve.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Instance {
            study_instance_uid: Some(study),
            series_instance_uid: Some(series),
            sop_instance_uid,
        } if study.as_str() == "1.2.3"
            && series.as_str() == "1.2.3.4"
            && sop_instance_uid.as_str() == "1.2.3.4.5"
    ));
}

#[tokio::test]
async fn wado_invalid_uid_path_returns_bad_request() {
    let response = request(
        router(Arc::new(FakeRetrieve::default())),
        "/studies/1..2",
        "multipart/related; type=\"application/dicom\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn wado_unsupported_accept_returns_not_acceptable() {
    let response = request(
        router(Arc::new(FakeRetrieve::default())),
        "/studies/1.2.3",
        "application/dicom+json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wado_unsupported_requested_transfer_syntax_returns_not_acceptable() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![fake_instance("1.2.3.4.5", "ONE")],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        "multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.4.50\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wado_matching_requested_transfer_syntax_returns_native_object() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![FakeInstance {
                sop_instance_uid: "1.2.3.4.5",
                transfer_syntax_uid: OTHER_TS,
                body: b"JPEG".to_vec(),
            }],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
        "multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.4.50\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("transfer-syntax=\"1.2.840.10008.1.2.4.50\""));
    assert!(body.contains("JPEG"));
}

#[tokio::test]
async fn wado_uri_rejects_missing_required_params() {
    let cases = [
        "/wado?studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5",
        "/wado?requestType=WADO&seriesUID=1.2.3.4&objectUID=1.2.3.4.5",
        "/wado?requestType=WADO&studyUID=1.2.3&objectUID=1.2.3.4.5",
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4",
    ];

    for uri in cases {
        let response = request(router_uri(Arc::new(FakeRetrieve::default())), uri, "*/*").await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
}

#[tokio::test]
async fn wado_uri_rejects_invalid_required_params() {
    let response = request(
        router_uri(Arc::new(FakeRetrieve::default())),
        "/wado?requestType=WADO&studyUID=1..2&seriesUID=1.2.3.4&objectUID=1.2.3.4.5",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn wado_uri_dicom_object_returns_application_dicom() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![fake_instance("1.2.3.4.5", "ONE")],
        ..FakeRetrieve::default()
    });
    let response = request(
        router_uri(retrieve.clone()),
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5&contentType=application/dicom",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom; transfer-syntax=\"1.2.840.10008.1.2.1\""
    );
    assert_eq!(response_text(response).await, "ONE");
    let requests = retrieve.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Instance {
            study_instance_uid: Some(study),
            series_instance_uid: Some(series),
            sop_instance_uid,
        } if study.as_str() == "1.2.3"
            && series.as_str() == "1.2.3.4"
            && sop_instance_uid.as_str() == "1.2.3.4.5"
    ));
}

#[tokio::test]
async fn wado_uri_missing_object_returns_not_found() {
    let response = request(
        router_uri(Arc::new(FakeRetrieve::default())),
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn wado_uri_rendered_request_is_not_acceptable_before_rendered_phase() {
    let response = request(
        router_uri(Arc::new(FakeRetrieve::default())),
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5&contentType=image/jpeg",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wado_uri_unsupported_transfer_syntax_without_transcoding_is_not_acceptable() {
    let response = request(
        router_uri(Arc::new(FakeRetrieve {
            instances: vec![fake_instance("1.2.3.4.5", "ONE")],
            ..FakeRetrieve::default()
        })),
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5&transferSyntax=1.2.840.10008.1.2.4.50",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wado_uri_matching_transfer_syntax_returns_native_object() {
    let response = request(
        router_uri(Arc::new(FakeRetrieve {
            instances: vec![FakeInstance {
                sop_instance_uid: "1.2.3.4.5",
                transfer_syntax_uid: OTHER_TS,
                body: b"JPEG".to_vec(),
            }],
            ..FakeRetrieve::default()
        })),
        "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5&transferSyntax=1.2.840.10008.1.2.4.50",
        "*/*",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom; transfer-syntax=\"1.2.840.10008.1.2.4.50\""
    );
    assert_eq!(response_text(response).await, "JPEG");
}

#[tokio::test]
async fn wado_instance_rendered_returns_jpeg() {
    let response = request(
        router_with_render(Arc::new(FakeRetrieve {
            instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered",
        "image/jpeg",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "image/jpeg");
    let body = response_bytes(response).await;
    assert!(body.starts_with(&[0xff, 0xd8, 0xff]), "{body:?}");
}

#[tokio::test]
async fn wado_instance_rendered_returns_png() {
    let response = request(
        router_with_render(Arc::new(FakeRetrieve {
            instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered",
        "image/png",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "image/png");
    let body = response_bytes(response).await;
    assert!(body.starts_with(b"\x89PNG\r\n\x1a\n"), "{body:?}");
}

#[tokio::test]
async fn wado_study_rendered_returns_multipart_images() {
    let response = request(
        router_with_render(Arc::new(FakeRetrieve {
            instances: vec![
                fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255]),
                fake_dicom_instance("1.2.3.4.6", &[255, 128, 64, 0]),
            ],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/rendered",
        "multipart/related; type=\"image/jpeg\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(content_type.contains("multipart/related"), "{content_type}");
    assert!(
        content_type.contains("type=\"image/jpeg\""),
        "{content_type}"
    );
    let body = response_bytes(response).await;
    let text = String::from_utf8_lossy(&body);
    assert_eq!(text.matches("Content-Type: image/jpeg").count(), 2);
}

#[tokio::test]
async fn wado_thumbnail_endpoints_return_single_image() {
    for path in [
        "/studies/1.2.3/thumbnail",
        "/studies/1.2.3/series/1.2.3.4/thumbnail",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/thumbnail",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1/thumbnail",
    ] {
        let response = request(
            router_with_render(Arc::new(FakeRetrieve {
                instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
                ..FakeRetrieve::default()
            })),
            path,
            "image/png",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()[header::CONTENT_TYPE], "image/png");
    }
}

#[tokio::test]
async fn wado_frames_rendered_returns_one_image_per_frame() {
    let response = request(
        router_with_render(Arc::new(FakeRetrieve {
            instances: vec![fake_dicom_instance(
                "1.2.3.4.5",
                &[0, 64, 128, 255, 255, 128, 64, 0],
            )],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1,2/rendered",
        "multipart/related; type=\"image/png\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(
        content_type.contains("type=\"image/png\""),
        "{content_type}"
    );
    let body = response_bytes(response).await;
    let text = String::from_utf8_lossy(&body);
    assert_eq!(text.matches("Content-Type: image/png").count(), 2);
}

#[tokio::test]
async fn wado_rendered_accepts_viewport_window_and_quality() {
    let response = request(
        router_with_render(Arc::new(FakeRetrieve {
            instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?viewport=1,1&window=128,256&quality=70",
        "image/jpeg",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "image/jpeg");
}

#[tokio::test]
async fn wado_rendered_rejects_unsupported_params() {
    for uri in [
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?iccprofile=yes",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?annotation=patient",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?presentationUID=1.2.3",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?region=0,0,1,1",
    ] {
        let response = request(
            router_with_render(Arc::new(FakeRetrieve {
                instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
                ..FakeRetrieve::default()
            })),
            uri,
            "image/jpeg",
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE, "{uri}");
    }
}

#[test]
fn wado_uri_records_route_and_validated_uid_span_fields() {
    let _guard = TRACING_CAPTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let records = tracing_records();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::callsite::rebuild_interest_cache();
    let response = runtime.block_on(async {
        request(
            router_uri(Arc::new(FakeRetrieve {
                instances: vec![fake_instance("1.2.3.4.5", "ONE")],
                ..FakeRetrieve::default()
            })),
            "/wado?requestType=WADO&studyUID=1.2.3&seriesUID=1.2.3.4&objectUID=1.2.3.4.5&contentType=application/dicom&transferSyntax=1.2.840.10008.1.2.1",
            "*/*",
        )
        .await
    });

    assert_eq!(response.status(), StatusCode::OK);
    let records = records.lock().unwrap().join("\n");
    assert!(records.contains("dicomweb.service=WADO-URI"), "{records}");
    assert!(records.contains("dicomweb.resource=object"), "{records}");
    assert!(records.contains("http.route=/wado"), "{records}");
    assert!(
        records.contains("dicom.study_instance_uid=1.2.3"),
        "{records}"
    );
    assert!(
        records.contains("dicom.series_instance_uid=1.2.3.4"),
        "{records}"
    );
    assert!(
        records.contains("dicom.sop_instance_uid=1.2.3.4.5"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.requested_content_type=application/dicom"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.requested_transfer_syntax_uid=1.2.840.10008.1.2.1"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.selected_media_type=application/dicom; transfer-syntax=\"1.2.840.10008.1.2.1\""),
        "{records}"
    );
    assert!(!records.contains("requestType"), "{records}");
    assert!(!records.contains("ONE"), "{records}");
}

#[test]
fn wado_records_retrieve_span_fields() {
    let _guard = TRACING_CAPTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let records = tracing_records();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::callsite::rebuild_interest_cache();
    let (response, metadata_response) = runtime.block_on(async {
        let response = request(
            router(Arc::new(FakeRetrieve {
                instances: vec![fake_instance("1.2.3.4.5", "ONE")],
                ..FakeRetrieve::default()
            })),
            "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5",
            "multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.1\"",
        )
        .await;
        let metadata_response = request(
            router_with_metadata(
                Arc::new(FakeRetrieve::default()),
                Arc::new(FakeMetadata {
                    rows: vec![metadata_row_with_bulk_data()],
                    ..FakeMetadata::default()
                }),
            ),
            "/studies/1.2.3/metadata",
            "multipart/related; type=\"application/dicom+xml\"",
        )
        .await;
        (response, metadata_response)
    });

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(metadata_response.status(), StatusCode::OK);
    let records = records.lock().unwrap().join("\n");
    assert!(records.contains("dicomweb.service=WADO-RS"), "{records}");
    assert!(records.contains("dicomweb.resource=dicom"), "{records}");
    assert!(records.contains("dicomweb.resource=metadata"), "{records}");
    assert!(
        records.contains("http.route=/studies/{study}/series/{series}/instances/{instance}"),
        "{records}"
    );
    assert!(
        records.contains("dicom.study_instance_uid=1.2.3"),
        "{records}"
    );
    assert!(
        records.contains("dicom.series_instance_uid=1.2.3.4"),
        "{records}"
    );
    assert!(
        records.contains("dicom.sop_instance_uid=1.2.3.4.5"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.selected_media_type=multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.1\""),
        "{records}"
    );
    assert!(
        records.contains(
            "dicomweb.selected_media_type=multipart/related; type=\"application/dicom+xml\""
        ),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.requested_transfer_syntax_uid=1.2.840.10008.1.2.1"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.returned_transfer_syntax_uid=1.2.840.10008.1.2.1"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.retrieve.instance_count=1"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.metadata.row_count=1"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.metadata.bulk_data_uri_count=2"),
        "{records}"
    );
    assert!(!records.contains("ONE"), "{records}");
    assert!(!records.contains("Doe^Jane"), "{records}");
}

#[test]
fn wado_rendered_records_backend_and_cache_span_fields_without_body_bytes() {
    let _guard = TRACING_CAPTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let records = tracing_records();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::callsite::rebuild_interest_cache();
    let response = runtime.block_on(async {
        request(
            router_with_render(Arc::new(FakeRetrieve {
                instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
                ..FakeRetrieve::default()
            })),
            "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered?quality=80",
            "image/jpeg",
        )
        .await
    });

    assert_eq!(response.status(), StatusCode::OK);
    let records = records.lock().unwrap().join("\n");
    assert!(records.contains("dicomweb.service=WADO-RS"), "{records}");
    assert!(records.contains("dicomweb.resource=rendered"), "{records}");
    assert!(
        records
            .contains("http.route=/studies/{study}/series/{series}/instances/{instance}/rendered"),
        "{records}"
    );
    assert!(
        records.contains("dicom.study_instance_uid=1.2.3"),
        "{records}"
    );
    assert!(
        records.contains("dicom.series_instance_uid=1.2.3.4"),
        "{records}"
    );
    assert!(
        records.contains("dicom.sop_instance_uid=1.2.3.4.5"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.selected_media_type=image/jpeg"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.renderer_backend=native"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.render_cache_result=bypass"),
        "{records}"
    );
    assert!(!records.contains("255, 216"), "{records}");
}

#[test]
fn wado_rendered_records_cache_miss_store_and_hit() {
    let _guard = TRACING_CAPTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let records = tracing_records();
    let cache_dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::callsite::rebuild_interest_cache();
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![fake_dicom_instance("1.2.3.4.5", &[0, 64, 128, 255])],
        ..FakeRetrieve::default()
    });
    runtime.block_on(async {
        let router = router_with_render_options(
            retrieve,
            WadoRenderOptions {
                cache: Some(RenderCacheConfig {
                    directory: cache_dir.path().to_path_buf(),
                    ttl: None,
                    max_bytes: None,
                }),
                ..WadoRenderOptions::default()
            },
        );
        for _ in 0..2 {
            let response = request(
                router.clone(),
                "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/rendered",
                "image/png",
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
        }
    });

    let records = records.lock().unwrap().join("\n");
    assert!(
        records.contains("dicomweb.render_cache_result=miss"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.render_cache_result=store"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.render_cache_result=hit"),
        "{records}"
    );
}

#[tokio::test]
async fn wado_empty_retrieve_returns_not_found() {
    let response = request(
        router(Arc::new(FakeRetrieve::default())),
        "/studies/1.2.3",
        "multipart/related; type=\"application/dicom\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn wado_study_metadata_returns_dicom_json_array_without_retrieving_objects() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![fake_instance("1.2.3.4.5", "SHOULD_NOT_READ")],
        ..FakeRetrieve::default()
    });
    let metadata = Arc::new(FakeMetadata {
        rows: vec![
            metadata_row("1.2.3.4", "1.2.3.4.5"),
            metadata_row("1.2.3.5", "1.2.3.5.6"),
        ],
        ..FakeMetadata::default()
    });
    let response = request(
        router_with_metadata(retrieve.clone(), metadata.clone()),
        "/studies/1.2.3/metadata",
        "application/dicom+json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/dicom+json"
    );
    let payload = response_json(response).await;
    assert_eq!(payload.as_array().unwrap().len(), 2);
    assert!(payload[0].get("00020010").is_none());
    assert_eq!(payload[0]["00080018"]["Value"][0], "1.2.3.4.5");
    assert_eq!(
        payload[0]["00081190"]["Value"][0],
        "http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
    );
    assert!(retrieve.requests.lock().unwrap().is_empty());
    assert_eq!(metadata.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn wado_metadata_supports_xml_resources() {
    for path in [
        "/studies/1.2.3/metadata",
        "/studies/1.2.3/series/1.2.3.4/metadata",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/metadata",
    ] {
        let response = request(
            router_with_metadata(
                Arc::new(FakeRetrieve::default()),
                Arc::new(FakeMetadata {
                    rows: vec![metadata_row_with_bulk_data()],
                    ..FakeMetadata::default()
                }),
            ),
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
        assert!(body.contains(r#"<DicomAttribute tag="00081190" vr="UR" keyword="RetrieveURL"><Value number="1">http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5</Value></DicomAttribute>"#), "{body}");
        assert!(body.contains(r#"<DicomAttribute tag="7FE00010" vr="OB" keyword="PixelData"><BulkData uri="http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010"/></DicomAttribute>"#), "{body}");
        assert!(body.contains(r#"<DicomAttribute tag="54000100" vr="SQ" keyword="WaveformSequence"><Item number="1"><DicomAttribute tag="54001010" vr="OW" keyword="WaveformData"><BulkData uri="http://pacs.example.test/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/54000100/0/54001010"/></DicomAttribute></Item></DicomAttribute>"#), "{body}");
    }
}

#[tokio::test]
async fn wado_metadata_xml_rewrites_urls_under_nested_mount() {
    let response = nested_router_with_metadata(
        Arc::new(FakeRetrieve::default()),
        Arc::new(FakeMetadata {
            rows: vec![metadata_row_with_bulk_data()],
            ..FakeMetadata::default()
        }),
    )
    .oneshot(
        Request::builder()
            .method("GET")
            .uri("/dicom-web/studies/1.2.3/metadata")
            .header(
                header::ACCEPT,
                "multipart/related; type=\"application/dicom+xml\"",
            )
            .header(header::HOST, "public.example.test")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(
        body.contains(
            "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
        ),
        "{body}"
    );
    assert!(
        body.contains("http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/54000100/0/54001010"),
        "{body}"
    );
}

#[tokio::test]
async fn wado_metadata_q_values_choose_json_or_xml() {
    let metadata = || {
        Arc::new(FakeMetadata {
            rows: vec![metadata_row("1.2.3.4", "1.2.3.4.5")],
            ..FakeMetadata::default()
        })
    };
    let response = request(
        router_with_metadata(Arc::new(FakeRetrieve::default()), metadata()),
        "/studies/1.2.3/metadata",
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

    let response = request(
        router_with_metadata(Arc::new(FakeRetrieve::default()), metadata()),
        "/studies/1.2.3/metadata",
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
async fn wado_series_metadata_scopes_to_requested_series() {
    let metadata = Arc::new(FakeMetadata {
        rows: vec![
            metadata_row("1.2.3.4", "1.2.3.4.5"),
            metadata_row("1.2.3.5", "1.2.3.5.6"),
        ],
        ..FakeMetadata::default()
    });
    let response = request(
        router_with_metadata(Arc::new(FakeRetrieve::default()), metadata.clone()),
        "/studies/1.2.3/series/1.2.3.5/metadata",
        "application/dicom+json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload.as_array().unwrap().len(), 1);
    assert_eq!(payload[0]["00080018"]["Value"][0], "1.2.3.5.6");
    let requests = metadata.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Series {
            study_instance_uid: Some(study),
            series_instance_uid,
        } if study.as_str() == "1.2.3" && series_instance_uid.as_str() == "1.2.3.5"
    ));
}

#[tokio::test]
async fn wado_instance_metadata_scopes_to_requested_instance() {
    let metadata = Arc::new(FakeMetadata {
        rows: vec![metadata_row("1.2.3.4", "1.2.3.4.5")],
        ..FakeMetadata::default()
    });
    let response = request(
        router_with_metadata(Arc::new(FakeRetrieve::default()), metadata.clone()),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/metadata",
        "application/dicom+json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(payload.as_array().unwrap().len(), 1);
    let requests = metadata.requests.lock().unwrap();
    assert!(matches!(
        &requests[0],
        RetrieveScope::Instance {
            study_instance_uid: Some(study),
            series_instance_uid: Some(series),
            sop_instance_uid,
        } if study.as_str() == "1.2.3"
            && series.as_str() == "1.2.3.4"
            && sop_instance_uid.as_str() == "1.2.3.4.5"
    ));
}

#[tokio::test]
async fn wado_metadata_rewrites_bulk_data_uri_under_nested_mount() {
    let metadata = Arc::new(FakeMetadata {
        rows: vec![metadata_row_with_bulk_data()],
        ..FakeMetadata::default()
    });
    let response = nested_router_with_metadata(Arc::new(FakeRetrieve::default()), metadata)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/dicom-web/studies/1.2.3/metadata")
                .header(header::ACCEPT, "application/dicom+json")
                .header(header::HOST, "public.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(
        payload[0]["00081190"]["Value"][0],
        "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
    );
    assert_eq!(
        payload[0]["7FE00010"]["BulkDataURI"],
        "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010"
    );
    assert!(payload[0]["7FE00010"].get("InlineBinary").is_none());
    assert_eq!(
        payload[0]["54000100"]["Value"][0]["54001010"]["BulkDataURI"],
        "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/54000100/0/54001010"
    );
}

#[tokio::test]
async fn wado_metadata_rewrites_vr_only_bulk_data_markers() {
    let metadata = Arc::new(FakeMetadata {
        rows: vec![metadata_row_with_vr_only_bulk_data_marker()],
        ..FakeMetadata::default()
    });
    let response = nested_router_with_metadata(Arc::new(FakeRetrieve::default()), metadata)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/dicom-web/studies/1.2.3/metadata")
                .header(header::ACCEPT, "application/dicom+json")
                .header(header::HOST, "public.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(
        payload[0]["54000100"]["Value"][0]["54001010"]["BulkDataURI"],
        "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/54000100/0/54001010"
    );
}

#[tokio::test]
async fn wado_metadata_rewrites_value_bulk_data_markers() {
    let metadata = Arc::new(FakeMetadata {
        rows: vec![metadata_row_with_value_bulk_data_marker()],
        ..FakeMetadata::default()
    });
    let response = nested_router_with_metadata(Arc::new(FakeRetrieve::default()), metadata)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/dicom-web/studies/1.2.3/metadata")
                .header(header::ACCEPT, "application/dicom+json")
                .header(header::HOST, "public.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload = response_json(response).await;
    assert_eq!(
        payload[0]["00283010"]["Value"][0]["00283006"]["BulkDataURI"],
        "http://public.example.test/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/00283010/0/00283006"
    );
    assert!(
        payload[0]["00283010"]["Value"][0]["00283006"]
            .get("Value")
            .is_none()
    );
}

#[tokio::test]
async fn wado_metadata_returns_not_found_when_no_rows_match() {
    let response = request(
        router_with_metadata(
            Arc::new(FakeRetrieve::default()),
            Arc::new(FakeMetadata::default()),
        ),
        "/studies/1.2.3/metadata",
        "application/dicom+json",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn wado_metadata_rejects_unsupported_accept() {
    let response = request(
        router_with_metadata(
            Arc::new(FakeRetrieve::default()),
            Arc::new(FakeMetadata {
                rows: vec![metadata_row("1.2.3.4", "1.2.3.4.5")],
                ..FakeMetadata::default()
            }),
        ),
        "/studies/1.2.3/metadata",
        "application/dicom",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wado_bulk_data_resolves_metadata_uri_under_nested_mount() {
    let retrieve = Arc::new(FakeRetrieve {
        instances: vec![dicom_instance(
            "1.2.3.4.5",
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Some(&[9, 10, 11, 12]),
        )],
        ..FakeRetrieve::default()
    });
    let metadata = Arc::new(FakeMetadata {
        rows: vec![metadata_row_with_bulk_data()],
        ..FakeMetadata::default()
    });
    let router = Router::new().nest("/pacs/dicom-web", router_with_metadata(retrieve, metadata));

    let metadata_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/pacs/dicom-web/studies/1.2.3/metadata")
                .header(header::ACCEPT, "application/dicom+json")
                .header(header::HOST, "public.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metadata_response.status(), StatusCode::OK);
    let payload = response_json(metadata_response).await;
    let bulk_uri = payload[0]["7FE00010"]["BulkDataURI"].as_str().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(bulk_uri.strip_prefix("http://public.example.test").unwrap())
                .header(
                    header::ACCEPT,
                    "multipart/related; type=\"application/octet-stream\"",
                )
                .header(header::HOST, "public.example.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(content_type.contains("multipart/related"));
    assert!(content_type.contains("type=\"application/octet-stream\""));
    let body = response_bytes(response).await;
    assert!(
        body.windows(8)
            .any(|window| window == [1, 2, 3, 4, 5, 6, 7, 8])
    );
    let body_text = String::from_utf8_lossy(&body);
    assert!(
        body_text.contains(
            "Content-Location: http://public.example.test/pacs/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010"
        ),
        "{body_text}"
    );
}

#[tokio::test]
async fn wado_bulk_data_resolves_nested_sequence_path() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![dicom_instance(
                "1.2.3.4.5",
                &[1, 2, 3, 4],
                Some(&[21, 22, 23, 24]),
            )],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/54000100/0/54001010",
        "multipart/related; type=\"application/octet-stream\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    assert!(body.windows(4).any(|window| window == [21, 22, 23, 24]));
}

#[tokio::test]
async fn wado_pixel_data_resources_return_multipart_octet_stream() {
    for path in [
        "/studies/1.2.3/pixeldata",
        "/studies/1.2.3/series/1.2.3.4/pixeldata",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/pixeldata",
    ] {
        let response = request(
            router(Arc::new(FakeRetrieve {
                instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
                ..FakeRetrieve::default()
            })),
            path,
            "multipart/related; type=\"application/octet-stream\"",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
        assert!(content_type.contains("type=\"application/octet-stream\""));
        let body = response_bytes(response).await;
        assert!(
            body.windows(4).any(|window| window == [1, 2, 3, 4]),
            "{path}"
        );
    }
}

#[tokio::test]
async fn wado_bulk_pixel_and_frames_wildcard_accept_selects_octet_stream() {
    for path in [
        "/studies/1.2.3/pixeldata",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1",
    ] {
        let response = request(
            router(Arc::new(FakeRetrieve {
                instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
                ..FakeRetrieve::default()
            })),
            path,
            "*/*",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let content_type = response.headers()[header::CONTENT_TYPE].to_str().unwrap();
        assert!(content_type.contains("multipart/related"), "{content_type}");
        assert!(
            content_type.contains("type=\"application/octet-stream\""),
            "{content_type}"
        );
    }
}

#[tokio::test]
async fn wado_frames_return_requested_uncompressed_frames() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4, 5, 6, 7, 8], None)],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1,2",
        "multipart/related; type=\"application/octet-stream\"",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    assert!(body.windows(4).any(|window| window == [1, 2, 3, 4]));
    assert!(body.windows(4).any(|window| window == [5, 6, 7, 8]));
    let body_text = String::from_utf8_lossy(&body);
    assert!(body_text.contains("/frames/1"));
    assert!(body_text.contains("/frames/2"));
}

#[tokio::test]
async fn wado_invalid_frame_lists_return_bad_request() {
    for frames in ["0", "abc", "1,,2", "1,1", "2,1"] {
        let response = request(
            router(Arc::new(FakeRetrieve {
                instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
                ..FakeRetrieve::default()
            })),
            &format!("/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/{frames}"),
            "multipart/related; type=\"application/octet-stream\"",
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{frames}");
    }
}

#[tokio::test]
async fn wado_bulk_pixel_and_frames_reject_unsupported_media() {
    for path in [
        "/studies/1.2.3/pixeldata",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010",
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1",
    ] {
        let response = request(
            router(Arc::new(FakeRetrieve {
                instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
                ..FakeRetrieve::default()
            })),
            path,
            "application/dicom",
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE, "{path}");
    }
}

#[tokio::test]
async fn wado_missing_bulk_path_and_frame_return_not_found() {
    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/00283006",
        "multipart/related; type=\"application/octet-stream\"",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = request(
        router(Arc::new(FakeRetrieve {
            instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4], None)],
            ..FakeRetrieve::default()
        })),
        "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/2",
        "multipart/related; type=\"application/octet-stream\"",
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[test]
fn wado_records_bulk_frame_and_part_span_fields() {
    let _guard = TRACING_CAPTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let records = tracing_records();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::callsite::rebuild_interest_cache();
    let response = runtime.block_on(async {
        request(
            router(Arc::new(FakeRetrieve {
                instances: vec![dicom_instance("1.2.3.4.5", &[1, 2, 3, 4, 5, 6, 7, 8], None)],
                ..FakeRetrieve::default()
            })),
            "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/frames/1,2",
            "multipart/related; type=\"application/octet-stream\"",
        )
        .await
    });

    assert_eq!(response.status(), StatusCode::OK);
    let records = records.lock().unwrap().join("\n");
    assert!(records.contains("dicomweb.resource=frames"), "{records}");
    assert!(
        records.contains("dicomweb.requested_frame_count=2"),
        "{records}"
    );
    assert!(
        records.contains("dicomweb.returned_part_count=2"),
        "{records}"
    );
    assert!(
        records.contains(
            "dicomweb.selected_media_type=multipart/related; type=\"application/octet-stream\""
        ),
        "{records}"
    );
    assert!(!records.contains("[1, 2, 3, 4"), "{records}");
}

#[tokio::test]
async fn wado_capabilities_advertise_native_dicom_retrieve_only() {
    let payload = response_json(
        router(Arc::new(FakeRetrieve::default()))
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

    assert!(text.contains("RetrieveStudy"));
    assert!(text.contains("RetrieveSeries"));
    assert!(text.contains("RetrieveBulkData"));
    assert!(text.contains("RetrieveStudyPixelData"));
    assert!(text.contains("RetrieveFrames"));
    assert!(text.contains("application/dicom"));
    assert!(text.contains("application/octet-stream"));
    assert!(text.contains("\"*\""));
    assert!(!text.contains("application/dicom+json"));
    assert!(!text.contains("rendered"));
    assert!(!text.contains("thumbnail"));
}

#[tokio::test]
async fn wado_capabilities_advertise_rendered_when_registered() {
    let payload = response_json(
        router_with_render(Arc::new(FakeRetrieve::default()))
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

    assert!(text.contains("RetrieveStudyRendered"));
    assert!(text.contains("RetrieveSeriesRendered"));
    assert!(text.contains("RetrieveInstanceRendered"));
    assert!(text.contains("RetrieveRenderedFrames"));
    assert!(text.contains("RetrieveStudyThumbnail"));
    assert!(text.contains("RetrieveFrameThumbnail"));
    assert!(text.contains("image/jpeg"));
    assert!(text.contains("image/png"));
    assert!(text.contains("viewport"));
    assert!(text.contains("window"));
    assert!(text.contains("quality"));
}

#[tokio::test]
async fn wado_uri_capabilities_are_advertised_only_when_registered() {
    let wado_rs_payload = response_json(
        router(Arc::new(FakeRetrieve::default()))
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
    assert!(!wado_rs_payload.to_string().contains("\"wado\""));

    let wado_uri_payload = response_json(
        router_uri(Arc::new(FakeRetrieve::default()))
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
    let text = wado_uri_payload.to_string();
    assert!(text.contains("\"wado\""));
    assert!(text.contains("requestType"));
    assert!(text.contains("studyUID"));
    assert!(text.contains("seriesUID"));
    assert!(text.contains("objectUID"));
    assert!(text.contains("application/dicom"));
}

#[tokio::test]
async fn wado_capabilities_advertise_json_metadata_when_registered() {
    let payload = response_json(
        router_with_metadata(
            Arc::new(FakeRetrieve::default()),
            Arc::new(FakeMetadata::default()),
        )
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

    assert!(text.contains("RetrieveStudyMetadata"));
    assert!(text.contains("RetrieveSeriesMetadata"));
    assert!(text.contains("RetrieveInstanceMetadata"));
    assert!(text.contains("application/dicom+json"));
    assert!(text.contains("multipart/related; type=\\\"application/dicom+xml\\\""));
}

fn router(retrieve: Arc<FakeRetrieve>) -> Router {
    DicomWebRouter::new()
        .register(WadoRsProvider::new(retrieve))
        .into_router()
}

fn router_uri(retrieve: Arc<FakeRetrieve>) -> Router {
    DicomWebRouter::new()
        .register(WadoUriProvider::new(retrieve))
        .into_router()
}

fn router_with_metadata(retrieve: Arc<FakeRetrieve>, metadata: Arc<FakeMetadata>) -> Router {
    DicomWebRouter::new()
        .register(WadoRsProvider::new(retrieve).with_metadata_repository(metadata))
        .into_router()
}

fn router_with_render(retrieve: Arc<FakeRetrieve>) -> Router {
    DicomWebRouter::new()
        .register(WadoRsProvider::new(retrieve.clone()))
        .register(RenderedWadoRsProvider::new(retrieve))
        .into_router()
}

fn router_with_render_options(retrieve: Arc<FakeRetrieve>, options: WadoRenderOptions) -> Router {
    DicomWebRouter::new()
        .register(WadoRsProvider::new(retrieve.clone()))
        .register(RenderedWadoRsProvider::with_options(retrieve, options))
        .into_router()
}

fn nested_router_with_metadata(retrieve: Arc<FakeRetrieve>, metadata: Arc<FakeMetadata>) -> Router {
    Router::new().nest("/dicom-web", router_with_metadata(retrieve, metadata))
}

async fn request(router: Router, uri: &str, accept: &str) -> axum::response::Response {
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
    let body = response_bytes(response).await;
    String::from_utf8(body.to_vec()).unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = response_bytes(response).await;
    serde_json::from_slice(&body).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> axum::body::Bytes {
    to_bytes(response.into_body(), usize::MAX).await.unwrap()
}

fn fake_instance(sop_instance_uid: &'static str, body: &'static str) -> FakeInstance {
    FakeInstance {
        sop_instance_uid,
        transfer_syntax_uid: NATIVE_TS,
        body: body.as_bytes().to_vec(),
    }
}

fn dicom_instance(
    sop_instance_uid: &'static str,
    pixel_data: &[u8],
    waveform_data: Option<&[u8]>,
) -> FakeInstance {
    FakeInstance {
        sop_instance_uid,
        transfer_syntax_uid: NATIVE_TS,
        body: dicom_bytes(sop_instance_uid, pixel_data, waveform_data),
    }
}

fn identity(sop_instance_uid: &str) -> DicomInstanceIdentity {
    identity_for("1.2.3.4", sop_instance_uid)
}

fn identity_for(series_uid: &str, sop_instance_uid: &str) -> DicomInstanceIdentity {
    DicomInstanceIdentity::new(
        StudyInstanceUid::new("1.2.3").unwrap(),
        SeriesInstanceUid::new(series_uid).unwrap(),
        SopInstanceUid::new(sop_instance_uid).unwrap(),
        SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
    )
}

fn metadata_row(series_uid: &str, sop_instance_uid: &str) -> InstanceMetadata {
    InstanceMetadata::new(
        identity_for(series_uid, sop_instance_uid),
        format!(
            r#"{{
                "00020010": {{"vr": "UI", "Value": ["1.2.840.10008.1.2.1"]}},
                "00080018": {{"vr": "UI", "Value": ["{sop_instance_uid}"]}},
                "00100010": {{"vr": "PN", "Value": [{{"Alphabetic": "Doe^Jane"}}]}}
            }}"#
        ),
    )
}

fn metadata_row_with_bulk_data() -> InstanceMetadata {
    InstanceMetadata::new(
        identity("1.2.3.4.5"),
        r#"{
            "00080018": {"vr": "UI", "Value": ["1.2.3.4.5"]},
            "7FE00010": {"vr": "OB", "InlineBinary": "AAAA"},
            "54000100": {
                "vr": "SQ",
                "Value": [
                    {
                        "54001010": {"vr": "OW", "BulkDataURI": "stored/waveform"}
                    }
                ]
            }
        }"#,
    )
}

fn metadata_row_with_vr_only_bulk_data_marker() -> InstanceMetadata {
    InstanceMetadata::new(
        identity("1.2.3.4.5"),
        r#"{
            "00080018": {"vr": "UI", "Value": ["1.2.3.4.5"]},
            "54000100": {
                "vr": "SQ",
                "Value": [
                    {
                        "54001010": {"vr": "OW"}
                    }
                ]
            }
        }"#,
    )
}

fn metadata_row_with_value_bulk_data_marker() -> InstanceMetadata {
    InstanceMetadata::new(
        identity("1.2.3.4.5"),
        r#"{
            "00080018": {"vr": "UI", "Value": ["1.2.3.4.5"]},
            "00283010": {
                "vr": "SQ",
                "Value": [
                    {
                        "00283006": {"vr": "US", "Value": [0, 1, 2, 3]}
                    }
                ]
            }
        }"#,
    )
}

fn dicom_bytes(sop_instance_uid: &str, pixel_data: &[u8], waveform_data: Option<&[u8]>) -> Vec<u8> {
    let mut object = InMemDicomObject::from_element_iter([
        DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, "1.2.3"),
        DataElement::new(tags::SERIES_INSTANCE_UID, VR::UI, "1.2.3.4"),
        DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, sop_instance_uid),
        DataElement::new(tags::SOP_CLASS_UID, VR::UI, "1.2.840.10008.5.1.4.1.1.2"),
        DataElement::new(tags::ROWS, VR::US, PrimitiveValue::from(2_u16)),
        DataElement::new(tags::COLUMNS, VR::US, PrimitiveValue::from(2_u16)),
        DataElement::new(tags::SAMPLES_PER_PIXEL, VR::US, PrimitiveValue::from(1_u16)),
        DataElement::new(tags::PHOTOMETRIC_INTERPRETATION, VR::CS, "MONOCHROME2"),
        DataElement::new(tags::BITS_ALLOCATED, VR::US, PrimitiveValue::from(8_u16)),
        DataElement::new(tags::BITS_STORED, VR::US, PrimitiveValue::from(8_u16)),
        DataElement::new(tags::HIGH_BIT, VR::US, PrimitiveValue::from(7_u16)),
        DataElement::new(
            tags::PIXEL_REPRESENTATION,
            VR::US,
            PrimitiveValue::from(0_u16),
        ),
        DataElement::new(
            tags::NUMBER_OF_FRAMES,
            VR::IS,
            (pixel_data.len() / 4).to_string(),
        ),
        DataElement::new(tags::PIXEL_DATA, VR::OB, PrimitiveValue::from(pixel_data)),
    ]);
    if let Some(waveform_data) = waveform_data {
        let item = InMemDicomObject::from_element_iter([DataElement::new(
            tags::WAVEFORM_DATA,
            VR::OW,
            PrimitiveValue::from(waveform_data),
        )]);
        object.put(DataElement::new(
            tags::WAVEFORM_SEQUENCE,
            VR::SQ,
            DicomValue::new_sequence(vec![item], dicom_core::Length::UNDEFINED),
        ));
    }
    let object = object
        .with_meta(FileMetaTableBuilder::new().transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN))
        .unwrap();
    let mut bytes = Vec::new();
    object.write_all(&mut bytes).unwrap();
    bytes
}

fn fake_dicom_instance(sop_instance_uid: &'static str, pixel_data: &[u8]) -> FakeInstance {
    FakeInstance {
        sop_instance_uid,
        transfer_syntax_uid: NATIVE_TS,
        body: dicom_bytes(sop_instance_uid, pixel_data, None),
    }
}

fn scope_matches(scope: &RetrieveScope, row: &InstanceMetadata) -> bool {
    match scope {
        RetrieveScope::Study { study_instance_uid } => {
            &row.identity.study_instance_uid == study_instance_uid
        }
        RetrieveScope::Series {
            study_instance_uid,
            series_instance_uid,
        } => {
            study_instance_uid
                .as_ref()
                .is_none_or(|study_uid| row.identity.study_instance_uid == *study_uid)
                && row.identity.series_instance_uid == *series_instance_uid
        }
        RetrieveScope::Instance {
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        } => {
            study_instance_uid
                .as_ref()
                .is_none_or(|study_uid| row.identity.study_instance_uid == *study_uid)
                && series_instance_uid
                    .as_ref()
                    .is_none_or(|series_uid| row.identity.series_instance_uid == *series_uid)
                && row.identity.sop_instance_uid == *sop_instance_uid
        }
        RetrieveScope::Patient { .. } => false,
    }
}

fn tracing_records() -> Arc<Mutex<Vec<String>>> {
    let records = TRACING_RECORDS
        .get_or_init(|| {
            let records = Arc::new(Mutex::new(Vec::new()));
            let subscriber = tracing_subscriber::registry().with(CaptureLayer {
                records: records.clone(),
            });
            let _ = tracing::subscriber::set_global_default(subscriber);
            records
        })
        .clone();
    records
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    records
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: tracing::Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        Interest::always()
    }

    fn enabled(&self, _metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = CaptureVisitor::default();
        attrs.record(&mut visitor);
        self.records.lock().unwrap().extend(visitor.records);
    }

    fn on_record(
        &self,
        _id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = CaptureVisitor::default();
        values.record(&mut visitor);
        self.records.lock().unwrap().extend(visitor.records);
    }
}

#[derive(Default)]
struct CaptureVisitor {
    records: Vec<String>,
}

impl Visit for CaptureVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.records.push(format!("{}={value:?}", field.name()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.records.push(format!("{}={value}", field.name()));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.records.push(format!("{}={value}", field.name()));
    }
}
