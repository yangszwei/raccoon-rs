use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use raccoon_contract_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use raccoon_service_ingest::{IngestBatchRepositoryStatus, IngestObjectOutcome, IngestResult};
use serde_json::{Value, json};

use crate::{DicomWebUrlBase, MediaType, MediaTypeParams, content_type};

pub(crate) fn storage_response(
    headers: &HeaderMap,
    uri: &Uri,
    results: &[IngestResult],
    repository_status: &IngestBatchRepositoryStatus,
) -> Response {
    let status = storage_status(results, repository_status);
    record_counts(results);
    record_status(status);
    let body = axum::Json(storage_response_json(
        results,
        DicomWebUrlBase::from_request(headers, uri).as_ref(),
    ));
    (
        status,
        [(
            header::CONTENT_TYPE,
            content_type(MediaType::ApplicationDicomJson, &MediaTypeParams::default()),
        )],
        body,
    )
        .into_response()
}

fn storage_status(
    results: &[IngestResult],
    repository_status: &IngestBatchRepositoryStatus,
) -> StatusCode {
    if matches!(
        repository_status,
        IngestBatchRepositoryStatus::Failed { .. }
    ) {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    let stored = results
        .iter()
        .filter(|result| result.outcome.is_stored())
        .count();
    if stored == results.len() {
        StatusCode::OK
    } else if stored > 0 {
        StatusCode::ACCEPTED
    } else if results.iter().any(|result| {
        matches!(
            result.outcome,
            IngestObjectOutcome::RejectedStudyMismatch { .. }
        )
    }) {
        StatusCode::CONFLICT
    } else if results
        .iter()
        .any(|result| matches!(result.outcome, IngestObjectOutcome::RejectedTooLarge { .. }))
    {
        StatusCode::PAYLOAD_TOO_LARGE
    } else if results.iter().any(|result| {
        matches!(
            result.outcome,
            IngestObjectOutcome::ObjectStoreFailed { .. }
                | IngestObjectOutcome::RepositoryFailed { .. }
        )
    }) {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    }
}

fn record_counts(results: &[IngestResult]) {
    let successful = results
        .iter()
        .filter(|result| result.outcome.is_stored())
        .count();
    let failed = results.len().saturating_sub(successful);
    let span = tracing::Span::current();
    span.record("dicomweb.object_count", results.len());
    span.record("dicomweb.successful_object_count", successful);
    span.record("dicomweb.failed_object_count", failed);
}

fn record_status(status: StatusCode) {
    let span = tracing::Span::current();
    span.record("http.response.status_code", status.as_u16());
    if status.is_success() {
        return;
    }

    let status_code = status.as_u16().to_string();
    let reason = status.canonical_reason().unwrap_or("HTTP error");
    span.record("error.type", status_code.as_str());
    span.record("dicomweb.error_type", status_code.as_str());
    span.record("error.message", reason);
    if status.is_server_error() {
        tracing::error!("dicomweb store completed with failures");
    } else {
        tracing::warn!("dicomweb store completed with failures");
    }
}

fn storage_response_json(results: &[IngestResult], url_base: Option<&DicomWebUrlBase>) -> Value {
    let mut referenced = Vec::new();
    let mut failed = Vec::new();
    for result in results {
        let item = storage_item_json(result, url_base);
        if result.outcome.is_stored() {
            referenced.push(item);
        } else {
            failed.push(item);
        }
    }

    let mut object = serde_json::Map::new();
    if !referenced.is_empty() {
        object.insert(
            "00081199".to_string(),
            json!({ "vr": "SQ", "Value": referenced }),
        );
    }
    if !failed.is_empty() {
        object.insert(
            "00081198".to_string(),
            json!({ "vr": "SQ", "Value": failed }),
        );
    }
    Value::Object(object)
}

fn storage_item_json(result: &IngestResult, url_base: Option<&DicomWebUrlBase>) -> Value {
    let mut object = serde_json::Map::new();
    if let Some(sop_class_uid) = result.identity.sop_class_uid.as_ref() {
        object.insert(
            "00081150".to_string(),
            json!({ "vr": "UI", "Value": [sop_class_uid] }),
        );
    }
    if let Some(sop_instance_uid) = result.identity.sop_instance_uid.as_ref() {
        object.insert(
            "00081155".to_string(),
            json!({ "vr": "UI", "Value": [sop_instance_uid] }),
        );
    }
    if result.outcome.is_stored() {
        if let Some(retrieve_url) = retrieve_url(result, url_base) {
            object.insert(
                "00081190".to_string(),
                json!({ "vr": "UR", "Value": [retrieve_url] }),
            );
        }
        if warning_reason(&result.outcome).is_some() {
            object.insert(
                "00081196".to_string(),
                json!({ "vr": "US", "Value": [warning_reason(&result.outcome).unwrap()] }),
            );
        }
    } else {
        object.insert(
            "00081197".to_string(),
            json!({ "vr": "US", "Value": [failure_reason(&result.outcome)] }),
        );
    }
    Value::Object(object)
}

fn retrieve_url(result: &IngestResult, url_base: Option<&DicomWebUrlBase>) -> Option<String> {
    let base = url_base?;
    let study = StudyInstanceUid::new(result.identity.study_instance_uid.clone()?).ok()?;
    let series = SeriesInstanceUid::new(result.identity.series_instance_uid.clone()?).ok()?;
    let instance = SopInstanceUid::new(result.identity.sop_instance_uid.clone()?).ok()?;
    Some(
        base.instance_retrieve_url(&study, &series, &instance)
            .to_string(),
    )
}

fn warning_reason(_outcome: &IngestObjectOutcome) -> Option<u16> {
    None
}

fn failure_reason(outcome: &IngestObjectOutcome) -> u16 {
    match outcome {
        IngestObjectOutcome::RejectedTooLarge { .. } => 0xA700,
        IngestObjectOutcome::RejectedUnsupportedSopClass { .. } => 0x0122,
        IngestObjectOutcome::ObjectStoreFailed { .. }
        | IngestObjectOutcome::RepositoryFailed { .. } => 0x0110,
        IngestObjectOutcome::Stored
        | IngestObjectOutcome::RejectedCannotUnderstand { .. }
        | IngestObjectOutcome::RejectedStudyMismatch { .. }
        | IngestObjectOutcome::RejectedChecksumMismatch { .. } => 0xC000,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use raccoon_service_ingest::{
        IngestBatchRepositoryStatus, IngestObjectId, IngestObjectIdentity, IngestObjectState,
        IngestPayloadRepresentation, IngestUploadId,
    };
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    use super::*;

    #[test]
    fn status_is_accepted_for_partial_success() {
        let results = vec![
            result(IngestObjectOutcome::Stored),
            result(IngestObjectOutcome::RejectedCannotUnderstand {
                reason: "bad object".to_string(),
            }),
        ];

        assert_eq!(
            storage_status(&results, &IngestBatchRepositoryStatus::Recorded),
            StatusCode::ACCEPTED
        );
    }

    #[test]
    fn response_includes_retrieve_url_from_request_base() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "pacs.example.test".parse().unwrap());
        let uri: Uri = "/dicomweb/studies".parse().unwrap();
        let json = storage_response_json(
            &[result(IngestObjectOutcome::Stored)],
            DicomWebUrlBase::from_request(&headers, &uri).as_ref(),
        );

        assert_eq!(
            json["00081199"]["Value"][0]["00081190"]["Value"][0],
            "http://pacs.example.test/dicomweb/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
        );
    }

    #[test]
    fn records_counts_and_error_class_without_payload() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            records: records.clone(),
        });
        let _guard = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!(
            "stow-rs store",
            dicomweb.service = "STOW-RS",
            dicomweb.resource = "studies",
            http.route = "/studies",
            dicomweb.object_count = tracing::field::Empty,
            dicomweb.successful_object_count = tracing::field::Empty,
            dicomweb.failed_object_count = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
            dicomweb.error_type = tracing::field::Empty,
            error.message = tracing::field::Empty,
        );
        let _entered = span.enter();
        let headers = HeaderMap::new();
        let uri: Uri = "/studies".parse().unwrap();

        let _ = storage_response(
            &headers,
            &uri,
            &[result(IngestObjectOutcome::Stored)],
            &IngestBatchRepositoryStatus::Failed {
                reason: "repository unavailable".to_string(),
            },
        );

        let records = records.lock().unwrap().join("\n");
        assert!(records.contains("dicomweb.service=STOW-RS"), "{records}");
        assert!(records.contains("dicomweb.resource=studies"), "{records}");
        assert!(records.contains("http.route=/studies"), "{records}");
        assert!(records.contains("dicomweb.object_count=1"), "{records}");
        assert!(
            records.contains("dicomweb.successful_object_count=1"),
            "{records}"
        );
        assert!(
            records.contains("dicomweb.failed_object_count=0"),
            "{records}"
        );
        assert!(
            records.contains("http.response.status_code=500"),
            "{records}"
        );
        assert!(records.contains("error.type=500"), "{records}");
        assert!(records.contains("dicomweb.error_type=500"), "{records}");
        assert!(
            records.contains("error.message=Internal Server Error"),
            "{records}"
        );
        assert!(!records.contains("DICOMDATA"));
    }

    fn result(outcome: IngestObjectOutcome) -> IngestResult {
        IngestResult {
            ingest_object_id: IngestObjectId::new(),
            upload_id: IngestUploadId::new(),
            object_key: None,
            content_length: None,
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
}
