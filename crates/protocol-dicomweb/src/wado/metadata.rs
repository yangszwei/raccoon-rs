use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use raccoon_contract_dicom::{DicomInstanceIdentity, is_bulk_data_element};
use raccoon_service_retrieve::{InstanceMetadata, RetrieveScope};
use serde_json::{Value, json};
use tracing::Span;

use crate::media::{self, MediaType};
use crate::{BulkDataPath, DicomWebError, DicomWebState, DicomWebUrlBase};

const WADO_METADATA_XML_BOUNDARY: &str = "wado-metadata-dicom-xml";

pub(crate) async fn metadata_response(
    state: DicomWebState,
    headers: &HeaderMap,
    uri: &Uri,
    scope: RetrieveScope,
) -> Result<Response, DicomWebError> {
    super::retrieve::record_scope(&scope);
    let selected = media::negotiate_dicom_json_or_xml_multipart(headers).map_err(record_error)?;
    Span::current().record(
        "dicomweb.selected_media_type",
        selected.selected_media_type(),
    );

    let repository = state.metadata.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "WADO-RS metadata repository is not registered".to_string(),
        ))
    })?;
    let rows = repository.find_metadata(&scope).await.map_err(|error| {
        record_error(DicomWebError::Internal(format!("metadata failed: {error}")))
    })?;
    if rows.is_empty() {
        return Err(record_error(DicomWebError::NotFound(
            "no matching DICOM metadata".to_string(),
        )));
    }
    Span::current().record("dicomweb.metadata.row_count", rows.len());

    let base = DicomWebUrlBase::from_request(headers, uri);
    let mut bulk_data_uri_count = 0_u64;
    let mut datasets = Vec::with_capacity(rows.len());
    for row in rows {
        datasets.push(
            metadata_json(&row, base.as_ref(), &mut bulk_data_uri_count).map_err(record_error)?,
        );
    }
    Span::current().record("dicomweb.metadata.bulk_data_uri_count", bulk_data_uri_count);

    match selected {
        media::DicomJsonOrXmlMultipart::Json => serde_json::to_vec(&datasets)
            .map(media::dicom_json_response)
            .map_err(|error| {
                record_error(DicomWebError::Internal(format!(
                    "metadata serialization failed: {error}"
                )))
            }),
        media::DicomJsonOrXmlMultipart::XmlMultipart => {
            let body = dicom_xml_multipart(&datasets).map_err(record_error)?;
            Ok(media::multipart_related_response(
                body,
                WADO_METADATA_XML_BOUNDARY,
                MediaType::ApplicationDicomXml,
                None,
            ))
        }
    }
}

fn dicom_xml_multipart(datasets: &[Value]) -> Result<String, DicomWebError> {
    let mut body = String::new();
    for dataset in datasets {
        body.push_str("--");
        body.push_str(WADO_METADATA_XML_BOUNDARY);
        body.push_str("\r\nContent-Type: application/dicom+xml\r\n\r\n");
        body.push_str(&crate::xml::native_dicom_model_xml(dataset)?);
        body.push_str("\r\n");
    }
    body.push_str("--");
    body.push_str(WADO_METADATA_XML_BOUNDARY);
    body.push_str("--\r\n");
    Ok(body)
}

fn metadata_json(
    row: &InstanceMetadata,
    base: Option<&DicomWebUrlBase>,
    bulk_data_uri_count: &mut u64,
) -> Result<Value, DicomWebError> {
    let mut value = serde_json::from_str::<Value>(&row.attributes_json)
        .map_err(|error| DicomWebError::Internal(format!("metadata parse failed: {error}")))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| DicomWebError::Internal("metadata root is not an object".to_string()))?;

    let file_meta_tags = object
        .keys()
        .filter(|tag| tag.starts_with("0002"))
        .cloned()
        .collect::<Vec<_>>();
    for tag in file_meta_tags {
        object.remove(&tag);
    }

    insert_retrieve_url(object, &row.identity, base);
    replace_bulk_data_markers(
        object,
        &row.identity,
        base,
        &mut Vec::new(),
        bulk_data_uri_count,
    );
    Ok(value)
}

fn insert_retrieve_url(
    object: &mut serde_json::Map<String, Value>,
    identity: &DicomInstanceIdentity,
    base: Option<&DicomWebUrlBase>,
) {
    let url = retrieve_url(identity, base);
    object.insert(
        "00081190".to_string(),
        json!({
            "vr": "UR",
            "Value": [url],
        }),
    );
}

fn replace_bulk_data_markers(
    object: &mut serde_json::Map<String, Value>,
    identity: &DicomInstanceIdentity,
    base: Option<&DicomWebUrlBase>,
    path: &mut Vec<String>,
    bulk_data_uri_count: &mut u64,
) {
    let tags = object.keys().cloned().collect::<Vec<_>>();
    for tag in tags {
        path.push(tag.clone());
        if let Some(items) = object
            .get_mut(&tag)
            .and_then(|element| element.get_mut("Value"))
            .and_then(Value::as_array_mut)
        {
            for (index, item) in items.iter_mut().enumerate() {
                if let Some(item_object) = item.as_object_mut() {
                    path.push(index.to_string());
                    replace_bulk_data_markers(
                        item_object,
                        identity,
                        base,
                        path,
                        bulk_data_uri_count,
                    );
                    path.pop();
                }
            }
        }

        if should_replace_bulk_data(object.get(&tag), &tag, path) {
            let vr = object
                .get(&tag)
                .and_then(|element| element.get("vr"))
                .cloned()
                .unwrap_or_else(|| json!("UN"));
            object.insert(
                tag,
                json!({
                    "vr": vr,
                    "BulkDataURI": bulk_data_uri(identity, base, path),
                }),
            );
            *bulk_data_uri_count += 1;
        }
        path.pop();
    }
}

fn should_replace_bulk_data(element: Option<&Value>, tag: &str, path: &[String]) -> bool {
    let Some(element) = element else {
        return false;
    };
    element.get("BulkDataURI").is_some()
        || is_bulk_data_element(tag, parent_sequence_from_bulk_path(path))
}

fn parent_sequence_from_bulk_path(path: &[String]) -> Option<&str> {
    if path.len() >= 3 {
        Some(path[path.len() - 3].as_str())
    } else {
        None
    }
}

fn retrieve_url(identity: &DicomInstanceIdentity, base: Option<&DicomWebUrlBase>) -> String {
    if let Some(base) = base {
        return base
            .instance_retrieve_url(
                &identity.study_instance_uid,
                &identity.series_instance_uid,
                &identity.sop_instance_uid,
            )
            .to_string();
    }
    format!(
        "/studies/{}/series/{}/instances/{}",
        identity.study_instance_uid, identity.series_instance_uid, identity.sop_instance_uid
    )
}

fn bulk_data_uri(
    identity: &DicomInstanceIdentity,
    base: Option<&DicomWebUrlBase>,
    path: &[String],
) -> String {
    let bulk_path = BulkDataPath::new(format!(
        "/studies/{}/series/{}/instances/{}/bulkdata/{}",
        identity.study_instance_uid,
        identity.series_instance_uid,
        identity.sop_instance_uid,
        path.join("/")
    ));
    if let Some(base) = base {
        base.bulk_data_uri(&bulk_path).to_string()
    } else {
        bulk_path.as_path().to_string()
    }
}

fn record_error(error: DicomWebError) -> DicomWebError {
    Span::current().record("error.type", error.http_error_class());
    error
}
