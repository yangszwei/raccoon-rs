use std::collections::HashMap;
use std::io::Cursor;

use axum::body::Body;
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use base64::Engine;
use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::StandardDataDictionary;
use dicom_dictionary_std::uids;
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use futures_util::TryStreamExt;
use raccoon_contract_dicom::{
    SeriesInstanceUid, SopClassUid, SopInstanceUid, SpecificCharacterSet,
    SpecificCharacterSetError, StudyInstanceUid, default_repertoire_text,
};
use raccoon_contract_object_store::ByteStream;
use raccoon_service_ingest::{
    IngestBatchRepositoryStatus, IngestObjectId, IngestObjectIdentity, IngestObjectOutcome,
    IngestPayloadRepresentation, IngestRequest, IngestResult, IngestSource, IngestUploadId,
};
use serde_json::{Value, json};

use super::response;
use crate::instrumentation::record_error;
use crate::media::{APPLICATION_DICOM_JSON, APPLICATION_DICOM_XML, APPLICATION_OCTET_STREAM};
use crate::{DicomWebError, DicomWebState};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum MetadataMedia {
    Json,
    Xml,
}

pub(crate) struct StoreMetadataRequest {
    pub(crate) state: DicomWebState,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
    pub(crate) body: Body,
    pub(crate) expected_study: Option<StudyInstanceUid>,
    pub(crate) metadata_media: MetadataMedia,
    pub(crate) boundary: String,
    pub(crate) request_content_type: String,
}

impl MetadataMedia {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Xml => "XML",
        }
    }

    pub(crate) fn content_type(self) -> &'static str {
        match self {
            Self::Json => APPLICATION_DICOM_JSON,
            Self::Xml => APPLICATION_DICOM_XML,
        }
    }
}

#[derive(Debug)]
struct RequestPart {
    content_type: String,
    content_location: Option<String>,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct PreparedInstance {
    identity: IngestObjectIdentity,
    outcome: PreparedOutcome,
}

#[derive(Debug)]
enum PreparedOutcome {
    Reconstructed(Vec<u8>),
    Rejected(IngestObjectOutcome),
}

pub(crate) async fn store(request: StoreMetadataRequest) -> Result<Response, DicomWebError> {
    let StoreMetadataRequest {
        state,
        headers,
        uri,
        body,
        expected_study,
        metadata_media,
        boundary,
        request_content_type,
    } = request;
    let service = state
        .ingest
        .ok_or(DicomWebError::Internal(
            "STOW-RS ingest service is not registered".to_string(),
        ))
        .map_err(record_error)?;
    tracing::Span::current().record("dicomweb.store.metadata_media_type", metadata_media.label());

    let options = state.stow.unwrap_or_default();
    let parts = collect_parts(
        body,
        boundary,
        options.max_part_size_bytes(),
        options.max_part_count(),
    )
    .await?;
    let bulk_parts = bulk_parts(&parts)?;
    let metadata_parts = parts
        .iter()
        .filter(|part| same_media_type(&part.content_type, metadata_media.content_type()))
        .collect::<Vec<_>>();
    if metadata_parts.is_empty() {
        return Err(record_error(DicomWebError::bad_request(
            "STOW-RS metadata plus bulk data request did not contain a metadata part",
        )));
    }

    let upload_id = IngestUploadId::new();
    let mut requests = Vec::new();
    let mut local_results = Vec::new();
    let mut missing_bulk_count = 0_usize;

    for part in metadata_parts {
        let instances = parse_metadata_instances(&part.bytes, metadata_media)?;
        for mut metadata in instances {
            let identity = identity_from_metadata(&metadata);
            let prepared = match validate_identity(&identity, expected_study.as_ref()) {
                Ok(()) => match replace_bulk_references(&mut metadata, &bulk_parts) {
                    Ok(()) => match reconstruct_dicom_file(&metadata) {
                        Ok(bytes) => PreparedInstance {
                            identity,
                            outcome: PreparedOutcome::Reconstructed(bytes),
                        },
                        Err(reason) => PreparedInstance {
                            identity,
                            outcome: PreparedOutcome::Rejected(
                                IngestObjectOutcome::RejectedCannotUnderstand { reason },
                            ),
                        },
                    },
                    Err(missing) => {
                        missing_bulk_count = missing_bulk_count.saturating_add(1);
                        PreparedInstance {
                            identity,
                            outcome: PreparedOutcome::Rejected(
                                IngestObjectOutcome::RejectedCannotUnderstand {
                                    reason: format!("missing STOW-RS bulk data part {missing}"),
                                },
                            ),
                        }
                    }
                },
                Err(outcome) => PreparedInstance {
                    identity,
                    outcome: PreparedOutcome::Rejected(outcome),
                },
            };
            match prepared.outcome {
                PreparedOutcome::Reconstructed(bytes) => {
                    let mut request =
                        IngestRequest::new(upload_id.clone(), ByteStream::from(bytes))
                            .with_payload_representation(IngestPayloadRepresentation::DicomFile)
                            .with_identity_hints(prepared.identity)
                            .with_source(IngestSource {
                                protocol: Some("dicomweb".to_string()),
                                content_type: Some(request_content_type.clone()),
                                ..IngestSource::default()
                            });
                    if let Some(study) = expected_study.as_ref() {
                        request = request.with_expected_study_instance_uid(study.as_str());
                    }
                    requests.push(request);
                }
                PreparedOutcome::Rejected(outcome) => {
                    local_results.push(IngestResult::rejected(
                        IngestObjectId::new(),
                        upload_id.clone(),
                        prepared.identity,
                        IngestPayloadRepresentation::DicomWebMetadataAndBulkData,
                        Some(uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string()),
                        outcome,
                    ));
                }
            }
        }
    }

    tracing::Span::current().record("dicomweb.store.bulk_part_count", bulk_parts.len());
    tracing::Span::current().record("dicomweb.store.missing_bulk_part_count", missing_bulk_count);
    tracing::Span::current().record(
        "dicomweb.store.reconstructed_instance_count",
        requests.len(),
    );

    let batch = if requests.is_empty() {
        IngestBatchResultParts {
            results: Vec::new(),
            repository_status: IngestBatchRepositoryStatus::Recorded,
        }
    } else {
        let batch = service.ingest_upload_objects(requests).await;
        IngestBatchResultParts {
            results: batch.object_results,
            repository_status: batch.repository_status,
        }
    };

    let mut results = local_results;
    results.extend(batch.results);
    if results.is_empty() {
        return Err(record_error(DicomWebError::bad_request(
            "STOW-RS request did not contain any DICOM instances",
        )));
    }

    Ok(response::storage_response(
        &headers,
        &uri,
        &results,
        &batch.repository_status,
    ))
}

struct IngestBatchResultParts {
    results: Vec<IngestResult>,
    repository_status: IngestBatchRepositoryStatus,
}

async fn collect_parts(
    body: Body,
    boundary: String,
    max_part_size_bytes: Option<u64>,
    max_part_count: Option<usize>,
) -> Result<Vec<RequestPart>, DicomWebError> {
    let body_stream = body
        .into_data_stream()
        .map_err(|error| std::io::Error::other(error.to_string()));
    let mut multipart = multer::Multipart::new(body_stream, boundary);
    let mut parts = Vec::new();
    let mut part_index = 0_u64;
    while let Some(mut field) = multipart.next_field().await.map_err(|error| {
        record_error(DicomWebError::bad_request(format!(
            "invalid multipart body: {error}"
        )))
    })? {
        part_index = part_index.saturating_add(1);
        if let Some(max_part_count) = max_part_count
            && part_index as usize > max_part_count
        {
            return Err(record_error(DicomWebError::payload_too_large(format!(
                "STOW-RS request exceeds configured maximum of {max_part_count} multipart parts"
            ))));
        }
        let content_type = field
            .content_type()
            .map(ToString::to_string)
            .ok_or_else(|| {
                record_error(DicomWebError::unsupported_media_type(
                    "missing STOW-RS part Content-Type",
                ))
            })?;
        let content_location = field
            .headers()
            .get("content-location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = read_field_bytes(&mut field, max_part_size_bytes).await?;
        SpanRecord::part_count(part_index);
        parts.push(RequestPart {
            content_type,
            content_location,
            bytes,
        });
    }
    Ok(parts)
}

async fn read_field_bytes(
    field: &mut multer::Field<'_>,
    max_part_size_bytes: Option<u64>,
) -> Result<Vec<u8>, DicomWebError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(|error| {
        record_error(DicomWebError::bad_request(format!(
            "invalid multipart part: {error}"
        )))
    })? {
        if let Some(max_part_size_bytes) = max_part_size_bytes
            && bytes.len().saturating_add(chunk.len()) > max_part_size_bytes as usize
        {
            return Err(record_error(DicomWebError::payload_too_large(format!(
                "STOW-RS multipart part exceeds configured maximum of {max_part_size_bytes} bytes"
            ))));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn bulk_parts(parts: &[RequestPart]) -> Result<HashMap<String, &[u8]>, DicomWebError> {
    let mut bulk_parts = HashMap::new();
    for part in parts {
        if same_media_type(&part.content_type, APPLICATION_OCTET_STREAM) {
            if let Some(content_location) = part.content_location.as_deref() {
                bulk_parts.insert(content_location.to_string(), part.bytes.as_slice());
                bulk_parts.insert(normalize_reference(content_location), part.bytes.as_slice());
            }
        } else if !same_media_type(&part.content_type, APPLICATION_DICOM_JSON)
            && !same_media_type(&part.content_type, APPLICATION_DICOM_XML)
        {
            return Err(record_error(DicomWebError::unsupported_media_type(
                format!(
                    "unsupported STOW-RS bulk data part Content-Type {}",
                    part.content_type
                ),
            )));
        }
    }
    Ok(bulk_parts)
}

fn parse_metadata_instances(
    bytes: &[u8],
    metadata_media: MetadataMedia,
) -> Result<Vec<Value>, DicomWebError> {
    match metadata_media {
        MetadataMedia::Json => {
            let value: Value = serde_json::from_slice(bytes).map_err(|error| {
                record_error(DicomWebError::bad_request(format!(
                    "invalid STOW-RS DICOM JSON metadata: {error}"
                )))
            })?;
            Ok(match value {
                Value::Array(values) => values,
                Value::Object(_) => vec![value],
                _ => {
                    return Err(record_error(DicomWebError::bad_request(
                        "STOW-RS DICOM JSON metadata root must be an object or array",
                    )));
                }
            })
        }
        MetadataMedia::Xml => {
            let text = std::str::from_utf8(bytes).map_err(|error| {
                record_error(DicomWebError::bad_request(format!(
                    "invalid STOW-RS DICOM XML metadata encoding: {error}"
                )))
            })?;
            Ok(vec![native_xml_to_json(text)?])
        }
    }
}

fn replace_bulk_references(
    metadata: &mut Value,
    bulk_parts: &HashMap<String, &[u8]>,
) -> Result<(), String> {
    let Some(object) = metadata.as_object_mut() else {
        return Ok(());
    };
    replace_bulk_references_in_object(object, bulk_parts)
}

fn replace_bulk_references_in_object(
    object: &mut serde_json::Map<String, Value>,
    bulk_parts: &HashMap<String, &[u8]>,
) -> Result<(), String> {
    for element in object.values_mut() {
        let Some(element_object) = element.as_object_mut() else {
            continue;
        };
        if let Some(uri) = element_object
            .remove("BulkDataURI")
            .and_then(|value| value.as_str().map(str::to_string))
        {
            let key = normalize_reference(&uri);
            let Some(bytes) = bulk_parts
                .get(&uri)
                .copied()
                .or_else(|| bulk_parts.get(&key).copied())
            else {
                return Err(uri);
            };
            element_object.insert(
                "InlineBinary".to_string(),
                Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            );
        }
        if let Some(items) = element_object
            .get_mut("Value")
            .and_then(Value::as_array_mut)
        {
            for item in items {
                if let Some(item_object) = item.as_object_mut() {
                    replace_bulk_references_in_object(item_object, bulk_parts)?;
                }
            }
        }
    }
    Ok(())
}

fn reconstruct_dicom_file(metadata: &Value) -> Result<Vec<u8>, String> {
    let mut metadata = metadata.clone();
    validate_and_prepare_specific_character_set(&mut metadata)?;
    let object: InMemDicomObject = dicom_json::from_value(metadata)
        .map_err(|error| format!("failed to parse DICOM JSON metadata: {error}"))?;
    let object = object
        .with_meta(FileMetaTableBuilder::new().transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN))
        .map_err(|error| format!("failed to build DICOM file meta: {error}"))?;
    let mut bytes = Vec::new();
    object
        .write_all(Cursor::new(&mut bytes))
        .map_err(|error| format!("failed to write DICOM file: {error}"))?;
    Ok(bytes)
}

fn validate_and_prepare_specific_character_set(metadata: &mut Value) -> Result<(), String> {
    let charset = metadata_specific_character_set(metadata)?;
    let contains_non_default_text = metadata_contains_non_default_text(metadata);
    match charset {
        Some(charset) if !charset.is_supported() => {
            return Err(format!(
                "unsupported Specific Character Set {} for STOW-RS metadata reconstruction",
                charset.label()
            ));
        }
        Some(charset) => {
            validate_metadata_text_encodable(metadata, &charset)?;
        }
        None => {
            if contains_non_default_text {
                add_utf8_specific_character_set(metadata)?;
                let charset = SpecificCharacterSet::parse_terms(["ISO_IR 192"])
                    .map_err(specific_character_set_rejection)?;
                validate_metadata_text_encodable(metadata, &charset)?;
            } else {
                validate_metadata_text_encodable(
                    metadata,
                    &SpecificCharacterSet::default_repertoire(),
                )?;
            }
        }
    }
    Ok(())
}

fn metadata_specific_character_set(
    metadata: &Value,
) -> Result<Option<SpecificCharacterSet>, String> {
    let Some(values) = metadata
        .get("00080005")
        .and_then(|element| element.get("Value"))
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };
    let mut terms = Vec::new();
    for value in values {
        let Some(value) = value.as_str() else {
            return Err("Specific Character Set values must be strings".to_string());
        };
        terms.extend(value.split('\\').map(|term| term.trim().to_string()));
    }
    SpecificCharacterSet::parse_terms(terms)
        .map(Some)
        .map_err(specific_character_set_rejection)
}

fn specific_character_set_rejection(error: SpecificCharacterSetError) -> String {
    format!("invalid Specific Character Set: {error}")
}

fn add_utf8_specific_character_set(metadata: &mut Value) -> Result<(), String> {
    let object = metadata
        .as_object_mut()
        .ok_or_else(|| "STOW-RS DICOM JSON metadata root must be an object".to_string())?;
    object.insert(
        "00080005".to_string(),
        json!({ "vr": "CS", "Value": ["ISO_IR 192"] }),
    );
    Ok(())
}

fn validate_metadata_text_encodable(
    metadata: &Value,
    charset: &SpecificCharacterSet,
) -> Result<(), String> {
    let Some(object) = metadata.as_object() else {
        return Ok(());
    };
    for (tag, element) in object {
        let vr = element
            .get("vr")
            .and_then(Value::as_str)
            .and_then(parse_vr)
            .or_else(|| parse_tag_key(tag).map(vr_for_tag));
        match vr {
            Some(VR::SQ) => {
                if let Some(items) = element.get("Value").and_then(Value::as_array) {
                    for item in items {
                        validate_metadata_text_encodable(item, charset)?;
                    }
                }
            }
            Some(VR::AE | VR::LO | VR::LT | VR::PN | VR::SH | VR::ST | VR::UC | VR::UT) => {
                validate_element_text_encodable(element, vr == Some(VR::PN), charset)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_element_text_encodable(
    element: &Value,
    pn: bool,
    charset: &SpecificCharacterSet,
) -> Result<(), String> {
    let Some(values) = element.get("Value").and_then(Value::as_array) else {
        return Ok(());
    };
    for value in values {
        if pn {
            validate_pn_value_encodable(value, charset)?;
        } else if let Some(text) = value.as_str() {
            charset
                .encode_text(text)
                .map_err(|error| format!("failed to encode STOW-RS text: {error}"))?;
        }
    }
    Ok(())
}

fn validate_pn_value_encodable(
    value: &Value,
    charset: &SpecificCharacterSet,
) -> Result<(), String> {
    if let Some(text) = value.as_str() {
        charset
            .encode_text(text)
            .map_err(|error| format!("failed to encode STOW-RS PN text: {error}"))?;
        return Ok(());
    }
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    for key in ["Alphabetic", "Ideographic", "Phonetic"] {
        if let Some(text) = object.get(key).and_then(Value::as_str) {
            charset
                .encode_text(text)
                .map_err(|error| format!("failed to encode STOW-RS PN text: {error}"))?;
        }
    }
    Ok(())
}

fn metadata_contains_non_default_text(metadata: &Value) -> bool {
    let Some(object) = metadata.as_object() else {
        return false;
    };
    object.iter().any(|(tag, element)| {
        let vr = element
            .get("vr")
            .and_then(Value::as_str)
            .and_then(parse_vr)
            .or_else(|| parse_tag_key(tag).map(vr_for_tag));
        match vr {
            Some(VR::SQ) => element
                .get("Value")
                .and_then(Value::as_array)
                .is_some_and(|items| items.iter().any(metadata_contains_non_default_text)),
            Some(VR::AE | VR::LO | VR::LT | VR::PN | VR::SH | VR::ST | VR::UC | VR::UT) => {
                element_text_values_contain_non_default(element, vr == Some(VR::PN))
            }
            _ => false,
        }
    })
}

fn element_text_values_contain_non_default(element: &Value, pn: bool) -> bool {
    element
        .get("Value")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            values.iter().any(|value| {
                if pn {
                    pn_value_contains_non_default(value)
                } else {
                    value
                        .as_str()
                        .is_some_and(|text| !default_repertoire_text(text))
                }
            })
        })
}

fn pn_value_contains_non_default(value: &Value) -> bool {
    if let Some(text) = value.as_str() {
        return !default_repertoire_text(text);
    }
    value.as_object().is_some_and(|object| {
        ["Alphabetic", "Ideographic", "Phonetic"].iter().any(|key| {
            object
                .get(*key)
                .and_then(Value::as_str)
                .is_some_and(|text| !default_repertoire_text(text))
        })
    })
}

fn parse_vr(value: &str) -> Option<VR> {
    match value {
        "AE" => Some(VR::AE),
        "LO" => Some(VR::LO),
        "LT" => Some(VR::LT),
        "PN" => Some(VR::PN),
        "SH" => Some(VR::SH),
        "SQ" => Some(VR::SQ),
        "ST" => Some(VR::ST),
        "UC" => Some(VR::UC),
        "UT" => Some(VR::UT),
        _ => None,
    }
}

fn parse_tag_key(value: &str) -> Option<dicom_core::Tag> {
    if value.len() != 8 {
        return None;
    }
    let group = u16::from_str_radix(&value[..4], 16).ok()?;
    let element = u16::from_str_radix(&value[4..], 16).ok()?;
    Some(dicom_core::Tag(group, element))
}

fn vr_for_tag(tag: dicom_core::Tag) -> VR {
    StandardDataDictionary
        .by_tag(tag)
        .and_then(|entry| entry.vr().exact())
        .unwrap_or(VR::LO)
}

fn identity_from_metadata(metadata: &Value) -> IngestObjectIdentity {
    IngestObjectIdentity {
        sop_class_uid: value_string(metadata, "00080016"),
        study_instance_uid: value_string(metadata, "0020000D"),
        series_instance_uid: value_string(metadata, "0020000E"),
        sop_instance_uid: value_string(metadata, "00080018"),
    }
}

fn value_string(metadata: &Value, tag: &str) -> Option<String> {
    metadata
        .get(tag)
        .and_then(|element| element.get("Value"))
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn validate_identity(
    identity: &IngestObjectIdentity,
    expected_study: Option<&StudyInstanceUid>,
) -> Result<(), IngestObjectOutcome> {
    let study = validate_required_uid::<StudyInstanceUid>(
        identity.study_instance_uid.as_deref(),
        "StudyInstanceUID",
    )?;
    validate_required_uid::<SeriesInstanceUid>(
        identity.series_instance_uid.as_deref(),
        "SeriesInstanceUID",
    )?;
    validate_required_uid::<SopInstanceUid>(
        identity.sop_instance_uid.as_deref(),
        "SOPInstanceUID",
    )?;
    validate_required_uid::<SopClassUid>(identity.sop_class_uid.as_deref(), "SOPClassUID")?;
    if let Some(expected_study) = expected_study
        && expected_study != &study
    {
        return Err(IngestObjectOutcome::RejectedStudyMismatch {
            expected_study_instance_uid: expected_study.as_str().to_string(),
            actual_study_instance_uid: Some(study.as_str().to_string()),
        });
    }
    Ok(())
}

fn validate_required_uid<T>(value: Option<&str>, label: &str) -> Result<T, IngestObjectOutcome>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let Some(value) = value else {
        return Err(IngestObjectOutcome::RejectedCannotUnderstand {
            reason: format!("missing {label}"),
        });
    };
    value
        .parse::<T>()
        .map_err(|error| IngestObjectOutcome::RejectedCannotUnderstand {
            reason: format!("invalid {label}: {error}"),
        })
}

fn same_media_type(value: &str, expected: &str) -> bool {
    value
        .split(';')
        .next()
        .map(str::trim)
        .is_some_and(|value| value.eq_ignore_ascii_case(expected))
}

fn normalize_reference(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("cid:")
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string()
}

fn native_xml_to_json(text: &str) -> Result<Value, DicomWebError> {
    let mut object = serde_json::Map::new();
    let mut rest = text;
    while let Some(start) = rest.find("<DicomAttribute") {
        rest = &rest[start..];
        let Some(start_end) = rest.find('>') else {
            return Err(record_error(DicomWebError::bad_request(
                "invalid DICOM XML attribute",
            )));
        };
        let start_tag = &rest[..=start_end];
        let tag = xml_attr(start_tag, "tag").ok_or_else(|| {
            record_error(DicomWebError::bad_request(
                "DICOM XML attribute is missing tag",
            ))
        })?;
        let vr = xml_attr(start_tag, "vr").unwrap_or_else(|| "UN".to_string());
        let close = "</DicomAttribute>";
        let Some(end) = rest[start_end + 1..].find(close) else {
            return Err(record_error(DicomWebError::bad_request(
                "invalid DICOM XML attribute body",
            )));
        };
        let body = &rest[start_end + 1..start_end + 1 + end];
        object.insert(tag, xml_element_json(&vr, body));
        rest = &rest[start_end + 1 + end + close.len()..];
    }
    Ok(Value::Object(object))
}

fn xml_element_json(vr: &str, body: &str) -> Value {
    if let Some(uri) = xml_empty_attr(body, "BulkData", "uri") {
        return json!({ "vr": vr, "BulkDataURI": xml_unescape(&uri) });
    }
    if let Some(value) = xml_text(body, "InlineBinary") {
        return json!({ "vr": vr, "InlineBinary": xml_unescape(&value) });
    }
    let values = xml_values(body)
        .into_iter()
        .map(|value| Value::String(xml_unescape(&value)))
        .collect::<Vec<_>>();
    if values.is_empty() {
        json!({ "vr": vr })
    } else if vr == "PN" {
        json!({ "vr": vr, "Value": values.into_iter().map(|value| json!({ "Alphabetic": value })).collect::<Vec<_>>() })
    } else {
        json!({ "vr": vr, "Value": values })
    }
}

fn xml_values(body: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(value) = xml_text(rest, "Value") {
        values.push(value.clone());
        if let Some(end) = rest.find("</Value>") {
            rest = &rest[end + "</Value>".len()..];
        } else {
            break;
        }
    }
    values
}

fn xml_attr(start_tag: &str, name: &str) -> Option<String> {
    let pattern = format!("{name}=\"");
    let start = start_tag.find(&pattern)? + pattern.len();
    let end = start_tag[start..].find('"')?;
    Some(xml_unescape(&start_tag[start..start + end]))
}

fn xml_empty_attr(body: &str, element: &str, attr: &str) -> Option<String> {
    let start = body.find(&format!("<{element} "))?;
    let end = body[start..].find("/>")?;
    xml_attr(&body[start..start + end], attr)
}

fn xml_text(body: &str, element: &str) -> Option<String> {
    let start_token = format!("<{element}");
    let start = body.find(&start_token)?;
    let start_end = body[start..].find('>')? + start + 1;
    let end_token = format!("</{element}>");
    let end = body[start_end..].find(&end_token)? + start_end;
    Some(body[start_end..end].to_string())
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

struct SpanRecord;

impl SpanRecord {
    fn part_count(count: u64) {
        tracing::Span::current().record("dicomweb.store.part_count", count);
    }
}

#[cfg(test)]
mod tests {
    use dicom_object::{DicomAttribute, DicomObject};

    use super::*;

    fn minimal_metadata(extra: serde_json::Map<String, Value>) -> Value {
        let mut object = serde_json::Map::from_iter([
            (
                "00080016".to_string(),
                json!({"vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"]}),
            ),
            (
                "00080018".to_string(),
                json!({"vr": "UI", "Value": ["1.2.3.4.5"]}),
            ),
            (
                "0020000D".to_string(),
                json!({"vr": "UI", "Value": ["1.2.3"]}),
            ),
            (
                "0020000E".to_string(),
                json!({"vr": "UI", "Value": ["1.2.3.4"]}),
            ),
        ]);
        object.extend(extra);
        Value::Object(object)
    }

    #[test]
    fn reconstruction_adds_utf8_charset_for_non_default_text() {
        let metadata = minimal_metadata(serde_json::Map::from_iter([(
            "00100010".to_string(),
            json!({"vr": "PN", "Value": [{"Alphabetic": "Café^Renée"}]}),
        )]));

        let bytes = reconstruct_dicom_file(&metadata).expect("UTF-8 metadata reconstructs");
        let object = dicom_object::from_reader(std::io::Cursor::new(bytes))
            .expect("reconstructed DICOM parses");

        assert_eq!(
            object
                .attr_by_name("SpecificCharacterSet")
                .unwrap()
                .to_str()
                .unwrap()
                .as_ref(),
            "ISO_IR 192"
        );
    }

    #[test]
    fn reconstruction_rejects_default_charset_with_non_default_text() {
        let metadata = minimal_metadata(serde_json::Map::from_iter([
            (
                "00080005".to_string(),
                json!({"vr": "CS", "Value": ["ISO_IR 6"]}),
            ),
            (
                "00100010".to_string(),
                json!({"vr": "PN", "Value": [{"Alphabetic": "Café^Renée"}]}),
            ),
        ]));

        let error = reconstruct_dicom_file(&metadata)
            .expect_err("default repertoire cannot encode non-ASCII text");

        assert!(error.contains("default repertoire"));
    }

    #[test]
    fn reconstruction_rejects_unsupported_charset_with_non_default_text() {
        let metadata = minimal_metadata(serde_json::Map::from_iter([
            (
                "00080005".to_string(),
                json!({"vr": "CS", "Value": ["ISO 2022 IR 159"]}),
            ),
            (
                "00081030".to_string(),
                json!({"vr": "LO", "Value": ["検査"]}),
            ),
        ]));

        let error = reconstruct_dicom_file(&metadata)
            .expect_err("unsupported charset cannot reconstruct non-ASCII text");

        assert!(error.contains("unsupported Specific Character Set"));
    }

    #[test]
    fn reconstruction_supports_iso2022_jis_text() {
        let metadata = minimal_metadata(serde_json::Map::from_iter([
            (
                "00080005".to_string(),
                json!({"vr": "CS", "Value": ["ISO 2022 IR 87"]}),
            ),
            (
                "00081030".to_string(),
                json!({"vr": "LO", "Value": ["検査"]}),
            ),
        ]));

        let bytes = reconstruct_dicom_file(&metadata).expect("ISO 2022 IR 87 reconstructs");
        let object = dicom_object::from_reader(std::io::Cursor::new(bytes))
            .expect("reconstructed DICOM parses");

        let attr = object.attr_by_name("StudyDescription").unwrap();
        let value = attr.to_str().unwrap();
        assert!(value.starts_with("検査"), "{value}");
    }
}
