use std::io::Cursor;

use axum::body::Body;
use axum::http::{HeaderMap, Uri, header};
use axum::response::Response;
use dicom_core::value::Value as DicomValue;
use dicom_core::{Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{
    DefaultDicomObject, FileMetaTableBuilder, InMemDicomObject, collector::DicomCollector,
};
use raccoon_contract_dicom::DicomInstanceIdentity;
use raccoon_contract_object_store::Bytes;
use raccoon_service_retrieve::RetrieveScope;
use tracing::Span;

use super::retrieve::{CollectedInstance, collect_instances};
use crate::media::{
    self, AvailableRepresentation, MediaType, MediaTypeParams, SelectedRepresentation,
};
use crate::{DicomWebError, DicomWebState, DicomWebUrlBase, FrameList};

const BOUNDARY: &str = "raccoon-dicomweb-bulkdata-boundary";

#[derive(Debug, Clone)]
struct BulkPart {
    identity: DicomInstanceIdentity,
    location_suffix: String,
    bytes: Bytes,
}

pub(crate) async fn bulk_data_response(
    state: DicomWebState,
    headers: &HeaderMap,
    uri: &Uri,
    scope: RetrieveScope,
    path: &str,
) -> Result<Response, DicomWebError> {
    super::retrieve::record_scope(&scope);
    let selected = negotiate_octet_stream_accept(headers).map_err(record_error)?;
    record_selected(&selected);

    let parts = collect_bulk_parts(state, scope, |instance| {
        let object = parse_dicom_object(instance)?;
        let element = element_at_path(&object, path)?;
        let bytes = primitive_bytes(element)?;
        Ok(vec![BulkPart {
            identity: instance.identity.clone(),
            location_suffix: format!("bulkdata/{path}"),
            bytes,
        }])
    })
    .await?;

    multipart_octet_stream_response(parts, headers, uri)
}

pub(crate) async fn pixel_data_response(
    state: DicomWebState,
    headers: &HeaderMap,
    uri: &Uri,
    scope: RetrieveScope,
) -> Result<Response, DicomWebError> {
    super::retrieve::record_scope(&scope);
    let selected = negotiate_octet_stream_accept(headers).map_err(record_error)?;
    record_selected(&selected);

    let parts = collect_bulk_parts(state, scope, |instance| {
        let object = parse_dicom_object(instance)?;
        let bytes = pixel_data_bytes(&object)?;
        Ok(vec![BulkPart {
            identity: instance.identity.clone(),
            location_suffix: "pixeldata".to_string(),
            bytes,
        }])
    })
    .await?;

    multipart_octet_stream_response(parts, headers, uri)
}

pub(crate) async fn frames_response(
    state: DicomWebState,
    headers: &HeaderMap,
    uri: &Uri,
    scope: RetrieveScope,
    frames: &FrameList,
) -> Result<Response, DicomWebError> {
    super::retrieve::record_scope(&scope);
    Span::current().record(
        "dicomweb.requested_frame_count",
        frames.frames().len() as u64,
    );
    let selected = negotiate_octet_stream_accept(headers).map_err(record_error)?;
    record_selected(&selected);

    let parts = collect_bulk_parts(state, scope, |instance| {
        let object = parse_dicom_object(instance)?;
        let pixel_data = pixel_data_bytes(&object)?;
        let frame_size = frame_size(&object)?;
        let frame_count = frame_count(&object, pixel_data.len(), frame_size)?;
        frames
            .frames()
            .iter()
            .copied()
            .map(|frame| {
                if frame as usize > frame_count {
                    return Err(DicomWebError::NotFound(format!("frame {frame} is missing")));
                }
                let start = (frame as usize - 1) * frame_size;
                let end = start + frame_size;
                if end > pixel_data.len() {
                    return Err(DicomWebError::NotFound(format!("frame {frame} is missing")));
                }
                Ok(BulkPart {
                    identity: instance.identity.clone(),
                    location_suffix: format!("frames/{frame}"),
                    bytes: Bytes::copy_from_slice(&pixel_data[start..end]),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await?;

    multipart_octet_stream_response(parts, headers, uri)
}

async fn collect_bulk_parts(
    state: DicomWebState,
    scope: RetrieveScope,
    mut extract: impl FnMut(&CollectedInstance) -> Result<Vec<BulkPart>, DicomWebError>,
) -> Result<Vec<BulkPart>, DicomWebError> {
    let service = state.retrieve.ok_or_else(|| {
        record_error(DicomWebError::Internal(
            "WADO-RS retrieve service is not registered".to_string(),
        ))
    })?;
    let instances = collect_instances(service.as_ref(), scope)
        .await
        .map_err(record_error)?;

    let mut parts = Vec::new();
    for instance in &instances {
        parts.extend(extract(instance).map_err(record_error)?);
    }
    if parts.is_empty() {
        return Err(record_error(DicomWebError::NotFound(
            "no matching bulk data".to_string(),
        )));
    }
    Span::current().record("dicomweb.returned_part_count", parts.len() as u64);
    Ok(parts)
}

fn negotiate_octet_stream_accept(
    headers: &HeaderMap,
) -> Result<SelectedRepresentation, DicomWebError> {
    if !headers.contains_key(header::ACCEPT) {
        return Ok(SelectedRepresentation {
            media_type: MediaType::MultipartRelated,
            params: MediaTypeParams {
                type_: Some(media::APPLICATION_OCTET_STREAM.to_string()),
                transfer_syntax: None,
                charset: None,
            },
        });
    }
    media::negotiate_representation(
        headers,
        None,
        &[AvailableRepresentation {
            media_type: MediaType::MultipartRelated,
            params: MediaTypeParams {
                type_: Some(media::APPLICATION_OCTET_STREAM.to_string()),
                transfer_syntax: None,
                charset: None,
            },
        }],
    )
}

fn multipart_octet_stream_response(
    parts: Vec<BulkPart>,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<Response, DicomWebError> {
    let mut body = Vec::new();
    let base = DicomWebUrlBase::from_request(headers, uri);
    for part in parts {
        body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
        body.extend_from_slice(
            format!(
                "Content-Location: {}\r\n\r\n",
                content_location(&part.identity, &part.location_suffix, base.as_ref())
            )
            .as_bytes(),
        );
        body.extend_from_slice(&part.bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    Ok(media::multipart_related_response(
        Body::from(body),
        BOUNDARY,
        MediaType::OctetStream,
        None,
    ))
}

fn content_location(
    identity: &DicomInstanceIdentity,
    suffix: &str,
    base: Option<&DicomWebUrlBase>,
) -> String {
    let path = format!(
        "/studies/{}/series/{}/instances/{}/{}",
        identity.study_instance_uid,
        identity.series_instance_uid,
        identity.sop_instance_uid,
        suffix
    );
    if let Some(base) = base {
        base.bulk_data_uri(&crate::BulkDataPath::new(path))
            .to_string()
    } else {
        path
    }
}

fn parse_dicom_object(instance: &CollectedInstance) -> Result<DefaultDicomObject, DicomWebError> {
    match dicom_object::from_reader(Cursor::new(instance.body.clone())) {
        Ok(object) => Ok(object),
        Err(file_error) => collect_full_dataset(instance).map_err(|dataset_error| {
            DicomWebError::Internal(format!(
                "DICOM parse failed: Part 10 parse failed: {file_error}; dataset parse failed: {dataset_error}"
            ))
        }),
    }
}

fn collect_full_dataset(instance: &CollectedInstance) -> Result<DefaultDicomObject, String> {
    let transfer_syntax_uid = instance
        .transfer_syntax_uid
        .as_ref()
        .map(|uid| uid.as_str())
        .unwrap_or(uids::EXPLICIT_VR_LITTLE_ENDIAN);
    let mut collector = DicomCollector::new_with_ts(
        std::io::BufReader::new(Cursor::new(instance.body.clone())),
        transfer_syntax_uid.to_string(),
    );
    let mut object = InMemDicomObject::new_empty();
    collector
        .read_dataset_to_end(&mut object)
        .map_err(|error| error.to_string())?;
    object
        .with_meta(FileMetaTableBuilder::new().transfer_syntax(transfer_syntax_uid))
        .map_err(|error| error.to_string())
}

fn element_at_path<'a>(
    object: &'a InMemDicomObject,
    path: &str,
) -> Result<&'a dicom_object::mem::InMemElement, DicomWebError> {
    let components = path
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    if components.is_empty() {
        return Err(DicomWebError::NotFound(
            "missing bulk data path".to_string(),
        ));
    }
    element_at_components(object, &components)
}

fn element_at_components<'a>(
    object: &'a InMemDicomObject,
    components: &[&str],
) -> Result<&'a dicom_object::mem::InMemElement, DicomWebError> {
    let tag = parse_tag(components[0])?;
    let element = object
        .get(tag)
        .ok_or_else(|| DicomWebError::NotFound("bulk data path not found".to_string()))?;
    if components.len() == 1 {
        return Ok(element);
    }
    let item_index = components[1]
        .parse::<usize>()
        .map_err(|_| DicomWebError::NotFound("bulk data path not found".to_string()))?;
    let DicomValue::Sequence(sequence) = element.value() else {
        return Err(DicomWebError::NotFound(
            "bulk data path not found".to_string(),
        ));
    };
    let item = sequence
        .items()
        .get(item_index)
        .ok_or_else(|| DicomWebError::NotFound("bulk data path not found".to_string()))?;
    if components.len() == 2 {
        return Err(DicomWebError::NotFound(
            "bulk data path not found".to_string(),
        ));
    }
    element_at_components(item, &components[2..])
}

fn parse_tag(value: &str) -> Result<Tag, DicomWebError> {
    if value.len() != 8 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DicomWebError::NotFound(
            "bulk data path not found".to_string(),
        ));
    }
    let group = u16::from_str_radix(&value[..4], 16)
        .map_err(|_| DicomWebError::NotFound("bulk data path not found".to_string()))?;
    let element = u16::from_str_radix(&value[4..], 16)
        .map_err(|_| DicomWebError::NotFound("bulk data path not found".to_string()))?;
    Ok(Tag(group, element))
}

fn primitive_bytes(element: &dicom_object::mem::InMemElement) -> Result<Bytes, DicomWebError> {
    let bytes = element
        .value()
        .to_bytes()
        .map_err(|_| DicomWebError::NotFound("bulk data path not found".to_string()))?
        .into_owned();
    Ok(Bytes::from(bytes))
}

fn pixel_data_bytes(object: &DefaultDicomObject) -> Result<Bytes, DicomWebError> {
    let element = object
        .element(tags::PIXEL_DATA)
        .map_err(|_| DicomWebError::NotFound("Pixel Data is missing".to_string()))?;
    if element.vr() != VR::OB && element.vr() != VR::OW {
        return Err(DicomWebError::NotFound("Pixel Data is missing".to_string()));
    }
    element
        .value()
        .to_bytes()
        .map(|bytes| Bytes::from(bytes.into_owned()))
        .map_err(|_| DicomWebError::not_acceptable("encapsulated Pixel Data is not supported"))
}

fn frame_size(object: &DefaultDicomObject) -> Result<usize, DicomWebError> {
    let rows = required_usize(object, tags::ROWS)?;
    let columns = required_usize(object, tags::COLUMNS)?;
    let samples_per_pixel = required_usize(object, tags::SAMPLES_PER_PIXEL)?;
    let bits_allocated = required_usize(object, tags::BITS_ALLOCATED)?;
    let bytes_per_sample = bits_allocated
        .checked_add(7)
        .and_then(|bits| bits.checked_div(8))
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| DicomWebError::Internal("invalid BitsAllocated".to_string()))?;
    rows.checked_mul(columns)
        .and_then(|value| value.checked_mul(samples_per_pixel))
        .and_then(|value| value.checked_mul(bytes_per_sample))
        .filter(|size| *size > 0)
        .ok_or_else(|| DicomWebError::Internal("invalid frame geometry".to_string()))
}

fn frame_count(
    object: &DefaultDicomObject,
    pixel_data_len: usize,
    frame_size: usize,
) -> Result<usize, DicomWebError> {
    if let Ok(element) = object.element(tags::NUMBER_OF_FRAMES) {
        let count = element
            .value()
            .to_int::<usize>()
            .map_err(|_| DicomWebError::Internal("invalid NumberOfFrames".to_string()))?;
        return Ok(count);
    }
    Ok(pixel_data_len / frame_size)
}

fn required_usize(object: &DefaultDicomObject, tag: Tag) -> Result<usize, DicomWebError> {
    object
        .element(tag)
        .map_err(|_| DicomWebError::Internal(format!("missing required image tag {tag}")))?
        .value()
        .to_int::<usize>()
        .map_err(|_| DicomWebError::Internal(format!("invalid required image tag {tag}")))
}

fn record_selected(selected: &SelectedRepresentation) {
    Span::current().record("dicomweb.selected_media_type", selected.content_type());
}

fn record_error(error: DicomWebError) -> DicomWebError {
    Span::current().record("error.type", error.http_error_class());
    error
}
