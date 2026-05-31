use std::io::{BufReader, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::str::FromStr;

use dicom_object::{
    DicomCollector, DicomCollectorOptions, DicomObject, InMemDicomObject, file::ReadPreamble,
};
use futures_util::StreamExt;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    TransferSyntaxUid,
};
use raccoon_contract_object_store::{ByteStream, Bytes, ObjectKey, Result as ObjectStoreResult};
use raccoon_service_ingest::IngestPayloadRepresentation;
use tokio::runtime::Handle;

use crate::error::SyncParseError;
use crate::model::{SyncInstanceRecord, SyncSeriesRecord, SyncStudyRecord};

/// Metadata parsed from one stored DICOM object for sync.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedSyncObject {
    pub study: SyncStudyRecord,
    pub series: SyncSeriesRecord,
    pub instance: SyncInstanceRecord,
}

/// Bounded-memory DICOM parser for sync.
#[derive(Clone, Debug, Default)]
pub struct DicomSyncParser;

impl DicomSyncParser {
    pub fn new() -> Self {
        Self
    }

    pub async fn parse(
        &self,
        body: ByteStream,
        object_key: ObjectKey,
        object_size_bytes: u64,
        payload_representation: IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
        max_metadata_bytes: Option<u64>,
    ) -> Result<ParsedSyncObject, SyncParseError> {
        let handle = Handle::current();
        tokio::task::spawn_blocking(move || {
            parse_stream_blocking(
                handle,
                body,
                object_key,
                object_size_bytes,
                payload_representation,
                transfer_syntax_uid,
                max_metadata_bytes,
            )
        })
        .await
        .map_err(|error| SyncParseError::ParserTask(error.to_string()))?
    }
}

fn parse_stream_blocking(
    handle: Handle,
    body: ByteStream,
    object_key: ObjectKey,
    object_size_bytes: u64,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
    max_metadata_bytes: Option<u64>,
) -> Result<ParsedSyncObject, SyncParseError> {
    let reader = BoundedByteStreamReader::new(handle, body, max_metadata_bytes);
    parse_reader(
        BufReader::new(reader),
        object_key,
        object_size_bytes,
        payload_representation,
        transfer_syntax_uid,
    )
}

fn parse_reader<R: Read + Seek>(
    reader: BufReader<R>,
    object_key: ObjectKey,
    object_size_bytes: u64,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
) -> Result<ParsedSyncObject, SyncParseError> {
    match payload_representation {
        IngestPayloadRepresentation::DicomDataSet => {
            parse_dataset_reader(reader, object_key, object_size_bytes, transfer_syntax_uid)
        }
        IngestPayloadRepresentation::DicomFile | IngestPayloadRepresentation::Unknown => {
            parse_file_reader(reader, object_key, object_size_bytes)
        }
        IngestPayloadRepresentation::DicomWebMetadataAndBulkData => Err(
            SyncParseError::cannot_understand("DICOMweb metadata+bulkdata sync is not supported"),
        ),
    }
}

fn parse_file_reader<R: Read + Seek>(
    reader: BufReader<R>,
    object_key: ObjectKey,
    object_size_bytes: u64,
) -> Result<ParsedSyncObject, SyncParseError> {
    let mut collector = DicomCollector::new(reader);
    let transfer_syntax_uid = collector
        .read_file_meta()
        .map(|file_meta| file_meta.transfer_syntax().to_string())
        .map_err(|error| map_reader_or_parse_error(error, "failed to parse DICOM file meta"))?;

    let mut object = InMemDicomObject::new_empty();
    collector
        .read_dataset_up_to_pixeldata(&mut object)
        .map_err(|error| map_reader_or_parse_error(error, "failed to parse DICOM metadata"))?;

    project_object(
        object,
        object_key,
        object_size_bytes,
        Some(transfer_syntax_uid),
    )
}

fn parse_dataset_reader<R: Read + Seek>(
    reader: BufReader<R>,
    object_key: ObjectKey,
    object_size_bytes: u64,
    transfer_syntax_uid: Option<String>,
) -> Result<ParsedSyncObject, SyncParseError> {
    let transfer_syntax_uid = transfer_syntax_uid.ok_or_else(|| {
        SyncParseError::cannot_understand("DICOM data set sync requires a transfer syntax UID")
    })?;
    let mut collector = DicomCollectorOptions::new()
        .read_preamble(ReadPreamble::Never)
        .expected_ts(transfer_syntax_uid.clone())
        .from_reader(reader);
    let mut object = InMemDicomObject::new_empty();
    collector
        .read_dataset_up_to_pixeldata(&mut object)
        .map_err(|error| map_reader_or_parse_error(error, "failed to parse DICOM data set"))?;
    project_object(
        object,
        object_key,
        object_size_bytes,
        Some(transfer_syntax_uid),
    )
}

fn map_reader_or_parse_error(
    error: impl std::fmt::Display,
    context: &'static str,
) -> SyncParseError {
    let message = error.to_string();
    if let Some(max_metadata_bytes) = parse_metadata_limit(&message) {
        SyncParseError::MetadataTooLarge { max_metadata_bytes }
    } else {
        SyncParseError::cannot_understand(format!("{context}: {error}"))
    }
}

fn parse_metadata_limit(message: &str) -> Option<u64> {
    message
        .find(METADATA_LIMIT_PREFIX)
        .map(|index| &message[index + METADATA_LIMIT_PREFIX.len()..])
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|rest| rest.parse::<u64>().ok())
}

const METADATA_LIMIT_PREFIX: &str = "raccoon metadata limit exceeded: ";

type BoxObjectStream =
    Pin<Box<dyn raccoon_contract_object_store::Stream<Item = ObjectStoreResult<Bytes>> + Send>>;

struct BoundedByteStreamReader {
    handle: Handle,
    stream: BoxObjectStream,
    buffer: Vec<u8>,
    position: u64,
    eof: bool,
    max_metadata_bytes: Option<u64>,
}

impl BoundedByteStreamReader {
    fn new(handle: Handle, body: ByteStream, max_metadata_bytes: Option<u64>) -> Self {
        Self {
            handle,
            stream: body.into_stream(),
            buffer: Vec::new(),
            position: 0,
            eof: false,
            max_metadata_bytes,
        }
    }

    fn ensure_available(&mut self, target_len: usize) -> std::io::Result<()> {
        while self.buffer.len() < target_len && !self.eof {
            let next = self.handle.block_on(self.stream.next());
            match next {
                Some(Ok(chunk)) => self.push_chunk(chunk)?,
                Some(Err(error)) => return Err(std::io::Error::other(error)),
                None => self.eof = true,
            }
        }
        Ok(())
    }

    fn push_chunk(&mut self, chunk: Bytes) -> std::io::Result<()> {
        let next_len = self.buffer.len().saturating_add(chunk.len());
        if let Some(max) = self.max_metadata_bytes
            && next_len > max as usize
        {
            return Err(std::io::Error::other(format!(
                "{METADATA_LIMIT_PREFIX}{max}"
            )));
        }
        self.buffer.extend_from_slice(&chunk);
        Ok(())
    }
}

impl Read for BoundedByteStreamReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        let target_len = self
            .position
            .saturating_add(out.len() as u64)
            .min(usize::MAX as u64) as usize;
        self.ensure_available(target_len)?;

        let start = self.position.min(usize::MAX as u64) as usize;
        if start >= self.buffer.len() {
            return Ok(0);
        }

        let end = self.buffer.len().min(start + out.len());
        let len = end - start;
        out[..len].copy_from_slice(&self.buffer[start..end]);
        self.position += len as u64;
        Ok(len)
    }
}

impl Seek for BoundedByteStreamReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let next = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => if offset.is_negative() {
                self.position.checked_sub(offset.unsigned_abs())
            } else {
                self.position.checked_add(offset as u64)
            }
            .ok_or_else(|| std::io::Error::other("seek position overflow"))?,
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "bounded object reader does not support SeekFrom::End",
                ));
            }
        };
        self.position = next;
        Ok(self.position)
    }
}

fn project_object(
    object: InMemDicomObject,
    object_key: ObjectKey,
    object_size_bytes: u64,
    transfer_syntax_uid: Option<String>,
) -> Result<ParsedSyncObject, SyncParseError> {
    let study_instance_uid = parse_uid::<StudyInstanceUid>(
        required_text(&object, "StudyInstanceUID")?,
        "StudyInstanceUID",
    )?;
    let series_instance_uid = parse_uid::<SeriesInstanceUid>(
        required_text(&object, "SeriesInstanceUID")?,
        "SeriesInstanceUID",
    )?;
    let sop_instance_uid =
        parse_uid::<SopInstanceUid>(required_text(&object, "SOPInstanceUID")?, "SOPInstanceUID")?;
    let sop_class_uid =
        parse_uid::<SopClassUid>(required_text(&object, "SOPClassUID")?, "SOPClassUID")?;
    let transfer_syntax_uid = transfer_syntax_uid
        .map(TransferSyntaxUid::new)
        .transpose()
        .map_err(|error| {
            SyncParseError::validation(format!("invalid TransferSyntaxUID: {error}"))
        })?;

    let patient_id = optional_text(&object, "PatientID");
    let patient_name = optional_text(&object, "PatientName");
    let patient_birth_date = optional_text(&object, "PatientBirthDate");
    let patient_sex = optional_text(&object, "PatientSex");
    let study_date = optional_text(&object, "StudyDate");
    let study_time = optional_text(&object, "StudyTime");
    let accession_number = optional_text(&object, "AccessionNumber");
    let study_id = optional_text(&object, "StudyID");
    let study_description = optional_text(&object, "StudyDescription");
    let referring_physician_name = optional_text(&object, "ReferringPhysicianName");
    let modality = optional_text(&object, "Modality");
    let series_number = optional_i64(&object, "SeriesNumber")?;
    let series_date = optional_text(&object, "SeriesDate");
    let series_time = optional_text(&object, "SeriesTime");
    let series_description = optional_text(&object, "SeriesDescription");
    let body_part_examined = optional_text(&object, "BodyPartExamined");
    let instance_number = optional_i64(&object, "InstanceNumber")?;
    let acquisition_date_time = optional_text(&object, "AcquisitionDateTime");

    let attributes_json =
        serde_json::to_string(&dicom_json::DicomJson::from(object)).map_err(|error| {
            SyncParseError::cannot_understand(format!("failed to serialize DICOM JSON: {error}"))
        })?;

    Ok(ParsedSyncObject {
        study: SyncStudyRecord {
            study_instance_uid: study_instance_uid.clone(),
            patient_id,
            patient_name,
            patient_birth_date,
            patient_sex,
            study_date,
            study_time,
            accession_number,
            study_id,
            study_description,
            referring_physician_name,
        },
        series: SyncSeriesRecord {
            series_instance_uid: series_instance_uid.clone(),
            study_instance_uid: study_instance_uid.clone(),
            modality,
            series_number,
            series_date,
            series_time,
            series_description,
            body_part_examined,
        },
        instance: SyncInstanceRecord {
            identity: DicomInstanceIdentity::new(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
                sop_class_uid,
            ),
            instance_number,
            acquisition_date_time,
            transfer_syntax_uid,
            object_key,
            object_size_bytes,
            attributes_json,
        },
    })
}

fn required_text(object: &InMemDicomObject, name: &'static str) -> Result<String, SyncParseError> {
    optional_text(object, name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SyncParseError::validation(format!("missing {name}")))
}

fn optional_text(object: &InMemDicomObject, name: &'static str) -> Option<String> {
    object
        .attr_by_name(name)
        .ok()
        .and_then(|attr| attr.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_i64(
    object: &InMemDicomObject,
    name: &'static str,
) -> Result<Option<i64>, SyncParseError> {
    optional_text(object, name)
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|error| SyncParseError::validation(format!("invalid {name}: {error}")))
        })
        .transpose()
}

fn parse_uid<T>(value: String, name: &'static str) -> Result<T, SyncParseError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    T::from_str(&value)
        .map_err(|error| SyncParseError::validation(format!("invalid {name}: {error}")))
}

#[cfg(test)]
mod tests {
    use dicom_core::{DataElement, VR};
    use dicom_dictionary_std::tags;
    use serde_json::Value;

    use super::*;

    #[test]
    fn parser_error_categories_are_terminal_data_failures() {
        let cannot = SyncParseError::cannot_understand("bad file meta");
        let validation = SyncParseError::validation("missing SOPInstanceUID");

        assert!(matches!(cannot, SyncParseError::CannotUnderstand { .. }));
        assert!(matches!(validation, SyncParseError::Validation { .. }));
    }

    #[test]
    fn required_identity_validation_rejects_missing_tags() {
        let object = InMemDicomObject::new_empty();
        let err = project_object(
            object,
            ObjectKey::new("ingest/1").unwrap(),
            0,
            Some("1.2.840.10008.1.2.1".to_string()),
        )
        .expect_err("missing identity is invalid");

        assert!(matches!(err, SyncParseError::Validation { .. }));
        assert!(err.to_string().contains("StudyInstanceUID"));
    }

    #[test]
    fn projection_serializes_json_after_extracting_owned_fields() {
        let mut object = InMemDicomObject::new_empty();
        object.put(DataElement::new(tags::STUDY_INSTANCE_UID, VR::UI, "1.2.3"));
        object.put(DataElement::new(
            tags::SERIES_INSTANCE_UID,
            VR::UI,
            "1.2.3.4",
        ));
        object.put(DataElement::new(
            tags::SOP_INSTANCE_UID,
            VR::UI,
            "1.2.3.4.5",
        ));
        object.put(DataElement::new(
            tags::SOP_CLASS_UID,
            VR::UI,
            "1.2.840.10008.5.1.4.1.1.2",
        ));
        object.put(DataElement::new(tags::PATIENT_ID, VR::LO, "P1"));
        object.put(DataElement::new(tags::MODALITY, VR::CS, "CT"));
        object.put(DataElement::new(tags::INSTANCE_NUMBER, VR::IS, "7"));

        let parsed = project_object(
            object,
            ObjectKey::new("ingest/1").unwrap(),
            42,
            Some("1.2.840.10008.1.2.1".to_string()),
        )
        .expect("valid object projects");

        assert_eq!(parsed.study.patient_id.as_deref(), Some("P1"));
        assert_eq!(parsed.series.modality.as_deref(), Some("CT"));
        assert_eq!(parsed.instance.instance_number, Some(7));
        let json: Value = serde_json::from_str(&parsed.instance.attributes_json).unwrap();
        assert!(json.get("00100020").is_some());
        assert!(json.get("7FE00010").is_none());
    }

    #[test]
    fn pixel_data_tag_is_known_large_binary_boundary() {
        assert_eq!(tags::PIXEL_DATA.to_string(), "(7FE0,0010)");
    }

    #[tokio::test]
    async fn bounded_stream_reader_enforces_metadata_limit() {
        let body = ByteStream::from_chunks([
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"5678"),
            Bytes::from_static(b"90"),
        ]);
        let handle = Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut reader = BoundedByteStreamReader::new(handle, body, Some(8));
            let mut first = [0u8; 4];
            reader.read_exact(&mut first).expect("first chunk readable");
            assert_eq!(&first, b"1234");

            let mut second = [0u8; 5];
            let error = reader
                .read_exact(&mut second)
                .expect_err("reading beyond metadata limit fails");

            assert!(error.to_string().contains(METADATA_LIMIT_PREFIX));
        })
        .await
        .expect("blocking reader test finishes");
    }

    #[tokio::test]
    async fn bounded_stream_reader_has_no_default_metadata_limit() {
        let body = ByteStream::from_chunks([
            Bytes::from_static(b"1234"),
            Bytes::from_static(b"5678"),
            Bytes::from_static(b"90"),
        ]);
        let handle = Handle::current();

        tokio::task::spawn_blocking(move || {
            let mut reader = BoundedByteStreamReader::new(handle, body, None);
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .expect("unconfigured reader has no cap");

            assert_eq!(&bytes, b"1234567890");
        })
        .await
        .expect("blocking reader test finishes");
    }
}
