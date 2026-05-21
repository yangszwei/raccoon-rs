use std::collections::BTreeSet;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use dicom_dictionary_std::tags;
use dicom_object::{
    DicomAttribute, DicomCollector, DicomCollectorOptions, InMemDicomObject, file::ReadPreamble,
};
use thiserror::Error;

use crate::{IngestObjectIdentity, IngestPayloadRepresentation, ParsedDicomObject};

/// Failure while parsing enough DICOM structure for storage acceptance.
#[derive(Debug, Error)]
pub(crate) enum DicomIntakeError {
    #[error("cannot understand DICOM object: {reason}")]
    CannotUnderstand {
        reason: String,
        identity: Box<IngestObjectIdentity>,
        payload_representation: IngestPayloadRepresentation,
        transfer_syntax_uid: Option<String>,
    },
}

pub(crate) async fn parse_dicom_intake_file(
    path: impl AsRef<Path>,
    request_representation: IngestPayloadRepresentation,
    request_transfer_syntax_uid: Option<String>,
    identity_hints: IngestObjectIdentity,
) -> Result<ParsedDicomObject, DicomIntakeError> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || {
        parse_dicom_intake_file_blocking(
            path,
            request_representation,
            request_transfer_syntax_uid,
            identity_hints,
        )
    })
    .await
    .map_err(|error| DicomIntakeError::CannotUnderstand {
        reason: format!("DICOM intake parser task failed: {error}"),
        identity: Box::default(),
        payload_representation: IngestPayloadRepresentation::Unknown,
        transfer_syntax_uid: None,
    })?
}

fn parse_dicom_intake_file_blocking(
    path: PathBuf,
    request_representation: IngestPayloadRepresentation,
    request_transfer_syntax_uid: Option<String>,
    identity_hints: IngestObjectIdentity,
) -> Result<ParsedDicomObject, DicomIntakeError> {
    let (identity, payload_representation, transfer_syntax_uid) = match request_representation {
        IngestPayloadRepresentation::DicomDataSet => parse_dataset(
            &path,
            request_transfer_syntax_uid.as_deref(),
            request_representation,
        )?,
        IngestPayloadRepresentation::DicomWebMetadataAndBulkData => {
            return Err(DicomIntakeError::CannotUnderstand {
                reason: "DICOMweb metadata plus bulk data intake is not assembled into a DICOM data set yet".to_string(),
                identity: Box::new(identity_hints),
                payload_representation: request_representation,
                transfer_syntax_uid: request_transfer_syntax_uid,
            });
        }
        IngestPayloadRepresentation::DicomFile | IngestPayloadRepresentation::Unknown => {
            parse_file(&path, request_representation)?
        }
    };

    Ok(ParsedDicomObject {
        identity,
        payload_representation,
        transfer_syntax_uid,
    })
}

fn parse_file(
    path: &Path,
    request_representation: IngestPayloadRepresentation,
) -> Result<
    (
        IngestObjectIdentity,
        IngestPayloadRepresentation,
        Option<String>,
    ),
    DicomIntakeError,
> {
    let file = std::fs::File::open(path).map_err(|error| DicomIntakeError::CannotUnderstand {
        reason: format!("failed to open staged DICOM file: {error}"),
        identity: Box::default(),
        payload_representation: request_representation,
        transfer_syntax_uid: None,
    })?;
    let mut collector = DicomCollector::new(BufReader::new(file));
    let transfer_syntax_uid = collector
        .read_file_meta()
        .map(|file_meta| file_meta.transfer_syntax().to_string())
        .map_err(|error| DicomIntakeError::CannotUnderstand {
            reason: format!("failed to parse DICOM file meta: {error}"),
            identity: Box::default(),
            payload_representation: request_representation,
            transfer_syntax_uid: None,
        })?;
    let transfer_syntax_uid = Some(transfer_syntax_uid);
    let mut object = InMemDicomObject::new_empty();
    collector
        .read_dataset_up_to_pixeldata(&mut object)
        .map_err(|error| DicomIntakeError::CannotUnderstand {
            reason: format!("failed to parse DICOM metadata: {error}"),
            identity: Box::default(),
            payload_representation: request_representation,
            transfer_syntax_uid: transfer_syntax_uid.clone(),
        })?;
    let identity =
        identity_from_object(&object, request_representation, transfer_syntax_uid.clone())?;
    Ok((
        identity,
        IngestPayloadRepresentation::DicomFile,
        transfer_syntax_uid,
    ))
}

fn parse_dataset(
    path: &Path,
    transfer_syntax_uid: Option<&str>,
    request_representation: IngestPayloadRepresentation,
) -> Result<
    (
        IngestObjectIdentity,
        IngestPayloadRepresentation,
        Option<String>,
    ),
    DicomIntakeError,
> {
    let transfer_syntax_uid =
        transfer_syntax_uid.ok_or_else(|| DicomIntakeError::CannotUnderstand {
            reason: "DICOM data set intake requires a transfer syntax UID".to_string(),
            identity: Box::default(),
            payload_representation: request_representation,
            transfer_syntax_uid: None,
        })?;
    let transfer_syntax_uid = transfer_syntax_uid.to_string();
    let file = std::fs::File::open(path).map_err(|error| DicomIntakeError::CannotUnderstand {
        reason: format!("failed to open staged DICOM data set: {error}"),
        identity: Box::default(),
        payload_representation: request_representation,
        transfer_syntax_uid: Some(transfer_syntax_uid.clone()),
    })?;
    let mut collector = DicomCollectorOptions::new()
        .read_preamble(ReadPreamble::Never)
        .expected_ts(transfer_syntax_uid.clone())
        .from_reader(BufReader::new(file));
    let mut object = InMemDicomObject::new_empty();
    collector
        .read_dataset_up_to(tags::PIXEL_DATA, &mut object)
        .map_err(|error| DicomIntakeError::CannotUnderstand {
            reason: format!("failed to parse DICOM data set: {error}"),
            identity: Box::default(),
            payload_representation: request_representation,
            transfer_syntax_uid: Some(transfer_syntax_uid.clone()),
        })?;
    let transfer_syntax_uid = Some(transfer_syntax_uid);
    let identity =
        identity_from_object(&object, request_representation, transfer_syntax_uid.clone())?;
    Ok((
        identity,
        IngestPayloadRepresentation::DicomDataSet,
        transfer_syntax_uid,
    ))
}

fn identity_from_object<O>(
    object: &O,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
) -> Result<IngestObjectIdentity, DicomIntakeError>
where
    O: dicom_object::DicomObject,
{
    Ok(IngestObjectIdentity {
        sop_class_uid: required_string(
            object,
            "SOPClassUID",
            payload_representation,
            transfer_syntax_uid.clone(),
        )?,
        study_instance_uid: required_string(
            object,
            "StudyInstanceUID",
            payload_representation,
            transfer_syntax_uid.clone(),
        )?,
        series_instance_uid: required_string(
            object,
            "SeriesInstanceUID",
            payload_representation,
            transfer_syntax_uid.clone(),
        )?,
        sop_instance_uid: required_string(
            object,
            "SOPInstanceUID",
            payload_representation,
            transfer_syntax_uid,
        )?,
    })
}

fn required_string<O>(
    object: &O,
    name: &'static str,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
) -> Result<Option<String>, DicomIntakeError>
where
    O: dicom_object::DicomObject,
{
    let attribute =
        object
            .attr_by_name(name)
            .map_err(|error| DicomIntakeError::CannotUnderstand {
                reason: format!("missing or unreadable {name}: {error}"),
                identity: Box::default(),
                payload_representation,
                transfer_syntax_uid: transfer_syntax_uid.clone(),
            })?;
    let value = attribute
        .to_str()
        .map_err(|error| DicomIntakeError::CannotUnderstand {
            reason: format!("invalid string value for {name}: {error}"),
            identity: Box::default(),
            payload_representation,
            transfer_syntax_uid,
        })?;
    Ok(Some(value.trim().to_string()))
}

/// Policy input that contains only the storage-critical facts extracted from DICOM.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageAcceptanceInput {
    pub identity: IngestObjectIdentity,
}

/// Policy decision before object bytes are written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageAcceptanceDecision {
    Accept,
    RejectUnsupportedSopClass {
        sop_class_uid: Option<String>,
        reason: String,
    },
}

/// Storage acceptance policy evaluated by the ingest service.
pub trait StorageAcceptancePolicy: Send + Sync {
    fn evaluate(&self, input: &StorageAcceptanceInput) -> StorageAcceptanceDecision;
}

/// Storage policy that accepts all parseable SOP Classes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AcceptAllStorageAcceptancePolicy;

impl StorageAcceptancePolicy for AcceptAllStorageAcceptancePolicy {
    fn evaluate(&self, _input: &StorageAcceptanceInput) -> StorageAcceptanceDecision {
        StorageAcceptanceDecision::Accept
    }
}

/// Storage policy that accepts only the configured SOP Class UIDs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SopClassStorageAcceptancePolicy {
    supported_sop_classes: BTreeSet<String>,
}

impl SopClassStorageAcceptancePolicy {
    pub fn supported_sop_classes(sop_class_uids: impl IntoIterator<Item = String>) -> Self {
        Self {
            supported_sop_classes: sop_class_uids.into_iter().collect(),
        }
    }
}

impl StorageAcceptancePolicy for SopClassStorageAcceptancePolicy {
    fn evaluate(&self, input: &StorageAcceptanceInput) -> StorageAcceptanceDecision {
        if !self.supported_sop_classes.is_empty() {
            let sop_class_uid = input.identity.sop_class_uid.clone();
            if !sop_class_uid
                .as_ref()
                .is_some_and(|uid| self.supported_sop_classes.contains(uid))
            {
                return StorageAcceptanceDecision::RejectUnsupportedSopClass {
                    sop_class_uid,
                    reason: "SOP Class is not supported by storage policy".to_string(),
                };
            }
        }

        StorageAcceptanceDecision::Accept
    }
}
