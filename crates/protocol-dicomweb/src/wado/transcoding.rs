use std::io::Cursor;

use async_trait::async_trait;
use dicom_dictionary_std::uids;
use dicom_object::{DefaultDicomObject, FileMetaTableBuilder, InMemDicomObject};
use raccoon_contract_dicom::TransferSyntaxUid;
use raccoon_contract_object_store::{ByteStream, Bytes};
use raccoon_service_retrieve::RetrievedInstance;
use thiserror::Error;

use super::retrieve::{CollectedInstance, collect_instance};

const IMPLICIT_VR_LITTLE_ENDIAN: &str = uids::IMPLICIT_VR_LITTLE_ENDIAN;
const EXPLICIT_VR_LITTLE_ENDIAN: &str = uids::EXPLICIT_VR_LITTLE_ENDIAN;

/// Transfer syntax policy and backend for WADO native DICOM object retrieval.
#[derive(Clone)]
pub struct TransferSyntaxPolicy {
    transcoder: Option<std::sync::Arc<dyn DicomTranscoder>>,
    advertised_transfer_syntaxes: Vec<&'static str>,
}

impl TransferSyntaxPolicy {
    pub fn native_only() -> Self {
        Self {
            transcoder: None,
            advertised_transfer_syntaxes: vec![crate::capabilities::TRANSFER_SYNTAX_ANY],
        }
    }

    pub fn native_little_endian() -> Self {
        Self::with_transcoder(std::sync::Arc::new(NativeLittleEndianTranscoder::new()))
    }

    pub fn with_transcoder(transcoder: std::sync::Arc<dyn DicomTranscoder>) -> Self {
        Self::with_transcoder_targets(
            transcoder,
            vec![IMPLICIT_VR_LITTLE_ENDIAN, EXPLICIT_VR_LITTLE_ENDIAN],
        )
    }

    pub fn with_transcoder_targets(
        transcoder: std::sync::Arc<dyn DicomTranscoder>,
        targets: Vec<&'static str>,
    ) -> Self {
        let mut advertised_transfer_syntaxes = vec![crate::capabilities::TRANSFER_SYNTAX_ANY];
        advertised_transfer_syntaxes.extend(targets);
        Self {
            transcoder: Some(transcoder),
            advertised_transfer_syntaxes,
        }
    }

    pub fn advertised_transfer_syntaxes(&self) -> Vec<&'static str> {
        self.advertised_transfer_syntaxes.clone()
    }

    pub fn transcoder(&self) -> Option<&std::sync::Arc<dyn DicomTranscoder>> {
        self.transcoder.as_ref()
    }

    pub fn allows_target(&self, target: &TransferSyntaxUid) -> bool {
        self.advertised_transfer_syntaxes
            .iter()
            .any(|uid| *uid == target.as_str())
    }
}

impl Default for TransferSyntaxPolicy {
    fn default() -> Self {
        Self::native_only()
    }
}

impl std::fmt::Debug for TransferSyntaxPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransferSyntaxPolicy")
            .field(
                "transcoder",
                &self.transcoder.as_ref().map(|_| "DicomTranscoder"),
            )
            .field(
                "advertised_transfer_syntaxes",
                &self.advertised_transfer_syntaxes,
            )
            .finish()
    }
}

#[async_trait]
pub trait DicomTranscoder: Send + Sync {
    fn backend(&self) -> &'static str;

    fn supports(&self, source: Option<&TransferSyntaxUid>, target: &TransferSyntaxUid) -> bool;

    async fn transcode(
        &self,
        instance: RetrievedInstance,
        target: &TransferSyntaxUid,
    ) -> Result<TranscodedInstance, TranscodeError>;
}

pub struct TranscodedInstance {
    pub instance: RetrievedInstance,
}

#[derive(Debug, Error)]
pub enum TranscodeError {
    #[error("unsupported transfer syntax conversion from {source_transfer_syntax:?} to {target}")]
    Unsupported {
        source_transfer_syntax: Option<TransferSyntaxUid>,
        target: TransferSyntaxUid,
    },
    #[error("DICOM parse failed: {0}")]
    Parse(String),
    #[error("DICOM write failed: {0}")]
    Write(String),
    #[error("object stream failed: {0}")]
    Stream(String),
}

/// Native backend for uncompressed Little Endian implicit/explicit conversion.
#[derive(Debug, Default)]
pub struct NativeLittleEndianTranscoder;

impl NativeLittleEndianTranscoder {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DicomTranscoder for NativeLittleEndianTranscoder {
    fn backend(&self) -> &'static str {
        "native"
    }

    fn supports(&self, source: Option<&TransferSyntaxUid>, target: &TransferSyntaxUid) -> bool {
        source
            .map(TransferSyntaxUid::as_str)
            .is_some_and(is_supported_little_endian)
            && is_supported_little_endian(target.as_str())
    }

    async fn transcode(
        &self,
        instance: RetrievedInstance,
        target: &TransferSyntaxUid,
    ) -> Result<TranscodedInstance, TranscodeError> {
        if !self.supports(instance.transfer_syntax_uid.as_ref(), target) {
            return Err(TranscodeError::Unsupported {
                source_transfer_syntax: instance.transfer_syntax_uid.clone(),
                target: target.clone(),
            });
        }

        let identity = instance.identity.clone();
        let collected = collect_instance(instance)
            .await
            .map_err(|error| TranscodeError::Stream(error.to_string()))?;
        let object = parse_object(&collected)?;
        let mut bytes = Vec::new();
        object
            .into_inner()
            .with_meta(FileMetaTableBuilder::new().transfer_syntax(target.as_str()))
            .map_err(|error| TranscodeError::Write(error.to_string()))?
            .write_all(&mut bytes)
            .map_err(|error| TranscodeError::Write(error.to_string()))?;

        Ok(TranscodedInstance {
            instance: RetrievedInstance {
                identity,
                transfer_syntax_uid: Some(target.clone()),
                content_length: bytes.len() as u64,
                body: ByteStream::once(Bytes::from(bytes)),
            },
        })
    }
}

fn is_supported_little_endian(uid: &str) -> bool {
    matches!(uid, IMPLICIT_VR_LITTLE_ENDIAN | EXPLICIT_VR_LITTLE_ENDIAN)
}

fn parse_object(instance: &CollectedInstance) -> Result<DefaultDicomObject, TranscodeError> {
    match dicom_object::from_reader(Cursor::new(instance.body.clone())) {
        Ok(object) => Ok(object),
        Err(file_error) => parse_dataset(instance).map_err(|dataset_error| {
            TranscodeError::Parse(format!(
                "Part 10 parse failed: {file_error}; dataset parse failed: {dataset_error}"
            ))
        }),
    }
}

fn parse_dataset(instance: &CollectedInstance) -> Result<DefaultDicomObject, String> {
    let transfer_syntax_uid = instance
        .transfer_syntax_uid
        .as_ref()
        .map(TransferSyntaxUid::as_str)
        .unwrap_or(EXPLICIT_VR_LITTLE_ENDIAN);
    let mut collector = dicom_object::collector::DicomCollector::new_with_ts(
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

#[cfg(test)]
mod tests {
    use dicom_core::value::Value as DicomValue;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_dictionary_std::tags;
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    };

    use super::*;

    #[tokio::test]
    async fn native_transcoder_rewrites_file_meta_to_target() {
        let target = TransferSyntaxUid::new(IMPLICIT_VR_LITTLE_ENDIAN).unwrap();
        let output = transcode_fixture(EXPLICIT_VR_LITTLE_ENDIAN, &target).await;
        let object = dicom_object::from_reader(Cursor::new(collected_bytes(output).await)).unwrap();

        assert_eq!(
            object.meta().transfer_syntax.trim_end_matches('\0'),
            IMPLICIT_VR_LITTLE_ENDIAN
        );
    }

    #[tokio::test]
    async fn native_transcoder_preserves_identity_uids_and_pixel_data() {
        let target = TransferSyntaxUid::new(IMPLICIT_VR_LITTLE_ENDIAN).unwrap();
        let output = transcode_fixture(EXPLICIT_VR_LITTLE_ENDIAN, &target).await;
        let object = dicom_object::from_reader(Cursor::new(collected_bytes(output).await)).unwrap();

        assert_eq!(string_value(&object, tags::STUDY_INSTANCE_UID), "1.2.3");
        assert_eq!(string_value(&object, tags::SERIES_INSTANCE_UID), "1.2.3.4");
        assert_eq!(string_value(&object, tags::SOP_INSTANCE_UID), "1.2.3.4.5");
        assert_eq!(
            object
                .element(tags::PIXEL_DATA)
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            &[1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn native_transcoder_rejects_unsupported_source_or_target() {
        let transcoder = NativeLittleEndianTranscoder::new();
        let unsupported_source = RetrievedInstance {
            identity: identity(),
            transfer_syntax_uid: Some(TransferSyntaxUid::new("1.2.840.10008.1.2.4.50").unwrap()),
            content_length: 0,
            body: ByteStream::empty(),
        };
        let target = TransferSyntaxUid::new(EXPLICIT_VR_LITTLE_ENDIAN).unwrap();

        assert!(matches!(
            transcoder.transcode(unsupported_source, &target).await,
            Err(TranscodeError::Unsupported { .. })
        ));
        assert!(!transcoder.supports(
            Some(&TransferSyntaxUid::new(EXPLICIT_VR_LITTLE_ENDIAN).unwrap()),
            &TransferSyntaxUid::new("1.2.840.10008.1.2.4.50").unwrap()
        ));
    }

    async fn transcode_fixture(
        source: &'static str,
        target: &TransferSyntaxUid,
    ) -> RetrievedInstance {
        NativeLittleEndianTranscoder::new()
            .transcode(
                RetrievedInstance {
                    identity: identity(),
                    transfer_syntax_uid: Some(TransferSyntaxUid::new(source).unwrap()),
                    content_length: 0,
                    body: ByteStream::once(dicom_bytes(source)),
                },
                target,
            )
            .await
            .unwrap()
            .instance
    }

    fn dicom_bytes(transfer_syntax: &'static str) -> Vec<u8> {
        let object = InMemDicomObject::from_element_iter([
            DataElement::new(
                tags::STUDY_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from("1.2.3"),
            ),
            DataElement::new(
                tags::SERIES_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from("1.2.3.4"),
            ),
            DataElement::new(
                tags::SOP_INSTANCE_UID,
                VR::UI,
                PrimitiveValue::from("1.2.3.4.5"),
            ),
            DataElement::new(
                tags::SOP_CLASS_UID,
                VR::UI,
                PrimitiveValue::from("1.2.840.10008.5.1.4.1.1.2"),
            ),
            DataElement::new(
                tags::PIXEL_DATA,
                VR::OB,
                DicomValue::Primitive(PrimitiveValue::from([1_u8, 2, 3, 4].as_slice())),
            ),
        ]);
        let object = object
            .with_meta(FileMetaTableBuilder::new().transfer_syntax(transfer_syntax))
            .unwrap();
        let mut bytes = Vec::new();
        object.write_all(&mut bytes).unwrap();
        bytes
    }

    fn identity() -> DicomInstanceIdentity {
        DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.4").unwrap(),
            SopInstanceUid::new("1.2.3.4.5").unwrap(),
            SopClassUid::new("1.2.840.10008.5.1.4.1.1.2").unwrap(),
        )
    }

    fn string_value(object: &DefaultDicomObject, tag: dicom_core::Tag) -> String {
        object.element(tag).unwrap().to_str().unwrap().into_owned()
    }

    async fn collected_bytes(instance: RetrievedInstance) -> Vec<u8> {
        let mut stream = instance.body.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        bytes
    }
}
