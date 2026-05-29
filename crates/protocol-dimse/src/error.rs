use raccoon_service_application_entity_registry::AeTitleError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DimseError {
    #[error(transparent)]
    Ul(#[from] dicom_ul::association::Error),

    #[error(transparent)]
    DicomRead(Box<dicom_object::ReadError>),

    #[error(transparent)]
    DicomWrite(Box<dicom_object::WriteError>),

    #[error("protocol violation: {0}")]
    Protocol(String),

    #[error("peer requested release")]
    PeerReleaseRequested,

    #[error("peer aborted association")]
    PeerAborted,

    #[error("connection closed")]
    ConnectionClosed,

    #[error(transparent)]
    InvalidAeTitle(#[from] AeTitleError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl DimseError {
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }
}

impl From<dicom_object::ReadError> for DimseError {
    fn from(err: dicom_object::ReadError) -> Self {
        Self::DicomRead(Box::new(err))
    }
}

impl From<dicom_object::WriteError> for DimseError {
    fn from(err: dicom_object::WriteError) -> Self {
        Self::DicomWrite(Box::new(err))
    }
}
