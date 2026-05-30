//! DICOM value types shared across service and protocol crate boundaries.
//!
//! This crate provides validated UID new types, DICOM instance hierarchy types,
//! normalized metadata value objects, and the query/retrieve level enum. It is
//! the shared type vocabulary for query, retrieve, and protocol crates. Level
//! types are per information model: [`StudyRootQueryRetrieveLevel`] for Study Root and
//! [`PatientRootQueryRetrieveLevel`] for Patient Root — keeping invalid level/model
//! combinations unrepresentable at the type level.

mod identity;
mod level;
mod metadata;
mod uid;

pub use identity::{DicomInstanceIdentity, DicomSeriesIdentity, DicomStudyIdentity};
pub use level::{PatientRootQueryRetrieveLevel, StudyRootQueryRetrieveLevel};
pub use metadata::{
    DicomInstanceMetadata, DicomPatient, DicomSeriesMetadata, DicomStudyMetadata, PatientId,
    PatientIdError,
};
pub use uid::{
    DicomUidError, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
    TransferSyntaxUid,
};
