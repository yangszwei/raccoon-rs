//! DICOMweb HTTP protocol layer for Raccoon.
//!
//! This crate owns DICOMweb wire behavior and route composition. Concrete
//! QIDO-RS, STOW-RS, WADO-RS, WADO-URI, and rendered behavior is contributed
//! by providers.

mod capabilities;
mod error;
mod instrumentation;
mod media;
mod router;
mod state;
mod status;
mod uid;
mod url;

pub use capabilities::DicomWebFeatureSet;
pub use error::DicomWebError;
pub use instrumentation::{DicomUidFields, InstrumentationFields};
pub use media::{
    APPLICATION_DICOM, APPLICATION_DICOM_JSON, APPLICATION_DICOM_XML, APPLICATION_OCTET_STREAM,
    IMAGE_JPEG, IMAGE_PNG, MULTIPART_RELATED, MediaRange, MediaType, MediaTypeParams,
    SelectedRepresentation, content_type, dicom_json_response, dicomweb_status_report_response,
    multipart_related_response, negotiate_response, parse_accept,
};
pub use router::{DicomWebProvider, DicomWebRouteRegistry, DicomWebRouter};
pub use state::DicomWebState;
pub use status::DicomWebStatus;
pub use uid::{
    FrameList, series_instance_uid, sop_class_uid, sop_instance_uid, study_instance_uid,
    transfer_syntax_uid,
};
pub use url::{BulkDataPath, BulkDataUri, DicomWebUrlBase, RetrieveUrl};
