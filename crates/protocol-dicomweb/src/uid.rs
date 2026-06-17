//! DICOM UID parsing helpers shared by DICOMweb endpoints.

use raccoon_contract_dicom::{
    SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid, TransferSyntaxUid,
};

use crate::DicomWebError;

pub fn study_instance_uid(
    value: impl Into<String>,
    context: impl AsRef<str>,
) -> Result<StudyInstanceUid, DicomWebError> {
    StudyInstanceUid::new(value).map_err(|error| DicomWebError::invalid_uid(context, error))
}

pub fn series_instance_uid(
    value: impl Into<String>,
    context: impl AsRef<str>,
) -> Result<SeriesInstanceUid, DicomWebError> {
    SeriesInstanceUid::new(value).map_err(|error| DicomWebError::invalid_uid(context, error))
}

pub fn sop_instance_uid(
    value: impl Into<String>,
    context: impl AsRef<str>,
) -> Result<SopInstanceUid, DicomWebError> {
    SopInstanceUid::new(value).map_err(|error| DicomWebError::invalid_uid(context, error))
}

pub fn sop_class_uid(
    value: impl Into<String>,
    context: impl AsRef<str>,
) -> Result<SopClassUid, DicomWebError> {
    SopClassUid::new(value).map_err(|error| DicomWebError::invalid_uid(context, error))
}

pub fn transfer_syntax_uid(
    value: impl Into<String>,
    context: impl AsRef<str>,
) -> Result<TransferSyntaxUid, DicomWebError> {
    TransferSyntaxUid::new(value).map_err(|error| DicomWebError::invalid_uid(context, error))
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FrameList(Vec<u32>);

impl FrameList {
    pub fn parse(value: &str) -> Result<Self, DicomWebError> {
        let frames = value
            .split(',')
            .map(str::trim)
            .map(|frame| {
                if frame.is_empty() || !frame.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(DicomWebError::bad_request("invalid frame list"));
                }
                let frame = frame
                    .parse::<u32>()
                    .map_err(|_| DicomWebError::bad_request("invalid frame list"))?;
                if frame == 0 {
                    return Err(DicomWebError::bad_request("frame numbers are one-based"));
                }
                Ok(frame)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if frames.is_empty() {
            return Err(DicomWebError::bad_request("frame list must not be empty"));
        }
        if frames.windows(2).any(|window| window[0] >= window[1]) {
            return Err(DicomWebError::bad_request(
                "frame list must be strictly ascending",
            ));
        }
        Ok(Self(frames))
    }

    pub fn frames(&self) -> &[u32] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;

    #[test]
    fn parses_path_and_query_values_into_contract_uid_types() {
        let study = study_instance_uid("1.2.3", "path StudyInstanceUID").expect("study UID");
        let series = series_instance_uid("1.2.3.4", "path SeriesInstanceUID").expect("series UID");
        let sop = sop_instance_uid("1.2.3.4.5", "path SOPInstanceUID").expect("SOP UID");
        let sop_class =
            sop_class_uid("1.2.840.10008.5.1.4.1.1.2", "query SOPClassUID").expect("SOP Class UID");
        let transfer_syntax = transfer_syntax_uid("1.2.840.10008.1.2.1", "accept transfer-syntax")
            .expect("Transfer Syntax UID");

        assert_eq!(study.as_str(), "1.2.3");
        assert_eq!(series.as_str(), "1.2.3.4");
        assert_eq!(sop.as_str(), "1.2.3.4.5");
        assert_eq!(sop_class.as_str(), "1.2.840.10008.5.1.4.1.1.2");
        assert_eq!(transfer_syntax.as_str(), "1.2.840.10008.1.2.1");
    }

    #[test]
    fn invalid_uid_maps_to_bad_request() {
        let error = study_instance_uid("1..2", "path StudyInstanceUID").expect_err("invalid UID");

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert!(error.to_string().contains("path StudyInstanceUID"));
    }

    #[test]
    fn parses_frame_list() {
        let frames = FrameList::parse("1, 3,5").expect("frame list");

        assert_eq!(frames.frames(), &[1, 3, 5]);
    }
}
