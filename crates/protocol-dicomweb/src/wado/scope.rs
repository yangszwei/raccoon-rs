use raccoon_contract_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use raccoon_service_retrieve::RetrieveScope;

use crate::{DicomWebError, series_instance_uid, sop_instance_uid, study_instance_uid};

pub(crate) fn study_scope(study: String) -> Result<RetrieveScope, DicomWebError> {
    Ok(RetrieveScope::Study {
        study_instance_uid: parse_study(study)?,
    })
}

pub(crate) fn series_scope(study: String, series: String) -> Result<RetrieveScope, DicomWebError> {
    Ok(RetrieveScope::Series {
        study_instance_uid: Some(parse_study(study)?),
        series_instance_uid: parse_series(series)?,
    })
}

pub(crate) fn instance_scope(
    study: String,
    series: String,
    instance: String,
) -> Result<RetrieveScope, DicomWebError> {
    Ok(RetrieveScope::Instance {
        study_instance_uid: Some(parse_study(study)?),
        series_instance_uid: Some(parse_series(series)?),
        sop_instance_uid: parse_instance(instance)?,
    })
}

fn parse_study(value: String) -> Result<StudyInstanceUid, DicomWebError> {
    study_instance_uid(value, "path StudyInstanceUID")
}

fn parse_series(value: String) -> Result<SeriesInstanceUid, DicomWebError> {
    series_instance_uid(value, "path SeriesInstanceUID")
}

fn parse_instance(value: String) -> Result<SopInstanceUid, DicomWebError> {
    sop_instance_uid(value, "path SOPInstanceUID")
}
