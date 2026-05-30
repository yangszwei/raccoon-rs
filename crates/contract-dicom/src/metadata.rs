use crate::uid::TransferSyntaxUid;

/// Normalizes an optional string by trimming ASCII whitespace and mapping
/// blank strings to [`None`].
///
/// DICOM string values are frequently space-padded to an even byte length.
/// Normalizing on intake keeps metadata comparisons and display consistent.
fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Normalized patient-facing metadata.
///
/// Fields are trimmed and blank values collapsed to [`None`] on construction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DicomPatient {
    patient_id: Option<String>,
    patient_name: Option<String>,
}

impl DicomPatient {
    /// Creates normalized patient metadata.
    pub fn new(patient_id: Option<String>, patient_name: Option<String>) -> Self {
        Self {
            patient_id: normalize_optional(patient_id),
            patient_name: normalize_optional(patient_name),
        }
    }

    /// Returns the patient ID if present.
    pub fn patient_id(&self) -> Option<&str> {
        self.patient_id.as_deref()
    }

    /// Returns the patient name if present.
    pub fn patient_name(&self) -> Option<&str> {
        self.patient_name.as_deref()
    }
}

/// Normalized study-level metadata.
///
/// Fields are trimmed and blank values collapsed to [`None`] on construction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DicomStudyMetadata {
    accession_number: Option<String>,
    study_id: Option<String>,
}

impl DicomStudyMetadata {
    /// Creates normalized study metadata.
    pub fn new(accession_number: Option<String>, study_id: Option<String>) -> Self {
        Self {
            accession_number: normalize_optional(accession_number),
            study_id: normalize_optional(study_id),
        }
    }

    /// Returns the accession number if present.
    pub fn accession_number(&self) -> Option<&str> {
        self.accession_number.as_deref()
    }

    /// Returns the study ID if present.
    pub fn study_id(&self) -> Option<&str> {
        self.study_id.as_deref()
    }
}

/// Normalized series-level metadata.
///
/// Fields are trimmed and blank values collapsed to [`None`] on construction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DicomSeriesMetadata {
    modality: Option<String>,
    series_number: Option<u32>,
}

impl DicomSeriesMetadata {
    /// Creates normalized series metadata.
    pub fn new(modality: Option<String>, series_number: Option<u32>) -> Self {
        Self {
            modality: normalize_optional(modality),
            series_number,
        }
    }

    /// Returns the modality if present.
    pub fn modality(&self) -> Option<&str> {
        self.modality.as_deref()
    }

    /// Returns the series number if present.
    pub fn series_number(&self) -> Option<u32> {
        self.series_number
    }
}

/// Normalized instance-level metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DicomInstanceMetadata {
    instance_number: Option<u32>,
    transfer_syntax_uid: Option<TransferSyntaxUid>,
}

impl DicomInstanceMetadata {
    /// Creates instance metadata.
    pub fn new(
        instance_number: Option<u32>,
        transfer_syntax_uid: Option<TransferSyntaxUid>,
    ) -> Self {
        Self {
            instance_number,
            transfer_syntax_uid,
        }
    }

    /// Returns the instance number if present.
    pub fn instance_number(&self) -> Option<u32> {
        self.instance_number
    }

    /// Returns the transfer syntax UID if present.
    pub fn transfer_syntax_uid(&self) -> Option<&TransferSyntaxUid> {
        self.transfer_syntax_uid.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uid::TransferSyntaxUid;

    #[test]
    fn patient_trims_whitespace_from_fields() {
        let patient = DicomPatient::new(
            Some("  PAT-001  ".to_string()),
            Some("  Jane Doe  ".to_string()),
        );

        assert_eq!(patient.patient_id(), Some("PAT-001"));
        assert_eq!(patient.patient_name(), Some("Jane Doe"));
    }

    #[test]
    fn patient_normalizes_blank_fields_to_none() {
        let patient = DicomPatient::new(Some("   ".to_string()), None);

        assert_eq!(patient.patient_id(), None);
        assert_eq!(patient.patient_name(), None);
    }

    #[test]
    fn study_metadata_trims_whitespace_from_fields() {
        let study = DicomStudyMetadata::new(
            Some("  ACC-123  ".to_string()),
            Some("  STUDY-1  ".to_string()),
        );

        assert_eq!(study.accession_number(), Some("ACC-123"));
        assert_eq!(study.study_id(), Some("STUDY-1"));
    }

    #[test]
    fn study_metadata_normalizes_blank_fields_to_none() {
        let study = DicomStudyMetadata::new(Some(String::new()), None);

        assert_eq!(study.accession_number(), None);
    }

    #[test]
    fn series_metadata_trims_whitespace_from_modality() {
        let series = DicomSeriesMetadata::new(Some("  CT  ".to_string()), Some(3));

        assert_eq!(series.modality(), Some("CT"));
        assert_eq!(series.series_number(), Some(3));
    }

    #[test]
    fn instance_metadata_exposes_transfer_syntax_uid() {
        let ts_uid = TransferSyntaxUid::new("1.2.840.10008.1.2.1").expect("valid UID");
        let metadata = DicomInstanceMetadata::new(Some(5), Some(ts_uid.clone()));

        assert_eq!(metadata.instance_number(), Some(5));
        assert_eq!(metadata.transfer_syntax_uid(), Some(&ts_uid));
    }

    #[test]
    fn instance_metadata_allows_absent_fields() {
        let metadata = DicomInstanceMetadata::new(None, None);

        assert_eq!(metadata.instance_number(), None);
        assert_eq!(metadata.transfer_syntax_uid(), None);
    }
}
