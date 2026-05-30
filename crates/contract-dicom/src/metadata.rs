use std::fmt;
use std::str::FromStr;

use thiserror::Error;

use crate::uid::TransferSyntaxUid;

/// Validation error for a [`PatientId`] construction attempt.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PatientIdError {
    /// The value was empty or contained only whitespace.
    #[error("patient ID must not be blank")]
    Blank,
}

/// A validated, normalized DICOM Patient ID (tag 0010,0020).
///
/// The raw value is trimmed of leading/trailing ASCII whitespace on
/// construction, matching the normalization that [`DicomPatient`] applies
/// to ingest-time data. Blank values (empty or all-whitespace) are rejected.
///
/// No other format constraints are applied because the LO value representation
/// (PS3.5 Section 6.2) has no structured syntax — patient IDs are free-form,
/// system-assigned strings.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PatientId(String);

impl PatientId {
    /// Creates a validated, normalized Patient ID.
    ///
    /// Trims leading and trailing ASCII whitespace. Returns
    /// [`PatientIdError::Blank`] if the result is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, PatientIdError> {
        let raw = value.into();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(PatientIdError::Blank);
        }
        if trimmed.len() == raw.len() {
            Ok(Self(raw))
        } else {
            Ok(Self(trimmed.to_owned()))
        }
    }

    /// Returns the Patient ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the Patient ID into its string representation.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for PatientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for PatientId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for PatientId {
    type Err = PatientIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

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
    fn patient_id_trims_whitespace() {
        let id = PatientId::new("  PAT-001  ").unwrap();
        assert_eq!(id.as_str(), "PAT-001");
        assert_eq!(id.to_string(), "PAT-001");
    }

    #[test]
    fn patient_id_rejects_blank() {
        for value in ["", "  ", "\t", " \t "] {
            assert!(
                PatientId::new(value).is_err(),
                "expected rejection for {value:?}"
            );
        }
    }

    #[test]
    fn patient_id_accepts_non_blank() {
        assert!(PatientId::new("PAT-001").is_ok());
        assert!(PatientId::new(" PAT-001 ").is_ok());
    }

    #[test]
    fn patient_id_as_ref_returns_trimmed_value() {
        let id = PatientId::new("  X  ").unwrap();
        let s: &str = id.as_ref();
        assert_eq!(s, "X");
    }

    #[test]
    fn patient_id_parse_round_trips() {
        let id: PatientId = "PAT-001".parse().unwrap();
        assert_eq!(id.as_str(), "PAT-001");
    }

    #[test]
    fn patient_id_parse_rejects_blank() {
        assert!("".parse::<PatientId>().is_err());
        assert!("  ".parse::<PatientId>().is_err());
    }

    #[test]
    fn patient_id_new_avoids_allocation_when_no_trimming_needed() {
        let id = PatientId::new("PAT-001").unwrap();
        assert_eq!(id.as_str(), "PAT-001");
        let id_trimmed = PatientId::new("  PAT-001  ").unwrap();
        assert_eq!(id_trimmed.as_str(), "PAT-001");
    }

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
