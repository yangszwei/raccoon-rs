/// Query/Retrieve level for the DICOM Patient Root Information Model.
///
/// Represents valid values of the Query/Retrieve Level attribute (0008,0052)
/// when the negotiated SOP class is a Patient Root service
/// (e.g. `1.2.840.10008.5.1.4.1.2.1.1` for C-FIND).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PatientRootQueryRetrieveLevel {
    /// Patient-level scope.
    Patient,
    /// Study-level scope.
    Study,
    /// Series-level scope.
    Series,
    /// Instance-level scope.
    Image,
}

impl PatientRootQueryRetrieveLevel {
    /// Returns the DICOM attribute value string for this level.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Patient => "PATIENT",
            Self::Study => "STUDY",
            Self::Series => "SERIES",
            Self::Image => "IMAGE",
        }
    }
}

/// Query/Retrieve level for the DICOM Study Root Information Model.
///
/// Represents valid values of the Query/Retrieve Level attribute (0008,0052)
/// when the negotiated SOP class is a Study Root service
/// (e.g. `1.2.840.10008.5.1.4.1.2.2.1` for C-FIND).
///
/// When the Patient Root Information Model is supported, a separate
/// `PatientRootQueryRetrieveLevel` type will be introduced rather than adding a `Patient`
/// variant here. The information model is determined by the negotiated SOP
/// class, not by the level attribute, so the two models warrant distinct types.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StudyRootQueryRetrieveLevel {
    /// Study-level scope.
    Study,
    /// Series-level scope.
    Series,
    /// Instance-level scope.
    Image,
}

impl StudyRootQueryRetrieveLevel {
    /// Returns the DICOM attribute value string for this level.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Study => "STUDY",
            Self::Series => "SERIES",
            Self::Image => "IMAGE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patient_root_patient_level_returns_dicom_keyword() {
        assert_eq!(PatientRootQueryRetrieveLevel::Patient.as_str(), "PATIENT");
    }

    #[test]
    fn patient_root_study_level_returns_dicom_keyword() {
        assert_eq!(PatientRootQueryRetrieveLevel::Study.as_str(), "STUDY");
    }

    #[test]
    fn patient_root_series_level_returns_dicom_keyword() {
        assert_eq!(PatientRootQueryRetrieveLevel::Series.as_str(), "SERIES");
    }

    #[test]
    fn patient_root_image_level_returns_dicom_keyword() {
        assert_eq!(PatientRootQueryRetrieveLevel::Image.as_str(), "IMAGE");
    }

    #[test]
    fn study_root_study_level_returns_dicom_keyword() {
        assert_eq!(StudyRootQueryRetrieveLevel::Study.as_str(), "STUDY");
    }

    #[test]
    fn study_root_series_level_returns_dicom_keyword() {
        assert_eq!(StudyRootQueryRetrieveLevel::Series.as_str(), "SERIES");
    }

    #[test]
    fn study_root_image_level_returns_dicom_keyword() {
        assert_eq!(StudyRootQueryRetrieveLevel::Image.as_str(), "IMAGE");
    }
}
