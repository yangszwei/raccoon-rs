use crate::uid::{SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid};

/// Study-level DICOM identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DicomStudyIdentity {
    pub study_instance_uid: StudyInstanceUid,
}

impl DicomStudyIdentity {
    /// Creates a study identity.
    pub fn new(study_instance_uid: StudyInstanceUid) -> Self {
        Self { study_instance_uid }
    }
}

/// Series-level DICOM identity, anchored to its parent study.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DicomSeriesIdentity {
    pub study_instance_uid: StudyInstanceUid,
    pub series_instance_uid: SeriesInstanceUid,
}

impl DicomSeriesIdentity {
    /// Creates a series identity.
    pub fn new(
        study_instance_uid: StudyInstanceUid,
        series_instance_uid: SeriesInstanceUid,
    ) -> Self {
        Self {
            study_instance_uid,
            series_instance_uid,
        }
    }

    /// Returns the parent study identity.
    pub fn study_identity(&self) -> DicomStudyIdentity {
        DicomStudyIdentity::new(self.study_instance_uid.clone())
    }
}

/// Instance-level DICOM identity, anchored to its parent series and study.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DicomInstanceIdentity {
    pub study_instance_uid: StudyInstanceUid,
    pub series_instance_uid: SeriesInstanceUid,
    pub sop_instance_uid: SopInstanceUid,
    pub sop_class_uid: SopClassUid,
}

impl DicomInstanceIdentity {
    /// Creates an instance identity.
    pub fn new(
        study_instance_uid: StudyInstanceUid,
        series_instance_uid: SeriesInstanceUid,
        sop_instance_uid: SopInstanceUid,
        sop_class_uid: SopClassUid,
    ) -> Self {
        Self {
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
            sop_class_uid,
        }
    }

    /// Returns the parent study identity.
    pub fn study_identity(&self) -> DicomStudyIdentity {
        DicomStudyIdentity::new(self.study_instance_uid.clone())
    }

    /// Returns the parent series identity.
    pub fn series_identity(&self) -> DicomSeriesIdentity {
        DicomSeriesIdentity::new(
            self.study_instance_uid.clone(),
            self.series_instance_uid.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uid::{SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid};

    fn study_uid() -> StudyInstanceUid {
        StudyInstanceUid::new("1.2.3").expect("valid UID")
    }

    fn series_uid() -> SeriesInstanceUid {
        SeriesInstanceUid::new("1.2.3.1").expect("valid UID")
    }

    fn sop_instance_uid() -> SopInstanceUid {
        SopInstanceUid::new("1.2.3.1.1").expect("valid UID")
    }

    fn sop_class_uid() -> SopClassUid {
        SopClassUid::new("1.2.840.10008.5.1.4.1.1.4").expect("valid UID")
    }

    #[test]
    fn series_identity_returns_parent_study_identity() {
        let series = DicomSeriesIdentity::new(study_uid(), series_uid());

        assert_eq!(
            series.study_identity(),
            DicomStudyIdentity::new(study_uid())
        );
    }

    #[test]
    fn instance_identity_returns_parent_series_identity() {
        let instance = DicomInstanceIdentity::new(
            study_uid(),
            series_uid(),
            sop_instance_uid(),
            sop_class_uid(),
        );

        assert_eq!(
            instance.series_identity(),
            DicomSeriesIdentity::new(study_uid(), series_uid())
        );
    }

    #[test]
    fn instance_identity_returns_parent_study_identity() {
        let instance = DicomInstanceIdentity::new(
            study_uid(),
            series_uid(),
            sop_instance_uid(),
            sop_class_uid(),
        );

        assert_eq!(
            instance.study_identity(),
            DicomStudyIdentity::new(study_uid())
        );
    }
}
