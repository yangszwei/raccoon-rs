use raccoon_contract_dicom::{PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

/// The scope of a DICOM retrieve operation.
///
/// Maps to the Query/Retrieve Level (0008,0052) values used by C-MOVE and
/// C-GET (PS3.4 Section C.4), and to the URL hierarchy used by WADO-RS
/// (PS3.18 Section 10.4). The unique key at each level follows PS3.4
/// Table C.4.1-1.
///
/// [`Patient`] applies only to the Patient Root Information Model; Study Root
/// and WADO-RS operate from [`Study`] and below.
///
/// This enum is intentionally **exhaustive**: the four variants correspond
/// exactly to the four Q/R levels defined by the DICOM standard (PS3.4
/// C.4.1). Adding a variant is a breaking change and requires a matching arm
/// in [`RetrieveScope::label`].
///
/// [`Patient`]: RetrieveScope::Patient
/// [`Study`]: RetrieveScope::Study
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrieveScope {
    /// All instances belonging to a patient (Patient Root C-MOVE/C-GET only).
    ///
    /// The [`PatientId`] wrapper trims whitespace and rejects blank values at
    /// construction time, consistent with the normalization applied by
    /// [`DicomPatient`](raccoon_contract_dicom::DicomPatient) at ingest.
    Patient { patient_id: PatientId },

    /// All instances belonging to a study.
    Study {
        study_instance_uid: StudyInstanceUid,
    },

    /// All instances belonging to a series.
    Series {
        /// Optional parent Study Instance UID constraint.
        ///
        /// DICOMweb WADO-RS supplies this through the URL hierarchy. DIMSE
        /// C-GET/C-MOVE may omit it when the request Identifier only carries
        /// the series unique key.
        study_instance_uid: Option<StudyInstanceUid>,
        series_instance_uid: SeriesInstanceUid,
    },

    /// A single SOP instance.
    Instance {
        /// Optional parent Study Instance UID constraint from hierarchical
        /// protocols such as WADO-RS.
        study_instance_uid: Option<StudyInstanceUid>,
        /// Optional parent Series Instance UID constraint from hierarchical
        /// protocols such as WADO-RS.
        series_instance_uid: Option<SeriesInstanceUid>,
        sop_instance_uid: SopInstanceUid,
    },
}

impl RetrieveScope {
    /// Returns a short ASCII label for this scope level.
    ///
    /// Suitable for tracing span fields and metrics dimensions. The match is
    /// exhaustive — adding a variant requires a new arm here, which the
    /// compiler enforces.
    ///
    /// The returned string values (`"patient"`, `"study"`, `"series"`,
    /// `"instance"`) are part of the crate's public API. Renaming a label
    /// is a breaking change.
    pub fn label(&self) -> &'static str {
        match self {
            RetrieveScope::Patient { .. } => "patient",
            RetrieveScope::Study { .. } => "study",
            RetrieveScope::Series { .. } => "series",
            RetrieveScope::Instance { .. } => "instance",
        }
    }
}

/// A protocol-neutral DICOM retrieve request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrieveRequest {
    pub scope: RetrieveScope,
}

impl RetrieveRequest {
    pub fn new(scope: RetrieveScope) -> Self {
        Self { scope }
    }
}

#[cfg(test)]
mod tests {
    use raccoon_contract_dicom::{PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

    use super::*;

    fn study_uid() -> StudyInstanceUid {
        StudyInstanceUid::new("1.2.3").expect("valid UID")
    }

    fn series_uid() -> SeriesInstanceUid {
        SeriesInstanceUid::new("1.2.3.4").expect("valid UID")
    }

    fn sop_uid() -> SopInstanceUid {
        SopInstanceUid::new("1.2.3.4.5").expect("valid UID")
    }

    fn patient_id() -> PatientId {
        PatientId::new("PAT-001").expect("valid patient ID")
    }

    #[test]
    fn patient_scope_stores_patient_id() {
        let scope = RetrieveScope::Patient {
            patient_id: patient_id(),
        };

        assert!(
            matches!(&scope, RetrieveScope::Patient { patient_id } if patient_id.as_str() == "PAT-001")
        );
    }

    #[test]
    fn study_scope_stores_uid() {
        let scope = RetrieveScope::Study {
            study_instance_uid: study_uid(),
        };

        assert!(matches!(scope, RetrieveScope::Study { .. }));
    }

    #[test]
    fn series_scope_stores_uid() {
        let scope = RetrieveScope::Series {
            study_instance_uid: None,
            series_instance_uid: series_uid(),
        };

        assert!(matches!(scope, RetrieveScope::Series { .. }));
    }

    #[test]
    fn instance_scope_stores_uid() {
        let scope = RetrieveScope::Instance {
            study_instance_uid: None,
            series_instance_uid: None,
            sop_instance_uid: sop_uid(),
        };

        assert!(matches!(scope, RetrieveScope::Instance { .. }));
    }

    #[test]
    fn retrieve_request_wraps_scope() {
        let scope = RetrieveScope::Study {
            study_instance_uid: study_uid(),
        };
        let request = RetrieveRequest::new(scope.clone());

        assert_eq!(request.scope, scope);
    }

    #[test]
    fn scope_label_returns_correct_string_for_each_variant() {
        assert_eq!(
            RetrieveScope::Patient {
                patient_id: PatientId::new("P1").unwrap()
            }
            .label(),
            "patient"
        );
        assert_eq!(
            RetrieveScope::Study {
                study_instance_uid: study_uid()
            }
            .label(),
            "study"
        );
        assert_eq!(
            RetrieveScope::Series {
                study_instance_uid: None,
                series_instance_uid: series_uid()
            }
            .label(),
            "series"
        );
        assert_eq!(
            RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid: sop_uid()
            }
            .label(),
            "instance"
        );
    }
}
