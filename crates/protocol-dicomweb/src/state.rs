use crate::DicomWebFeatureSet;

/// Shared Axum state for mounted DICOMweb endpoints.
#[derive(Debug, Clone, Default)]
pub struct DicomWebState {
    pub features: DicomWebFeatureSet,
}
