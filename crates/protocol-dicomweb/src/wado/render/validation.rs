use super::{RenderError, RenderParams};
use crate::DicomWebError;

pub(crate) fn render_error(error: RenderError) -> DicomWebError {
    match error {
        RenderError::NotFound => DicomWebError::NotFound("no matching DICOM instances".to_string()),
        RenderError::NotAcceptable(message) => DicomWebError::not_acceptable(message),
        RenderError::Failed(message) => DicomWebError::Internal(message),
    }
}

pub(crate) fn validate_render_params(params: &RenderParams) -> Result<(), DicomWebError> {
    if params
        .annotation
        .as_deref()
        .is_some_and(|value| !value.eq_ignore_ascii_case("none"))
    {
        return Err(DicomWebError::not_acceptable(
            "annotation rendering is not supported",
        ));
    }
    if params.iccprofile.is_some() {
        return Err(DicomWebError::not_acceptable(
            "iccprofile rendering is not supported",
        ));
    }
    if params.presentation_state.is_some() {
        return Err(DicomWebError::not_acceptable(
            "presentation state rendering is not supported",
        ));
    }
    Ok(())
}

pub(crate) fn validate_thumbnail_params(params: &RenderParams) -> Result<(), DicomWebError> {
    if params.window.is_some()
        || params.annotation.is_some()
        || params.iccprofile.is_some()
        || params.presentation_state.is_some()
    {
        return Err(DicomWebError::bad_request(
            "thumbnail resources support only viewport and quality",
        ));
    }
    Ok(())
}
