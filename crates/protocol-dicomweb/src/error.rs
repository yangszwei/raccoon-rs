use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use raccoon_contract_dicom::DicomUidError;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DicomWebError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not acceptable: {0}")]
    NotAcceptable(String),
    #[error("payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("unsupported media type: {0}")]
    UnsupportedMediaType(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl DicomWebError {
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::NotAcceptable(_) => "not_acceptable",
            Self::PayloadTooLarge(_) => "payload_too_large",
            Self::UnsupportedMediaType(_) => "unsupported_media_type",
            Self::NotImplemented(_) => "not_implemented",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Internal(_) => "internal",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn http_error_class(&self) -> &'static str {
        match self.status_code().as_u16() {
            400 => "400",
            404 => "404",
            406 => "406",
            409 => "409",
            413 => "413",
            415 => "415",
            500 => "500",
            501 => "501",
            _ => "http_error",
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn invalid_uid(context: impl AsRef<str>, error: DicomUidError) -> Self {
        Self::BadRequest(format!("invalid {}: {error}", context.as_ref()))
    }

    pub fn not_acceptable(message: impl Into<String>) -> Self {
        Self::NotAcceptable(message.into())
    }

    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::PayloadTooLarge(message.into())
    }

    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::UnsupportedMediaType(message.into())
    }
}

impl IntoResponse for DicomWebError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrorBody {
            error: String,
            error_type: &'static str,
        }

        let status = self.status_code();
        let body = ErrorBody {
            error: self.to_string(),
            error_type: self.error_type(),
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use raccoon_contract_dicom::StudyInstanceUid;

    use super::*;

    #[test]
    fn maps_uid_errors_to_bad_request_with_context() {
        let error = StudyInstanceUid::new("1..2").expect_err("invalid UID");
        let response = DicomWebError::invalid_uid("path StudyInstanceUID", error).into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn maps_http_error_statuses() {
        assert_eq!(
            DicomWebError::NotFound("missing".to_string()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DicomWebError::NotAcceptable("media".to_string()).status_code(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            DicomWebError::Conflict("study mismatch".to_string()).status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            DicomWebError::payload_too_large("limit exceeded").status_code(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            DicomWebError::unsupported_media_type("payload").status_code(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            DicomWebError::Internal("database".to_string()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            DicomWebError::NotAcceptable("media".to_string()).http_error_class(),
            "406"
        );
    }
}
