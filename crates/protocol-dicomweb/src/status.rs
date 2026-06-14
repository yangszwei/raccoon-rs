use axum::http::StatusCode;

/// HTTP status values commonly returned by DICOMweb providers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DicomWebStatus {
    Ok,
    NoContent,
    Accepted,
    BadRequest,
    NotFound,
    Conflict,
    NotAcceptable,
    PayloadTooLarge,
    UnsupportedMediaType,
    NotImplemented,
    InternalServerError,
}

impl DicomWebStatus {
    pub const fn status_code(self) -> StatusCode {
        match self {
            Self::Ok => StatusCode::OK,
            Self::NoContent => StatusCode::NO_CONTENT,
            Self::Accepted => StatusCode::ACCEPTED,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::NotAcceptable => StatusCode::NOT_ACCEPTABLE,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            Self::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
