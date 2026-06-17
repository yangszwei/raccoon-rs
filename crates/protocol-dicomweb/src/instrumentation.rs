use tracing::Span;

use crate::DicomWebError;

/// Standard DICOMweb span fields owned by this protocol crate.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct InstrumentationFields {
    pub http_request_method: Option<&'static str>,
    pub http_route: Option<&'static str>,
    pub url_path: Option<String>,
    pub dicomweb_service: Option<&'static str>,
    pub dicomweb_resource: Option<&'static str>,
    pub dicom_uids: DicomUidFields,
    pub selected_media_type: Option<String>,
    pub selected_transfer_syntax: Option<String>,
    pub error_type: Option<&'static str>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct DicomUidFields {
    pub study_instance_uid: Option<String>,
    pub series_instance_uid: Option<String>,
    pub sop_instance_uid: Option<String>,
}

pub(crate) fn record_error(error: DicomWebError) -> DicomWebError {
    let status = error.status_code();
    let http_error_class = error.http_error_class();
    let error_type = error.error_type();
    let message = error.to_string();
    let span = Span::current();

    span.record("http.response.status_code", status.as_u16());
    span.record("error.type", http_error_class);
    span.record("dicomweb.error_type", error_type);
    span.record("error.message", message.as_str());

    if status.is_server_error() {
        tracing::error!("dicomweb request failed");
    } else {
        tracing::warn!("dicomweb request rejected");
    }

    error
}

impl InstrumentationFields {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn http_request(mut self, method: &'static str, route: &'static str, path: &str) -> Self {
        self.http_request_method = Some(method);
        self.http_route = Some(route);
        self.url_path = Some(path.to_string());
        self
    }

    pub fn dicomweb(mut self, service: &'static str, resource: &'static str) -> Self {
        self.dicomweb_service = Some(service);
        self.dicomweb_resource = Some(resource);
        self
    }

    pub fn study_instance_uid(mut self, uid: impl Into<String>) -> Self {
        self.dicom_uids.study_instance_uid = Some(uid.into());
        self
    }

    pub fn series_instance_uid(mut self, uid: impl Into<String>) -> Self {
        self.dicom_uids.series_instance_uid = Some(uid.into());
        self
    }

    pub fn sop_instance_uid(mut self, uid: impl Into<String>) -> Self {
        self.dicom_uids.sop_instance_uid = Some(uid.into());
        self
    }

    pub fn selected_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.selected_media_type = Some(media_type.into());
        self
    }

    pub fn selected_transfer_syntax(mut self, transfer_syntax: impl Into<String>) -> Self {
        self.selected_transfer_syntax = Some(transfer_syntax.into());
        self
    }

    pub fn error_type(mut self, error_type: &'static str) -> Self {
        self.error_type = Some(error_type);
        self
    }

    pub fn record_on_current_span(&self) {
        self.record_on(&Span::current());
    }

    pub fn record_on(&self, span: &Span) {
        if let Some(value) = self.http_request_method {
            span.record("http.request.method", value);
        }
        if let Some(value) = self.http_route {
            span.record("http.route", value);
        }
        if let Some(value) = self.url_path.as_deref() {
            span.record("url.path", value);
        }
        if let Some(value) = self.dicomweb_service {
            span.record("dicomweb.service", value);
        }
        if let Some(value) = self.dicomweb_resource {
            span.record("dicomweb.resource", value);
        }
        if let Some(value) = self.dicom_uids.study_instance_uid.as_deref() {
            span.record("dicom.study_instance_uid", value);
        }
        if let Some(value) = self.dicom_uids.series_instance_uid.as_deref() {
            span.record("dicom.series_instance_uid", value);
        }
        if let Some(value) = self.dicom_uids.sop_instance_uid.as_deref() {
            span.record("dicom.sop_instance_uid", value);
        }
        if let Some(value) = self.selected_media_type.as_deref() {
            span.record("dicomweb.selected_media_type", value);
        }
        if let Some(value) = self.selected_transfer_syntax.as_deref() {
            span.record("dicom.transfer_syntax_uid", value);
        }
        if let Some(value) = self.error_type {
            span.record("error.type", value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InstrumentationFields;

    #[test]
    fn builds_and_records_standard_route_fields() {
        let fields = InstrumentationFields::new()
            .http_request("GET", "/studies/{study_uid}", "/studies/1.2.3")
            .dicomweb("WADO-RS", "studies")
            .study_instance_uid("1.2.3")
            .selected_media_type("application/dicom+json")
            .selected_transfer_syntax("1.2.840.10008.1.2.1")
            .error_type("406");

        assert_eq!(fields.http_request_method, Some("GET"));
        assert_eq!(fields.http_route, Some("/studies/{study_uid}"));
        assert_eq!(fields.url_path.as_deref(), Some("/studies/1.2.3"));
        assert_eq!(fields.dicomweb_service, Some("WADO-RS"));
        assert_eq!(fields.dicomweb_resource, Some("studies"));
        assert_eq!(
            fields.selected_media_type.as_deref(),
            Some("application/dicom+json")
        );
        assert_eq!(
            fields.selected_transfer_syntax.as_deref(),
            Some("1.2.840.10008.1.2.1")
        );
        assert_eq!(fields.error_type, Some("406"));

        fields.record_on_current_span();
    }
}
