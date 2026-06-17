use std::cmp::Ordering;
use std::fmt;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use raccoon_contract_dicom::TransferSyntaxUid;

use crate::DicomWebError;

pub const APPLICATION_DICOM: &str = "application/dicom";
pub const APPLICATION_DICOM_JSON: &str = "application/dicom+json";
pub const APPLICATION_DICOM_XML: &str = "application/dicom+xml";
pub const APPLICATION_OCTET_STREAM: &str = "application/octet-stream";
pub const MULTIPART_RELATED: &str = "multipart/related";
pub const MULTIPART_RELATED_DICOM_XML: &str = "multipart/related; type=\"application/dicom+xml\"";
pub const IMAGE_JPEG: &str = "image/jpeg";
pub const IMAGE_PNG: &str = "image/png";

/// DICOMweb media types shared by providers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MediaType {
    ApplicationDicom,
    ApplicationDicomJson,
    ApplicationDicomXml,
    MultipartRelated,
    ImageJpeg,
    ImagePng,
    OctetStream,
}

impl MediaType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApplicationDicom => APPLICATION_DICOM,
            Self::ApplicationDicomJson => APPLICATION_DICOM_JSON,
            Self::ApplicationDicomXml => APPLICATION_DICOM_XML,
            Self::MultipartRelated => MULTIPART_RELATED,
            Self::ImageJpeg => IMAGE_JPEG,
            Self::ImagePng => IMAGE_PNG,
            Self::OctetStream => APPLICATION_OCTET_STREAM,
        }
    }

    fn from_type_subtype(type_: &str, subtype: &str) -> Option<Self> {
        match (type_, subtype) {
            ("application", "dicom") => Some(Self::ApplicationDicom),
            ("application", "dicom+json") => Some(Self::ApplicationDicomJson),
            ("application", "dicom+xml") => Some(Self::ApplicationDicomXml),
            ("multipart", "related") => Some(Self::MultipartRelated),
            ("image", "jpeg") => Some(Self::ImageJpeg),
            ("image", "png") => Some(Self::ImagePng),
            ("application", "octet-stream") => Some(Self::OctetStream),
            _ => None,
        }
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct MediaTypeParams {
    pub type_: Option<String>,
    pub transfer_syntax: Option<String>,
    pub charset: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MediaRange {
    pub media_type: Option<MediaType>,
    wildcard_type: Option<String>,
    pub type_wildcard: bool,
    pub subtype_wildcard: bool,
    pub params: MediaTypeParams,
    pub q_millis: u16,
    order: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SelectedRepresentation {
    pub media_type: MediaType,
    pub params: MediaTypeParams,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AvailableRepresentation {
    pub media_type: MediaType,
    pub params: MediaTypeParams,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DicomJsonOrXmlMultipart {
    Json,
    XmlMultipart,
}

impl SelectedRepresentation {
    pub fn content_type(&self) -> String {
        content_type(self.media_type, &self.params)
    }

    pub fn transfer_syntax_uid(
        &self,
        context: impl AsRef<str>,
    ) -> Result<Option<TransferSyntaxUid>, DicomWebError> {
        self.params
            .transfer_syntax
            .as_deref()
            .map(|value| {
                TransferSyntaxUid::new(value.to_string())
                    .map_err(|error| DicomWebError::invalid_uid(context, error))
            })
            .transpose()
    }
}

impl DicomJsonOrXmlMultipart {
    pub fn selected_media_type(self) -> &'static str {
        match self {
            Self::Json => APPLICATION_DICOM_JSON,
            Self::XmlMultipart => MULTIPART_RELATED_DICOM_XML,
        }
    }
}

pub fn negotiate_response(
    headers: &HeaderMap,
    accept_query: Option<&str>,
    available: &[MediaType],
) -> Result<SelectedRepresentation, DicomWebError> {
    let available = available
        .iter()
        .copied()
        .map(AvailableRepresentation::from)
        .collect::<Vec<_>>();
    negotiate_representation(headers, accept_query, &available)
}

pub fn negotiate_representation(
    headers: &HeaderMap,
    accept_query: Option<&str>,
    available: &[AvailableRepresentation],
) -> Result<SelectedRepresentation, DicomWebError> {
    if available.is_empty() {
        return Err(DicomWebError::not_acceptable(
            "no DICOMweb response representations are available",
        ));
    }

    let source = accept_query
        .filter(|value| !value.trim().is_empty())
        .map(str::trim)
        .map(Ok)
        .or_else(|| {
            headers.get(header::ACCEPT).map(|value| {
                value
                    .to_str()
                    .map(str::trim)
                    .map_err(|_| DicomWebError::not_acceptable("invalid Accept header"))
            })
        })
        .transpose()?;

    let Some(source) = source else {
        return Ok(SelectedRepresentation {
            media_type: available[0].media_type,
            params: available[0].params.clone(),
        });
    };

    let ranges = parse_accept(source)?;
    ranges
        .iter()
        .filter(|range| range.q_millis > 0)
        .flat_map(|range| {
            available
                .iter()
                .filter(|candidate| range.matches_representation(candidate))
                .map(|candidate| range.select(candidate))
        })
        .next()
        .ok_or_else(|| {
            DicomWebError::not_acceptable("no acceptable DICOMweb response representation")
        })
}

pub fn negotiate_dicom_json_or_xml_multipart(
    headers: &HeaderMap,
) -> Result<DicomJsonOrXmlMultipart, DicomWebError> {
    let Some(value) = headers.get(header::ACCEPT) else {
        return Ok(DicomJsonOrXmlMultipart::Json);
    };
    let value = value
        .to_str()
        .map_err(|_| DicomWebError::not_acceptable("invalid Accept header"))?;
    let ranges = parse_accept(value)?;
    ranges
        .iter()
        .filter(|range| range.q_millis > 0)
        .flat_map(|range| {
            [
                DicomJsonOrXmlMultipart::Json,
                DicomJsonOrXmlMultipart::XmlMultipart,
            ]
            .into_iter()
            .filter(move |candidate| range.matches_dicom_json_or_xml_multipart(*candidate))
        })
        .next()
        .ok_or_else(|| {
            DicomWebError::not_acceptable("no acceptable DICOMweb response representation")
        })
}

pub fn parse_accept(value: &str) -> Result<Vec<MediaRange>, DicomWebError> {
    let mut ranges = value
        .split(',')
        .enumerate()
        .map(|(order, item)| parse_media_range(order, item))
        .collect::<Result<Vec<_>, _>>()?;

    ranges.sort_by(|left, right| {
        right
            .q_millis
            .cmp(&left.q_millis)
            .then_with(|| right.specificity().cmp(&left.specificity()))
            .then_with(|| left.order.cmp(&right.order))
    });
    Ok(ranges)
}

pub fn content_type(media_type: MediaType, params: &MediaTypeParams) -> String {
    let mut value = media_type.as_str().to_string();
    if let Some(type_) = &params.type_ {
        value.push_str("; type=\"");
        value.push_str(type_);
        value.push('"');
    }
    if let Some(transfer_syntax) = &params.transfer_syntax {
        value.push_str("; transfer-syntax=\"");
        value.push_str(transfer_syntax);
        value.push('"');
    }
    if let Some(charset) = &params.charset {
        value.push_str("; charset=");
        value.push_str(charset);
    }
    value
}

pub fn dicom_json_response(body: impl Into<Body>) -> Response {
    response_with_content_type(
        StatusCode::OK,
        content_type(MediaType::ApplicationDicomJson, &MediaTypeParams::default()),
        body,
    )
}

pub fn multipart_related_response(
    body: impl Into<Body>,
    boundary: &str,
    part_type: MediaType,
    transfer_syntax: Option<&str>,
) -> Response {
    let params = MediaTypeParams {
        type_: Some(part_type.as_str().to_string()),
        transfer_syntax: transfer_syntax.map(str::to_string),
        charset: None,
    };
    let mut content_type = content_type(MediaType::MultipartRelated, &params);
    content_type.push_str("; boundary=");
    content_type.push_str(boundary);
    response_with_content_type(StatusCode::OK, content_type, body)
}

pub fn dicomweb_status_report_response(body: impl Into<Body>) -> Response {
    response_with_content_type(
        StatusCode::OK,
        content_type(MediaType::ApplicationDicomJson, &MediaTypeParams::default()),
        body,
    )
}

fn response_with_content_type(
    status: StatusCode,
    content_type: String,
    body: impl Into<Body>,
) -> Response {
    let mut response = (status, body.into()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type).expect("valid DICOMweb Content-Type"),
    );
    response
}

impl MediaRange {
    fn matches_representation(&self, candidate: &AvailableRepresentation) -> bool {
        if !self.matches(candidate.media_type) {
            return false;
        }
        match (
            self.params.type_.as_deref(),
            candidate.params.type_.as_deref(),
        ) {
            (Some(requested), Some(available)) => requested == available,
            _ => true,
        }
    }

    fn select(&self, candidate: &AvailableRepresentation) -> SelectedRepresentation {
        let mut params = candidate.params.clone();
        if self.params.type_.is_some() {
            params.type_ = self.params.type_.clone();
        }
        if self.params.transfer_syntax.is_some() {
            params.transfer_syntax = self.params.transfer_syntax.clone();
        }
        if self.params.charset.is_some() {
            params.charset = self.params.charset.clone();
        }
        SelectedRepresentation {
            media_type: candidate.media_type,
            params,
        }
    }

    fn matches(&self, candidate: MediaType) -> bool {
        if self.type_wildcard {
            return true;
        }
        if self.subtype_wildcard {
            return self
                .wildcard_type
                .as_deref()
                .is_some_and(|type_| type_ == type_part(candidate));
        }
        self.media_type == Some(candidate)
    }

    fn matches_dicom_json_or_xml_multipart(&self, candidate: DicomJsonOrXmlMultipart) -> bool {
        match candidate {
            DicomJsonOrXmlMultipart::Json => self.matches(MediaType::ApplicationDicomJson),
            DicomJsonOrXmlMultipart::XmlMultipart => {
                self.matches(MediaType::MultipartRelated)
                    && self
                        .params
                        .type_
                        .as_deref()
                        .is_none_or(|type_| type_.eq_ignore_ascii_case(APPLICATION_DICOM_XML))
            }
        }
    }

    fn specificity(&self) -> u8 {
        match (self.type_wildcard, self.subtype_wildcard) {
            (true, _) => 0,
            (_, true) => 1,
            _ => 2,
        }
    }
}

fn parse_media_range(order: usize, item: &str) -> Result<MediaRange, DicomWebError> {
    let item = item.trim();
    if item.is_empty() {
        return Err(DicomWebError::not_acceptable("empty Accept item"));
    }

    let mut parts = item.split(';');
    let full_type = parts.next().expect("split yields first item").trim();
    let (type_, subtype) = full_type
        .split_once('/')
        .ok_or_else(|| DicomWebError::not_acceptable("invalid Accept media range"))?;
    let type_ = type_.trim().to_ascii_lowercase();
    let subtype = subtype.trim().to_ascii_lowercase();
    let type_wildcard = type_ == "*" && subtype == "*";
    let subtype_wildcard = subtype == "*" && !type_wildcard;
    let media_type = if type_wildcard || subtype_wildcard {
        MediaType::from_type_subtype(&type_, "dicom")
    } else {
        MediaType::from_type_subtype(&type_, &subtype)
    };

    let mut params = MediaTypeParams::default();
    let mut q_millis = 1000;
    for part in parts {
        let Some((name, value)) = part.trim().split_once('=') else {
            return Err(DicomWebError::not_acceptable("invalid Accept parameter"));
        };
        let name = name.trim().to_ascii_lowercase();
        let value = unquote(value.trim()).to_string();
        match name.as_str() {
            "q" => q_millis = parse_q_millis(&value)?,
            "type" => params.type_ = Some(value.to_ascii_lowercase()),
            "transfer-syntax" => params.transfer_syntax = Some(value),
            "charset" => params.charset = Some(value),
            _ => {}
        }
    }

    Ok(MediaRange {
        media_type,
        wildcard_type: subtype_wildcard.then_some(type_),
        type_wildcard,
        subtype_wildcard,
        params,
        q_millis,
        order,
    })
}

fn parse_q_millis(value: &str) -> Result<u16, DicomWebError> {
    let q = value
        .parse::<f32>()
        .map_err(|_| DicomWebError::not_acceptable("invalid Accept q-value"))?;
    if !(0.0..=1.0).contains(&q) {
        return Err(DicomWebError::not_acceptable("invalid Accept q-value"));
    }
    Ok((q * 1000.0).round() as u16)
}

fn type_part(media_type: MediaType) -> &'static str {
    media_type
        .as_str()
        .split_once('/')
        .map(|(type_, _)| type_)
        .expect("media type includes slash")
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

impl Ord for MediaRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.q_millis.cmp(&other.q_millis)
    }
}

impl PartialOrd for MediaRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<MediaType> for AvailableRepresentation {
    fn from(media_type: MediaType) -> Self {
        Self {
            media_type,
            params: MediaTypeParams::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::*;

    #[test]
    fn q_value_negotiation_prefers_highest_q() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/dicom+json;q=0.1, application/dicom+xml;q=0.9"),
        );

        let selected = negotiate_response(
            &headers,
            None,
            &[
                MediaType::ApplicationDicomJson,
                MediaType::ApplicationDicomXml,
            ],
        )
        .expect("selected media");

        assert_eq!(selected.media_type, MediaType::ApplicationDicomXml);
    }

    #[test]
    fn accept_query_parameter_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/dicom+xml"),
        );

        let selected = negotiate_response(
            &headers,
            Some("application/dicom+json"),
            &[
                MediaType::ApplicationDicomJson,
                MediaType::ApplicationDicomXml,
            ],
        )
        .expect("selected media");

        assert_eq!(selected.media_type, MediaType::ApplicationDicomJson);
    }

    #[test]
    fn parses_media_type_params() {
        let ranges = parse_accept(
            "multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.1\"; charset=utf-8",
        )
        .expect("valid Accept");

        assert_eq!(ranges[0].media_type, Some(MediaType::MultipartRelated));
        assert_eq!(ranges[0].params.type_.as_deref(), Some(APPLICATION_DICOM));
        assert_eq!(
            ranges[0].params.transfer_syntax.as_deref(),
            Some("1.2.840.10008.1.2.1")
        );
        assert_eq!(ranges[0].params.charset.as_deref(), Some("utf-8"));
    }

    #[test]
    fn wildcard_accepts_first_available_representation() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("image/*"));

        let selected =
            negotiate_response(&headers, None, &[MediaType::ImagePng, MediaType::ImageJpeg])
                .expect("selected media");

        assert_eq!(selected.media_type, MediaType::ImagePng);
    }

    #[test]
    fn wildcard_selects_available_representation_params() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("*/*"));

        let selected = negotiate_representation(
            &headers,
            None,
            &[AvailableRepresentation {
                media_type: MediaType::MultipartRelated,
                params: MediaTypeParams {
                    type_: Some(APPLICATION_OCTET_STREAM.to_string()),
                    transfer_syntax: None,
                    charset: None,
                },
            }],
        )
        .expect("selected media");

        assert_eq!(selected.media_type, MediaType::MultipartRelated);
        assert_eq!(
            selected.params.type_.as_deref(),
            Some(APPLICATION_OCTET_STREAM)
        );
    }

    #[test]
    fn incompatible_multipart_type_falls_through_to_wildcard() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(
                "multipart/related; type=\"application/dicom\";q=1.0, */*;q=0.5",
            ),
        );

        let selected = negotiate_representation(
            &headers,
            None,
            &[AvailableRepresentation {
                media_type: MediaType::MultipartRelated,
                params: MediaTypeParams {
                    type_: Some(APPLICATION_OCTET_STREAM.to_string()),
                    transfer_syntax: None,
                    charset: None,
                },
            }],
        )
        .expect("selected media");

        assert_eq!(selected.media_type, MediaType::MultipartRelated);
        assert_eq!(
            selected.params.type_.as_deref(),
            Some(APPLICATION_OCTET_STREAM)
        );
    }

    #[test]
    fn rejects_unacceptable_representation() {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, HeaderValue::from_static("image/jpeg"));

        let error = negotiate_response(&headers, None, &[MediaType::ApplicationDicomJson])
            .expect_err("not acceptable");

        assert_eq!(error.status_code(), StatusCode::NOT_ACCEPTABLE);
    }

    #[test]
    fn constructs_dicomweb_content_type() {
        let value = content_type(
            MediaType::MultipartRelated,
            &MediaTypeParams {
                type_: Some(APPLICATION_DICOM.to_string()),
                transfer_syntax: Some("1.2.840.10008.1.2.1".to_string()),
                charset: None,
            },
        );

        assert_eq!(
            value,
            "multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.1\""
        );
    }

    #[test]
    fn parses_transfer_syntax_param_into_contract_uid_type() {
        let selected = negotiate_response(
            &HeaderMap::new(),
            Some("multipart/related; type=\"application/dicom\"; transfer-syntax=\"1.2.840.10008.1.2.1\""),
            &[MediaType::MultipartRelated],
        )
        .expect("selected media");

        let transfer_syntax = selected
            .transfer_syntax_uid("accept transfer-syntax")
            .expect("valid transfer syntax")
            .expect("present transfer syntax");

        assert_eq!(transfer_syntax.as_str(), "1.2.840.10008.1.2.1");
    }
}
