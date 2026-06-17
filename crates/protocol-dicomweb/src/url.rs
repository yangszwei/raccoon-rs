use std::fmt;

use axum::http::{HeaderMap, Uri, header};
use raccoon_contract_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RetrieveUrl(String);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BulkDataUri(String);

/// Request-derived URL context used to emit DICOMweb absolute URLs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DicomWebUrlBase {
    origin: String,
    base_path: String,
}

impl DicomWebUrlBase {
    pub fn from_request(headers: &HeaderMap, uri: &Uri) -> Option<Self> {
        let (scheme, authority) = normalized_origin(headers, uri)?;
        let base_path = dicomweb_base_path(uri.path());
        Some(Self {
            origin: format!("{scheme}://{authority}"),
            base_path,
        })
    }

    pub fn from_origin_and_base_path(
        origin: impl Into<String>,
        base_path: impl Into<String>,
    ) -> Self {
        Self {
            origin: origin.into(),
            base_path: trim_base_path(base_path.into()),
        }
    }

    pub fn study_retrieve_url(&self, study: &StudyInstanceUid) -> RetrieveUrl {
        RetrieveUrl(self.absolute_path(&format!("/studies/{study}")))
    }

    pub fn series_retrieve_url(
        &self,
        study: &StudyInstanceUid,
        series: &SeriesInstanceUid,
    ) -> RetrieveUrl {
        RetrieveUrl(self.absolute_path(&format!("/studies/{study}/series/{series}")))
    }

    pub fn instance_retrieve_url(
        &self,
        study: &StudyInstanceUid,
        series: &SeriesInstanceUid,
        instance: &SopInstanceUid,
    ) -> RetrieveUrl {
        RetrieveUrl(self.absolute_path(&format!(
            "/studies/{study}/series/{series}/instances/{instance}"
        )))
    }

    pub fn bulk_data_uri(&self, path: &BulkDataPath) -> BulkDataUri {
        BulkDataUri(self.absolute_path(path.as_path()))
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    fn absolute_path(&self, path: &str) -> String {
        format!("{}{}{}", self.origin, self.base_path, path)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BulkDataPath(String);

impl BulkDataPath {
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        Self(path)
    }

    pub fn as_path(&self) -> &str {
        &self.0
    }
}

impl RetrieveUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl BulkDataUri {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RetrieveUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for BulkDataUri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn normalized_origin(headers: &HeaderMap, uri: &Uri) -> Option<(String, String)> {
    if let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) {
        return Some((scheme.to_string(), authority.as_str().to_string()));
    }

    let authority = header_str(headers, "x-forwarded-host")
        .or_else(|| header_str(headers, header::HOST.as_str()))?
        .trim()
        .to_string();
    if authority.is_empty() {
        return None;
    }
    let scheme = header_str(headers, "x-forwarded-proto").unwrap_or("http");
    Some((scheme.to_string(), authority))
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn dicomweb_base_path(path: &str) -> String {
    [
        "/studies",
        "/series",
        "/instances",
        "/patients",
        "/metadata",
        "/frames",
        "/bulkdata",
        "/rendered",
        "/thumbnail",
    ]
    .iter()
    .filter_map(|marker| path.find(marker).map(|index| &path[..index]))
    .min_by_key(|prefix| prefix.len())
    .map(trim_base_path)
    .unwrap_or_default()
}

fn trim_base_path(path: impl Into<String>) -> String {
    let path = path.into();
    if path == "/" {
        String::new()
    } else {
        path.trim_end_matches('/').to_string()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, Uri, header};

    use super::*;

    fn study() -> StudyInstanceUid {
        StudyInstanceUid::new("1.2.3").unwrap()
    }

    fn series() -> SeriesInstanceUid {
        SeriesInstanceUid::new("1.2.3.4").unwrap()
    }

    fn instance() -> SopInstanceUid {
        SopInstanceUid::new("1.2.3.4.5").unwrap()
    }

    #[test]
    fn derives_urls_under_root_mount() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:8080"));
        let uri: Uri = "/studies?limit=1".parse().unwrap();

        let base = DicomWebUrlBase::from_request(&headers, &uri).expect("base URL");

        assert_eq!(base.origin(), "http://localhost:8080");
        assert_eq!(base.base_path(), "");
        assert_eq!(
            base.instance_retrieve_url(&study(), &series(), &instance())
                .as_str(),
            "http://localhost:8080/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5"
        );
    }

    #[test]
    fn preserves_nested_mount_path() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("pacs.example.test"));
        let uri: Uri = "/pacs/dicom-web/studies/1.2.3/series".parse().unwrap();

        let base = DicomWebUrlBase::from_request(&headers, &uri).expect("base URL");

        assert_eq!(base.base_path(), "/pacs/dicom-web");
        assert_eq!(
            base.study_retrieve_url(&study()).as_str(),
            "http://pacs.example.test/pacs/dicom-web/studies/1.2.3"
        );
    }

    #[test]
    fn uses_normalized_absolute_request_uri_for_forwarded_values() {
        let headers = HeaderMap::new();
        let uri: Uri = "https://public.example.test/dicom-web/studies"
            .parse()
            .unwrap();

        let base = DicomWebUrlBase::from_request(&headers, &uri).expect("base URL");

        assert_eq!(base.origin(), "https://public.example.test");
        assert_eq!(base.base_path(), "/dicom-web");
    }

    #[test]
    fn uses_forwarded_origin_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("internal:8080"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("public.example.test"),
        );
        let uri: Uri = "/dicom-web/studies".parse().unwrap();

        let base = DicomWebUrlBase::from_request(&headers, &uri).expect("base URL");

        assert_eq!(base.origin(), "https://public.example.test");
    }

    #[test]
    fn derives_bulk_data_uri_from_same_base() {
        let base = DicomWebUrlBase::from_origin_and_base_path(
            "https://pacs.example.test",
            "/pacs/dicom-web",
        );
        let path = BulkDataPath::new(
            "/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010",
        );

        assert_eq!(
            base.bulk_data_uri(&path).as_str(),
            "https://pacs.example.test/pacs/dicom-web/studies/1.2.3/series/1.2.3.4/instances/1.2.3.4.5/bulkdata/7FE00010"
        );
    }
}
