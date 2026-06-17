use serde::Serialize;

use crate::media::{
    APPLICATION_DICOM, APPLICATION_DICOM_JSON, APPLICATION_DICOM_XML, APPLICATION_OCTET_STREAM,
    IMAGE_JPEG, IMAGE_PNG, MULTIPART_RELATED_DICOM_XML,
};

pub const TRANSFER_SYNTAX_ANY: &str = "*";

/// DICOMweb transaction and representation features registered by providers.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct DicomWebFeatureSet {
    pub qido_rs: Option<QidoRsCapabilities>,
    pub stow_rs: Option<StowRsCapabilities>,
    pub wado_rs: Option<WadoRsCapabilities>,
    pub wado_uri: Option<WadoUriCapabilities>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QidoRsCapabilities {
    pub resources: Vec<QidoRsResource>,
    pub response_media_types: Vec<&'static str>,
    pub query_parameters: Vec<&'static str>,
    pub max_results: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum QidoRsResource {
    Studies,
    Series,
    Instances,
    StudySeries,
    StudyInstances,
    SeriesInstances,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StowRsCapabilities {
    pub resources: Vec<StowRsResource>,
    pub request_media_types: Vec<&'static str>,
    pub response_media_types: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_upload_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StowRsResource {
    Studies,
    Study,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WadoRsCapabilities {
    pub resources: Vec<WadoRsResource>,
    pub response_media_types: Vec<&'static str>,
    pub transfer_syntaxes: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WadoRsMetadataCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rendered: Option<RenderedCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail: Option<RenderedCapabilities>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WadoRsResource {
    Study,
    Series,
    Instance,
    BulkData,
    PixelData,
    Frames,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WadoRsMetadataCapabilities {
    pub response_media_types: Vec<&'static str>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WadoUriCapabilities {
    pub response_media_types: Vec<&'static str>,
    pub query_parameters: Vec<&'static str>,
    pub transfer_syntaxes: Vec<&'static str>,
}

/// Rendered resource capabilities advertised by this DICOMweb service.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedCapabilities {
    pub media_types: Vec<&'static str>,
    pub parameters: Vec<&'static str>,
}

/// WADL JSON form used by the standard Retrieve Capabilities transaction.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct CapabilitiesDescription {
    pub application: WadLApplication,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct WadLApplication {
    pub resources: WadLResources,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct WadLResources {
    #[serde(rename = "@base")]
    pub base: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resource: Vec<WadLResource>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct WadLResource {
    #[serde(rename = "@path")]
    pub path: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub method: Vec<WadLMethod>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resource: Vec<WadLResource>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct WadLMethod {
    #[serde(rename = "@name")]
    pub name: &'static str,
    #[serde(rename = "@id")]
    pub id: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<WadLRequest>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub response: Vec<WadLResponse>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub struct WadLRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub param: Vec<WadLParam>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub representation: Vec<WadLRepresentation>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct WadLResponse {
    #[serde(rename = "@status")]
    pub status: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub representation: Vec<WadLRepresentation>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct WadLRepresentation {
    #[serde(rename = "@mediaType")]
    pub media_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub param: Vec<WadLParam>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct WadLParam {
    #[serde(rename = "@name")]
    pub name: &'static str,
    #[serde(rename = "@style")]
    pub style: &'static str,
    #[serde(skip_serializing_if = "Option::is_none", rename = "@default")]
    pub default_value: Option<&'static str>,
}

impl DicomWebFeatureSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn enable_qido_rs(&mut self) {
        self.qido_rs = Some(QidoRsCapabilities {
            resources: vec![
                QidoRsResource::Studies,
                QidoRsResource::Series,
                QidoRsResource::Instances,
                QidoRsResource::StudySeries,
                QidoRsResource::StudyInstances,
                QidoRsResource::SeriesInstances,
            ],
            response_media_types: vec![APPLICATION_DICOM_JSON, MULTIPART_RELATED_DICOM_XML],
            query_parameters: vec![
                "limit",
                "offset",
                "fuzzymatching",
                "timezoneoffset",
                "includefield",
            ],
            max_results: 100,
        });
    }

    pub fn enable_stow_rs(&mut self) {
        self.stow_rs = Some(StowRsCapabilities {
            resources: vec![StowRsResource::Studies, StowRsResource::Study],
            request_media_types: vec![
                APPLICATION_DICOM,
                APPLICATION_DICOM_JSON,
                APPLICATION_DICOM_XML,
            ],
            response_media_types: vec![APPLICATION_DICOM_JSON],
            max_upload_size_bytes: None,
        });
    }

    pub fn enable_wado_rs(&mut self) {
        self.wado_rs.get_or_insert_with(|| WadoRsCapabilities {
            resources: vec![
                WadoRsResource::Study,
                WadoRsResource::Series,
                WadoRsResource::Instance,
                WadoRsResource::BulkData,
                WadoRsResource::PixelData,
                WadoRsResource::Frames,
            ],
            response_media_types: vec![APPLICATION_DICOM, APPLICATION_OCTET_STREAM],
            transfer_syntaxes: vec![TRANSFER_SYNTAX_ANY],
            metadata: None,
            rendered: None,
            thumbnail: None,
        });
    }

    pub fn enable_wado_rs_metadata(&mut self) {
        if let Some(wado_rs) = &mut self.wado_rs {
            wado_rs.metadata = Some(WadoRsMetadataCapabilities {
                response_media_types: vec![APPLICATION_DICOM_JSON, MULTIPART_RELATED_DICOM_XML],
            });
        }
    }

    pub fn enable_wado_uri(&mut self) {
        self.wado_uri = Some(WadoUriCapabilities {
            response_media_types: vec![APPLICATION_DICOM],
            query_parameters: vec![
                "requestType",
                "studyUID",
                "seriesUID",
                "objectUID",
                "contentType",
                "transferSyntax",
            ],
            transfer_syntaxes: vec![TRANSFER_SYNTAX_ANY],
        });
    }

    pub fn enable_xml(&mut self) {
        if let Some(qido_rs) = &mut self.qido_rs {
            qido_rs
                .response_media_types
                .push(MULTIPART_RELATED_DICOM_XML);
        }
    }

    pub fn enable_rendered(&mut self) {
        if let Some(wado_rs) = &mut self.wado_rs {
            wado_rs.rendered = Some(RenderedCapabilities {
                media_types: vec![IMAGE_JPEG, IMAGE_PNG],
                parameters: vec!["viewport", "window", "quality"],
            });
        }
    }

    pub fn enable_thumbnail(&mut self) {
        if let Some(wado_rs) = &mut self.wado_rs {
            wado_rs.thumbnail = Some(RenderedCapabilities {
                media_types: vec![IMAGE_JPEG, IMAGE_PNG],
                parameters: vec!["viewport", "quality"],
            });
        }
    }

    pub fn enable_transcoding(&mut self) {
        if let Some(wado_rs) = &mut self.wado_rs {
            wado_rs.transfer_syntaxes.push("1.2.840.10008.1.2.1");
        }
        if let Some(wado_uri) = &mut self.wado_uri {
            wado_uri.transfer_syntaxes.push("1.2.840.10008.1.2.1");
        }
    }

    pub fn set_max_query_results(&mut self, max_results: u32) {
        if let Some(qido_rs) = &mut self.qido_rs {
            qido_rs.max_results = max_results;
        }
    }

    pub fn set_max_stow_upload_size_bytes(&mut self, max_upload_size_bytes: u64) {
        if let Some(stow_rs) = &mut self.stow_rs {
            stow_rs.max_upload_size_bytes = Some(max_upload_size_bytes);
        }
    }

    pub fn transaction_count(&self) -> usize {
        usize::from(self.qido_rs.is_some())
            + usize::from(self.stow_rs.is_some())
            + usize::from(self.wado_rs.is_some())
            + usize::from(self.wado_uri.is_some())
    }

    pub fn resource_count(&self) -> usize {
        self.qido_rs
            .as_ref()
            .map_or(0, |capabilities| capabilities.resources.len())
            + self
                .stow_rs
                .as_ref()
                .map_or(0, |capabilities| capabilities.resources.len())
            + self
                .wado_rs
                .as_ref()
                .map_or(0, |capabilities| capabilities.resources.len())
            + usize::from(self.wado_uri.is_some())
    }

    pub fn media_type_count(&self) -> usize {
        self.qido_rs
            .as_ref()
            .map_or(0, |capabilities| capabilities.response_media_types.len())
            + self.stow_rs.as_ref().map_or(0, |capabilities| {
                capabilities.request_media_types.len() + capabilities.response_media_types.len()
            })
            + self.wado_rs.as_ref().map_or(0, |capabilities| {
                capabilities.response_media_types.len()
                    + capabilities
                        .metadata
                        .as_ref()
                        .map_or(0, |metadata| metadata.response_media_types.len())
                    + capabilities
                        .rendered
                        .as_ref()
                        .map_or(0, |rendered| rendered.media_types.len())
                    + capabilities
                        .thumbnail
                        .as_ref()
                        .map_or(0, |thumbnail| thumbnail.media_types.len())
            })
            + self
                .wado_uri
                .as_ref()
                .map_or(0, |capabilities| capabilities.response_media_types.len())
    }

    pub fn capabilities_description(&self, base: impl Into<String>) -> CapabilitiesDescription {
        CapabilitiesDescription {
            application: WadLApplication {
                resources: WadLResources {
                    base: base.into(),
                    resource: self.wadl_resources(),
                },
            },
        }
    }

    fn wadl_resources(&self) -> Vec<WadLResource> {
        let mut resources = Vec::new();
        if self.qido_rs.is_some() || self.stow_rs.is_some() || self.wado_rs.is_some() {
            resources.push(self.studies_resource());
        }
        if let Some(qido_rs) = &self.qido_rs {
            resources.extend(qido_top_level_resources(qido_rs));
        }
        if let Some(wado_uri) = &self.wado_uri {
            resources.push(wado_uri_resource(wado_uri));
        }
        resources
    }

    fn studies_resource(&self) -> WadLResource {
        let mut methods = Vec::new();
        if let Some(qido_rs) = &self.qido_rs {
            methods.push(qido_method("SearchForStudies", qido_rs));
        }
        if let Some(stow_rs) = &self.stow_rs {
            methods.push(stow_method("StoreInstances", stow_rs));
        }

        let mut child_resources = Vec::new();
        if self.qido_rs.is_some() || self.stow_rs.is_some() || self.wado_rs.is_some() {
            child_resources.push(study_instance_resource(self));
        }

        WadLResource {
            path: "studies",
            method: methods,
            resource: child_resources,
        }
    }
}

fn qido_top_level_resources(qido_rs: &QidoRsCapabilities) -> Vec<WadLResource> {
    vec![
        WadLResource {
            path: "series",
            method: vec![qido_method("SearchForSeries", qido_rs)],
            resource: Vec::new(),
        },
        WadLResource {
            path: "instances",
            method: vec![qido_method("SearchForInstances", qido_rs)],
            resource: Vec::new(),
        },
    ]
}

fn study_instance_resource(features: &DicomWebFeatureSet) -> WadLResource {
    let mut methods = Vec::new();
    if let Some(wado_rs) = &features.wado_rs {
        methods.push(wado_method("RetrieveStudy", wado_rs));
    }
    if let Some(stow_rs) = &features.stow_rs {
        methods.push(stow_method("StoreStudyInstances", stow_rs));
    }

    let mut child_resources = Vec::new();
    if let Some(qido_rs) = &features.qido_rs {
        child_resources.push(WadLResource {
            path: "series",
            method: vec![qido_method("SearchForStudySeries", qido_rs)],
            resource: vec![series_instance_resource(features)],
        });
        child_resources.push(WadLResource {
            path: "instances",
            method: vec![qido_method("SearchForStudyInstances", qido_rs)],
            resource: Vec::new(),
        });
    } else if features.wado_rs.is_some() {
        child_resources.push(WadLResource {
            path: "series",
            method: Vec::new(),
            resource: vec![series_instance_resource(features)],
        });
    }
    if let Some(metadata) = features
        .wado_rs
        .as_ref()
        .and_then(|wado_rs| wado_rs.metadata.as_ref())
    {
        child_resources.push(metadata_resource("RetrieveStudyMetadata", metadata));
    }
    if let Some(wado_rs) = &features.wado_rs {
        child_resources.push(octet_stream_resource(
            "pixeldata",
            "RetrieveStudyPixelData",
            wado_rs,
        ));
        if let Some(rendered) = &wado_rs.rendered {
            child_resources.push(rendered_resource(
                "rendered",
                "RetrieveStudyRendered",
                rendered,
            ));
        }
        if let Some(thumbnail) = &wado_rs.thumbnail {
            child_resources.push(rendered_resource(
                "thumbnail",
                "RetrieveStudyThumbnail",
                thumbnail,
            ));
        }
    }

    WadLResource {
        path: "{StudyInstance}",
        method: methods,
        resource: child_resources,
    }
}

fn series_instance_resource(features: &DicomWebFeatureSet) -> WadLResource {
    let mut child_resources = Vec::new();
    if let Some(qido_rs) = &features.qido_rs {
        child_resources.push(WadLResource {
            path: "instances",
            method: vec![qido_method("SearchForStudySeriesInstances", qido_rs)],
            resource: instance_resources(features),
        });
    } else if features.wado_rs.is_some() {
        child_resources.push(WadLResource {
            path: "instances",
            method: Vec::new(),
            resource: instance_resources(features),
        });
    }
    if let Some(metadata) = features
        .wado_rs
        .as_ref()
        .and_then(|wado_rs| wado_rs.metadata.as_ref())
    {
        child_resources.push(metadata_resource("RetrieveSeriesMetadata", metadata));
    }
    if let Some(wado_rs) = &features.wado_rs {
        child_resources.push(octet_stream_resource(
            "pixeldata",
            "RetrieveSeriesPixelData",
            wado_rs,
        ));
        if let Some(rendered) = &wado_rs.rendered {
            child_resources.push(rendered_resource(
                "rendered",
                "RetrieveSeriesRendered",
                rendered,
            ));
        }
        if let Some(thumbnail) = &wado_rs.thumbnail {
            child_resources.push(rendered_resource(
                "thumbnail",
                "RetrieveSeriesThumbnail",
                thumbnail,
            ));
        }
    }

    WadLResource {
        path: "{SeriesInstance}",
        method: features
            .wado_rs
            .as_ref()
            .map(|wado_rs| vec![wado_method("RetrieveSeries", wado_rs)])
            .unwrap_or_default(),
        resource: child_resources,
    }
}

fn instance_resources(features: &DicomWebFeatureSet) -> Vec<WadLResource> {
    let Some(wado_rs) = &features.wado_rs else {
        return Vec::new();
    };
    let mut child_resources = Vec::new();
    if let Some(metadata) = &wado_rs.metadata {
        child_resources.push(metadata_resource("RetrieveInstanceMetadata", metadata));
    }
    child_resources.push(octet_stream_resource(
        "bulkdata/{BulkDataPath}",
        "RetrieveBulkData",
        wado_rs,
    ));
    child_resources.push(octet_stream_resource(
        "pixeldata",
        "RetrieveInstancePixelData",
        wado_rs,
    ));
    if let Some(rendered) = &wado_rs.rendered {
        child_resources.push(rendered_resource(
            "rendered",
            "RetrieveInstanceRendered",
            rendered,
        ));
    }
    if let Some(thumbnail) = &wado_rs.thumbnail {
        child_resources.push(rendered_resource(
            "thumbnail",
            "RetrieveInstanceThumbnail",
            thumbnail,
        ));
    }
    child_resources.push(frames_resource(wado_rs));
    vec![WadLResource {
        path: "{SOPInstance}",
        method: vec![wado_method("RetrieveInstance", wado_rs)],
        resource: child_resources,
    }]
}

fn frames_resource(wado_rs: &WadoRsCapabilities) -> WadLResource {
    let mut resources = Vec::new();
    if let Some(rendered) = &wado_rs.rendered {
        resources.push(rendered_resource(
            "rendered",
            "RetrieveRenderedFrames",
            rendered,
        ));
    }
    if let Some(thumbnail) = &wado_rs.thumbnail {
        resources.push(rendered_resource(
            "thumbnail",
            "RetrieveFrameThumbnail",
            thumbnail,
        ));
    }
    WadLResource {
        path: "frames/{FrameList}",
        method: vec![wado_octet_stream_method("RetrieveFrames", wado_rs)],
        resource: resources,
    }
}

fn octet_stream_resource(
    path: &'static str,
    id: &'static str,
    wado_rs: &WadoRsCapabilities,
) -> WadLResource {
    WadLResource {
        path,
        method: vec![wado_octet_stream_method(id, wado_rs)],
        resource: Vec::new(),
    }
}

fn metadata_resource(id: &'static str, metadata: &WadoRsMetadataCapabilities) -> WadLResource {
    WadLResource {
        path: "metadata",
        method: vec![wado_metadata_method(id, metadata)],
        resource: Vec::new(),
    }
}

fn rendered_resource(
    path: &'static str,
    id: &'static str,
    rendered: &RenderedCapabilities,
) -> WadLResource {
    WadLResource {
        path,
        method: vec![wado_rendered_method(id, rendered)],
        resource: Vec::new(),
    }
}

fn qido_method(id: &'static str, qido_rs: &QidoRsCapabilities) -> WadLMethod {
    WadLMethod {
        name: "GET",
        id,
        request: Some(WadLRequest {
            param: qido_rs
                .query_parameters
                .iter()
                .copied()
                .map(|name| WadLParam {
                    name,
                    style: "query",
                    default_value: None,
                })
                .collect(),
            representation: Vec::new(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: qido_rs
                .response_media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }],
    }
}

fn stow_method(id: &'static str, stow_rs: &StowRsCapabilities) -> WadLMethod {
    WadLMethod {
        name: "POST",
        id,
        request: Some(WadLRequest {
            param: stow_rs
                .max_upload_size_bytes
                .map(|_| WadLParam {
                    name: "maxUploadSizeBytes",
                    style: "plain",
                    default_value: None,
                })
                .into_iter()
                .collect(),
            representation: stow_rs
                .request_media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: stow_rs
                .response_media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }],
    }
}

fn wado_method(id: &'static str, wado_rs: &WadoRsCapabilities) -> WadLMethod {
    WadLMethod {
        name: "GET",
        id,
        request: Some(WadLRequest {
            param: wado_rs
                .transfer_syntaxes
                .iter()
                .copied()
                .map(|transfer_syntax| WadLParam {
                    name: "transfer-syntax",
                    style: "plain",
                    default_value: Some(transfer_syntax),
                })
                .collect(),
            representation: Vec::new(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: wado_rs
                .response_media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }],
    }
}

fn wado_octet_stream_method(id: &'static str, wado_rs: &WadoRsCapabilities) -> WadLMethod {
    WadLMethod {
        name: "GET",
        id,
        request: Some(WadLRequest {
            param: wado_rs
                .transfer_syntaxes
                .iter()
                .copied()
                .map(|transfer_syntax| WadLParam {
                    name: "transfer-syntax",
                    style: "plain",
                    default_value: Some(transfer_syntax),
                })
                .collect(),
            representation: Vec::new(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: vec![representation(APPLICATION_OCTET_STREAM)],
        }],
    }
}

fn wado_metadata_method(id: &'static str, metadata: &WadoRsMetadataCapabilities) -> WadLMethod {
    WadLMethod {
        name: "GET",
        id,
        request: Some(WadLRequest {
            param: Vec::new(),
            representation: Vec::new(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: metadata
                .response_media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }],
    }
}

fn wado_rendered_method(id: &'static str, rendered: &RenderedCapabilities) -> WadLMethod {
    WadLMethod {
        name: "GET",
        id,
        request: Some(WadLRequest {
            param: rendered
                .parameters
                .iter()
                .copied()
                .map(|name| WadLParam {
                    name,
                    style: "query",
                    default_value: None,
                })
                .collect(),
            representation: Vec::new(),
        }),
        response: vec![WadLResponse {
            status: "200",
            representation: rendered
                .media_types
                .iter()
                .copied()
                .map(representation)
                .collect(),
        }],
    }
}

fn wado_uri_resource(wado_uri: &WadoUriCapabilities) -> WadLResource {
    WadLResource {
        path: "wado",
        method: vec![WadLMethod {
            name: "GET",
            id: "RetrieveDicomInstance",
            request: Some(WadLRequest {
                param: wado_uri
                    .query_parameters
                    .iter()
                    .copied()
                    .map(|name| WadLParam {
                        name,
                        style: "query",
                        default_value: None,
                    })
                    .collect(),
                representation: Vec::new(),
            }),
            response: vec![WadLResponse {
                status: "200",
                representation: wado_uri
                    .response_media_types
                    .iter()
                    .copied()
                    .map(representation)
                    .collect(),
            }],
        }],
        resource: Vec::new(),
    }
}

fn representation(media_type: &'static str) -> WadLRepresentation {
    WadLRepresentation {
        media_type,
        param: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::DicomWebFeatureSet;

    #[test]
    fn enabling_wado_rs_does_not_clear_optional_wado_capabilities() {
        let mut features = DicomWebFeatureSet::empty();

        features.enable_wado_rs();
        features.enable_wado_rs_metadata();
        features.enable_rendered();
        features.enable_thumbnail();
        features.enable_wado_rs();

        let wado = features.wado_rs.expect("WADO-RS capabilities");
        assert!(wado.metadata.is_some());
        assert!(wado.rendered.is_some());
        assert!(wado.thumbnail.is_some());
    }
}
