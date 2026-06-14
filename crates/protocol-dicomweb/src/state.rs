use std::fmt;
use std::sync::Arc;

use raccoon_service_ingest::IngestService;
use raccoon_service_query::QueryService;
use raccoon_service_retrieve::{MetadataRepository, RetrieveService};

use crate::DicomWebFeatureSet;
use crate::stow::StowRsProviderOptions;
use crate::wado::{RenderService, TransferSyntaxPolicy};

/// Shared Axum state for mounted DICOMweb endpoints.
#[derive(Clone, Default)]
pub struct DicomWebState {
    pub features: DicomWebFeatureSet,
    pub ingest: Option<Arc<dyn IngestService>>,
    pub query: Option<Arc<dyn QueryService>>,
    pub retrieve: Option<Arc<dyn RetrieveService>>,
    pub metadata: Option<Arc<dyn MetadataRepository>>,
    pub wado_rs_transfer_syntax_policy: Option<TransferSyntaxPolicy>,
    pub wado_uri_transfer_syntax_policy: Option<TransferSyntaxPolicy>,
    pub render: Option<Arc<dyn RenderService>>,
    pub render_default_media_type: Option<String>,
    pub stow: Option<StowRsProviderOptions>,
}

impl fmt::Debug for DicomWebState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DicomWebState")
            .field("features", &self.features)
            .field("ingest", &self.ingest.as_ref().map(|_| "IngestService"))
            .field("query", &self.query.as_ref().map(|_| "QueryService"))
            .field(
                "retrieve",
                &self.retrieve.as_ref().map(|_| "RetrieveService"),
            )
            .field(
                "metadata",
                &self.metadata.as_ref().map(|_| "MetadataRepository"),
            )
            .field(
                "wado_rs_transfer_syntax_policy",
                &self.wado_rs_transfer_syntax_policy,
            )
            .field(
                "wado_uri_transfer_syntax_policy",
                &self.wado_uri_transfer_syntax_policy,
            )
            .field("render", &self.render.as_ref().map(|_| "RenderService"))
            .field("render_default_media_type", &self.render_default_media_type)
            .field("stow", &self.stow)
            .finish()
    }
}
