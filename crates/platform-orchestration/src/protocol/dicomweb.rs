use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use raccoon_platform_config::component::dcmtk::DcmtkConfig;
use raccoon_platform_config::component::dicomweb::DicomWebConfig;
use raccoon_protocol_dicomweb::{
    DicomWebRouter, QidoRsProvider, RenderCacheConfig, RenderedWadoRsProvider, StowRsProvider,
    WadoRenderOptions, WadoRsProvider, WadoUriProvider,
};
use raccoon_service_ingest::IngestService;
use raccoon_service_query::QueryService;
use raccoon_service_retrieve::{MetadataRepository, RetrieveService};
use tracing::info;

#[cfg(windows)]
const DCMTK_DCM2IMG_COMMAND: &str = "dcm2img.exe";
#[cfg(not(windows))]
const DCMTK_DCM2IMG_COMMAND: &str = "dcm2img";

/// Build an Axum router with all concrete DICOMweb providers.
pub fn build_dicomweb_router(
    ingest_service: Arc<dyn IngestService>,
    query_service: Arc<dyn QueryService>,
    retrieve_service: Arc<dyn RetrieveService>,
    metadata_repository: Arc<dyn MetadataRepository>,
    dicomweb_config: &DicomWebConfig,
    dcmtk_config: &DcmtkConfig,
) -> Router {
    let render_options = wado_render_options(dicomweb_config, dcmtk_config);

    let router = DicomWebRouter::new()
        .register(QidoRsProvider::new(query_service))
        .register(
            WadoRsProvider::new(retrieve_service.clone())
                .with_metadata_repository(metadata_repository),
        )
        .register(RenderedWadoRsProvider::with_options(
            retrieve_service.clone(),
            render_options,
        ))
        .register(WadoUriProvider::new(retrieve_service))
        .register(StowRsProvider::new(ingest_service));

    let features = router.feature_set();
    info!(
        dicomweb.provider.qido = features.qido_rs.is_some(),
        dicomweb.provider.stow = features.stow_rs.is_some(),
        dicomweb.provider.wado = features.wado_rs.is_some(),
        dicomweb.provider.wado_uri = features.wado_uri.is_some(),
        dicomweb.transaction_count = features.transaction_count(),
        "DICOMweb providers registered"
    );

    router.into_router()
}

/// Build rendered WADO-RS options from platform configuration.
pub fn wado_render_options(
    dicomweb_config: &DicomWebConfig,
    dcmtk_config: &DcmtkConfig,
) -> WadoRenderOptions {
    WadoRenderOptions {
        dcmtk_path: dcmtk_dcm2img_path(dcmtk_config),
        cache: dicomweb_config
            .render_cache
            .enabled
            .then(|| RenderCacheConfig {
                directory: dicomweb_config.render_cache.directory.clone(),
                ttl: dicomweb_config.render_cache.ttl(),
                max_bytes: dicomweb_config.render_cache.max_bytes,
            }),
        default_media_type: dicomweb_config
            .render_cache
            .default_rendered_media_type
            .clone(),
    }
}

fn dcmtk_dcm2img_path(config: &DcmtkConfig) -> Option<PathBuf> {
    config
        .path
        .as_ref()
        .map(|path| path.join(DCMTK_DCM2IMG_COMMAND))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use raccoon_platform_config::component::dcmtk::DcmtkConfig;
    use raccoon_platform_config::component::dicomweb::DicomWebConfig;

    use super::{DCMTK_DCM2IMG_COMMAND, wado_render_options};

    #[test]
    fn render_options_use_configured_values() {
        let mut dicomweb = DicomWebConfig::default();
        dicomweb.render_cache.directory = PathBuf::from("/var/lib/raccoon/rendered");
        dicomweb.render_cache.ttl_seconds = Some(60);
        dicomweb.render_cache.max_bytes = Some(2048);
        dicomweb.render_cache.default_rendered_media_type = "image/png".to_string();
        let dcmtk = DcmtkConfig::default();

        let options = wado_render_options(&dicomweb, &dcmtk);
        let cache = options.cache.expect("render cache is configured");

        assert_eq!(options.dcmtk_path, None);
        assert_eq!(cache.directory, PathBuf::from("/var/lib/raccoon/rendered"));
        assert_eq!(cache.ttl, Some(Duration::from_secs(60)));
        assert_eq!(cache.max_bytes, Some(2048));
        assert_eq!(options.default_media_type, "image/png");
    }

    #[test]
    fn render_options_can_disable_cache_and_resolve_dcmtk_bin() {
        let mut dicomweb = DicomWebConfig::default();
        dicomweb.render_cache.enabled = false;
        let dcmtk = DcmtkConfig {
            path: Some(PathBuf::from("/opt/dcmtk/bin")),
        };

        let options = wado_render_options(&dicomweb, &dcmtk);

        assert!(options.cache.is_none());
        assert_eq!(
            options.dcmtk_path,
            Some(PathBuf::from("/opt/dcmtk/bin").join(DCMTK_DCM2IMG_COMMAND))
        );
    }
}
