#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = raccoon_platform_orchestration::platform::config::load_retrieve_config()?;

    let _telemetry = raccoon_platform_orchestration::platform::telemetry::init_telemetry(
        &config.app,
        &config.telemetry,
    )?;

    let app = raccoon_platform_orchestration::app::build_retrieve_app(&config).await?;

    raccoon_platform_orchestration::platform::runtime::run_runtime(
        app,
        &config.runtime,
        raccoon_platform_orchestration::platform::runtime::install_ctrl_c_handler,
    )
    .await?;

    Ok(())
}
