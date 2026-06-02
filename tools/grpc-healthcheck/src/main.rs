use std::time::Duration;

use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .ok_or("usage: grpc-healthcheck <grpc-endpoint> [service-name]")?;
    let service = args.next().unwrap_or_default();

    let channel = Endpoint::from_shared(endpoint)?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(2))
        .connect()
        .await?;

    let response = HealthClient::new(channel)
        .check(HealthCheckRequest { service })
        .await?
        .into_inner();

    match ServingStatus::try_from(response.status) {
        Ok(ServingStatus::Serving) => Ok(()),
        Ok(status) => Err(format!("gRPC health status is {status:?}").into()),
        Err(_) => Err(format!("unknown gRPC health status: {}", response.status).into()),
    }
}
