//! Reusable gRPC transport configuration.

use serde::Deserialize;

const DEFAULT_GRPC_SERVER_BIND_ADDRESS: &str = "127.0.0.1:50051";
const DEFAULT_GRPC_CLIENT_ENDPOINT: &str = "http://127.0.0.1:50051";

/// gRPC server listener settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct GrpcServerConfig {
    /// Socket address where the gRPC server accepts requests.
    pub bind_address: String,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            bind_address: DEFAULT_GRPC_SERVER_BIND_ADDRESS.to_string(),
        }
    }
}

/// gRPC client connection settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct GrpcClientConfig {
    /// Endpoint URI used to connect to the gRPC service.
    pub endpoint: String,

    /// Optional connection timeout in seconds.
    pub connect_timeout_seconds: Option<u64>,

    /// Optional request timeout in seconds.
    pub request_timeout_seconds: Option<u64>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_GRPC_CLIENT_ENDPOINT.to_string(),
            connect_timeout_seconds: None,
            request_timeout_seconds: None,
        }
    }
}
