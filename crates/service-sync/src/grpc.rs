use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tokio_util::sync::CancellationToken;
use tonic::codegen::{Body, Bytes, StdError};
use tonic::metadata::{AsciiMetadataKey, KeyRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::{Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{SyncBatchResult, SyncError, SyncService, SyncWorkerId};

pub mod proto {
    tonic::include_proto!("raccoon.sync.v1");
}

pub use proto::dicom_sync_service_client::DicomSyncServiceClient;
pub use proto::dicom_sync_service_server::{DicomSyncService, DicomSyncServiceServer};

const RPC_SYSTEM_NAME: &str = "grpc";
const RPC_SERVICE_SYNC: &str = "raccoon.sync.v1.DicomSyncService";
const RPC_METHOD_SYNC_ONCE: &str = "SyncOnce";
const METHOD_SYNC_ONCE: &str = "raccoon.sync.v1.DicomSyncService/SyncOnce";

/// gRPC-backed sync service client.
#[derive(Debug)]
pub struct GrpcSyncServiceClient<T = tonic::transport::Channel> {
    inner: DicomSyncServiceClient<T>,
    server_address: Option<String>,
    poll_interval: Duration,
}

impl<T> GrpcSyncServiceClient<T> {
    /// Build a sync client from a generated tonic client.
    pub fn new(inner: DicomSyncServiceClient<T>) -> Self {
        Self {
            inner,
            server_address: None,
            poll_interval: Duration::from_secs(1),
        }
    }

    /// Build a sync client and record the server address in client spans.
    pub fn with_server_address(
        inner: DicomSyncServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            server_address: Some(server_address.into()),
            poll_interval: Duration::from_secs(1),
        }
    }

    /// Override the delay between remote sync batches in `run_until_shutdown`.
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }
}

impl GrpcSyncServiceClient<tonic::transport::Channel> {
    /// Connect to a sync service server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = DicomSyncServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

#[async_trait]
impl<T> SyncService for GrpcSyncServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    #[tracing::instrument(
        skip(self),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_SYNC,
            rpc.method = RPC_METHOD_SYNC_ONCE,
            rpc.response.status_code = tracing::field::Empty,
            sync.worker_id = %worker_id,
            server.address = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn sync_once(&self, worker_id: SyncWorkerId) -> Result<SyncBatchResult, SyncError> {
        if let Some(addr) = &self.server_address {
            Span::current().record("server.address", addr.as_str());
        }

        let mut client = self.inner.clone();
        let request = request_with_trace_context(proto::SyncOnceRequest::from(worker_id));
        let response = client
            .sync_once(request)
            .await
            .map_err(grpc_status_to_sync_err)?;
        let result =
            SyncBatchResult::try_from(response.into_inner()).map_err(grpc_status_to_sync_err)?;

        record_ok();
        Ok(result)
    }

    async fn run_until_shutdown(
        &self,
        worker_id: SyncWorkerId,
        shutdown: CancellationToken,
    ) -> Result<(), SyncError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = self.sync_once(worker_id.clone()) => {
                    result?;
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }
}

/// Tonic service adapter for a sync service implementation.
pub struct SyncGrpcService {
    service: Arc<dyn SyncService>,
}

impl SyncGrpcService {
    /// Build a service adapter for a sync service implementation.
    pub fn new(service: impl SyncService + 'static) -> Self {
        Self {
            service: Arc::new(service),
        }
    }

    /// Build a service adapter from a shared sync service handle.
    pub fn from_shared(service: Arc<dyn SyncService>) -> Self {
        Self { service }
    }

    /// Convert this adapter into the generated tonic server type.
    pub fn into_server(self) -> DicomSyncServiceServer<Self>
    where
        Self: DicomSyncService,
    {
        DicomSyncServiceServer::new(self)
    }
}

#[async_trait]
impl DicomSyncService for SyncGrpcService {
    async fn sync_once(
        &self,
        request: Request<proto::SyncOnceRequest>,
    ) -> Result<Response<proto::SyncOnceResponse>, Status> {
        let rpc_span = tracing::info_span!(
            METHOD_SYNC_ONCE,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_SYNC,
            rpc.method = RPC_METHOD_SYNC_ONCE,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        set_span_parent_from_metadata(&rpc_span, request.metadata());

        async move {
            let worker_id = SyncWorkerId::try_from(request.into_inner()).map_err(record_error)?;

            match self.service.sync_once(worker_id).await {
                Ok(result) => {
                    record_ok();
                    Ok(Response::new(result.into()))
                }
                Err(error) => Err(record_error(sync_error_to_status(error))),
            }
        }
        .instrument(rpc_span)
        .await
    }
}

impl From<SyncWorkerId> for proto::SyncOnceRequest {
    fn from(worker_id: SyncWorkerId) -> Self {
        Self {
            worker_id: worker_id.to_string(),
        }
    }
}

impl TryFrom<proto::SyncOnceRequest> for SyncWorkerId {
    type Error = Status;

    fn try_from(request: proto::SyncOnceRequest) -> Result<Self, Self::Error> {
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }

        Ok(Self::new(request.worker_id))
    }
}

impl From<SyncBatchResult> for proto::SyncOnceResponse {
    fn from(result: SyncBatchResult) -> Self {
        Self {
            claimed: result.claimed as u64,
            synced: result.synced as u64,
            quarantined: result.quarantined as u64,
            retryable_failures: result.retryable_failures as u64,
        }
    }
}

impl TryFrom<proto::SyncOnceResponse> for SyncBatchResult {
    type Error = Status;

    fn try_from(response: proto::SyncOnceResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            claimed: usize::try_from(response.claimed)
                .map_err(|_| Status::internal("claimed count outside usize range"))?,
            synced: usize::try_from(response.synced)
                .map_err(|_| Status::internal("synced count outside usize range"))?,
            quarantined: usize::try_from(response.quarantined)
                .map_err(|_| Status::internal("quarantined count outside usize range"))?,
            retryable_failures: usize::try_from(response.retryable_failures)
                .map_err(|_| Status::internal("retryable failure count outside usize range"))?,
        })
    }
}

fn request_with_trace_context<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    inject_current_trace_context(request.metadata_mut());
    request
}

fn grpc_status_to_sync_err(status: Status) -> SyncError {
    record_error_fields(&status);
    SyncError::Remote(format!(
        "grpc status {}: {}",
        code_name(status.code()),
        status.message()
    ))
}

fn sync_error_to_status(error: SyncError) -> Status {
    match error {
        SyncError::Remote(message) => Status::unavailable(message),
        other => Status::internal(other.to_string()),
    }
}

fn record_error(status: Status) -> Status {
    record_error_fields(&status);
    status
}

fn record_ok() {
    Span::current().record("rpc.response.status_code", code_name(tonic::Code::Ok));
}

fn record_error_fields(status: &Status) {
    let code = code_name(status.code());
    Span::current().record("rpc.response.status_code", code);
    Span::current().record("error.type", code);
}

fn code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

fn set_span_parent_from_metadata(span: &Span, metadata: &MetadataMap) {
    let parent_cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(metadata))
    });
    if parent_cx.has_active_span() {
        let _ = span.set_parent(parent_cx);
    }
}

fn inject_current_trace_context(metadata: &mut MetadataMap) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MetadataInjector(metadata));
    });
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(key) = AsciiMetadataKey::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = MetadataValue::try_from(value.as_str()) else {
            return;
        };
        self.0.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use opentelemetry::propagation::{Extractor, Injector};
    use tonic::metadata::MetadataMap;

    use super::*;

    struct FakeSyncService {
        result: Mutex<Result<SyncBatchResult, String>>,
    }

    #[async_trait]
    impl SyncService for FakeSyncService {
        async fn sync_once(&self, worker_id: SyncWorkerId) -> Result<SyncBatchResult, SyncError> {
            assert_eq!(worker_id.as_str(), "worker-1");
            self.result
                .lock()
                .unwrap()
                .clone()
                .map_err(SyncError::Remote)
        }

        async fn run_until_shutdown(
            &self,
            _worker_id: SyncWorkerId,
            _shutdown: CancellationToken,
        ) -> Result<(), SyncError> {
            Ok(())
        }
    }

    #[test]
    fn metadata_carrier_injects_and_extracts_trace_context() {
        const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        let mut metadata = MetadataMap::new();
        MetadataInjector(&mut metadata).set("traceparent", TRACEPARENT.to_string());
        let extractor = MetadataExtractor(&metadata);

        assert_eq!(extractor.get("traceparent"), Some(TRACEPARENT));
        assert!(extractor.keys().contains(&"traceparent"));
    }

    #[test]
    fn sync_once_request_validates_worker_id() {
        assert!(
            SyncWorkerId::try_from(proto::SyncOnceRequest {
                worker_id: "".to_string()
            })
            .is_err()
        );
        assert_eq!(
            SyncWorkerId::try_from(proto::SyncOnceRequest {
                worker_id: "worker-1".to_string()
            })
            .unwrap()
            .as_str(),
            "worker-1"
        );
    }

    #[test]
    fn sync_batch_result_round_trips_through_proto() {
        let result = SyncBatchResult {
            claimed: 3,
            synced: 2,
            quarantined: 1,
            retryable_failures: 4,
        };

        let proto = proto::SyncOnceResponse::from(result.clone());
        let recovered = SyncBatchResult::try_from(proto).unwrap();

        assert_eq!(recovered, result);
    }

    #[tokio::test]
    async fn server_adapter_returns_sync_result() {
        let adapter = SyncGrpcService::new(FakeSyncService {
            result: Mutex::new(Ok(SyncBatchResult {
                claimed: 3,
                synced: 2,
                quarantined: 1,
                retryable_failures: 0,
            })),
        });

        let response = adapter
            .sync_once(Request::new(proto::SyncOnceRequest {
                worker_id: "worker-1".to_string(),
            }))
            .await
            .expect("sync once succeeds")
            .into_inner();

        assert_eq!(response.claimed, 3);
        assert_eq!(response.synced, 2);
        assert_eq!(response.quarantined, 1);
        assert_eq!(response.retryable_failures, 0);
    }

    #[tokio::test]
    async fn server_adapter_rejects_missing_worker_id() {
        let adapter = SyncGrpcService::new(FakeSyncService {
            result: Mutex::new(Ok(SyncBatchResult::empty())),
        });

        let status = adapter
            .sync_once(Request::new(proto::SyncOnceRequest {
                worker_id: String::new(),
            }))
            .await
            .expect_err("missing worker id should fail");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn server_adapter_maps_service_errors() {
        let adapter = SyncGrpcService::new(FakeSyncService {
            result: Mutex::new(Err("remote unavailable".to_string())),
        });

        let status = adapter
            .sync_once(Request::new(proto::SyncOnceRequest {
                worker_id: "worker-1".to_string(),
            }))
            .await
            .expect_err("service error should fail");

        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
