use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use raccoon_contract_object_store::ObjectStoreError;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::{Body, Bytes, StdError};
use tonic::metadata::{AsciiMetadataKey, KeyRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::{Instrument, Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    IngestBatchRepositoryStatus, IngestChecksum, IngestChecksumAlgorithm, IngestObjectId,
    IngestObjectIdentity, IngestObjectOutcome, IngestObjectState, IngestPayloadRepresentation,
    IngestRequest, IngestResult, IngestService, IngestSource, IngestUploadId, IngestUploadResult,
};

pub mod proto {
    #![allow(clippy::large_enum_variant)]
    tonic::include_proto!("raccoon.ingest.v1");
}

pub use proto::ingest_transport_service_client::IngestTransportServiceClient;
pub use proto::ingest_transport_service_server::{
    IngestTransportService, IngestTransportServiceServer,
};

const RPC_SYSTEM_NAME: &str = "grpc";
const METHOD_STREAM_INGEST: &str = "raccoon.ingest.v1.IngestTransportService/StreamIngest";
const DEFAULT_MAX_BODY_CHUNK_BYTES: usize = 1 << 20;
const DEFAULT_MAX_DECODING_MESSAGE_BYTES: usize = DEFAULT_MAX_BODY_CHUNK_BYTES + 1024;
const SPOOL_DIR_NAME: &str = "raccoon-grpc-ingest";

/// gRPC-backed ingest transport client.
#[derive(Debug)]
pub struct GrpcIngestTransportClient<T = tonic::transport::Channel> {
    inner: Mutex<IngestTransportServiceClient<T>>,
    server_address: Option<String>,
}

impl<T> GrpcIngestTransportClient<T> {
    /// Build an ingest transport client from a generated tonic client.
    pub fn new(inner: IngestTransportServiceClient<T>) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: None,
        }
    }

    /// Build an ingest transport client and record the server address in spans.
    pub fn with_server_address(
        inner: IngestTransportServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: Some(server_address.into()),
        }
    }
}

impl GrpcIngestTransportClient<tonic::transport::Channel> {
    /// Connect to an ingest transport server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = IngestTransportServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

impl<T> GrpcIngestTransportClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Streams an ingest session to a remote server.
    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_STREAM_INGEST,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    pub async fn stream_ingest<S>(
        &self,
        request: S,
    ) -> Result<Response<tonic::codec::Streaming<proto::IngestTransportResponse>>, Status>
    where
        S: tonic::IntoStreamingRequest<Message = proto::IngestTransportRequest>,
    {
        let mut client = self.inner.lock().await;
        let mut request = request.into_streaming_request();
        let context = Span::current().context();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut MetadataInjector(request.metadata_mut()));
        });
        client.stream_ingest(request).await
    }
}

/// Tonic service adapter for an ingest service implementation.
pub struct IngestTransportGrpcService {
    service: Arc<dyn IngestService>,
    max_body_chunk_bytes: usize,
    max_decoding_message_bytes: usize,
}

impl IngestTransportGrpcService {
    /// Build a service adapter for an ingest service implementation.
    pub fn new(service: impl IngestService + 'static) -> Self {
        let service: Arc<dyn IngestService> = Arc::new(service);
        Self {
            service,
            max_body_chunk_bytes: DEFAULT_MAX_BODY_CHUNK_BYTES,
            max_decoding_message_bytes: DEFAULT_MAX_DECODING_MESSAGE_BYTES,
        }
    }

    /// Build a service adapter from a shared ingest service handle.
    pub fn from_shared(service: Arc<dyn IngestService>) -> Self {
        Self {
            service,
            max_body_chunk_bytes: DEFAULT_MAX_BODY_CHUNK_BYTES,
            max_decoding_message_bytes: DEFAULT_MAX_DECODING_MESSAGE_BYTES,
        }
    }

    /// Overrides the largest request chunk accepted before the transport rejects it.
    pub fn with_max_body_chunk_bytes(mut self, max_body_chunk_bytes: usize) -> Self {
        self.max_body_chunk_bytes = max_body_chunk_bytes.max(1);
        self.max_decoding_message_bytes = self
            .max_decoding_message_bytes
            .max(self.max_body_chunk_bytes.saturating_add(1024));
        self
    }

    /// Overrides the largest decoded request message accepted by tonic.
    pub fn with_max_decoding_message_bytes(mut self, max_decoding_message_bytes: usize) -> Self {
        self.max_decoding_message_bytes = max_decoding_message_bytes.max(1);
        self
    }

    /// Convert this adapter into the generated tonic server type.
    pub fn into_server(self) -> IngestTransportServiceServer<Self>
    where
        Self: IngestTransportService,
    {
        let max_decoding_message_bytes = self.max_decoding_message_bytes;
        IngestTransportServiceServer::new(self)
            .max_decoding_message_size(max_decoding_message_bytes)
    }
}

#[async_trait]
impl IngestTransportService for IngestTransportGrpcService {
    type StreamIngestStream = ReceiverStream<Result<proto::IngestTransportResponse, Status>>;

    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_STREAM_INGEST,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn stream_ingest(
        &self,
        request: Request<tonic::Streaming<proto::IngestTransportRequest>>,
    ) -> Result<Response<Self::StreamIngestStream>, Status> {
        set_current_span_parent(request.metadata());
        let request_stream = request.into_inner();
        let (response_tx, response_rx) = mpsc::channel(8);
        let service = self.service.clone();
        let max_body_chunk_bytes = self.max_body_chunk_bytes;
        let span = Span::current();
        let response_tx_for_errors = response_tx.clone();

        tokio::spawn(
            async move {
                let result =
                    process_stream(service, request_stream, response_tx, max_body_chunk_bytes)
                        .await;

                if let Err(status) = &result {
                    Span::current().record(
                        "rpc.response.status_code",
                        tracing::field::display(status.code()),
                    );
                } else {
                    record_ok();
                }

                if let Err(status) = result {
                    let _ = response_tx_for_errors.send(Err(status)).await;
                }
            }
            .instrument(span),
        );

        Ok(Response::new(ReceiverStream::new(response_rx)))
    }
}

async fn process_stream<S>(
    service: Arc<dyn IngestService>,
    mut request_stream: S,
    response_tx: mpsc::Sender<Result<proto::IngestTransportResponse, Status>>,
    max_body_chunk_bytes: usize,
) -> Result<(), Status>
where
    S: futures_util::Stream<Item = Result<proto::IngestTransportRequest, Status>> + Unpin,
{
    let mut session: Option<SessionState> = None;
    while let Some(message) = match request_stream.next().await {
        Some(Ok(message)) => Some(message),
        Some(Err(status)) => {
            abort_active_object(&mut session);
            return Err(status);
        }
        None => None,
    } {
        let message = match message.message {
            Some(message) => message,
            None => {
                abort_active_object(&mut session);
                return Err(Status::invalid_argument(
                    "ingest transport request missing message body",
                ));
            }
        };

        match message {
            proto::ingest_transport_request::Message::SessionBegin(begin) => {
                if session.is_some() {
                    abort_active_object(&mut session);
                    return Err(Status::invalid_argument(
                        "ingest transport session already started",
                    ));
                }
                session = Some(SessionState::try_from(begin)?);
            }
            proto::ingest_transport_request::Message::ObjectBegin(begin) => {
                let session = match session.as_mut() {
                    Some(session) => session,
                    None => {
                        abort_active_object(&mut session);
                        return Err(Status::invalid_argument(
                            "ingest transport session not started",
                        ));
                    }
                };
                if session.active_object.is_some() {
                    abort_session_state(session);
                    return Err(Status::invalid_argument(
                        "ingest transport object already started",
                    ));
                }

                let object_begin = match ObjectBeginState::try_from(begin) {
                    Ok(object_begin) => object_begin,
                    Err(status) => {
                        abort_session_state(session);
                        return Err(status);
                    }
                };
                let active_object = match ActiveObject::new(object_begin).await {
                    Ok(active_object) => active_object,
                    Err(status) => {
                        abort_session_state(session);
                        return Err(status);
                    }
                };
                session.active_object = Some(active_object);
            }
            proto::ingest_transport_request::Message::BodyChunk(chunk) => {
                let session = match session.as_mut() {
                    Some(session) => session,
                    None => {
                        abort_active_object(&mut session);
                        return Err(Status::invalid_argument(
                            "ingest transport session not started",
                        ));
                    }
                };
                let active = match session.active_object.as_mut() {
                    Some(active) => active,
                    None => {
                        abort_session_state(session);
                        return Err(Status::invalid_argument(
                            "ingest transport body chunk without active object",
                        ));
                    }
                };
                if chunk.data.len() > max_body_chunk_bytes {
                    abort_session_state(session);
                    return Err(Status::resource_exhausted(
                        "ingest body chunk exceeds configured maximum",
                    ));
                }
                if let Err(status) = active.write_chunk(chunk.data).await {
                    abort_session_state(session);
                    return Err(status);
                }
            }
            proto::ingest_transport_request::Message::ObjectEnd(_) => {
                let session = match session.as_mut() {
                    Some(session) => session,
                    None => {
                        abort_active_object(&mut session);
                        return Err(Status::invalid_argument(
                            "ingest transport session not started",
                        ));
                    }
                };
                let active = match session.active_object.take() {
                    Some(active) => active,
                    None => {
                        abort_session_state(session);
                        return Err(Status::invalid_argument(
                            "ingest transport object end without active object",
                        ));
                    }
                };
                let (request, spool_path) = match active
                    .into_ingest_request(session.upload_id.clone(), session.source.clone())
                    .await
                {
                    Ok(request) => request,
                    Err(status) => {
                        abort_session_state(session);
                        return Err(status);
                    }
                };
                session.requests.push(request);
                session.spooled_paths.push(spool_path);
            }
            proto::ingest_transport_request::Message::SessionEnd(_) => {
                let mut session = match session.take() {
                    Some(session) => session,
                    None => {
                        abort_active_object(&mut session);
                        return Err(Status::invalid_argument(
                            "ingest transport session not started",
                        ));
                    }
                };
                if session.active_object.is_some() {
                    let mut session = Some(session);
                    abort_active_object(&mut session);
                    return Err(Status::invalid_argument(
                        "ingest transport session ended with active object",
                    ));
                }

                let requests = std::mem::take(&mut session.requests);
                let batch_result = service.ingest_upload_objects(requests).await;
                cleanup_spooled_files(&mut session);
                let upload_result = IngestUploadResult::from_outcomes(
                    session.upload_id.clone(),
                    batch_result
                        .object_results
                        .iter()
                        .map(|result| result.outcome.clone())
                        .collect(),
                );
                let repository_status = batch_result.repository_status.clone();

                for result in batch_result.object_results {
                    send_response(
                        &response_tx,
                        proto::IngestTransportResponse {
                            message: Some(proto::ingest_transport_response::Message::ObjectResult(
                                result.into(),
                            )),
                        },
                    )
                    .await?;
                }

                send_response(
                    &response_tx,
                    proto::IngestTransportResponse {
                        message: Some(proto::ingest_transport_response::Message::UploadResult(
                            proto::IngestUploadResult {
                                repository_status: Some(repository_status.into()),
                                ..upload_result.into()
                            },
                        )),
                    },
                )
                .await?;
                record_ok();
                return Ok(());
            }
        }
    }

    abort_active_object(&mut session);
    Err(Status::invalid_argument(
        "ingest transport stream ended before session_end",
    ))
}

fn abort_active_object(session: &mut Option<SessionState>) {
    if let Some(session) = session.as_mut() {
        abort_session_state(session);
    }
}

fn abort_session_state(session: &mut SessionState) {
    if let Some(active_object) = session.active_object.take() {
        active_object.cleanup();
    }
    session.requests.clear();
    cleanup_spooled_files(session);
}

fn cleanup_spooled_files(session: &mut SessionState) {
    for path in session.spooled_paths.drain(..) {
        let _ = std::fs::remove_file(path);
    }
}

async fn send_response(
    response_tx: &mpsc::Sender<Result<proto::IngestTransportResponse, Status>>,
    response: proto::IngestTransportResponse,
) -> Result<(), Status> {
    response_tx
        .send(Ok(response))
        .await
        .map_err(|_| Status::cancelled("ingest response channel closed"))
}

fn record_ok() {
    Span::current().record("rpc.response.status_code", 0i64);
}

fn set_current_span_parent(metadata: &MetadataMap) {
    let parent_context = global::get_text_map_propagator(|propagator| {
        let extractor = MetadataExtractor(metadata);
        propagator.extract(&extractor)
    });
    let _ = Span::current().set_parent(parent_context);
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl<'a> Extractor for MetadataExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
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

impl<'a> Injector for MetadataInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = AsciiMetadataKey::from_bytes(key.as_bytes())
            && let Ok(value) = MetadataValue::try_from(value)
        {
            self.0.insert(key, value);
        }
    }
}

#[derive(Debug)]
struct SessionState {
    upload_id: IngestUploadId,
    source: IngestSource,
    requests: Vec<IngestRequest>,
    spooled_paths: Vec<PathBuf>,
    active_object: Option<ActiveObject>,
}

impl TryFrom<proto::IngestSessionBegin> for SessionState {
    type Error = Status;

    fn try_from(begin: proto::IngestSessionBegin) -> Result<Self, Self::Error> {
        Ok(Self {
            upload_id: IngestUploadId::from_str(begin.upload_id.as_str())
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
            source: begin.source.unwrap_or_default().try_into()?,
            requests: Vec::new(),
            spooled_paths: Vec::new(),
            active_object: None,
        })
    }
}

#[derive(Debug)]
struct ActiveObject {
    begin: ObjectBeginState,
    path: PathBuf,
    file: tokio::fs::File,
}

impl ActiveObject {
    async fn new(begin: ObjectBeginState) -> Result<Self, Status> {
        let spool_dir = std::env::temp_dir().join(SPOOL_DIR_NAME);
        tokio::fs::create_dir_all(&spool_dir)
            .await
            .map_err(|source| {
                Status::internal(format!(
                    "failed to create gRPC ingest spool directory: {source}"
                ))
            })?;

        for _ in 0..16 {
            let path = spool_dir.join(format!("{}.part", uuid::Uuid::now_v7()));
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(file) => return Ok(Self { begin, path, file }),
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(source) => {
                    return Err(Status::internal(format!(
                        "failed to create gRPC ingest spool file: {source}"
                    )));
                }
            }
        }

        Err(Status::internal(
            "failed to create unique gRPC ingest spool file",
        ))
    }

    async fn write_chunk(&mut self, chunk: Vec<u8>) -> Result<(), Status> {
        self.file.write_all(&chunk).await.map_err(|source| {
            Status::internal(format!("failed to write gRPC ingest body: {source}"))
        })
    }

    async fn into_ingest_request(
        self,
        upload_id: IngestUploadId,
        source: IngestSource,
    ) -> Result<(IngestRequest, PathBuf), Status> {
        if let Err(source) = self.file.sync_all().await {
            let Self { path, file, .. } = self;
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(Status::internal(format!(
                "failed to sync gRPC ingest body: {source}"
            )));
        }
        drop(self.file);

        let file = match tokio::fs::File::open(&self.path).await {
            Ok(file) => file,
            Err(source) => {
                let _ = std::fs::remove_file(self.path);
                return Err(Status::internal(format!(
                    "failed to read gRPC ingest body: {source}"
                )));
            }
        };
        let body_stream = tokio_util::io::ReaderStream::new(file).map(|result| {
            result.map_err(|source| {
                ObjectStoreError::backend_with_source("failed to read gRPC ingest body", source)
            })
        });
        let body = raccoon_contract_object_store::ByteStream::new(body_stream);
        let path = self.path;
        let request = IngestRequest {
            body,
            upload_id,
            ingest_object_id: self.begin.ingest_object_id,
            identity_hints: self.begin.identity_hints,
            source,
            payload_representation: self.begin.payload_representation,
            transfer_syntax_uid: self.begin.transfer_syntax_uid,
            expected_study_instance_uid: self.begin.expected_study_instance_uid,
            checksum: self.begin.checksum,
        };
        Ok((request, path))
    }

    fn cleanup(self) {
        let Self { path, file, .. } = self;
        drop(file);
        let _ = std::fs::remove_file(path);
    }
}

#[derive(Clone, Debug)]
struct ObjectBeginState {
    ingest_object_id: Option<IngestObjectId>,
    identity_hints: IngestObjectIdentity,
    payload_representation: IngestPayloadRepresentation,
    transfer_syntax_uid: Option<String>,
    expected_study_instance_uid: Option<String>,
    checksum: Option<IngestChecksum>,
}

impl TryFrom<proto::IngestObjectBegin> for ObjectBeginState {
    type Error = Status;

    fn try_from(begin: proto::IngestObjectBegin) -> Result<Self, Self::Error> {
        Ok(Self {
            ingest_object_id: begin
                .ingest_object_id
                .map(|value| {
                    IngestObjectId::from_str(value.as_str())
                        .map_err(|error| Status::invalid_argument(error.to_string()))
                })
                .transpose()?,
            identity_hints: begin.identity_hints.unwrap_or_default().try_into()?,
            payload_representation: begin.payload_representation.try_into()?,
            transfer_syntax_uid: begin.transfer_syntax_uid,
            expected_study_instance_uid: begin.expected_study_instance_uid,
            checksum: begin.checksum.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<proto::IngestSource> for IngestSource {
    type Error = Status;

    fn try_from(source: proto::IngestSource) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: source.request_id,
            correlation_id: source.correlation_id,
            source_ae: source.source_ae,
            source_application: source.source_application,
            protocol: source.protocol,
            content_type: source.content_type,
        })
    }
}

impl TryFrom<proto::IngestObjectIdentity> for IngestObjectIdentity {
    type Error = Status;

    fn try_from(identity: proto::IngestObjectIdentity) -> Result<Self, Self::Error> {
        Ok(Self {
            sop_class_uid: identity.sop_class_uid,
            study_instance_uid: identity.study_instance_uid,
            series_instance_uid: identity.series_instance_uid,
            sop_instance_uid: identity.sop_instance_uid,
        })
    }
}

impl TryFrom<proto::IngestChecksum> for IngestChecksum {
    type Error = Status;

    fn try_from(checksum: proto::IngestChecksum) -> Result<Self, Self::Error> {
        Ok(Self {
            algorithm: checksum.algorithm.try_into()?,
            value: checksum.value,
        })
    }
}

impl TryFrom<proto::IngestChecksumAlgorithm> for IngestChecksumAlgorithm {
    type Error = Status;

    fn try_from(algorithm: proto::IngestChecksumAlgorithm) -> Result<Self, Self::Error> {
        match algorithm {
            proto::IngestChecksumAlgorithm::Sha256 => Ok(Self::Sha256),
            proto::IngestChecksumAlgorithm::Unspecified => Err(Status::invalid_argument(
                "checksum algorithm must be specified",
            )),
        }
    }
}

impl TryFrom<i32> for IngestChecksumAlgorithm {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Sha256),
            _ => Err(Status::invalid_argument(
                "checksum algorithm must be specified",
            )),
        }
    }
}

impl TryFrom<proto::IngestPayloadRepresentation> for IngestPayloadRepresentation {
    type Error = Status;

    fn try_from(representation: proto::IngestPayloadRepresentation) -> Result<Self, Self::Error> {
        match representation {
            proto::IngestPayloadRepresentation::DicomFile => Ok(Self::DicomFile),
            proto::IngestPayloadRepresentation::DicomDataSet => Ok(Self::DicomDataSet),
            proto::IngestPayloadRepresentation::DicomWebMetadataAndBulkData => {
                Ok(Self::DicomWebMetadataAndBulkData)
            }
            proto::IngestPayloadRepresentation::Unknown => Ok(Self::Unknown),
            proto::IngestPayloadRepresentation::Unspecified => Err(Status::invalid_argument(
                "payload representation must be specified",
            )),
        }
    }
}

impl TryFrom<i32> for IngestPayloadRepresentation {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::DicomFile),
            2 => Ok(Self::DicomDataSet),
            3 => Ok(Self::DicomWebMetadataAndBulkData),
            4 => Ok(Self::Unknown),
            _ => Err(Status::invalid_argument(
                "payload representation must be specified",
            )),
        }
    }
}

impl From<IngestObjectState> for proto::IngestObjectState {
    fn from(state: IngestObjectState) -> Self {
        match state {
            IngestObjectState::Received => Self::Received,
            IngestObjectState::PendingSync => Self::PendingSync,
            IngestObjectState::Quarantined => Self::Quarantined,
        }
    }
}

impl From<IngestPayloadRepresentation> for proto::IngestPayloadRepresentation {
    fn from(representation: IngestPayloadRepresentation) -> Self {
        match representation {
            IngestPayloadRepresentation::DicomFile => Self::DicomFile,
            IngestPayloadRepresentation::DicomDataSet => Self::DicomDataSet,
            IngestPayloadRepresentation::DicomWebMetadataAndBulkData => {
                Self::DicomWebMetadataAndBulkData
            }
            IngestPayloadRepresentation::Unknown => Self::Unknown,
        }
    }
}

impl From<IngestChecksumAlgorithm> for proto::IngestChecksumAlgorithm {
    fn from(algorithm: IngestChecksumAlgorithm) -> Self {
        match algorithm {
            IngestChecksumAlgorithm::Sha256 => Self::Sha256,
        }
    }
}

impl From<IngestChecksum> for proto::IngestChecksum {
    fn from(checksum: IngestChecksum) -> Self {
        Self {
            algorithm: proto::IngestChecksumAlgorithm::from(checksum.algorithm) as i32,
            value: checksum.value,
        }
    }
}

impl From<&IngestObjectIdentity> for proto::IngestObjectIdentity {
    fn from(identity: &IngestObjectIdentity) -> Self {
        Self {
            sop_class_uid: identity.sop_class_uid.clone(),
            study_instance_uid: identity.study_instance_uid.clone(),
            series_instance_uid: identity.series_instance_uid.clone(),
            sop_instance_uid: identity.sop_instance_uid.clone(),
        }
    }
}

impl From<IngestObjectIdentity> for proto::IngestObjectIdentity {
    fn from(identity: IngestObjectIdentity) -> Self {
        (&identity).into()
    }
}

impl From<IngestObjectOutcome> for proto::IngestObjectOutcome {
    fn from(outcome: IngestObjectOutcome) -> Self {
        let message = match outcome {
            IngestObjectOutcome::Stored => Some(proto::ingest_object_outcome::Outcome::Stored(
                proto::IngestStoredOutcome {},
            )),
            IngestObjectOutcome::RejectedCannotUnderstand { reason } => Some(
                proto::ingest_object_outcome::Outcome::RejectedCannotUnderstand(
                    proto::IngestRejectedCannotUnderstand { reason },
                ),
            ),
            IngestObjectOutcome::RejectedUnsupportedSopClass {
                sop_class_uid,
                reason,
            } => Some(
                proto::ingest_object_outcome::Outcome::RejectedUnsupportedSopClass(
                    proto::IngestRejectedUnsupportedSopClass {
                        sop_class_uid,
                        reason,
                    },
                ),
            ),
            IngestObjectOutcome::RejectedStudyMismatch {
                expected_study_instance_uid,
                actual_study_instance_uid,
            } => Some(
                proto::ingest_object_outcome::Outcome::RejectedStudyMismatch(
                    proto::IngestRejectedStudyMismatch {
                        expected_study_instance_uid,
                        actual_study_instance_uid,
                    },
                ),
            ),
            IngestObjectOutcome::RejectedChecksumMismatch { expected, actual } => Some(
                proto::ingest_object_outcome::Outcome::RejectedChecksumMismatch(
                    proto::IngestRejectedChecksumMismatch { expected, actual },
                ),
            ),
            IngestObjectOutcome::RejectedTooLarge { max_content_length } => {
                Some(proto::ingest_object_outcome::Outcome::RejectedTooLarge(
                    proto::IngestRejectedTooLarge { max_content_length },
                ))
            }
            IngestObjectOutcome::ObjectStoreFailed { reason } => {
                Some(proto::ingest_object_outcome::Outcome::ObjectStoreFailed(
                    proto::IngestObjectStoreFailed { reason },
                ))
            }
            IngestObjectOutcome::RepositoryFailed { reason } => {
                Some(proto::ingest_object_outcome::Outcome::RepositoryFailed(
                    proto::IngestRepositoryFailed { reason },
                ))
            }
        };

        Self { outcome: message }
    }
}

impl From<IngestResult> for proto::IngestObjectResult {
    fn from(result: IngestResult) -> Self {
        Self {
            ingest_object_id: result.ingest_object_id.to_string(),
            upload_id: result.upload_id.to_string(),
            object_key: result.object_key.map(|key| key.into_string()),
            content_length: result.content_length,
            etag: result.etag,
            checksum: result.checksum.map(Into::into),
            identity: Some(result.identity.into()),
            payload_representation: proto::IngestPayloadRepresentation::from(
                result.payload_representation,
            ) as i32,
            transfer_syntax_uid: result.transfer_syntax_uid,
            state: proto::IngestObjectState::from(result.state) as i32,
            outcome: Some(result.outcome.into()),
        }
    }
}

impl From<IngestUploadResult> for proto::IngestUploadResult {
    fn from(result: IngestUploadResult) -> Self {
        Self {
            upload_id: result.upload_id.to_string(),
            total_object_count: result.total_object_count as u64,
            accepted_count: result.accepted_count as u64,
            failed_count: result.failed_count as u64,
            object_outcomes: result.object_outcomes.into_iter().map(Into::into).collect(),
            repository_status: Some(proto::IngestBatchRepositoryStatus {
                status: Some(proto::ingest_batch_repository_status::Status::Recorded(
                    proto::IngestBatchRepositoryRecorded {},
                )),
            }),
        }
    }
}

impl From<IngestBatchRepositoryStatus> for proto::IngestBatchRepositoryStatus {
    fn from(status: IngestBatchRepositoryStatus) -> Self {
        match status {
            IngestBatchRepositoryStatus::Recorded => Self {
                status: Some(proto::ingest_batch_repository_status::Status::Recorded(
                    proto::IngestBatchRepositoryRecorded {},
                )),
            },
            IngestBatchRepositoryStatus::Failed { reason } => Self {
                status: Some(proto::ingest_batch_repository_status::Status::Failed(
                    proto::IngestBatchRepositoryFailed { reason },
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures_util::StreamExt;
    use raccoon_contract_object_store::{
        ByteStream, Bytes, Object, ObjectKey, ObjectMetadata, ObjectStore, ObjectStoreError,
        PutResult,
    };

    use super::*;
    use crate::{IngestRepository, IngestRepositoryError};

    #[derive(Default)]
    struct FakeObjectStore {
        puts: Mutex<Vec<ObjectKey>>,
        deletes: Mutex<Vec<ObjectKey>>,
        bodies: Mutex<HashMap<ObjectKey, Bytes>>,
        fail_next: Mutex<Option<ObjectStoreError>>,
    }

    #[async_trait]
    impl ObjectStore for FakeObjectStore {
        async fn put(
            &self,
            key: ObjectKey,
            body: ByteStream,
        ) -> raccoon_contract_object_store::Result<PutResult> {
            if let Some(error) = self.fail_next.lock().unwrap().take() {
                return Err(error);
            }
            let bytes = collect_body_bytes(body).await?;
            self.puts.lock().unwrap().push(key.clone());
            self.bodies
                .lock()
                .unwrap()
                .insert(key.clone(), bytes.clone());
            Ok(PutResult::new(
                ObjectMetadata::new(key, bytes.len() as u64).with_etag("test-etag"),
            ))
        }

        async fn get(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<Object> {
            let body = self
                .bodies
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(Object::new(
                ObjectMetadata::new(key.clone(), body.len() as u64),
                ByteStream::once(body),
            ))
        }

        async fn head(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<ObjectMetadata> {
            let body = self
                .bodies
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(ObjectMetadata::new(key.clone(), body.len() as u64))
        }

        async fn delete(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<()> {
            self.deletes.lock().unwrap().push(key.clone());
            self.bodies.lock().unwrap().remove(key);
            Ok(())
        }
    }

    async fn collect_body_bytes(body: ByteStream) -> raccoon_contract_object_store::Result<Bytes> {
        let mut stream = body.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(Bytes::from(bytes))
    }

    #[derive(Default)]
    struct FakeRepository {
        records: Mutex<Vec<crate::ReceivedIngestObject>>,
        calls: Mutex<usize>,
        failures: Mutex<VecDeque<IngestRepositoryError>>,
    }

    #[async_trait]
    impl IngestRepository for FakeRepository {
        async fn record_received_objects(
            &self,
            records: &[crate::ReceivedIngestObject],
        ) -> Result<(), IngestRepositoryError> {
            *self.calls.lock().unwrap() += 1;
            if let Some(error) = self.failures.lock().unwrap().pop_front() {
                return Err(error);
            }
            self.records.lock().unwrap().extend(records.iter().cloned());
            Ok(())
        }
    }

    fn identity() -> IngestObjectIdentity {
        IngestObjectIdentity {
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.1.2".to_string()),
            study_instance_uid: Some("1.2.3".to_string()),
            series_instance_uid: Some("1.2.3.4".to_string()),
            sop_instance_uid: Some("1.2.3.4.5".to_string()),
        }
    }

    fn service_with_fakes() -> (
        crate::InMemoryIngestService,
        Arc<FakeObjectStore>,
        Arc<FakeRepository>,
    ) {
        let object_store = Arc::new(FakeObjectStore::default());
        let repository = Arc::new(FakeRepository::default());
        (
            crate::InMemoryIngestService::new(object_store.clone(), repository.clone()),
            object_store,
            repository,
        )
    }

    fn explicit_vr_dataset(identity: &IngestObjectIdentity) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_ui(
            &mut bytes,
            0x0008,
            0x0016,
            identity.sop_class_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0020,
            0x000D,
            identity.study_instance_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0020,
            0x000E,
            identity.series_instance_uid.as_deref().unwrap(),
        );
        push_ui(
            &mut bytes,
            0x0008,
            0x0018,
            identity.sop_instance_uid.as_deref().unwrap(),
        );
        bytes
    }

    fn push_ui(bytes: &mut Vec<u8>, group: u16, element: u16, value: &str) {
        bytes.extend_from_slice(&group.to_le_bytes());
        bytes.extend_from_slice(&element.to_le_bytes());
        bytes.extend_from_slice(b"UI");

        let mut value = value.as_bytes().to_vec();
        if value.len() % 2 == 1 {
            value.push(0);
        }

        bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&value);
    }

    fn upload_begin(upload_id: &IngestUploadId) -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::SessionBegin(
                proto::IngestSessionBegin {
                    upload_id: upload_id.to_string(),
                    source: Some(proto::IngestSource {
                        request_id: Some("request-1".to_string()),
                        protocol: Some("dicomweb".to_string()),
                        ..Default::default()
                    }),
                },
            )),
        }
    }

    fn object_begin(
        ingest_object_id: Option<IngestObjectId>,
        payload_representation: IngestPayloadRepresentation,
    ) -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::ObjectBegin(
                proto::IngestObjectBegin {
                    ingest_object_id: ingest_object_id.map(|id| id.to_string()),
                    identity_hints: Some(identity().into()),
                    payload_representation: proto::IngestPayloadRepresentation::from(
                        payload_representation,
                    ) as i32,
                    transfer_syntax_uid: Some("1.2.840.10008.1.2.1".to_string()),
                    expected_study_instance_uid: None,
                    checksum: None,
                },
            )),
        }
    }

    fn object_begin_with_checksum(checksum: IngestChecksum) -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::ObjectBegin(
                proto::IngestObjectBegin {
                    ingest_object_id: None,
                    identity_hints: Some(identity().into()),
                    payload_representation: proto::IngestPayloadRepresentation::from(
                        IngestPayloadRepresentation::DicomDataSet,
                    ) as i32,
                    transfer_syntax_uid: Some("1.2.840.10008.1.2.1".to_string()),
                    expected_study_instance_uid: None,
                    checksum: Some(checksum.into()),
                },
            )),
        }
    }

    fn body_chunk(bytes: impl AsRef<[u8]>) -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::BodyChunk(
                proto::IngestBodyChunk {
                    data: bytes.as_ref().to_vec(),
                },
            )),
        }
    }

    fn object_end() -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::ObjectEnd(
                proto::IngestObjectEnd {},
            )),
        }
    }

    fn upload_end() -> proto::IngestTransportRequest {
        proto::IngestTransportRequest {
            message: Some(proto::ingest_transport_request::Message::SessionEnd(
                proto::IngestSessionEnd {},
            )),
        }
    }

    async fn collect_stream_responses(
        service: impl IngestService + 'static,
        stream: impl futures_util::Stream<Item = proto::IngestTransportRequest> + Unpin,
    ) -> Result<Vec<proto::IngestTransportResponse>, Status> {
        let (response_tx, response_rx) = mpsc::channel(8);
        process_stream(
            Arc::new(service),
            stream.map(Ok),
            response_tx,
            DEFAULT_MAX_BODY_CHUNK_BYTES,
        )
        .await?;
        let mut responses = Vec::new();
        let mut response_rx = ReceiverStream::new(response_rx);
        while let Some(message) = response_rx.next().await {
            responses.push(message?);
        }
        Ok(responses)
    }

    #[test]
    fn object_key_layout_uses_generated_id_without_file_suffix() {
        let object_id = IngestObjectId::new();
        let key = crate::IngestObjectKeyBuilder::new()
            .build(&object_id)
            .expect("valid object key");

        assert_eq!(key.as_str(), format!("ingest/{object_id}"));
    }

    #[tokio::test]
    async fn successful_stream_ingest_returns_object_result_and_summary() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();

        let bytes = explicit_vr_dataset(&identity());
        let stream = tokio_stream::iter(vec![
            upload_begin(&upload_id),
            object_begin(None, IngestPayloadRepresentation::DicomDataSet),
            body_chunk(bytes.clone()),
            object_end(),
            upload_end(),
        ]);

        let responses = collect_stream_responses(service, stream)
            .await
            .expect("stream ingest should succeed");

        assert_eq!(responses.len(), 2);
        let object_result = match &responses[0].message {
            Some(proto::ingest_transport_response::Message::ObjectResult(result)) => result,
            _ => panic!("expected object result"),
        };
        assert_eq!(object_result.upload_id, upload_id.to_string());
        assert_eq!(
            object_result.state,
            proto::IngestObjectState::PendingSync as i32
        );
        let summary = match &responses[1].message {
            Some(proto::ingest_transport_response::Message::UploadResult(result)) => result,
            _ => panic!("expected upload result"),
        };
        assert_eq!(summary.upload_id, upload_id.to_string());
        assert_eq!(summary.total_object_count, 1);
        assert_eq!(summary.accepted_count, 1);
        assert_eq!(summary.failed_count, 0);
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn checksum_mismatch_is_streamed_as_per_object_rejection() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();
        let bytes = explicit_vr_dataset(&identity());

        let stream = tokio_stream::iter(vec![
            upload_begin(&upload_id),
            object_begin_with_checksum(IngestChecksum::sha256("deadbeef")),
            body_chunk(bytes),
            object_end(),
            upload_end(),
        ]);

        let responses = collect_stream_responses(service, stream)
            .await
            .expect("stream ingest should succeed");

        let object_result = match &responses[0].message {
            Some(proto::ingest_transport_response::Message::ObjectResult(result)) => result,
            _ => panic!("expected object result"),
        };
        assert_eq!(
            object_result.state,
            proto::IngestObjectState::Quarantined as i32
        );
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(repository.records.lock().unwrap().len(), 1);
        assert_eq!(responses.len(), 2);
    }

    #[tokio::test]
    async fn malformed_stream_fails_fast() {
        let (service, _object_store, _repository) = service_with_fakes();

        let stream = tokio_stream::iter(vec![body_chunk(Bytes::from_static(b"no session"))]);
        let response = collect_stream_responses(service, stream).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn oversized_body_chunk_is_rejected_by_transport() {
        let (service, _object_store, _repository) = service_with_fakes();

        let stream = tokio_stream::iter(vec![
            upload_begin(&IngestUploadId::new()),
            object_begin(None, IngestPayloadRepresentation::DicomDataSet),
            body_chunk(Bytes::from(vec![0; DEFAULT_MAX_BODY_CHUNK_BYTES + 1])),
        ]);
        let response = collect_stream_responses(service, stream).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn repository_failure_is_preserved_in_object_result() {
        let upload_id = IngestUploadId::new();
        let (service, _object_store, repository) = service_with_fakes();
        repository
            .failures
            .lock()
            .unwrap()
            .push_back(IngestRepositoryError::new("db down"));

        let stream = tokio_stream::iter(vec![
            upload_begin(&upload_id),
            object_begin(None, IngestPayloadRepresentation::DicomDataSet),
            body_chunk(explicit_vr_dataset(&identity())),
            object_end(),
            upload_end(),
        ]);
        let responses = collect_stream_responses(service, stream)
            .await
            .expect("stream ingest should succeed");

        let object_result = match &responses[0].message {
            Some(proto::ingest_transport_response::Message::ObjectResult(result)) => result,
            _ => panic!("expected object result"),
        };
        assert!(matches!(
            object_result
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.outcome.as_ref()),
            Some(proto::ingest_object_outcome::Outcome::Stored(_))
        ));
        let summary = match &responses[1].message {
            Some(proto::ingest_transport_response::Message::UploadResult(result)) => result,
            _ => panic!("expected upload result"),
        };
        assert!(matches!(
            summary
                .repository_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(proto::ingest_batch_repository_status::Status::Failed(_))
        ));
    }

    #[tokio::test]
    async fn multi_object_upload_reports_each_result_and_summary() {
        let upload_id = IngestUploadId::new();
        let (service, object_store, repository) = service_with_fakes();

        let bytes = explicit_vr_dataset(&identity());
        let stream = tokio_stream::iter(vec![
            upload_begin(&upload_id),
            object_begin(None, IngestPayloadRepresentation::DicomDataSet),
            body_chunk(bytes.clone()),
            object_end(),
            object_begin(
                Some(IngestObjectId::new()),
                IngestPayloadRepresentation::DicomDataSet,
            ),
            body_chunk(bytes),
            object_end(),
            upload_end(),
        ]);

        let responses = collect_stream_responses(service, stream)
            .await
            .expect("stream ingest should succeed");

        assert_eq!(responses.len(), 3);
        assert_eq!(object_store.puts.lock().unwrap().len(), 2);
        assert_eq!(*repository.calls.lock().unwrap(), 1);
        let summary = match &responses[2].message {
            Some(proto::ingest_transport_response::Message::UploadResult(result)) => result,
            _ => panic!("expected upload result"),
        };
        assert_eq!(summary.total_object_count, 2);
        assert_eq!(summary.accepted_count, 2);
    }
}
