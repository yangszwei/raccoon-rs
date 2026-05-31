use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use raccoon_contract_dicom::{
    DicomInstanceIdentity, PatientId, SeriesInstanceUid, SopClassUid, SopInstanceUid,
    StudyInstanceUid, TransferSyntaxUid,
};
use raccoon_contract_object_store::{ByteStream, Bytes, ObjectStoreError};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::{Body, Bytes as TonicBytes, StdError};
use tonic::metadata::{AsciiMetadataKey, KeyRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::{Instrument, Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::error::{RetrieveError, RetrieveRepositoryError};
use crate::model::{RetrieveResult, RetrievedInstance};
use crate::scope::RetrieveRequest;

pub mod proto {
    #![allow(clippy::large_enum_variant)]
    tonic::include_proto!("raccoon.retrieve.v1");
}

pub use proto::dicom_retrieve_service_client::DicomRetrieveServiceClient;
pub use proto::dicom_retrieve_service_server::{DicomRetrieveService, DicomRetrieveServiceServer};

use crate::service::RetrieveService;

const RPC_SYSTEM_NAME: &str = "grpc";
const RPC_SERVICE_RETRIEVE: &str = "raccoon.retrieve.v1.DicomRetrieveService";
const RPC_METHOD_RETRIEVE: &str = "Retrieve";
const METHOD_RETRIEVE: &str = "raccoon.retrieve.v1.DicomRetrieveService/Retrieve";

const DEFAULT_MAX_BODY_CHUNK_BYTES: usize = 1 << 20;
const DEFAULT_MAX_DECODING_MESSAGE_BYTES: usize = DEFAULT_MAX_BODY_CHUNK_BYTES + 1024;
const BODY_CHANNEL_CAPACITY: usize = 4;

enum ClientStreamState {
    BetweenInstances,
    InInstance(mpsc::Sender<Result<Bytes, ObjectStoreError>>),
}

/// gRPC-backed [`RetrieveService`] client.
#[derive(Debug)]
pub struct GrpcRetrieveServiceClient<T = tonic::transport::Channel> {
    inner: DicomRetrieveServiceClient<T>,
    server_address: Option<String>,
    max_decoding_message_bytes: usize,
}

impl<T> GrpcRetrieveServiceClient<T> {
    /// Build a retrieve client from a generated tonic client.
    pub fn new(inner: DicomRetrieveServiceClient<T>) -> Self {
        Self {
            inner,
            server_address: None,
            max_decoding_message_bytes: DEFAULT_MAX_DECODING_MESSAGE_BYTES,
        }
    }

    /// Build a retrieve client and record the server address in client spans.
    pub fn with_server_address(
        inner: DicomRetrieveServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            server_address: Some(server_address.into()),
            max_decoding_message_bytes: DEFAULT_MAX_DECODING_MESSAGE_BYTES,
        }
    }

    /// Set the maximum decoded response message size.
    pub fn with_max_decoding_message_bytes(mut self, max_decoding_message_bytes: usize) -> Self {
        self.max_decoding_message_bytes = max_decoding_message_bytes.max(1);
        self
    }
}

impl GrpcRetrieveServiceClient<tonic::transport::Channel> {
    /// Connect to a retrieve service server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = DicomRetrieveServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

#[async_trait]
impl<T> RetrieveService for GrpcRetrieveServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = TonicBytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_RETRIEVE,
            rpc.method = RPC_METHOD_RETRIEVE,
            rpc.response.status_code = tracing::field::Empty,
            server.address = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn retrieve(&self, request: RetrieveRequest) -> Result<RetrieveResult, RetrieveError> {
        if let Some(addr) = &self.server_address {
            Span::current().record("server.address", addr.as_str());
        }

        let mut client = self
            .inner
            .clone()
            .max_decoding_message_size(self.max_decoding_message_bytes);

        let proto_request = request_with_trace_context(proto::RetrieveRequest::from(request));
        let response_stream = client
            .retrieve(proto_request)
            .await
            .map_err(grpc_status_to_retrieve_err)?
            .into_inner();

        let (header_tx, header_rx) =
            oneshot::channel::<Result<(usize, Option<u64>), RetrieveError>>();
        let (instance_tx, instance_rx) =
            mpsc::channel::<Result<RetrievedInstance, RetrieveError>>(1);

        tokio::spawn(drive_retrieve_stream(
            response_stream,
            header_tx,
            instance_tx,
        ));

        let (instance_count, total_content_length) = header_rx.await.map_err(|_| {
            protocol_error("retrieve stream driver stopped before sending header")
        })??;

        record_ok();
        Ok(RetrieveResult {
            instance_count,
            total_content_length,
            stream: Box::pin(ReceiverStream::new(instance_rx)),
        })
    }
}

/// Drives a tonic response stream into a [`RetrieveResult`] body stream.
async fn drive_retrieve_stream(
    mut stream: impl Stream<Item = Result<proto::RetrieveResponse, Status>> + Unpin,
    header_tx: oneshot::Sender<Result<(usize, Option<u64>), RetrieveError>>,
    instance_tx: mpsc::Sender<Result<RetrievedInstance, RetrieveError>>,
) {
    let header = match stream.next().await {
        Some(Ok(msg)) => match msg.message {
            Some(proto::retrieve_response::Message::Header(h)) => h,
            _ => {
                let _ = header_tx.send(Err(protocol_error(
                    "retrieve stream did not start with RetrieveHeader",
                )));
                return;
            }
        },
        Some(Err(status)) => {
            let _ = header_tx.send(Err(grpc_status_to_retrieve_err(status)));
            return;
        }
        None => {
            let _ = header_tx.send(Err(protocol_error("retrieve stream ended before header")));
            return;
        }
    };

    let instance_count = header.instance_count as usize;
    let total_content_length = header.total_content_length;

    if header_tx
        .send(Ok((instance_count, total_content_length)))
        .is_err()
    {
        return;
    }

    let mut state = ClientStreamState::BetweenInstances;

    while let Some(msg_result) = stream.next().await {
        let msg = match msg_result {
            Ok(msg) => msg,
            Err(status) => {
                let message = format!("retrieve stream transport error: {status}");
                send_client_stream_error(state, &instance_tx, message).await;
                return;
            }
        };

        match msg.message {
            Some(proto::retrieve_response::Message::InstanceBegin(begin)) => {
                if let ClientStreamState::InInstance(tx) =
                    std::mem::replace(&mut state, ClientStreamState::BetweenInstances)
                {
                    let _ = tx
                        .send(Err(ObjectStoreError::backend(
                            "server sent InstanceBegin before InstanceEnd",
                        )))
                        .await;
                    return;
                }

                let identity_proto = match begin.identity {
                    Some(id) => id,
                    None => {
                        let _ = instance_tx
                            .send(Err(RetrieveError::Repository(
                                RetrieveRepositoryError::new(
                                    "server sent InstanceBegin with missing identity",
                                ),
                            )))
                            .await;
                        return;
                    }
                };
                let identity = match DicomInstanceIdentity::try_from(identity_proto) {
                    Ok(id) => id,
                    Err(e) => {
                        let _ = instance_tx
                            .send(Err(RetrieveError::Repository(
                                RetrieveRepositoryError::new(format!(
                                    "server sent unparseable instance identity: {e}"
                                )),
                            )))
                            .await;
                        return;
                    }
                };
                let transfer_syntax_uid = match parse_transfer_syntax_uid(begin.transfer_syntax_uid)
                {
                    Ok(ts) => ts,
                    Err(e) => {
                        let _ = instance_tx.send(Err(e)).await;
                        return;
                    }
                };

                let (chunk_tx, chunk_rx) =
                    mpsc::channel::<Result<Bytes, ObjectStoreError>>(BODY_CHANNEL_CAPACITY);

                let instance = RetrievedInstance {
                    identity,
                    transfer_syntax_uid,
                    content_length: begin.content_length,
                    body: ByteStream::new(ReceiverStream::new(chunk_rx)),
                };

                if instance_tx.send(Ok(instance)).await.is_err() {
                    return;
                }
                state = ClientStreamState::InInstance(chunk_tx);
            }
            Some(proto::retrieve_response::Message::BodyChunk(chunk)) => {
                if let ClientStreamState::InInstance(tx) = &state {
                    if tx.send(Ok(chunk.data)).await.is_err() {
                        return;
                    }
                } else {
                    let _ = instance_tx
                        .send(Err(protocol_error(
                            "server sent BodyChunk before InstanceBegin",
                        )))
                        .await;
                    return;
                }
            }
            Some(proto::retrieve_response::Message::InstanceEnd(_)) => {
                if matches!(state, ClientStreamState::InInstance(_)) {
                    state = ClientStreamState::BetweenInstances;
                } else {
                    let _ = instance_tx
                        .send(Err(protocol_error(
                            "server sent InstanceEnd before InstanceBegin",
                        )))
                        .await;
                    return;
                }
            }
            Some(proto::retrieve_response::Message::InstanceError(err)) => {
                match std::mem::replace(&mut state, ClientStreamState::BetweenInstances) {
                    ClientStreamState::BetweenInstances => {
                        let retrieve_err =
                            RetrieveError::ObjectStore(ObjectStoreError::backend(err.reason));
                        if instance_tx.send(Err(retrieve_err)).await.is_err() {
                            return;
                        }
                    }
                    ClientStreamState::InInstance(tx) => {
                        let _ = tx.send(Err(ObjectStoreError::backend(err.reason))).await;
                    }
                }
            }
            Some(proto::retrieve_response::Message::Header(_)) => {
                send_client_stream_error(
                    state,
                    &instance_tx,
                    "server sent duplicate RetrieveHeader",
                )
                .await;
                return;
            }
            None => {
                send_client_stream_error(state, &instance_tx, "server sent empty response message")
                    .await;
                return;
            }
        }
    }

    if let ClientStreamState::InInstance(tx) = state {
        let _ = tx
            .send(Err(ObjectStoreError::backend(
                "retrieve stream ended before InstanceEnd",
            )))
            .await;
    }
}

async fn send_client_stream_error(
    state: ClientStreamState,
    instance_tx: &mpsc::Sender<Result<RetrievedInstance, RetrieveError>>,
    message: impl Into<String>,
) {
    let message = message.into();
    match state {
        ClientStreamState::BetweenInstances => {
            let _ = instance_tx.send(Err(protocol_error(message))).await;
        }
        ClientStreamState::InInstance(tx) => {
            let _ = tx.send(Err(ObjectStoreError::backend(message))).await;
        }
    }
}

fn protocol_error(message: impl Into<String>) -> RetrieveError {
    RetrieveError::Repository(RetrieveRepositoryError::new(message))
}

fn parse_transfer_syntax_uid(
    uid: Option<String>,
) -> Result<Option<TransferSyntaxUid>, RetrieveError> {
    uid.map(|uid| {
        TransferSyntaxUid::new(uid).map_err(|e| {
            protocol_error(format!("server sent unparseable transfer syntax UID: {e}"))
        })
    })
    .transpose()
}

/// Tonic service adapter for a DICOM retrieve service implementation.
pub struct RetrieveGrpcService {
    service: Arc<dyn RetrieveService>,
    max_body_chunk_bytes: usize,
}

impl RetrieveGrpcService {
    /// Build a service adapter for a retrieve service implementation.
    pub fn new(service: impl RetrieveService + 'static) -> Self {
        Self {
            service: Arc::new(service),
            max_body_chunk_bytes: DEFAULT_MAX_BODY_CHUNK_BYTES,
        }
    }

    /// Build a service adapter from a shared retrieve service handle.
    pub fn from_shared(service: Arc<dyn RetrieveService>) -> Self {
        Self {
            service,
            max_body_chunk_bytes: DEFAULT_MAX_BODY_CHUNK_BYTES,
        }
    }

    /// Set the maximum bytes per `BodyChunk`.
    ///
    /// Clients must use a matching
    /// [`GrpcRetrieveServiceClient::with_max_decoding_message_bytes`] limit.
    pub fn with_max_body_chunk_bytes(mut self, max_body_chunk_bytes: usize) -> Self {
        self.max_body_chunk_bytes = max_body_chunk_bytes.max(1);
        self
    }

    /// Convert this adapter into the generated tonic server type.
    pub fn into_server(self) -> DicomRetrieveServiceServer<Self>
    where
        Self: DicomRetrieveService,
    {
        DicomRetrieveServiceServer::new(self)
    }
}

#[async_trait]
impl DicomRetrieveService for RetrieveGrpcService {
    type RetrieveStream = ReceiverStream<Result<proto::RetrieveResponse, Status>>;

    async fn retrieve(
        &self,
        request: Request<proto::RetrieveRequest>,
    ) -> Result<Response<Self::RetrieveStream>, Status> {
        let rpc_span = tracing::info_span!(
            METHOD_RETRIEVE,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_RETRIEVE,
            rpc.method = RPC_METHOD_RETRIEVE,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        set_span_parent_from_metadata(&rpc_span, request.metadata());

        async move {
            let retrieve_request =
                RetrieveRequest::try_from(request.into_inner()).map_err(record_error)?;

            let result = self
                .service
                .retrieve(retrieve_request)
                .await
                .map_err(|e| record_error(retrieve_error_to_status(e)))?;

            let (response_tx, response_rx) =
                mpsc::channel::<Result<proto::RetrieveResponse, Status>>(8);
            let max_chunk_bytes = self.max_body_chunk_bytes;
            let span = Span::current();

            tokio::spawn(
                async move {
                    let send_result =
                        stream_retrieve_result(result, &response_tx, max_chunk_bytes).await;

                    match &send_result {
                        Ok(()) => record_ok(),
                        Err(status) => {
                            record_error_fields(status);
                        }
                    }

                    if let Err(status) = send_result {
                        let _ = response_tx.send(Err(status)).await;
                    }
                }
                .instrument(span),
            );

            Ok(Response::new(ReceiverStream::new(response_rx)))
        }
        .instrument(rpc_span)
        .await
    }
}

/// Streams a [`RetrieveResult`] as gRPC responses.
async fn stream_retrieve_result(
    result: RetrieveResult,
    response_tx: &mpsc::Sender<Result<proto::RetrieveResponse, Status>>,
    max_chunk_bytes: usize,
) -> Result<(), Status> {
    send_response(
        response_tx,
        proto::RetrieveResponse {
            message: Some(proto::retrieve_response::Message::Header(
                proto::RetrieveHeader {
                    instance_count: result.instance_count as u64,
                    total_content_length: result.total_content_length,
                },
            )),
        },
    )
    .await?;

    let mut stream = result.stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(instance) => {
                if let Err(error) = send_instance(response_tx, instance, max_chunk_bytes).await {
                    match error {
                        SendInstanceError::Body(e) => {
                            send_response(
                                response_tx,
                                proto::RetrieveResponse {
                                    message: Some(
                                        proto::retrieve_response::Message::InstanceError(
                                            proto::InstanceError {
                                                reason: e.to_string(),
                                            },
                                        ),
                                    ),
                                },
                            )
                            .await?;
                        }
                        SendInstanceError::Status(status) => return Err(status),
                    }
                }
            }
            Err(
                RetrieveError::ObjectStore(e)
                | RetrieveError::ObjectStoreForInstance { source: e, .. },
            ) => {
                send_response(
                    response_tx,
                    proto::RetrieveResponse {
                        message: Some(proto::retrieve_response::Message::InstanceError(
                            proto::InstanceError {
                                reason: e.to_string(),
                            },
                        )),
                    },
                )
                .await?;
            }
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }
    }

    Ok(())
}

/// Sends one retrieved instance.
async fn send_instance(
    response_tx: &mpsc::Sender<Result<proto::RetrieveResponse, Status>>,
    instance: RetrievedInstance,
    max_chunk_bytes: usize,
) -> Result<(), SendInstanceError> {
    send_response(
        response_tx,
        proto::RetrieveResponse {
            message: Some(proto::retrieve_response::Message::InstanceBegin(
                proto::InstanceBegin {
                    identity: Some(proto::DicomInstanceIdentity::from(&instance.identity)),
                    transfer_syntax_uid: instance
                        .transfer_syntax_uid
                        .as_ref()
                        .map(|ts| ts.as_str().to_owned()),
                    content_length: instance.content_length,
                },
            )),
        },
    )
    .await
    .map_err(SendInstanceError::Status)?;

    let mut body = instance.body;
    while let Some(chunk_result) = body.next().await {
        let chunk = chunk_result.map_err(SendInstanceError::Body)?;
        if chunk.len() <= max_chunk_bytes {
            send_response(
                response_tx,
                proto::RetrieveResponse {
                    message: Some(proto::retrieve_response::Message::BodyChunk(
                        proto::BodyChunk { data: chunk },
                    )),
                },
            )
            .await
            .map_err(SendInstanceError::Status)?;
        } else {
            let mut offset = 0;
            while offset < chunk.len() {
                let end = (offset + max_chunk_bytes).min(chunk.len());
                send_response(
                    response_tx,
                    proto::RetrieveResponse {
                        message: Some(proto::retrieve_response::Message::BodyChunk(
                            proto::BodyChunk {
                                data: chunk.slice(offset..end),
                            },
                        )),
                    },
                )
                .await
                .map_err(SendInstanceError::Status)?;
                offset = end;
            }
        }
    }

    send_response(
        response_tx,
        proto::RetrieveResponse {
            message: Some(proto::retrieve_response::Message::InstanceEnd(
                proto::InstanceEnd {},
            )),
        },
    )
    .await
    .map_err(SendInstanceError::Status)
}

enum SendInstanceError {
    Body(ObjectStoreError),
    Status(Status),
}

async fn send_response(
    response_tx: &mpsc::Sender<Result<proto::RetrieveResponse, Status>>,
    response: proto::RetrieveResponse,
) -> Result<(), Status> {
    response_tx
        .send(Ok(response))
        .await
        .map_err(|_| Status::cancelled("retrieve response channel closed"))
}

impl From<RetrieveRequest> for proto::RetrieveRequest {
    fn from(request: RetrieveRequest) -> Self {
        Self {
            scope: Some(proto::RetrieveScope::from(request.scope)),
        }
    }
}

impl TryFrom<proto::RetrieveRequest> for RetrieveRequest {
    type Error = Status;

    fn try_from(req: proto::RetrieveRequest) -> Result<Self, Self::Error> {
        let scope = crate::RetrieveScope::try_from(
            req.scope
                .ok_or_else(|| Status::invalid_argument("retrieve scope must be specified"))?,
        )?;
        Ok(RetrieveRequest::new(scope))
    }
}

impl From<crate::RetrieveScope> for proto::RetrieveScope {
    fn from(scope: crate::RetrieveScope) -> Self {
        let scope = match scope {
            crate::RetrieveScope::Patient { patient_id } => {
                proto::retrieve_scope::Scope::PatientId(patient_id.as_str().to_owned())
            }
            crate::RetrieveScope::Study { study_instance_uid } => {
                proto::retrieve_scope::Scope::StudyInstanceUid(
                    study_instance_uid.as_str().to_owned(),
                )
            }
            crate::RetrieveScope::Series {
                series_instance_uid,
            } => proto::retrieve_scope::Scope::SeriesInstanceUid(
                series_instance_uid.as_str().to_owned(),
            ),
            crate::RetrieveScope::Instance { sop_instance_uid } => {
                proto::retrieve_scope::Scope::SopInstanceUid(sop_instance_uid.as_str().to_owned())
            }
        };
        Self { scope: Some(scope) }
    }
}

impl TryFrom<proto::RetrieveScope> for crate::RetrieveScope {
    type Error = Status;

    fn try_from(scope: proto::RetrieveScope) -> Result<Self, Self::Error> {
        match scope.scope {
            Some(proto::retrieve_scope::Scope::PatientId(id)) => PatientId::new(id)
                .map(|patient_id| crate::RetrieveScope::Patient { patient_id })
                .map_err(|e| Status::invalid_argument(e.to_string())),
            Some(proto::retrieve_scope::Scope::StudyInstanceUid(uid)) => StudyInstanceUid::new(uid)
                .map(|study_instance_uid| crate::RetrieveScope::Study { study_instance_uid })
                .map_err(|e| Status::invalid_argument(e.to_string())),
            Some(proto::retrieve_scope::Scope::SeriesInstanceUid(uid)) => {
                SeriesInstanceUid::new(uid)
                    .map(|series_instance_uid| crate::RetrieveScope::Series {
                        series_instance_uid,
                    })
                    .map_err(|e| Status::invalid_argument(e.to_string()))
            }
            Some(proto::retrieve_scope::Scope::SopInstanceUid(uid)) => SopInstanceUid::new(uid)
                .map(|sop_instance_uid| crate::RetrieveScope::Instance { sop_instance_uid })
                .map_err(|e| Status::invalid_argument(e.to_string())),
            None => Err(Status::invalid_argument("retrieve scope must be specified")),
        }
    }
}

impl From<&DicomInstanceIdentity> for proto::DicomInstanceIdentity {
    fn from(identity: &DicomInstanceIdentity) -> Self {
        Self {
            study_instance_uid: identity.study_instance_uid.as_str().to_owned(),
            series_instance_uid: identity.series_instance_uid.as_str().to_owned(),
            sop_instance_uid: identity.sop_instance_uid.as_str().to_owned(),
            sop_class_uid: identity.sop_class_uid.as_str().to_owned(),
        }
    }
}

impl TryFrom<proto::DicomInstanceIdentity> for DicomInstanceIdentity {
    type Error = Status;

    fn try_from(proto: proto::DicomInstanceIdentity) -> Result<Self, Self::Error> {
        let study_instance_uid = StudyInstanceUid::new(proto.study_instance_uid)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let series_instance_uid = SeriesInstanceUid::new(proto.series_instance_uid)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let sop_instance_uid = SopInstanceUid::new(proto.sop_instance_uid)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let sop_class_uid = SopClassUid::new(proto.sop_class_uid)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(DicomInstanceIdentity::new(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
            sop_class_uid,
        ))
    }
}

fn request_with_trace_context<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    inject_current_trace_context(request.metadata_mut());
    request
}

fn retrieve_error_to_status(error: RetrieveError) -> Status {
    match error {
        RetrieveError::InvalidRequest(msg) => Status::invalid_argument(msg),
        RetrieveError::Repository(e) => Status::internal(e.to_string()),
        RetrieveError::ObjectStore(e) | RetrieveError::ObjectStoreForInstance { source: e, .. } => {
            Status::internal(e.to_string())
        }
    }
}

fn grpc_status_to_retrieve_err(status: Status) -> RetrieveError {
    record_error_fields(&status);
    match status.code() {
        tonic::Code::InvalidArgument => RetrieveError::InvalidRequest(status.message().to_owned()),
        _ => RetrieveError::Repository(RetrieveRepositoryError::new(status.message().to_owned())),
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
        if let Ok(key) = AsciiMetadataKey::from_bytes(key.as_bytes())
            && let Ok(value) = MetadataValue::try_from(value.as_str())
        {
            self.0.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures_util::{StreamExt, stream};
    use raccoon_contract_dicom::PatientId;
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
        TransferSyntaxUid,
    };
    use raccoon_contract_object_store::{ByteStream, Bytes, ObjectKey, ObjectStoreError};
    use tonic::Request;

    use super::*;
    use crate::{
        InstanceRef, RetrieveRepository, RetrieveRepositoryError, RetrieveScope,
        StandardRetrieveService,
    };

    // — Helpers ——————————————————————————————————————————————————————————————

    fn study_uid() -> StudyInstanceUid {
        StudyInstanceUid::new("1.2.3").unwrap()
    }

    fn series_uid() -> SeriesInstanceUid {
        SeriesInstanceUid::new("1.2.3.4").unwrap()
    }

    fn sop_uid(n: u8) -> SopInstanceUid {
        SopInstanceUid::new(format!("1.2.3.4.5.{n}")).unwrap()
    }

    fn sop_class() -> SopClassUid {
        SopClassUid::new("1.2.840.10008.5.1.4.1.1.4").unwrap()
    }

    fn ts_uid() -> TransferSyntaxUid {
        TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap()
    }

    fn instance_identity(n: u8) -> DicomInstanceIdentity {
        DicomInstanceIdentity::new(study_uid(), series_uid(), sop_uid(n), sop_class())
    }

    fn instance_ref(n: u8) -> InstanceRef {
        let key = ObjectKey::new(format!("instances/{n}")).unwrap();
        InstanceRef::new(instance_identity(n), key)
            .with_transfer_syntax(ts_uid())
            .with_content_length(64)
    }

    // — Scope round-trip tests ————————————————————————————————————————————

    #[test]
    fn study_scope_round_trips_through_proto() {
        let scope = crate::RetrieveScope::Study {
            study_instance_uid: study_uid(),
        };
        let proto_scope = proto::RetrieveScope::from(scope.clone());
        let recovered = crate::RetrieveScope::try_from(proto_scope).unwrap();
        assert_eq!(recovered, scope);
    }

    #[test]
    fn series_scope_round_trips_through_proto() {
        let scope = crate::RetrieveScope::Series {
            series_instance_uid: series_uid(),
        };
        let proto_scope = proto::RetrieveScope::from(scope.clone());
        let recovered = crate::RetrieveScope::try_from(proto_scope).unwrap();
        assert_eq!(recovered, scope);
    }

    #[test]
    fn instance_scope_round_trips_through_proto() {
        let scope = crate::RetrieveScope::Instance {
            sop_instance_uid: sop_uid(1),
        };
        let proto_scope = proto::RetrieveScope::from(scope.clone());
        let recovered = crate::RetrieveScope::try_from(proto_scope).unwrap();
        assert_eq!(recovered, scope);
    }

    #[test]
    fn patient_scope_round_trips_through_proto() {
        let scope = crate::RetrieveScope::Patient {
            patient_id: PatientId::new("P001").unwrap(),
        };
        let proto_scope = proto::RetrieveScope::from(scope.clone());
        let recovered = crate::RetrieveScope::try_from(proto_scope).unwrap();
        assert_eq!(recovered, scope);
    }

    #[test]
    fn missing_scope_returns_invalid_argument() {
        let proto_scope = proto::RetrieveScope { scope: None };
        assert!(crate::RetrieveScope::try_from(proto_scope).is_err());
    }

    #[test]
    fn instance_identity_round_trips_through_proto() {
        let identity = instance_identity(1);
        let proto_id = proto::DicomInstanceIdentity::from(&identity);
        let recovered = DicomInstanceIdentity::try_from(proto_id).unwrap();
        assert_eq!(recovered, identity);
    }

    #[test]
    fn retrieve_request_round_trips_through_proto() {
        let request = RetrieveRequest::new(RetrieveScope::Study {
            study_instance_uid: study_uid(),
        });
        let proto_req = proto::RetrieveRequest::from(request.clone());
        let recovered = RetrieveRequest::try_from(proto_req).unwrap();
        assert_eq!(recovered, request);
    }

    #[test]
    fn missing_request_scope_returns_error() {
        let proto_req = proto::RetrieveRequest { scope: None };
        assert!(RetrieveRequest::try_from(proto_req).is_err());
    }

    #[test]
    fn invalid_transfer_syntax_uid_returns_error() {
        let error = parse_transfer_syntax_uid(Some("not a uid".to_owned()))
            .expect_err("invalid transfer syntax should not be ignored");

        assert!(matches!(error, RetrieveError::Repository(_)));
    }

    #[tokio::test]
    async fn client_driver_preserves_transport_error_before_header() {
        let (header_tx, header_rx) = oneshot::channel();
        let (instance_tx, _instance_rx) = mpsc::channel(1);

        drive_retrieve_stream(
            stream::iter([Err(Status::invalid_argument("bad retrieve request"))]),
            header_tx,
            instance_tx,
        )
        .await;

        let error = header_rx
            .await
            .expect("driver must report header result")
            .expect_err("pre-header transport error must fail retrieve");
        assert!(matches!(error, RetrieveError::InvalidRequest(_)));
        assert!(error.to_string().contains("bad retrieve request"));
    }

    #[tokio::test]
    async fn client_driver_reports_protocol_error_before_header() {
        let (header_tx, header_rx) = oneshot::channel();
        let (instance_tx, _instance_rx) = mpsc::channel(1);

        drive_retrieve_stream(
            stream::iter([Ok(proto::RetrieveResponse {
                message: Some(proto::retrieve_response::Message::InstanceEnd(
                    proto::InstanceEnd {},
                )),
            })]),
            header_tx,
            instance_tx,
        )
        .await;

        let error = header_rx
            .await
            .expect("driver must report header result")
            .expect_err("non-header first message must fail retrieve");
        assert!(matches!(error, RetrieveError::Repository(_)));
        assert!(
            error
                .to_string()
                .contains("retrieve stream did not start with RetrieveHeader")
        );
    }

    // — Server adapter tests ————————————————————————————————————————————————

    struct FakeRepository {
        refs: Vec<InstanceRef>,
    }

    #[async_trait]
    impl RetrieveRepository for FakeRepository {
        async fn find_instances_for_patient(
            &self,
            _: &PatientId,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            Ok(self.refs.clone())
        }

        async fn find_instances_for_study(
            &self,
            _: &StudyInstanceUid,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            Ok(self.refs.clone())
        }

        async fn find_instances_for_series(
            &self,
            _: &SeriesInstanceUid,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            Ok(self.refs.clone())
        }

        async fn find_instance(
            &self,
            _: &SopInstanceUid,
        ) -> Result<Option<InstanceRef>, RetrieveRepositoryError> {
            Ok(self.refs.first().cloned())
        }
    }

    struct FakeObjectStore {
        bodies: std::collections::HashMap<ObjectKey, Bytes>,
        fail_key: Option<ObjectKey>,
    }

    impl FakeObjectStore {
        fn new(refs: &[InstanceRef]) -> Self {
            let bodies = refs
                .iter()
                .map(|r| {
                    (
                        r.object_key.clone(),
                        Bytes::from(format!("payload-{}", r.object_key)),
                    )
                })
                .collect();
            Self {
                bodies,
                fail_key: None,
            }
        }

        fn failing_on(mut self, key: ObjectKey) -> Self {
            self.fail_key = Some(key);
            self
        }
    }

    #[async_trait]
    impl raccoon_contract_object_store::ObjectStore for FakeObjectStore {
        async fn put(
            &self,
            _: ObjectKey,
            _: ByteStream,
        ) -> raccoon_contract_object_store::Result<raccoon_contract_object_store::PutResult>
        {
            unimplemented!()
        }

        async fn get(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<raccoon_contract_object_store::Object>
        {
            if self.fail_key.as_ref() == Some(key) {
                return Err(ObjectStoreError::backend("simulated failure"));
            }
            let body = self
                .bodies
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            let len = body.len() as u64;
            Ok(raccoon_contract_object_store::Object::new(
                raccoon_contract_object_store::ObjectMetadata::new(key.clone(), len),
                ByteStream::once(body),
            ))
        }

        async fn head(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<raccoon_contract_object_store::ObjectMetadata>
        {
            let body = self
                .bodies
                .get(key)
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(raccoon_contract_object_store::ObjectMetadata::new(
                key.clone(),
                body.len() as u64,
            ))
        }

        async fn delete(&self, _: &ObjectKey) -> raccoon_contract_object_store::Result<()> {
            unimplemented!()
        }
    }

    fn service_with_refs(refs: &[InstanceRef]) -> RetrieveGrpcService {
        let repo = FakeRepository {
            refs: refs.to_vec(),
        };
        let store = FakeObjectStore::new(refs);
        let svc = StandardRetrieveService::new(Arc::new(repo), Arc::new(store));
        RetrieveGrpcService::new(svc)
    }

    async fn collect_stream(
        adapter: RetrieveGrpcService,
        scope: RetrieveScope,
    ) -> Vec<proto::RetrieveResponse> {
        let req = Request::new(proto::RetrieveRequest {
            scope: Some(proto::RetrieveScope::from(scope)),
        });
        let response = adapter.retrieve(req).await.expect("handler should succeed");
        let mut stream = response.into_inner();
        let mut messages = Vec::new();
        while let Some(Ok(msg)) = stream.next().await {
            messages.push(msg);
        }
        messages
    }

    #[tokio::test]
    async fn empty_scope_streams_only_header_with_zero_count() {
        let adapter = service_with_refs(&[]);
        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert!(matches!(
            messages[0].message,
            Some(proto::retrieve_response::Message::Header(ref h)) if h.instance_count == 0
        ));
    }

    #[tokio::test]
    async fn single_instance_stream_has_header_begin_chunks_end() {
        let refs = vec![instance_ref(1)];
        let adapter = service_with_refs(&refs);
        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        // header, instance_begin, at least one body_chunk, instance_end
        assert!(messages.len() >= 4);
        assert!(matches!(
            messages[0].message,
            Some(proto::retrieve_response::Message::Header(ref h)) if h.instance_count == 1
        ));
        assert!(matches!(
            messages[1].message,
            Some(proto::retrieve_response::Message::InstanceBegin(_))
        ));
        assert!(messages.iter().any(|m| matches!(
            m.message,
            Some(proto::retrieve_response::Message::BodyChunk(_))
        )));
        assert!(matches!(
            messages.last().unwrap().message,
            Some(proto::retrieve_response::Message::InstanceEnd(_))
        ));
    }

    #[tokio::test]
    async fn instance_begin_carries_identity_and_transfer_syntax() {
        let refs = vec![instance_ref(1)];
        let adapter = service_with_refs(&refs);
        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        let begin = messages
            .iter()
            .find_map(|m| match &m.message {
                Some(proto::retrieve_response::Message::InstanceBegin(b)) => Some(b),
                _ => None,
            })
            .expect("instance_begin must be present");

        assert!(begin.identity.is_some());
        assert_eq!(
            begin.transfer_syntax_uid.as_deref(),
            Some("1.2.840.10008.1.2.1")
        );
        assert!(begin.content_length > 0);
    }

    #[tokio::test]
    async fn multiple_instances_each_have_begin_and_end() {
        let refs = vec![instance_ref(1), instance_ref(2), instance_ref(3)];
        let adapter = service_with_refs(&refs);
        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        let header = &messages[0];
        assert!(matches!(
            header.message,
            Some(proto::retrieve_response::Message::Header(ref h)) if h.instance_count == 3
        ));

        let begin_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceBegin(_))
                )
            })
            .count();
        let end_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceEnd(_))
                )
            })
            .count();

        assert_eq!(begin_count, 3);
        assert_eq!(end_count, 3);
    }

    #[tokio::test]
    async fn object_store_failure_emits_instance_error_not_stream_abort() {
        let refs = vec![instance_ref(1), instance_ref(2)];
        let fail_key = refs[0].object_key.clone();
        let repo = FakeRepository { refs: refs.clone() };
        let store = FakeObjectStore::new(&refs).failing_on(fail_key);
        let svc = StandardRetrieveService::new(Arc::new(repo), Arc::new(store));
        let adapter = RetrieveGrpcService::new(svc);

        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        let error_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceError(_))
                )
            })
            .count();
        assert_eq!(error_count, 1);

        let end_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceEnd(_))
                )
            })
            .count();
        assert_eq!(end_count, 1, "successful instance must still emit end");
    }

    #[tokio::test]
    async fn body_stream_failure_emits_instance_error_and_continues() {
        let failing_instance = RetrievedInstance {
            identity: instance_identity(1),
            transfer_syntax_uid: Some(ts_uid()),
            content_length: 128,
            body: ByteStream::new(stream::iter([
                Ok(Bytes::from_static(b"partial")),
                Err(ObjectStoreError::backend("read failed")),
            ])),
        };
        let successful_instance = RetrievedInstance {
            identity: instance_identity(2),
            transfer_syntax_uid: Some(ts_uid()),
            content_length: 2,
            body: ByteStream::once(Bytes::from_static(b"ok")),
        };
        let result = RetrieveResult {
            instance_count: 2,
            total_content_length: Some(130),
            stream: Box::pin(stream::iter([
                Ok(failing_instance),
                Ok(successful_instance),
            ])),
        };
        let (response_tx, response_rx) =
            mpsc::channel::<Result<proto::RetrieveResponse, Status>>(8);

        stream_retrieve_result(result, &response_tx, DEFAULT_MAX_BODY_CHUNK_BYTES)
            .await
            .expect("per-instance body failure should not abort the stream");
        drop(response_tx);

        let messages = ReceiverStream::new(response_rx)
            .filter_map(|item| async { item.ok() })
            .collect::<Vec<_>>()
            .await;

        let error_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceError(_))
                )
            })
            .count();
        let begin_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceBegin(_))
                )
            })
            .count();
        let end_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::InstanceEnd(_))
                )
            })
            .count();

        assert_eq!(error_count, 1);
        assert_eq!(begin_count, 2, "later instances should still be sent");
        assert_eq!(end_count, 1, "only the successful instance should end");
    }

    #[tokio::test]
    async fn large_body_is_split_into_multiple_chunks() {
        let refs = vec![instance_ref(1)];
        let repo = FakeRepository { refs: refs.clone() };
        // A body of 3 bytes, chunk limit of 1 byte → 3 BodyChunk messages.
        let mut store = FakeObjectStore::new(&refs);
        store
            .bodies
            .insert(refs[0].object_key.clone(), Bytes::from_static(b"abc"));
        let svc = StandardRetrieveService::new(Arc::new(repo), Arc::new(store));
        let adapter = RetrieveGrpcService::new(svc).with_max_body_chunk_bytes(1);

        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        let chunk_count = messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message,
                    Some(proto::retrieve_response::Message::BodyChunk(_))
                )
            })
            .count();
        assert_eq!(chunk_count, 3);
    }

    #[tokio::test]
    async fn invalid_request_scope_returns_invalid_argument_status() {
        let adapter = service_with_refs(&[]);
        let req = Request::new(proto::RetrieveRequest { scope: None });
        let status = adapter
            .retrieve(req)
            .await
            .expect_err("missing scope must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn total_content_length_present_when_all_sizes_known() {
        let refs = vec![instance_ref(1), instance_ref(2)];
        let adapter = service_with_refs(&refs);
        let messages = collect_stream(
            adapter,
            RetrieveScope::Study {
                study_instance_uid: study_uid(),
            },
        )
        .await;

        let header = match &messages[0].message {
            Some(proto::retrieve_response::Message::Header(h)) => h,
            _ => panic!("first message must be header"),
        };
        assert_eq!(header.total_content_length, Some(2 * 64));
    }
}
