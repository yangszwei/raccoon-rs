use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tokio::sync::Mutex;
use tonic::codegen::{Body, Bytes, StdError};
use tonic::metadata::{AsciiMetadataKey, KeyRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    AeTitle, ApplicationEntityRegistry, ApplicationEntityRegistryError,
    ApplicationEntityRegistryWriter, LocalApplicationEntity as DomainLocalApplicationEntity,
    PeerApplicationEntity as DomainPeerApplicationEntity,
};

pub mod proto {
    tonic::include_proto!("raccoon.application_entity_registry.v1");
}

pub use proto::application_entity_registry_service_client::ApplicationEntityRegistryServiceClient;
pub use proto::application_entity_registry_service_server::{
    ApplicationEntityRegistryService, ApplicationEntityRegistryServiceServer,
};
pub use proto::application_entity_registry_writer_service_client::ApplicationEntityRegistryWriterServiceClient;
pub use proto::application_entity_registry_writer_service_server::{
    ApplicationEntityRegistryWriterService, ApplicationEntityRegistryWriterServiceServer,
};

const RPC_SYSTEM_NAME: &str = "grpc";
const METHOD_LIST_LOCALS: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryService/ListLocalApplicationEntities";
const METHOD_LIST_PEERS: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryService/ListPeerApplicationEntities";
const METHOD_GET_LOCAL: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryService/GetLocalApplicationEntity";
const METHOD_GET_PEER: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryService/GetPeerApplicationEntity";
const METHOD_INSERT_LOCAL: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryWriterService/InsertLocalApplicationEntity";
const METHOD_INSERT_PEER: &str = "raccoon.application_entity_registry.v1.ApplicationEntityRegistryWriterService/InsertPeerApplicationEntity";

/// gRPC-backed Application Entity registry client.
#[derive(Debug)]
pub struct GrpcApplicationEntityRegistryClient<T = tonic::transport::Channel> {
    inner: Mutex<ApplicationEntityRegistryServiceClient<T>>,
    server_address: Option<String>,
}

impl<T> GrpcApplicationEntityRegistryClient<T> {
    /// Build a registry client from a generated tonic client.
    pub fn new(inner: ApplicationEntityRegistryServiceClient<T>) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: None,
        }
    }

    /// Build a registry client and record the server address in client spans.
    pub fn with_server_address(
        inner: ApplicationEntityRegistryServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: Some(server_address.into()),
        }
    }
}

impl GrpcApplicationEntityRegistryClient<tonic::transport::Channel> {
    /// Connect to a registry server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = ApplicationEntityRegistryServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

/// gRPC-backed Application Entity registry writer client.
#[derive(Debug)]
pub struct GrpcApplicationEntityRegistryWriterClient<T = tonic::transport::Channel> {
    inner: Mutex<ApplicationEntityRegistryWriterServiceClient<T>>,
    server_address: Option<String>,
}

impl<T> GrpcApplicationEntityRegistryWriterClient<T> {
    /// Build a registry writer client from a generated tonic client.
    pub fn new(inner: ApplicationEntityRegistryWriterServiceClient<T>) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: None,
        }
    }

    /// Build a registry writer client and record the server address in client spans.
    pub fn with_server_address(
        inner: ApplicationEntityRegistryWriterServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner: Mutex::new(inner),
            server_address: Some(server_address.into()),
        }
    }
}

impl GrpcApplicationEntityRegistryWriterClient<tonic::transport::Channel> {
    /// Connect to a registry writer server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = ApplicationEntityRegistryWriterServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

#[async_trait]
impl<T> ApplicationEntityRegistry for GrpcApplicationEntityRegistryClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    #[instrument(
        skip(self),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_LIST_LOCALS,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn list_locals(
        &self,
    ) -> Result<Vec<DomainLocalApplicationEntity>, ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::ListLocalApplicationEntitiesRequest {});
        let result = client.list_local_application_entities(request).await;
        record_status(&result);

        let response = result?.into_inner();
        response
            .application_entities
            .into_iter()
            .map(DomainLocalApplicationEntity::try_from)
            .collect()
    }

    #[instrument(
        skip(self),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_LIST_PEERS,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn list_peers(
        &self,
    ) -> Result<Vec<DomainPeerApplicationEntity>, ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::ListPeerApplicationEntitiesRequest {});
        let result = client.list_peer_application_entities(request).await;
        record_status(&result);

        let response = result?.into_inner();
        response
            .application_entities
            .into_iter()
            .map(DomainPeerApplicationEntity::try_from)
            .collect()
    }

    #[instrument(
        skip(self),
        fields(
            ae.title = %ae_title,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_GET_LOCAL,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_local(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<DomainLocalApplicationEntity>, ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::GetLocalApplicationEntityRequest {
            title: ae_title.to_string(),
        });
        let result = client.get_local_application_entity(request).await;
        record_status(&result);

        result?
            .into_inner()
            .application_entity
            .map(DomainLocalApplicationEntity::try_from)
            .transpose()
    }

    #[instrument(
        skip(self),
        fields(
            ae.title = %ae_title,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_GET_PEER,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_peer(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<DomainPeerApplicationEntity>, ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::GetPeerApplicationEntityRequest {
            title: ae_title.to_string(),
        });
        let result = client.get_peer_application_entity(request).await;
        record_status(&result);

        result?
            .into_inner()
            .application_entity
            .map(DomainPeerApplicationEntity::try_from)
            .transpose()
    }
}

#[async_trait]
impl<T> ApplicationEntityRegistryWriter for GrpcApplicationEntityRegistryWriterClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    #[instrument(
        skip(self, local_ae),
        fields(
            ae.title = %local_ae.title(),
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_INSERT_LOCAL,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn insert_local(
        &mut self,
        local_ae: DomainLocalApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::InsertLocalApplicationEntityRequest {
            application_entity: Some(local_ae.into()),
        });
        let result = client.insert_local_application_entity(request).await;
        record_status(&result);

        result.map(|_| ()).map_err(Into::into)
    }

    #[instrument(
        skip(self, peer_ae),
        fields(
            ae.title = %peer_ae.title(),
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_INSERT_PEER,
            rpc.response.status_code = tracing::field::Empty,
            server.address = self.server_address.as_deref().unwrap_or_default(),
            error.type = tracing::field::Empty,
        )
    )]
    async fn insert_peer(
        &mut self,
        peer_ae: DomainPeerApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        let mut client = self.inner.lock().await;
        let request = request_with_trace_context(proto::InsertPeerApplicationEntityRequest {
            application_entity: Some(peer_ae.into()),
        });
        let result = client.insert_peer_application_entity(request).await;
        record_status(&result);

        result.map(|_| ()).map_err(Into::into)
    }
}

/// Tonic service adapter for an Application Entity registry implementation.
#[derive(Debug)]
pub struct ApplicationEntityRegistryGrpcService<R> {
    registry: Arc<Mutex<R>>,
}

impl<R> ApplicationEntityRegistryGrpcService<R> {
    /// Build a service adapter for a registry implementation.
    pub fn new(registry: R) -> Self {
        Self {
            registry: Arc::new(Mutex::new(registry)),
        }
    }

    /// Build a service adapter from a shared registry handle.
    pub fn from_shared(registry: Arc<Mutex<R>>) -> Self {
        Self { registry }
    }

    /// Convert this adapter into the generated tonic server type.
    pub fn into_server(self) -> ApplicationEntityRegistryServiceServer<Self>
    where
        Self: ApplicationEntityRegistryService,
    {
        ApplicationEntityRegistryServiceServer::new(self)
    }
}

/// Tonic service adapter for a registry writer implementation.
#[derive(Debug)]
pub struct ApplicationEntityRegistryWriterGrpcService<R> {
    registry: Arc<Mutex<R>>,
}

impl<R> ApplicationEntityRegistryWriterGrpcService<R> {
    /// Build a writer service adapter for a registry implementation.
    pub fn new(registry: R) -> Self {
        Self {
            registry: Arc::new(Mutex::new(registry)),
        }
    }

    /// Build a writer service adapter from a shared registry handle.
    pub fn from_shared(registry: Arc<Mutex<R>>) -> Self {
        Self { registry }
    }

    /// Convert this adapter into the generated tonic writer server type.
    pub fn into_server(self) -> ApplicationEntityRegistryWriterServiceServer<Self>
    where
        Self: ApplicationEntityRegistryWriterService,
    {
        ApplicationEntityRegistryWriterServiceServer::new(self)
    }
}

#[async_trait]
impl<R> ApplicationEntityRegistryService for ApplicationEntityRegistryGrpcService<R>
where
    R: ApplicationEntityRegistry + Send + Sync + 'static,
{
    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_LIST_LOCALS,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn list_local_application_entities(
        &self,
        request: Request<proto::ListLocalApplicationEntitiesRequest>,
    ) -> Result<Response<proto::ListLocalApplicationEntitiesResponse>, Status> {
        set_current_span_parent(request.metadata());
        let registry = self.registry.lock().await;
        let result = registry.list_locals().await;

        match result {
            Ok(application_entities) => {
                record_ok();
                Ok(Response::new(proto::ListLocalApplicationEntitiesResponse {
                    application_entities: application_entities
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                }))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }

    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_LIST_PEERS,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn list_peer_application_entities(
        &self,
        request: Request<proto::ListPeerApplicationEntitiesRequest>,
    ) -> Result<Response<proto::ListPeerApplicationEntitiesResponse>, Status> {
        set_current_span_parent(request.metadata());
        let registry = self.registry.lock().await;
        let result = registry.list_peers().await;

        match result {
            Ok(application_entities) => {
                record_ok();
                Ok(Response::new(proto::ListPeerApplicationEntitiesResponse {
                    application_entities: application_entities
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                }))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }

    #[instrument(
        skip(self, request),
        fields(
            ae.title = tracing::field::Empty,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_GET_LOCAL,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_local_application_entity(
        &self,
        request: Request<proto::GetLocalApplicationEntityRequest>,
    ) -> Result<Response<proto::GetLocalApplicationEntityResponse>, Status> {
        set_current_span_parent(request.metadata());
        let title = request.into_inner().title;
        Span::current().record("ae.title", title.as_str());
        let ae_title = AeTitle::from_str(title.as_str())
            .map_err(|error| record_error(Status::invalid_argument(error.to_string())))?;

        let registry = self.registry.lock().await;
        let result = registry.get_local(&ae_title).await;

        match result {
            Ok(application_entity) => {
                record_ok();
                Ok(Response::new(proto::GetLocalApplicationEntityResponse {
                    application_entity: application_entity.map(Into::into),
                }))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }

    #[instrument(
        skip(self, request),
        fields(
            ae.title = tracing::field::Empty,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_GET_PEER,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn get_peer_application_entity(
        &self,
        request: Request<proto::GetPeerApplicationEntityRequest>,
    ) -> Result<Response<proto::GetPeerApplicationEntityResponse>, Status> {
        set_current_span_parent(request.metadata());
        let title = request.into_inner().title;
        Span::current().record("ae.title", title.as_str());
        let ae_title = AeTitle::from_str(title.as_str())
            .map_err(|error| record_error(Status::invalid_argument(error.to_string())))?;

        let registry = self.registry.lock().await;
        let result = registry.get_peer(&ae_title).await;

        match result {
            Ok(application_entity) => {
                record_ok();
                Ok(Response::new(proto::GetPeerApplicationEntityResponse {
                    application_entity: application_entity.map(Into::into),
                }))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }
}

#[async_trait]
impl<R> ApplicationEntityRegistryWriterService for ApplicationEntityRegistryWriterGrpcService<R>
where
    R: ApplicationEntityRegistryWriter + Send + Sync + 'static,
{
    #[instrument(
        skip(self, request),
        fields(
            ae.title = tracing::field::Empty,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_INSERT_LOCAL,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn insert_local_application_entity(
        &self,
        request: Request<proto::InsertLocalApplicationEntityRequest>,
    ) -> Result<Response<proto::InsertLocalApplicationEntityResponse>, Status> {
        set_current_span_parent(request.metadata());
        let application_entity = request
            .into_inner()
            .application_entity
            .ok_or_else(|| record_error(Status::invalid_argument("missing application entity")))?;
        Span::current().record("ae.title", application_entity.title.as_str());
        let application_entity = DomainLocalApplicationEntity::try_from(application_entity)
            .map_err(|error| record_error(status_from_registry_error(error)))?;

        let mut registry = self.registry.lock().await;
        let result = registry.insert_local(application_entity).await;

        match result {
            Ok(()) => {
                record_ok();
                Ok(Response::new(
                    proto::InsertLocalApplicationEntityResponse {},
                ))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }

    #[instrument(
        skip(self, request),
        fields(
            ae.title = tracing::field::Empty,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.method = METHOD_INSERT_PEER,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn insert_peer_application_entity(
        &self,
        request: Request<proto::InsertPeerApplicationEntityRequest>,
    ) -> Result<Response<proto::InsertPeerApplicationEntityResponse>, Status> {
        set_current_span_parent(request.metadata());
        let application_entity = request
            .into_inner()
            .application_entity
            .ok_or_else(|| record_error(Status::invalid_argument("missing application entity")))?;
        Span::current().record("ae.title", application_entity.title.as_str());
        let application_entity = DomainPeerApplicationEntity::try_from(application_entity)
            .map_err(|error| record_error(status_from_registry_error(error)))?;

        let mut registry = self.registry.lock().await;
        let result = registry.insert_peer(application_entity).await;

        match result {
            Ok(()) => {
                record_ok();
                Ok(Response::new(proto::InsertPeerApplicationEntityResponse {}))
            }
            Err(error) => Err(record_error(status_from_registry_error(error))),
        }
    }
}

impl From<DomainLocalApplicationEntity> for proto::LocalApplicationEntity {
    fn from(application_entity: DomainLocalApplicationEntity) -> Self {
        Self {
            title: application_entity.title().to_string(),
            bind_address: application_entity.bind_addr().to_string(),
            max_concurrent_associations: application_entity.max_concurrent_associations() as u64,
            read_timeout_seconds: application_entity.read_timeout_seconds(),
            write_timeout_seconds: application_entity.write_timeout_seconds(),
            max_pdu_length: application_entity.max_pdu_length(),
        }
    }
}

impl TryFrom<proto::LocalApplicationEntity> for DomainLocalApplicationEntity {
    type Error = ApplicationEntityRegistryError;

    fn try_from(application_entity: proto::LocalApplicationEntity) -> Result<Self, Self::Error> {
        let max_concurrent_associations =
            usize::try_from(application_entity.max_concurrent_associations).map_err(|_| {
                ApplicationEntityRegistryError::MaxConcurrentAssociationsTooLarge(
                    application_entity.max_concurrent_associations,
                )
            })?;

        Self::try_new(
            application_entity.title.as_str(),
            application_entity.bind_address.as_str(),
            max_concurrent_associations,
            application_entity.read_timeout_seconds,
            application_entity.write_timeout_seconds,
            application_entity.max_pdu_length,
        )
        .map_err(Into::into)
    }
}

impl From<DomainPeerApplicationEntity> for proto::PeerApplicationEntity {
    fn from(application_entity: DomainPeerApplicationEntity) -> Self {
        Self {
            title: application_entity.title().to_string(),
            address: application_entity.addr().to_string(),
            connect_timeout_seconds: application_entity.connect_timeout_seconds(),
            read_timeout_seconds: application_entity.read_timeout_seconds(),
            write_timeout_seconds: application_entity.write_timeout_seconds(),
            max_pdu_length: application_entity.max_pdu_length(),
        }
    }
}

impl TryFrom<proto::PeerApplicationEntity> for DomainPeerApplicationEntity {
    type Error = ApplicationEntityRegistryError;

    fn try_from(application_entity: proto::PeerApplicationEntity) -> Result<Self, Self::Error> {
        Self::try_new(
            application_entity.title.as_str(),
            application_entity.address.as_str(),
            application_entity.connect_timeout_seconds,
            application_entity.read_timeout_seconds,
            application_entity.write_timeout_seconds,
            application_entity.max_pdu_length,
        )
        .map_err(Into::into)
    }
}

fn request_with_trace_context<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    inject_current_trace_context(request.metadata_mut());
    request
}

fn inject_current_trace_context(metadata: &mut MetadataMap) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MetadataInjector(metadata));
    });
}

fn set_current_span_parent(metadata: &MetadataMap) {
    let parent_context = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(metadata))
    });
    if parent_context.has_active_span() {
        let _ = Span::current().set_parent(parent_context);
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

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
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

fn status_from_registry_error(error: ApplicationEntityRegistryError) -> Status {
    match error {
        ApplicationEntityRegistryError::InvalidAe(error) => {
            Status::invalid_argument(error.to_string())
        }
        ApplicationEntityRegistryError::DuplicateAe(title) => {
            Status::already_exists(format!("duplicate application entity: {title}"))
        }
        ApplicationEntityRegistryError::GrpcStatus { code, message } => Status::new(code, message),
        ApplicationEntityRegistryError::MissingApplicationEntity => {
            Status::invalid_argument("missing application entity")
        }
        ApplicationEntityRegistryError::MaxConcurrentAssociationsTooLarge(value) => {
            Status::invalid_argument(format!(
                "max concurrent associations is too large for this platform: {value}"
            ))
        }
    }
}

fn record_status<T>(result: &Result<Response<T>, Status>) {
    match result {
        Ok(_) => record_ok(),
        Err(status) => {
            record_error_fields(status);
        }
    }
}

fn record_ok() {
    Span::current().record("rpc.response.status_code", code_name(tonic::Code::Ok));
}

fn record_error(status: Status) -> Status {
    record_error_fields(&status);
    status
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

#[cfg(test)]
mod tests {
    use opentelemetry::propagation::{Extractor, Injector};
    use tonic::Request;
    use tonic::metadata::MetadataMap;

    use super::{
        ApplicationEntityRegistryGrpcService, ApplicationEntityRegistryService,
        ApplicationEntityRegistryWriterGrpcService, ApplicationEntityRegistryWriterService,
        MetadataExtractor, MetadataInjector, proto,
    };
    use crate::{InMemoryApplicationEntityRegistry, local_ae, peer_ae};

    #[test]
    fn local_application_entity_round_trips_through_proto() {
        let application_entity = local_ae("LOCAL_AE", "127.0.0.1:11112");

        let proto_entity: proto::LocalApplicationEntity = application_entity.clone().into();
        let round_tripped: crate::LocalApplicationEntity =
            proto_entity.try_into().expect("proto should convert");

        assert_eq!(round_tripped, application_entity);
    }

    #[test]
    fn peer_application_entity_round_trips_through_proto() {
        let application_entity = peer_ae("PEER_AE", "192.0.2.10:104");

        let proto_entity: proto::PeerApplicationEntity = application_entity.clone().into();
        let round_tripped: crate::PeerApplicationEntity =
            proto_entity.try_into().expect("proto should convert");

        assert_eq!(round_tripped, application_entity);
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

    #[tokio::test]
    async fn grpc_server_adapter_uses_in_memory_registry() {
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");
        let registry = InMemoryApplicationEntityRegistry::try_new(
            vec![local_ae.clone()],
            vec![peer_ae.clone()],
        )
        .expect("registry should build");
        let service = ApplicationEntityRegistryGrpcService::new(registry);

        let local_response = service
            .get_local_application_entity(Request::new(proto::GetLocalApplicationEntityRequest {
                title: local_ae.title().to_string(),
            }))
            .await
            .expect("get local should succeed")
            .into_inner()
            .application_entity
            .expect("local AE should be present");

        assert_eq!(
            crate::LocalApplicationEntity::try_from(local_response)
                .expect("local proto should convert"),
            local_ae
        );

        let peer_response = service
            .list_peer_application_entities(Request::new(
                proto::ListPeerApplicationEntitiesRequest {},
            ))
            .await
            .expect("list peers should succeed")
            .into_inner()
            .application_entities;

        assert_eq!(peer_response.len(), 1);
        assert_eq!(
            crate::PeerApplicationEntity::try_from(peer_response.into_iter().next().unwrap())
                .expect("peer proto should convert"),
            peer_ae
        );
    }

    #[tokio::test]
    async fn grpc_writer_server_adapter_uses_writer_trait() {
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");
        let service = ApplicationEntityRegistryWriterGrpcService::new(
            InMemoryApplicationEntityRegistry::default(),
        );

        service
            .insert_local_application_entity(Request::new(
                proto::InsertLocalApplicationEntityRequest {
                    application_entity: Some(local_ae.into()),
                },
            ))
            .await
            .expect("insert local should succeed");

        service
            .insert_peer_application_entity(Request::new(
                proto::InsertPeerApplicationEntityRequest {
                    application_entity: Some(peer_ae.into()),
                },
            ))
            .await
            .expect("insert peer should succeed");
    }
}
