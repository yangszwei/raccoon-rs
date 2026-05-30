use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::Tag;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tonic::codegen::{Body, Bytes, StdError};
use tonic::metadata::{AsciiMetadataKey, KeyRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};
use tracing::{Instrument, Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    AttributePath, AttributePathSegment, AttributeValue, DicomQuery, MatchingRule,
    PatientRootQueryRetrieveLevel, Predicate, ProjectedAttribute, Projection, QueryError,
    QueryMatch, QueryPage, QueryPaging, QueryRepositoryError, QueryRetrieveView, QueryScope,
    RangeMatching, ResponseValue, SequenceMatching, StudyRootQueryRetrieveLevel,
};

pub mod proto {
    #![allow(clippy::large_enum_variant)]
    tonic::include_proto!("raccoon.query.v1");
}

pub use proto::dicom_query_service_client::DicomQueryServiceClient;
pub use proto::dicom_query_service_server::{DicomQueryService, DicomQueryServiceServer};

const RPC_SYSTEM_NAME: &str = "grpc";
const RPC_SERVICE_QUERY: &str = "raccoon.query.v1.DicomQueryService";
const RPC_METHOD_QUERY: &str = "Query";
const METHOD_QUERY: &str = "raccoon.query.v1.DicomQueryService/Query";

/// gRPC-backed DICOM query service client.
///
/// Implements [`crate::QueryService`], translating domain [`DicomQuery`]
/// requests into proto messages and converting proto responses back into
/// [`QueryPage`].
#[derive(Debug)]
pub struct GrpcQueryServiceClient<T = tonic::transport::Channel> {
    inner: DicomQueryServiceClient<T>,
    server_address: Option<String>,
}

impl<T> GrpcQueryServiceClient<T> {
    /// Build a query client from a generated tonic client.
    pub fn new(inner: DicomQueryServiceClient<T>) -> Self {
        Self {
            inner,
            server_address: None,
        }
    }

    /// Build a query client and record the server address in client spans.
    pub fn with_server_address(
        inner: DicomQueryServiceClient<T>,
        server_address: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            server_address: Some(server_address.into()),
        }
    }
}

impl GrpcQueryServiceClient<tonic::transport::Channel> {
    /// Connect to a query service server endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let endpoint = tonic::transport::Endpoint::new(dst)?;
        let server_address = endpoint.uri().to_string();
        let inner = DicomQueryServiceClient::connect(endpoint).await?;
        Ok(Self::with_server_address(inner, server_address))
    }
}

#[async_trait]
impl<T> crate::QueryService for GrpcQueryServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::Future: Send,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    #[instrument(
        skip(self, request),
        fields(
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_QUERY,
            rpc.method = RPC_METHOD_QUERY,
            rpc.response.status_code = tracing::field::Empty,
            server.address = tracing::field::Empty,
            error.type = tracing::field::Empty,
        )
    )]
    async fn query(&self, request: DicomQuery) -> Result<QueryPage, QueryError> {
        // Record server.address only when known; an absent attribute is preferable
        // to an empty string, which OTel backends would treat as a distinct value.
        if let Some(addr) = &self.server_address {
            Span::current().record("server.address", addr.as_str());
        }
        let mut client = self.inner.clone();
        let proto_request = request_with_trace_context(proto::QueryRequest::from(request));
        let result = client.query(proto_request).await;

        // Record span status only after the full response (including body parse)
        // so that a successful transport but invalid body is not recorded as OK.
        let response = result.map_err(grpc_status_to_query_err)?;
        let page = QueryPage::try_from(response.into_inner()).map_err(grpc_status_to_query_err)?;

        record_ok();
        Ok(page)
    }
}

/// Tonic service adapter for a DICOM query service implementation.
pub struct QueryGrpcService {
    service: Arc<dyn crate::QueryService>,
}

impl QueryGrpcService {
    /// Build a service adapter for a query service implementation.
    pub fn new(service: impl crate::QueryService + 'static) -> Self {
        Self {
            service: Arc::new(service),
        }
    }

    /// Build a service adapter from a shared query service handle.
    pub fn from_shared(service: Arc<dyn crate::QueryService>) -> Self {
        Self { service }
    }

    /// Convert this adapter into the generated tonic server type.
    pub fn into_server(self) -> DicomQueryServiceServer<Self>
    where
        Self: DicomQueryService,
    {
        DicomQueryServiceServer::new(self)
    }
}

#[async_trait]
impl DicomQueryService for QueryGrpcService {
    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        // Create the span before entering it so set_span_parent_from_metadata can
        // write the remote parent into the OTel SpanBuilder — set_parent is a
        // no-op once the span has been entered.
        let rpc_span = tracing::info_span!(
            METHOD_QUERY,
            rpc.system.name = RPC_SYSTEM_NAME,
            rpc.service = RPC_SERVICE_QUERY,
            rpc.method = RPC_METHOD_QUERY,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        set_span_parent_from_metadata(&rpc_span, request.metadata());

        async move {
            // Handle parse failure explicitly so record_error() is always called
            // before returning — the `?` shorthand would exit before span fields
            // are written.
            let query = DicomQuery::try_from(request.into_inner()).map_err(record_error)?;

            match self.service.query(query).await {
                Ok(page) => {
                    record_ok();
                    Ok(Response::new(page.into()))
                }
                Err(QueryError::InvalidQuery(msg)) => {
                    Err(record_error(Status::invalid_argument(msg)))
                }
                Err(QueryError::Repository(e)) => {
                    Err(record_error(Status::internal(e.to_string())))
                }
            }
        }
        .instrument(rpc_span)
        .await
    }
}

impl From<QueryScope> for proto::QueryScope {
    fn from(scope: QueryScope) -> Self {
        match scope {
            QueryScope::PatientRoot(level) => Self {
                scope: Some(proto::query_scope::Scope::PatientRoot(
                    proto::PatientRootLevel::from(level) as i32,
                )),
            },
            QueryScope::StudyRoot(level) => Self {
                scope: Some(proto::query_scope::Scope::StudyRoot(
                    proto::StudyRootLevel::from(level) as i32,
                )),
            },
        }
    }
}

impl TryFrom<proto::QueryScope> for QueryScope {
    type Error = Status;

    fn try_from(scope: proto::QueryScope) -> Result<Self, Self::Error> {
        match scope.scope {
            Some(proto::query_scope::Scope::PatientRoot(level)) => {
                let level = proto::PatientRootLevel::try_from(level)
                    .map_err(|_| Status::invalid_argument("unknown patient root level"))?;
                Ok(QueryScope::PatientRoot(
                    PatientRootQueryRetrieveLevel::try_from(level)?,
                ))
            }
            Some(proto::query_scope::Scope::StudyRoot(level)) => {
                let level = proto::StudyRootLevel::try_from(level)
                    .map_err(|_| Status::invalid_argument("unknown study root level"))?;
                Ok(QueryScope::StudyRoot(
                    StudyRootQueryRetrieveLevel::try_from(level)?,
                ))
            }
            None => Err(Status::invalid_argument("query scope must be specified")),
        }
    }
}

impl From<PatientRootQueryRetrieveLevel> for proto::PatientRootLevel {
    fn from(level: PatientRootQueryRetrieveLevel) -> Self {
        match level {
            PatientRootQueryRetrieveLevel::Patient => Self::Patient,
            PatientRootQueryRetrieveLevel::Study => Self::Study,
            PatientRootQueryRetrieveLevel::Series => Self::Series,
            PatientRootQueryRetrieveLevel::Image => Self::Image,
        }
    }
}

impl TryFrom<proto::PatientRootLevel> for PatientRootQueryRetrieveLevel {
    type Error = Status;

    fn try_from(level: proto::PatientRootLevel) -> Result<Self, Self::Error> {
        match level {
            proto::PatientRootLevel::Patient => Ok(Self::Patient),
            proto::PatientRootLevel::Study => Ok(Self::Study),
            proto::PatientRootLevel::Series => Ok(Self::Series),
            proto::PatientRootLevel::Image => Ok(Self::Image),
            proto::PatientRootLevel::Unspecified => Err(Status::invalid_argument(
                "patient root level must be specified",
            )),
        }
    }
}

impl From<StudyRootQueryRetrieveLevel> for proto::StudyRootLevel {
    fn from(level: StudyRootQueryRetrieveLevel) -> Self {
        match level {
            StudyRootQueryRetrieveLevel::Study => Self::Study,
            StudyRootQueryRetrieveLevel::Series => Self::Series,
            StudyRootQueryRetrieveLevel::Image => Self::Image,
        }
    }
}

impl TryFrom<proto::StudyRootLevel> for StudyRootQueryRetrieveLevel {
    type Error = Status;

    fn try_from(level: proto::StudyRootLevel) -> Result<Self, Self::Error> {
        match level {
            proto::StudyRootLevel::Study => Ok(Self::Study),
            proto::StudyRootLevel::Series => Ok(Self::Series),
            proto::StudyRootLevel::Image => Ok(Self::Image),
            proto::StudyRootLevel::Unspecified => Err(Status::invalid_argument(
                "study root level must be specified",
            )),
        }
    }
}

fn tag_to_u32(tag: Tag) -> u32 {
    (tag.0 as u32) << 16 | (tag.1 as u32)
}

fn u32_to_tag(value: u32) -> Tag {
    Tag((value >> 16) as u16, value as u16)
}

impl From<&AttributePath> for proto::AttributePath {
    fn from(path: &AttributePath) -> Self {
        Self {
            segments: path
                .segments()
                .iter()
                .map(|s| match s {
                    AttributePathSegment::Tag(tag) => proto::AttributePathSegment {
                        segment: Some(proto::attribute_path_segment::Segment::Tag(tag_to_u32(
                            *tag,
                        ))),
                    },
                    AttributePathSegment::AnyItem => proto::AttributePathSegment {
                        segment: Some(proto::attribute_path_segment::Segment::AnyItem(
                            proto::AnyItem {},
                        )),
                    },
                })
                .collect(),
        }
    }
}

impl From<AttributePath> for proto::AttributePath {
    fn from(path: AttributePath) -> Self {
        (&path).into()
    }
}

impl TryFrom<proto::AttributePath> for AttributePath {
    type Error = Status;

    fn try_from(path: proto::AttributePath) -> Result<Self, Self::Error> {
        let mut segments = path.segments.into_iter();

        let first = segments
            .next()
            .ok_or_else(|| Status::invalid_argument("attribute path must not be empty"))?;

        let first_tag = match first.segment {
            Some(proto::attribute_path_segment::Segment::Tag(value)) => u32_to_tag(value),
            Some(proto::attribute_path_segment::Segment::AnyItem(_)) => {
                return Err(Status::invalid_argument(
                    "attribute path cannot start with an item selector",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "attribute path segment must be specified",
                ));
            }
        };

        let mut result = AttributePath::from_tag(first_tag);
        for segment in segments {
            match segment.segment {
                Some(proto::attribute_path_segment::Segment::Tag(value)) => {
                    result = result.push_tag(u32_to_tag(value));
                }
                Some(proto::attribute_path_segment::Segment::AnyItem(_)) => {
                    result = result.push_any();
                }
                None => {
                    return Err(Status::invalid_argument(
                        "attribute path segment must be specified",
                    ));
                }
            }
        }
        Ok(result)
    }
}

impl From<RangeMatching> for proto::RangeValue {
    fn from(range: RangeMatching) -> Self {
        Self {
            start: range.start().map(str::to_owned),
            end: range.end().map(str::to_owned),
        }
    }
}

impl TryFrom<proto::RangeValue> for RangeMatching {
    type Error = Status;

    fn try_from(range: proto::RangeValue) -> Result<Self, Self::Error> {
        match (range.start, range.end) {
            (Some(start), Some(end)) => Ok(Self::closed(start, end)),
            (Some(start), None) => Ok(Self::from_start(start)),
            (None, Some(end)) => Ok(Self::until_end(end)),
            (None, None) => Err(Status::invalid_argument(
                "range value must have at least one bound",
            )),
        }
    }
}

impl From<MatchingRule> for proto::MatchingRule {
    fn from(rule: MatchingRule) -> Self {
        let rule = match rule {
            MatchingRule::SingleValue(v) => proto::matching_rule::Rule::SingleValue(v),
            MatchingRule::Universal => proto::matching_rule::Rule::Universal(proto::Universal {}),
            MatchingRule::Wildcard(p) => proto::matching_rule::Rule::Wildcard(p),
            MatchingRule::Range(r) => proto::matching_rule::Rule::Range(r.into()),
            MatchingRule::DateTimeRange(r) => proto::matching_rule::Rule::DatetimeRange(r.into()),
            MatchingRule::UidList(uids) => {
                proto::matching_rule::Rule::UidList(proto::UidList { uids })
            }
            MatchingRule::MultipleValues(values) => {
                proto::matching_rule::Rule::MultipleValues(proto::MultipleValues { values })
            }
            MatchingRule::EmptyValue => {
                proto::matching_rule::Rule::EmptyValue(proto::EmptyValue {})
            }
            MatchingRule::Sequence(seq) => {
                proto::matching_rule::Rule::Sequence(Box::new(proto::SequenceRule {
                    predicate: Some(Box::new((*seq.predicate).into())),
                }))
            }
        };
        Self { rule: Some(rule) }
    }
}

impl TryFrom<proto::MatchingRule> for MatchingRule {
    type Error = Status;

    fn try_from(rule: proto::MatchingRule) -> Result<Self, Self::Error> {
        match rule.rule {
            Some(proto::matching_rule::Rule::SingleValue(v)) => Ok(Self::SingleValue(v)),
            Some(proto::matching_rule::Rule::Universal(_)) => Ok(Self::Universal),
            Some(proto::matching_rule::Rule::Wildcard(p)) => Ok(Self::Wildcard(p)),
            Some(proto::matching_rule::Rule::Range(r)) => {
                Ok(Self::Range(RangeMatching::try_from(r)?))
            }
            Some(proto::matching_rule::Rule::DatetimeRange(r)) => {
                Ok(Self::DateTimeRange(RangeMatching::try_from(r)?))
            }
            Some(proto::matching_rule::Rule::UidList(list)) => Ok(Self::UidList(list.uids)),
            Some(proto::matching_rule::Rule::MultipleValues(mv)) => {
                Ok(Self::MultipleValues(mv.values))
            }
            Some(proto::matching_rule::Rule::EmptyValue(_)) => Ok(Self::EmptyValue),
            Some(proto::matching_rule::Rule::Sequence(seq)) => {
                let predicate = seq
                    .predicate
                    .ok_or_else(|| Status::invalid_argument("sequence rule missing predicate"))?;
                let predicate = Predicate::try_from(*predicate)?;
                Ok(Self::Sequence(SequenceMatching {
                    predicate: Box::new(predicate),
                }))
            }
            None => Err(Status::invalid_argument("matching rule must be specified")),
        }
    }
}

impl From<Predicate> for proto::Predicate {
    fn from(predicate: Predicate) -> Self {
        let predicate = match predicate {
            Predicate::All(items) => proto::predicate::Predicate::All(proto::AllPredicate {
                items: items.into_iter().map(Into::into).collect(),
            }),
            Predicate::Attribute(path, rule) => {
                proto::predicate::Predicate::Attribute(Box::new(proto::AttributePredicate {
                    path: Some(path.into()),
                    rule: Some(Box::new(rule.into())),
                }))
            }
        };
        Self {
            predicate: Some(predicate),
        }
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = Status;

    fn try_from(predicate: proto::Predicate) -> Result<Self, Self::Error> {
        match predicate.predicate {
            Some(proto::predicate::Predicate::All(all)) => {
                let items = all
                    .items
                    .into_iter()
                    .map(Predicate::try_from)
                    .collect::<Result<_, _>>()?;
                Ok(Self::All(items))
            }
            Some(proto::predicate::Predicate::Attribute(attr)) => {
                let path = attr
                    .path
                    .ok_or_else(|| Status::invalid_argument("attribute predicate missing path"))?;
                let path = AttributePath::try_from(path)?;
                let rule = attr
                    .rule
                    .ok_or_else(|| Status::invalid_argument("attribute predicate missing rule"))?;
                let rule = MatchingRule::try_from(*rule)?;
                Ok(Self::Attribute(path, rule))
            }
            None => Err(Status::invalid_argument("predicate must be specified")),
        }
    }
}

impl From<Projection> for proto::Projection {
    fn from(projection: Projection) -> Self {
        let proj = match projection {
            Projection::Fields(paths) => {
                proto::projection::Projection::Fields(proto::FieldsProjection {
                    paths: paths.into_iter().map(Into::into).collect(),
                })
            }
            Projection::All => proto::projection::Projection::All(proto::AllProjection {}),
            Projection::Default => {
                proto::projection::Projection::Default(proto::DefaultProjection {})
            }
        };
        Self {
            projection: Some(proj),
        }
    }
}

impl TryFrom<proto::Projection> for Projection {
    type Error = Status;

    fn try_from(projection: proto::Projection) -> Result<Self, Self::Error> {
        match projection.projection {
            Some(proto::projection::Projection::Fields(fields)) => {
                let paths = fields
                    .paths
                    .into_iter()
                    .map(AttributePath::try_from)
                    .collect::<Result<_, _>>()?;
                Ok(Self::Fields(paths))
            }
            Some(proto::projection::Projection::All(_)) => Ok(Self::All),
            Some(proto::projection::Projection::Default(_)) => Ok(Self::Default),
            None => Err(Status::invalid_argument("projection must be specified")),
        }
    }
}

impl From<QueryRetrieveView> for proto::QueryRetrieveView {
    fn from(view: QueryRetrieveView) -> Self {
        match view {
            QueryRetrieveView::Classic => Self::Classic,
            QueryRetrieveView::Enhanced => Self::Enhanced,
        }
    }
}

impl TryFrom<proto::QueryRetrieveView> for QueryRetrieveView {
    type Error = Status;

    fn try_from(view: proto::QueryRetrieveView) -> Result<Self, Self::Error> {
        match view {
            proto::QueryRetrieveView::Classic => Ok(Self::Classic),
            proto::QueryRetrieveView::Enhanced => Ok(Self::Enhanced),
            proto::QueryRetrieveView::Unspecified => Err(Status::invalid_argument(
                "query retrieve view must be specified",
            )),
        }
    }
}

impl From<QueryPaging> for proto::QueryPaging {
    fn from(paging: QueryPaging) -> Self {
        Self {
            offset: paging.offset(),
            limit: paging.limit(),
        }
    }
}

impl TryFrom<proto::QueryPaging> for QueryPaging {
    type Error = Status;

    fn try_from(paging: proto::QueryPaging) -> Result<Self, Self::Error> {
        QueryPaging::new(paging.offset, paging.limit)
            .map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

impl From<DicomQuery> for proto::QueryRequest {
    fn from(query: DicomQuery) -> Self {
        Self {
            scope: Some(query.scope.into()),
            predicate: query.predicate.map(Into::into),
            projection: Some(query.projection.into()),
            paging: query.paging.map(Into::into),
            fuzzy_matching: query.fuzzy_matching,
            timezone_offset_from_utc: query.timezone_offset_from_utc,
            specific_character_set: query
                .specific_character_set
                .map(|values| proto::SpecificCharacterSet { values }),
            query_retrieve_view: query
                .query_retrieve_view
                .map(|v| proto::QueryRetrieveView::from(v) as i32),
            relational_query: query.relational_query,
        }
    }
}

impl TryFrom<proto::QueryRequest> for DicomQuery {
    type Error = Status;

    fn try_from(req: proto::QueryRequest) -> Result<Self, Self::Error> {
        let scope = QueryScope::try_from(
            req.scope
                .ok_or_else(|| Status::invalid_argument("missing scope"))?,
        )?;
        let projection = Projection::try_from(
            req.projection
                .ok_or_else(|| Status::invalid_argument("missing projection"))?,
        )?;

        let mut query = DicomQuery::new(scope, projection)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        if let Some(predicate) = req.predicate {
            let predicate = Predicate::try_from(predicate)?;
            query = query
                .with_predicate(predicate)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        if let Some(paging) = req.paging {
            let paging = QueryPaging::try_from(paging)?;
            query = query.with_paging(paging);
        }

        if req.fuzzy_matching {
            query = query.with_fuzzy_matching();
        }

        if let Some(offset) = req.timezone_offset_from_utc {
            query = query
                .with_timezone_offset(offset)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        if let Some(charset) = req.specific_character_set {
            query = query
                .with_specific_character_set(charset.values)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        if let Some(view_i32) = req.query_retrieve_view {
            let view = proto::QueryRetrieveView::try_from(view_i32)
                .map_err(|_| Status::invalid_argument("invalid query retrieve view value"))?;
            let view = QueryRetrieveView::try_from(view)?;
            query = query.with_query_retrieve_view(view);
        }

        if req.relational_query {
            query = query.with_relational_query();
        }

        Ok(query)
    }
}

impl From<AttributeValue> for proto::AttributeValue {
    fn from(value: AttributeValue) -> Self {
        let value = match value {
            AttributeValue::Text(s) => proto::attribute_value::Value::Text(s),
            AttributeValue::Texts(v) => {
                proto::attribute_value::Value::Texts(proto::TextList { values: v })
            }
            AttributeValue::Sequence(items) => {
                proto::attribute_value::Value::Sequence(proto::SequenceValue {
                    items: items.into_iter().map(Into::into).collect(),
                })
            }
            AttributeValue::Binary(b) => proto::attribute_value::Value::Binary(b),
        };
        Self { value: Some(value) }
    }
}

impl TryFrom<proto::AttributeValue> for AttributeValue {
    type Error = Status;

    fn try_from(value: proto::AttributeValue) -> Result<Self, Self::Error> {
        match value.value {
            Some(proto::attribute_value::Value::Text(s)) => Ok(Self::Text(s)),
            Some(proto::attribute_value::Value::Texts(list)) => Ok(Self::Texts(list.values)),
            Some(proto::attribute_value::Value::Sequence(seq)) => {
                let items = seq
                    .items
                    .into_iter()
                    .map(QueryMatch::try_from)
                    .collect::<Result<_, _>>()?;
                Ok(Self::Sequence(items))
            }
            Some(proto::attribute_value::Value::Binary(b)) => Ok(Self::Binary(b.to_vec())),
            None => Err(Status::invalid_argument(
                "attribute value must be specified",
            )),
        }
    }
}

impl From<ResponseValue> for proto::ResponseValue {
    fn from(value: ResponseValue) -> Self {
        let value = match value {
            ResponseValue::Present(v) => proto::response_value::Value::Present(v.into()),
            ResponseValue::ZeroLength => {
                proto::response_value::Value::ZeroLength(proto::ZeroLength {})
            }
            ResponseValue::Absent => proto::response_value::Value::Absent(proto::Absent {}),
        };
        Self { value: Some(value) }
    }
}

impl TryFrom<proto::ResponseValue> for ResponseValue {
    type Error = Status;

    fn try_from(value: proto::ResponseValue) -> Result<Self, Self::Error> {
        match value.value {
            Some(proto::response_value::Value::Present(v)) => {
                Ok(Self::Present(AttributeValue::try_from(v)?))
            }
            Some(proto::response_value::Value::ZeroLength(_)) => Ok(Self::ZeroLength),
            Some(proto::response_value::Value::Absent(_)) => Ok(Self::Absent),
            None => Err(Status::invalid_argument("response value must be specified")),
        }
    }
}

impl From<ProjectedAttribute> for proto::ProjectedAttribute {
    fn from(attr: ProjectedAttribute) -> Self {
        Self {
            path: Some(attr.path.into()),
            value: Some(attr.value.into()),
        }
    }
}

impl TryFrom<proto::ProjectedAttribute> for ProjectedAttribute {
    type Error = Status;

    fn try_from(attr: proto::ProjectedAttribute) -> Result<Self, Self::Error> {
        let path = attr
            .path
            .ok_or_else(|| Status::invalid_argument("projected attribute missing path"))?;
        let path = AttributePath::try_from(path)?;
        let value = attr
            .value
            .ok_or_else(|| Status::invalid_argument("projected attribute missing value"))?;
        let value = ResponseValue::try_from(value)?;
        Ok(Self { path, value })
    }
}

impl From<QueryMatch> for proto::QueryMatch {
    fn from(qm: QueryMatch) -> Self {
        Self {
            attributes: qm.into_attributes().into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::QueryMatch> for QueryMatch {
    type Error = Status;

    fn try_from(qm: proto::QueryMatch) -> Result<Self, Self::Error> {
        let attributes = qm
            .attributes
            .into_iter()
            .map(ProjectedAttribute::try_from)
            .collect::<Result<_, _>>()?;
        QueryMatch::new(attributes).map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

impl From<QueryPage> for proto::QueryResponse {
    fn from(page: QueryPage) -> Self {
        Self {
            items: page.items.into_iter().map(Into::into).collect(),
            offset: page.offset,
            limit: page.limit,
            total: page.total,
        }
    }
}

impl TryFrom<proto::QueryResponse> for QueryPage {
    type Error = Status;

    fn try_from(response: proto::QueryResponse) -> Result<Self, Self::Error> {
        // limit=0 is the proto3 wire default when the server omits the field —
        // a server-side protocol violation, not a client query error.
        if response.limit == 0 {
            return Err(Status::internal("server response missing limit field"));
        }
        let items = response
            .items
            .into_iter()
            .map(QueryMatch::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self::new(
            items,
            response.offset,
            response.limit,
            response.total,
        ))
    }
}

fn request_with_trace_context<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    inject_current_trace_context(request.metadata_mut());
    request
}

/// Maps a gRPC [`Status`] to the appropriate [`QueryError`] variant, recording
/// the error in the current span. Used for both the transport error path and the
/// response body-parse error path in [`GrpcQueryServiceClient`].
///
/// [`tonic::Code::InvalidArgument`] is preserved as [`QueryError::InvalidQuery`]
/// so that callers can distinguish client-side validation failures (e.g. HTTP 400)
/// from infrastructure failures (e.g. HTTP 500) without inspecting the message
/// string. All other codes become [`QueryError::Repository`].
fn grpc_status_to_query_err(status: Status) -> QueryError {
    record_error_fields(&status);
    match status.code() {
        tonic::Code::InvalidArgument => QueryError::InvalidQuery(status.message().to_owned()),
        _ => QueryError::Repository(QueryRepositoryError::new(status.message().to_owned())),
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
    use std::sync::Arc;

    use async_trait::async_trait;
    use dicom_dictionary_std::tags;
    use opentelemetry::propagation::{Extractor, Injector};
    use tonic::Request;
    use tonic::metadata::MetadataMap;

    use super::*;
    use crate::{
        AttributePath, AttributeValue, DicomQuery, MatchingRule, Predicate, ProjectedAttribute,
        Projection, QueryMatch, QueryPage, QueryPaging, QueryRepositoryError, QueryRetrieveView,
        QueryScope, RangeMatching, ResponseValue, StandardQueryService,
        StudyRootQueryRetrieveLevel,
    };

    fn study_scope() -> QueryScope {
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study)
    }

    #[test]
    fn patient_root_scope_round_trips_through_proto() {
        use crate::PatientRootQueryRetrieveLevel;
        let scope = QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient);
        let proto_scope = proto::QueryScope::from(scope);
        let recovered = QueryScope::try_from(proto_scope).expect("round trip should succeed");
        assert_eq!(recovered, scope);
    }

    #[test]
    fn study_root_scope_round_trips_through_proto() {
        let scope = study_scope();
        let proto_scope = proto::QueryScope::from(scope);
        let recovered = QueryScope::try_from(proto_scope).expect("round trip should succeed");
        assert_eq!(recovered, scope);
    }

    #[test]
    fn query_scope_missing_scope_returns_error() {
        let proto_scope = proto::QueryScope { scope: None };
        assert!(QueryScope::try_from(proto_scope).is_err());
    }

    #[test]
    fn simple_attribute_path_round_trips_through_proto() {
        let path = AttributePath::from_tag(tags::STUDY_INSTANCE_UID);
        let proto_path = proto::AttributePath::from(path.clone());
        let recovered = AttributePath::try_from(proto_path).expect("round trip should succeed");
        assert_eq!(recovered, path);
    }

    #[test]
    fn nested_attribute_path_round_trips_through_proto() {
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);
        let proto_path = proto::AttributePath::from(path.clone());
        let recovered = AttributePath::try_from(proto_path).expect("round trip should succeed");
        assert_eq!(recovered, path);
    }

    #[test]
    fn empty_attribute_path_returns_error() {
        let proto_path = proto::AttributePath { segments: vec![] };
        assert!(AttributePath::try_from(proto_path).is_err());
    }

    #[test]
    fn tag_encoding_preserves_group_and_element() {
        // PATIENT_ID = (0010, 0020)
        let tag = tags::PATIENT_ID;
        let encoded = tag_to_u32(tag);
        let decoded = u32_to_tag(encoded);
        assert_eq!(decoded, tag);
    }

    #[test]
    fn single_value_rule_round_trips_through_proto() {
        let rule = MatchingRule::SingleValue("CT".to_string());
        let proto_rule = proto::MatchingRule::from(rule.clone());
        let recovered = MatchingRule::try_from(proto_rule).expect("round trip should succeed");
        assert_eq!(recovered, rule);
    }

    #[test]
    fn range_rule_round_trips_through_proto() {
        let rule = MatchingRule::Range(RangeMatching::closed("20260101", "20261231"));
        let proto_rule = proto::MatchingRule::from(rule.clone());
        let recovered = MatchingRule::try_from(proto_rule).expect("round trip should succeed");
        assert_eq!(recovered, rule);
    }

    #[test]
    fn uid_list_rule_round_trips_through_proto() {
        let rule = MatchingRule::UidList(vec![
            "1.2.840.10008.5.1.4.1.1.4".to_string(),
            "1.2.840.10008.5.1.4.1.1.2".to_string(),
        ]);
        let proto_rule = proto::MatchingRule::from(rule.clone());
        let recovered = MatchingRule::try_from(proto_rule).expect("round trip should succeed");
        assert_eq!(recovered, rule);
    }

    #[test]
    fn sequence_rule_round_trips_through_proto() {
        let rule = MatchingRule::Sequence(crate::SequenceMatching {
            predicate: Box::new(Predicate::Attribute(
                AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
                MatchingRule::SingleValue("STEP-1".to_string()),
            )),
        });
        let proto_rule = proto::MatchingRule::from(rule.clone());
        let recovered = MatchingRule::try_from(proto_rule).expect("round trip should succeed");
        assert_eq!(recovered, rule);
    }

    #[test]
    fn datetime_range_rule_round_trips_through_proto() {
        let rule =
            MatchingRule::DateTimeRange(RangeMatching::from_start("20260101120000.000000+0000"));
        let proto_rule = proto::MatchingRule::from(rule.clone());
        let recovered = MatchingRule::try_from(proto_rule).expect("round trip should succeed");
        assert_eq!(recovered, rule);
    }

    #[test]
    fn predicate_single_value_round_trips_through_proto() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::PATIENT_ID),
            MatchingRule::SingleValue("P001".to_string()),
        );
        let proto_pred = proto::Predicate::from(predicate.clone());
        let recovered = Predicate::try_from(proto_pred).expect("round trip should succeed");
        assert_eq!(recovered, predicate);
    }

    #[test]
    fn predicate_all_round_trips_through_proto() {
        let predicate = Predicate::All(vec![
            Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("P001".to_string()),
            ),
            Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_DATE),
                MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
            ),
        ]);
        let proto_pred = proto::Predicate::from(predicate.clone());
        let recovered = Predicate::try_from(proto_pred).expect("round trip should succeed");
        assert_eq!(recovered, predicate);
    }

    #[test]
    fn fields_projection_round_trips_through_proto() {
        let projection = Projection::Fields(vec![
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            AttributePath::from_tag(tags::PATIENT_ID),
        ]);
        let proto_proj = proto::Projection::from(projection.clone());
        let recovered = Projection::try_from(proto_proj).expect("round trip should succeed");
        assert!(matches!(recovered, Projection::Fields(ref paths) if paths.len() == 2));
    }

    #[test]
    fn all_projection_round_trips_through_proto() {
        let proto_proj = proto::Projection::from(Projection::All);
        let recovered = Projection::try_from(proto_proj).expect("round trip should succeed");
        assert!(matches!(recovered, Projection::All));
    }

    #[test]
    fn default_projection_round_trips_through_proto() {
        let proto_proj = proto::Projection::from(Projection::Default);
        let recovered = Projection::try_from(proto_proj).expect("round trip should succeed");
        assert!(matches!(recovered, Projection::Default));
    }

    #[test]
    fn dicom_query_with_all_fields_round_trips_through_proto() {
        let query = DicomQuery::new(
            study_scope(),
            Projection::Fields(vec![AttributePath::from_tag(tags::STUDY_INSTANCE_UID)]),
        )
        .expect("valid query")
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_DATE),
            MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
        ))
        .expect("valid predicate")
        .with_paging(QueryPaging::new(10, 25).expect("valid paging"))
        .with_fuzzy_matching()
        .with_timezone_offset("+0530")
        .expect("valid timezone")
        .with_specific_character_set(vec!["ISO_IR 192".to_string()])
        .expect("valid charset")
        .with_query_retrieve_view(QueryRetrieveView::Classic)
        .with_relational_query();

        let proto_req = proto::QueryRequest::from(query.clone());
        let recovered = DicomQuery::try_from(proto_req).expect("round trip should succeed");

        assert_eq!(recovered.scope(), query.scope());
        assert!(recovered.predicate().is_some());
        assert_eq!(
            recovered.paging().map(|p| (p.offset(), p.limit())),
            Some((10, 25))
        );
        assert!(recovered.fuzzy_matching());
        assert_eq!(recovered.timezone_offset_from_utc(), Some("+0530"));
        assert_eq!(
            recovered.specific_character_set(),
            Some(["ISO_IR 192".to_string()].as_slice())
        );
        assert_eq!(
            recovered.query_retrieve_view(),
            Some(QueryRetrieveView::Classic)
        );
        assert!(recovered.relational_query());
    }

    #[test]
    fn dicom_query_missing_scope_returns_error() {
        let proto_req = proto::QueryRequest {
            scope: None,
            projection: Some(proto::Projection::from(Projection::Default)),
            ..Default::default()
        };
        assert!(DicomQuery::try_from(proto_req).is_err());
    }

    #[test]
    fn dicom_query_missing_projection_returns_error() {
        let proto_req = proto::QueryRequest {
            scope: Some(proto::QueryScope::from(study_scope())),
            projection: None,
            ..Default::default()
        };
        assert!(DicomQuery::try_from(proto_req).is_err());
    }

    #[test]
    fn query_match_with_mixed_response_values_round_trips_through_proto() {
        let qm = QueryMatch::new(vec![
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::PATIENT_ID),
                value: ResponseValue::Present(AttributeValue::Text("P001".to_string())),
            },
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                value: ResponseValue::ZeroLength,
            },
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::MODALITY),
                value: ResponseValue::Absent,
            },
        ])
        .expect("valid query match");

        let proto_qm = proto::QueryMatch::from(qm.clone());
        let recovered = QueryMatch::try_from(proto_qm).expect("round trip should succeed");
        assert_eq!(recovered, qm);
    }

    #[test]
    fn attribute_value_texts_round_trips_through_proto() {
        let value = AttributeValue::Texts(vec!["CT".to_string(), "PT".to_string()]);
        let proto_val = proto::AttributeValue::from(value.clone());
        let recovered = AttributeValue::try_from(proto_val).expect("round trip should succeed");
        assert_eq!(recovered, value);
    }

    #[test]
    fn attribute_value_binary_round_trips_through_proto() {
        let value = AttributeValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let proto_val = proto::AttributeValue::from(value.clone());
        let recovered = AttributeValue::try_from(proto_val).expect("round trip should succeed");
        assert_eq!(recovered, value);
    }

    #[test]
    fn attribute_value_sequence_round_trips_through_proto() {
        let inner = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            value: ResponseValue::Present(AttributeValue::Text("STEP-1".to_string())),
        }])
        .expect("valid inner match");
        let value = AttributeValue::Sequence(vec![inner]);

        let proto_val = proto::AttributeValue::from(value.clone());
        let recovered = AttributeValue::try_from(proto_val).expect("round trip should succeed");
        assert_eq!(recovered, value);
    }

    #[test]
    fn query_page_round_trips_through_proto() {
        let page = QueryPage::new(vec![], 20, 10, Some(100));
        let proto_resp = proto::QueryResponse::from(page);
        let recovered = QueryPage::try_from(proto_resp).expect("round trip should succeed");
        assert_eq!(recovered.offset, 20);
        assert_eq!(recovered.limit, 10);
        assert_eq!(recovered.total, Some(100));
        assert!(recovered.items.is_empty());
    }

    #[test]
    fn query_response_with_zero_limit_is_rejected() {
        let proto_resp = proto::QueryResponse {
            items: vec![],
            offset: 0,
            limit: 0,
            total: Some(0),
        };
        let err = QueryPage::try_from(proto_resp).expect_err("limit=0 must be rejected");
        assert_eq!(err.code(), tonic::Code::Internal);
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

    struct FakeQueryRepository {
        page: QueryPage,
    }

    #[async_trait]
    impl crate::QueryRepository for FakeQueryRepository {
        async fn execute(&self, _: &DicomQuery) -> Result<QueryPage, QueryRepositoryError> {
            Ok(self.page.clone())
        }
    }

    #[tokio::test]
    async fn grpc_server_adapter_returns_empty_page_from_service() {
        let repository = Arc::new(FakeQueryRepository {
            page: QueryPage::empty(0, 10),
        });
        let service = StandardQueryService::new(repository);
        let adapter = QueryGrpcService::new(service);

        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        let request = Request::new(proto::QueryRequest::from(query));

        let response = adapter.query(request).await.expect("query should succeed");
        let body = response.into_inner();
        assert_eq!(body.items.len(), 0);
        assert_eq!(body.total, Some(0));
        assert_eq!(body.offset, 0);
        assert_eq!(body.limit, 10);
    }

    #[tokio::test]
    async fn grpc_server_adapter_returns_populated_match() {
        let qm = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_ID),
            value: ResponseValue::Present(AttributeValue::Text("P001".to_string())),
        }])
        .expect("valid query match");

        let repository = Arc::new(FakeQueryRepository {
            page: QueryPage::new(vec![qm], 0, 10, Some(1)),
        });
        let service = StandardQueryService::new(repository);
        let adapter = QueryGrpcService::new(service);

        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        let request = Request::new(proto::QueryRequest::from(query));

        let response = adapter.query(request).await.expect("query should succeed");
        let body = response.into_inner();
        assert_eq!(body.items.len(), 1);
        assert_eq!(body.total, Some(1));
    }

    #[tokio::test]
    async fn grpc_server_adapter_maps_repository_error_to_internal_status() {
        struct FailingRepository;

        #[async_trait]
        impl crate::QueryRepository for FailingRepository {
            async fn execute(&self, _: &DicomQuery) -> Result<QueryPage, QueryRepositoryError> {
                Err(QueryRepositoryError::new("database offline"))
            }
        }

        let service = StandardQueryService::new(Arc::new(FailingRepository));
        let adapter = QueryGrpcService::new(service);

        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        let request = Request::new(proto::QueryRequest::from(query));

        let status = adapter.query(request).await.expect_err("should fail");
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn grpc_server_adapter_maps_invalid_request_to_invalid_argument_status() {
        // A QueryRequest with a missing scope should produce INVALID_ARGUMENT before
        // it reaches the service at all.
        let service = StandardQueryService::new(Arc::new(FakeQueryRepository {
            page: QueryPage::empty(0, 10),
        }));
        let adapter = QueryGrpcService::new(service);

        let bad_request = proto::QueryRequest {
            scope: None,
            projection: Some(proto::Projection::from(Projection::Default)),
            ..Default::default()
        };
        let request = Request::new(bad_request);
        let status = adapter.query(request).await.expect_err("should fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }
}
