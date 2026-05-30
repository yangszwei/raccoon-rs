//! Protocol-neutral DICOM Query/Retrieve service.
//!
//! This crate defines the filter language ([`Predicate`], [`MatchingRule`],
//! [`AttributePath`]), the canonical query request ([`DicomQuery`]), the
//! repository contract ([`QueryRepository`]), and the service behavior
//! ([`QueryService`]). Protocol crates translate their wire formats into
//! [`DicomQuery`] via bridges in `platform-orchestration`; this crate contains
//! no DICOM wire-format knowledge.

mod error;
mod filter;
#[cfg(feature = "grpc")]
mod grpc;
mod query;
mod repository;
mod service;

pub use error::{QueryError, QueryRepositoryError};
pub use filter::{
    AttributePath, AttributePathError, AttributePathSegment, MatchingRule, Predicate,
    PredicateError, RangeMatching, SequenceMatching,
};
#[cfg(feature = "grpc")]
pub use grpc::{
    DicomQueryService, DicomQueryServiceClient, DicomQueryServiceServer, GrpcQueryServiceClient,
    QueryGrpcService,
};
pub use query::{
    AttributeValue, DicomQuery, ProjectedAttribute, Projection, QueryMatch, QueryPage, QueryPaging,
    QueryRetrieveView, QueryScope, ResponseValue, SortDirection, SortKey,
};
pub use repository::QueryRepository;
pub use raccoon_contract_dicom::{PatientRootQueryRetrieveLevel, StudyRootQueryRetrieveLevel};
pub use service::{QueryService, StandardQueryService};
