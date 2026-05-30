//! Protocol-neutral DICOM Retrieve service.
//!
//! Defines the retrieve scope, repository contract, and service behavior.
//! Protocol crates translate their wire formats into [`RetrieveRequest`] via
//! bridges in `platform-orchestration`; this crate contains no wire-format
//! knowledge.
//!
//! Scope resolution is eager (the [`InstanceRef`] list is resolved upfront so
//! [`RetrieveResult::instance_count`] is available before streaming begins),
//! while body delivery is lazy (the object store is contacted one instance at a
//! time as the caller consumes [`RetrieveResult::stream`]).

mod error;
mod model;
mod repository;
mod scope;
mod service;

pub use error::{RetrieveError, RetrieveRepositoryError};
pub use model::{InstanceRef, RetrieveResult, RetrieveStream, RetrievedInstance};
pub use repository::RetrieveRepository;
pub use scope::{RetrieveRequest, RetrieveScope};
pub use service::{RetrieveService, StandardRetrieveService};
