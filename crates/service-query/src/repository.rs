use async_trait::async_trait;

use crate::error::QueryRepositoryError;
use crate::query::{DicomQuery, QueryPage};

/// Read-side repository contract for DICOM catalog queries.
///
/// Implementors translate a [`DicomQuery`] (scope, predicate, projection,
/// paging) into backend-specific reads and return a page of projected
/// attribute sets. How predicates map to storage queries and how attributes
/// are projected is entirely the adapter's concern.
#[async_trait]
pub trait QueryRepository: Send + Sync {
    async fn execute(&self, query: &DicomQuery) -> Result<QueryPage, QueryRepositoryError>;
}
