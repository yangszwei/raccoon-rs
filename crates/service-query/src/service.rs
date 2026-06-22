use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::Instrument;

use crate::error::QueryError;
use crate::query::{DicomQuery, QueryPage};
use crate::repository::QueryRepository;

/// Protocol-neutral DICOM Query/Retrieve service behavior.
#[async_trait]
pub trait QueryService: Send + Sync {
    async fn query(&self, request: DicomQuery) -> Result<QueryPage, QueryError>;

    async fn read_model_revision(&self) -> Result<u64, QueryError>;
}

/// Repository-backed implementation of [`QueryService`].
pub struct StandardQueryService {
    repository: Arc<dyn QueryRepository>,
}

impl StandardQueryService {
    pub fn new(repository: Arc<dyn QueryRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl QueryService for StandardQueryService {
    async fn query(&self, request: DicomQuery) -> Result<QueryPage, QueryError> {
        let started_at = Instant::now();
        let page = self
            .repository
            .execute(&request)
            .instrument(tracing::info_span!(
                "query.service.repository",
                query.scope = ?request.scope(),
                query.has_predicate = request.predicate().is_some(),
                query.has_paging = request.paging().is_some(),
            ))
            .await
            .map_err(QueryError::Repository)?;
        tracing::info!(
            query.scope = ?request.scope(),
            query.result_count = page.items.len(),
            service.duration_ms = elapsed_ms(started_at),
            "Query service completed"
        );
        Ok(page)
    }

    async fn read_model_revision(&self) -> Result<u64, QueryError> {
        let started_at = Instant::now();
        let revision = self
            .repository
            .read_model_revision()
            .instrument(tracing::info_span!("query.service.read_model_revision"))
            .await
            .map_err(QueryError::Repository)?;
        tracing::info!(
            query.read_model_revision = revision,
            service.duration_ms = elapsed_ms(started_at),
            "Query service completed"
        );
        Ok(revision)
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use dicom_dictionary_std::tags;
    use raccoon_contract_dicom::StudyRootQueryRetrieveLevel;

    use super::*;
    use crate::error::QueryRepositoryError;
    use crate::filter::AttributePath;
    use crate::query::{DicomQuery, Projection, QueryMatch, QueryPage, QueryScope};

    fn study_query() -> DicomQuery {
        DicomQuery::new(
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
            Projection::Fields(vec![AttributePath::from_tag(tags::STUDY_INSTANCE_UID)]),
        )
        .expect("valid query")
    }

    struct FakeRepository {
        result: Result<QueryPage, QueryRepositoryError>,
    }

    impl FakeRepository {
        fn succeeding(items: Vec<QueryMatch>) -> Self {
            Self {
                result: Ok(QueryPage::new(items, 0, 50, None)),
            }
        }

        fn failing(message: &'static str) -> Self {
            Self {
                result: Err(QueryRepositoryError::new(message)),
            }
        }
    }

    #[async_trait]
    impl QueryRepository for FakeRepository {
        async fn execute(&self, _query: &DicomQuery) -> Result<QueryPage, QueryRepositoryError> {
            match &self.result {
                Ok(page) => Ok(QueryPage::new(
                    page.items.clone(),
                    page.offset,
                    page.limit,
                    page.total,
                )),
                Err(e) => Err(QueryRepositoryError::new(e.message())),
            }
        }

        async fn read_model_revision(&self) -> Result<u64, QueryRepositoryError> {
            Ok(7)
        }
    }

    #[tokio::test]
    async fn standard_service_returns_repository_results() {
        let matches = vec![QueryMatch::default(), QueryMatch::default()];
        let service = StandardQueryService::new(Arc::new(FakeRepository::succeeding(matches)));

        let page = service.query(study_query()).await.expect("query succeeds");

        assert_eq!(page.items.len(), 2);
    }

    #[tokio::test]
    async fn standard_service_propagates_repository_error_as_query_error() {
        let service =
            StandardQueryService::new(Arc::new(FakeRepository::failing("catalog offline")));

        let error = service
            .query(study_query())
            .await
            .expect_err("repository error propagates");

        assert!(matches!(error, QueryError::Repository(_)));
        assert!(error.to_string().contains("catalog offline"));
    }

    #[tokio::test]
    async fn standard_service_returns_read_model_revision() {
        let service = StandardQueryService::new(Arc::new(FakeRepository::succeeding(vec![])));

        let revision = service
            .read_model_revision()
            .await
            .expect("revision succeeds");

        assert_eq!(revision, 7);
    }
}
