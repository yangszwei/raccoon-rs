use std::sync::Arc;

use async_trait::async_trait;

use crate::error::QueryError;
use crate::query::{DicomQuery, QueryPage};
use crate::repository::QueryRepository;

/// Protocol-neutral DICOM Query/Retrieve service behavior.
#[async_trait]
pub trait QueryService: Send + Sync {
    async fn query(&self, request: DicomQuery) -> Result<QueryPage, QueryError>;
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
        self.repository
            .execute(&request)
            .await
            .map_err(QueryError::Repository)
    }
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
}
