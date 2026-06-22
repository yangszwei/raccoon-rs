use raccoon_adapter_ingest_repository_postgres::PostgresIngestRepository;

use crate::error::OrchestrationError;

/// Build a [`PostgresIngestRepository`] from a Postgres connection URL.
pub async fn build_postgres_ingest_repository(
    url: &str,
) -> Result<PostgresIngestRepository, OrchestrationError> {
    PostgresIngestRepository::open(url)
        .await
        .map_err(Into::into)
}
