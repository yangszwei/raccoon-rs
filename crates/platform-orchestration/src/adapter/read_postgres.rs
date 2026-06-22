use raccoon_adapter_read_postgres::PostgresReadRepository;

use crate::error::OrchestrationError;

/// Build a [`PostgresReadRepository`] from a Postgres connection URL.
pub async fn build_postgres_read_repository(
    url: &str,
) -> Result<PostgresReadRepository, OrchestrationError> {
    PostgresReadRepository::open(url).await.map_err(Into::into)
}
