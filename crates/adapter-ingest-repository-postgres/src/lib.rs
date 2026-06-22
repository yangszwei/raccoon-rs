//! Postgres adapter for the ingest write-side metadata repository.
//!
//! [`PostgresIngestRepository`] persists received ingest objects in Postgres.
//! Call [`PostgresIngestRepository::open`] to connect and run migrations in one
//! step, or [`PostgresIngestRepository::new`] to inject an existing pool.

mod error;
mod repository;

pub use error::PostgresIngestRepositoryError;
pub use repository::PostgresIngestRepository;
