//! Postgres adapter for the CQRS read-side DICOM repository.
//!
//! [`PostgresReadRepository`] implements [`QueryRepository`] from
//! `raccoon-service-query` and [`RetrieveRepository`] from
//! `raccoon-service-retrieve`. It hosts all read-side repository trait
//! implementations for Postgres, keeping the CQRS read-database concern in one
//! crate regardless of how many services consume it.
//!
//! ## Opening a repository
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use raccoon_adapter_read_postgres::PostgresReadRepository;
//!
//! let repo = PostgresReadRepository::open("postgres://raccoon:secret@localhost/raccoon").await?;
//! # Ok(()) }
//! ```
//!
//! ## Schema
//!
//! The database has three tables — `studies`, `series`, `instances` — populated
//! by the CQRS sync process from the ingest write database.  Attributes that
//! benefit from indexed filtering (patient ID, study date, modality, …) have
//! dedicated columns; everything else is stored as DICOM JSON in the
//! `instances.attributes` JSONB blob and queried via Postgres JSONB functions.
//!
//! ## Fuzzy matching
//!
//! Fuzzy semantic matching for PN attributes ([`DicomQuery::fuzzy_matching`])
//! uses the `fuzzystrmatch` extension's `soundex()` function as a pragmatic
//! phonetic approximation.

mod compile;
mod error;
mod project;
mod repository;
mod schema;

pub use error::PostgresReadRepositoryError;
pub use repository::PostgresReadRepository;
