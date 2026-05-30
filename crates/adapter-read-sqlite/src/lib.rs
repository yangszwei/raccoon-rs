//! SQLite adapter for the CQRS read-side DICOM repository.
//!
//! [`SqliteReadRepository`] implements [`QueryRepository`] from
//! `raccoon-service-query` (and, once designed, `RetrieveRepository` from
//! `raccoon-service-retrieve`). It hosts all read-side repository trait
//! implementations for SQLite, keeping the CQRS read-database concern in one
//! crate regardless of how many services consume it.
//!
//! ## Opening a repository
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use raccoon_adapter_read_sqlite::SqliteReadRepository;
//!
//! let repo = SqliteReadRepository::open("sqlite:///var/raccoon/read.db").await?;
//! # Ok(()) }
//! ```
//!
//! ## Schema
//!
//! The database has three tables — `studies`, `series`, `instances` — populated
//! by the CQRS sync process from the ingest write database.  Attributes that
//! benefit from indexed filtering (patient ID, study date, modality, …) have
//! dedicated columns; everything else is stored as DICOM JSON in the
//! `instances.attributes` blob and queried via SQLite's `json_extract`.
//!
//! ## Fuzzy matching
//!
//! Fuzzy semantic matching for PN attributes ([`DicomQuery::fuzzy_matching`])
//! is not yet supported; it silently falls back to exact/wildcard matching.
//! Full phonetic PN matching requires either an FTS5 extension or a
//! pre-computed normalised column; a `TODO` is left in [`repository`] for a
//! future migration.

mod compile;
mod error;
mod project;
mod repository;
mod schema;

pub use error::SqliteReadRepositoryError;
pub use repository::SqliteReadRepository;
