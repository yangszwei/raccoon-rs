//! SQLite adapter for the ingest write-side metadata repository.
//!
//! [`SqliteIngestRepository`] persists received ingest objects in a local SQLite
//! database. Call [`SqliteIngestRepository::open`] to connect and run migrations
//! in one step, or [`SqliteIngestRepository::new`] to inject an existing pool
//! (useful in tests).

mod error;
mod repository;

pub use error::SqliteIngestRepositoryError;
pub use repository::SqliteIngestRepository;
