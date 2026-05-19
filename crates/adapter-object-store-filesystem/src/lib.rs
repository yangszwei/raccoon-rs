//! Filesystem adapter for the Raccoon object store contract.
//!
//! [`FsObjectStore`] stores object keys under a configured root directory,
//! streams object reads from files, and writes uploads through a temporary file
//! before renaming them into place. It rejects symlink-backed paths, removes
//! failed upload temporary files, and syncs files and directories on Unix
//! filesystems.

mod body;
mod error;
mod store;
mod temp;

pub use store::FsObjectStore;
