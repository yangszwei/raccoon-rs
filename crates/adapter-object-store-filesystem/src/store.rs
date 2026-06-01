use std::path::{Path, PathBuf};

use async_trait::async_trait;
use raccoon_contract_object_store::{
    ByteStream, Object, ObjectKey, ObjectMetadata, ObjectStore, ObjectStoreError, PutResult, Result,
};
use tokio::fs::{self, File, OpenOptions};

use crate::body::{FileByteStream, write_body};
use crate::error::{map_io_error, map_io_error_with_message};
use crate::temp::{cleanup_temporary_files_under, create_temp_path};

/// Object store implementation backed by a local filesystem directory.
#[derive(Clone, Debug)]
pub struct FsObjectStore {
    root: PathBuf,
}

impl FsObjectStore {
    /// Creates a filesystem object store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the root directory used by this store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Recursively removes stale temporary files from interrupted uploads.
    pub async fn cleanup_temporary_files(&self) -> Result<usize> {
        cleanup_temporary_files_under(&self.root).await
    }

    fn path_for_key(&self, key: &ObjectKey) -> Result<PathBuf> {
        let mut path = self.root.clone();

        for segment in key.as_str().split('/') {
            path.push(segment);
        }

        Ok(path)
    }

    async fn metadata_for_key(&self, key: &ObjectKey) -> Result<ObjectMetadata> {
        let path = self.path_for_key(key)?;
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|err| map_io_error(err, Some(key)))?;

        if metadata.file_type().is_symlink() {
            return Err(ObjectStoreError::integrity(format!(
                "object path is a symbolic link: {key}"
            )));
        }

        if !metadata.is_file() {
            return Err(ObjectStoreError::integrity(format!(
                "object path is not a regular file: {key}"
            )));
        }

        Ok(ObjectMetadata::new(key.clone(), metadata.len()))
    }

    async fn ensure_safe_parent(&self, key: &ObjectKey) -> Result<()> {
        let mut path = self.root.clone();
        reject_symlink_or_non_directory(&path, "object store root").await?;

        let segments = key.as_str().split('/').collect::<Vec<_>>();
        for segment in segments.iter().take(segments.len().saturating_sub(1)) {
            path.push(segment);
            reject_symlink_or_non_directory(&path, "object parent directory").await?;
        }

        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FsObjectStore {
    async fn put(&self, key: ObjectKey, body: ByteStream) -> Result<PutResult> {
        let path = self.path_for_key(&key)?;
        let parent = path.parent().ok_or_else(|| {
            ObjectStoreError::invalid_request(format!("object key has no parent path: {key}"))
        })?;

        fs::create_dir_all(&self.root).await.map_err(|err| {
            map_io_error_with_message("failed to create object store root", err, None)
        })?;
        self.ensure_existing_parent_path_safe(&key).await?;
        fs::create_dir_all(parent).await.map_err(|err| {
            map_io_error_with_message("failed to create object directory", err, None)
        })?;
        self.ensure_safe_parent(&key).await?;
        ensure_destination_replaceable(&path, &key).await?;
        sync_directory(parent).await?;

        let temp_path = create_temp_path(parent);
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(|err| {
                map_io_error_with_message("failed to create temporary object file", err, None)
            })?;

        if let Err(err) = write_body(&mut temp_file, body).await {
            let _ = fs::remove_file(&temp_path).await;
            return Err(err);
        }

        if let Err(err) = temp_file.sync_all().await {
            let _ = fs::remove_file(&temp_path).await;
            return Err(map_io_error_with_message(
                "failed to sync temporary object file",
                err,
                None,
            ));
        }

        drop(temp_file);

        if let Err(err) = fs::rename(&temp_path, &path).await {
            let _ = fs::remove_file(&temp_path).await;
            return Err(map_io_error_with_message(
                "failed to move temporary object into place",
                err,
                None,
            ));
        }
        sync_directory(parent).await?;

        Ok(PutResult::new(self.metadata_for_key(&key).await?))
    }

    async fn get(&self, key: &ObjectKey) -> Result<Object> {
        let path = self.path_for_key(key)?;
        reject_symlink_object_path(&path, key).await?;
        let file = File::open(&path)
            .await
            .map_err(|err| map_io_error(err, Some(key)))?;
        let file_metadata = file
            .metadata()
            .await
            .map_err(|err| map_io_error(err, Some(key)))?;

        if !file_metadata.is_file() {
            return Err(ObjectStoreError::integrity(format!(
                "object path is not a regular file: {key}"
            )));
        }

        let metadata = ObjectMetadata::new(key.clone(), file_metadata.len());
        let body = ByteStream::new(FileByteStream::new(file));

        Ok(Object::new(metadata, body))
    }

    async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata> {
        self.metadata_for_key(key).await
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let path = self.path_for_key(key)?;
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(map_io_error(err, Some(key))),
        };

        if metadata.file_type().is_symlink() {
            return Err(ObjectStoreError::integrity(format!(
                "object path is a symbolic link: {key}"
            )));
        }

        if !metadata.is_file() {
            return Err(ObjectStoreError::integrity(format!(
                "object path is not a regular file: {key}"
            )));
        }

        fs::remove_file(&path)
            .await
            .map_err(|err| map_io_error(err, Some(key)))?;

        if let Some(parent) = path.parent() {
            sync_directory(parent).await?;
        }

        Ok(())
    }
}

impl FsObjectStore {
    async fn ensure_existing_parent_path_safe(&self, key: &ObjectKey) -> Result<()> {
        let mut path = self.root.clone();
        reject_symlink_or_non_directory(&path, "object store root").await?;

        let segments = key.as_str().split('/').collect::<Vec<_>>();
        for segment in segments.iter().take(segments.len().saturating_sub(1)) {
            path.push(segment);
            match fs::symlink_metadata(&path).await {
                Ok(metadata) => {
                    reject_directory_metadata(&path, "object parent directory", metadata)?
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(err) => {
                    return Err(map_io_error_with_message(
                        "failed to inspect object parent directory",
                        err,
                        None,
                    ));
                }
            }
        }

        Ok(())
    }
}

async fn reject_symlink_or_non_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).await.map_err(|err| {
        map_io_error_with_message(format!("failed to inspect {label}"), err, None)
    })?;

    reject_directory_metadata(path, label, metadata)
}

fn reject_directory_metadata(path: &Path, label: &str, metadata: std::fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        return Err(ObjectStoreError::integrity(format!(
            "{label} is a symbolic link: {}",
            path.display()
        )));
    }

    if !metadata.is_dir() {
        return Err(ObjectStoreError::integrity(format!(
            "{label} is not a directory: {}",
            path.display()
        )));
    }

    Ok(())
}

async fn ensure_destination_replaceable(path: &Path, key: &ObjectKey) -> Result<()> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(map_io_error_with_message(
                "failed to inspect object destination",
                err,
                Some(key),
            ));
        }
    };

    if metadata.file_type().is_symlink() {
        return Err(ObjectStoreError::integrity(format!(
            "object path is a symbolic link: {key}"
        )));
    }

    if !metadata.is_file() {
        return Err(ObjectStoreError::integrity(format!(
            "object path is not a regular file: {key}"
        )));
    }

    Ok(())
}

async fn reject_symlink_object_path(path: &Path, key: &ObjectKey) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|err| map_io_error(err, Some(key)))?;

    if metadata.file_type().is_symlink() {
        return Err(ObjectStoreError::integrity(format!(
            "object path is a symbolic link: {key}"
        )));
    }

    Ok(())
}

async fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let directory = File::open(path).await.map_err(|err| {
            map_io_error_with_message("failed to open directory for sync", err, None)
        })?;
        directory
            .sync_all()
            .await
            .map_err(|err| map_io_error_with_message("failed to sync directory", err, None))?;
    }

    #[cfg(not(unix))]
    {
        let _ = path;
    }

    Ok(())
}
