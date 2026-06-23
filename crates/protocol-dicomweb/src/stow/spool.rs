use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::StreamExt;
use raccoon_contract_object_store::{ByteStream, ObjectStoreError};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::DicomWebError;

#[cfg(test)]
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub(crate) struct SpoolFile {
    path: PathBuf,
    size_bytes: u64,
}

impl SpoolFile {
    pub(crate) fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(crate) async fn byte_stream(&self) -> Result<ByteStream, DicomWebError> {
        let file = tokio::fs::File::open(&self.path).await.map_err(|error| {
            DicomWebError::Internal(format!("failed to open STOW spool file: {error}"))
        })?;
        Ok(ByteStream::new(ReaderStream::new(file).map(|result| {
            result.map_err(|source| {
                ObjectStoreError::backend_with_source("failed to read STOW spool file", source)
            })
        })))
    }
}

impl Drop for SpoolFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(crate) async fn spool_field(
    field: multer::Field<'_>,
    part_index: u64,
    max_size_bytes: Option<u64>,
) -> Result<SpoolFile, DicomWebError> {
    let path = std::env::temp_dir().join(format!(
        "raccoon-dicomweb-stow-{}-{part_index}",
        uuid::Uuid::new_v4()
    ));
    spool_field_at_path(field, path, max_size_bytes).await
}

async fn spool_field_at_path(
    mut field: multer::Field<'_>,
    path: PathBuf,
    max_size_bytes: Option<u64>,
) -> Result<SpoolFile, DicomWebError> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(|error| {
            DicomWebError::Internal(format!("failed to create STOW spool file: {error}"))
        })?;
    let mut size_bytes = 0_u64;
    loop {
        let next = match field.chunk().await {
            Ok(next) => next,
            Err(error) => {
                cleanup_path(&path).await;
                return Err(DicomWebError::bad_request(format!(
                    "invalid multipart part: {error}"
                )));
            }
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk_len = chunk.len() as u64;
        if let Some(max_size_bytes) = max_size_bytes
            && size_bytes.saturating_add(chunk_len) > max_size_bytes
        {
            cleanup_path(&path).await;
            return Err(DicomWebError::payload_too_large(format!(
                "STOW-RS DICOM part exceeds configured maximum of {max_size_bytes} bytes"
            )));
        }
        size_bytes += chunk_len;
        if let Err(error) = file.write_all(&chunk).await {
            cleanup_path(&path).await;
            return Err(DicomWebError::Internal(format!(
                "failed to write STOW spool file: {error}"
            )));
        }
    }
    if let Err(error) = file.flush().await {
        cleanup_path(&path).await;
        return Err(DicomWebError::Internal(format!(
            "failed to flush STOW spool file: {error}"
        )));
    }
    Ok(SpoolFile { path, size_bytes })
}

async fn cleanup_path(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_file_drop_removes_temp_file() {
        let path = std::env::temp_dir().join(format!(
            "raccoon-dicomweb-stow-drop-test-{}",
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, b"PHI").expect("write spool file");

        drop(SpoolFile {
            path: path.clone(),
            size_bytes: 0,
        });

        assert!(!path.exists(), "spool file should be removed on drop");
    }
}
