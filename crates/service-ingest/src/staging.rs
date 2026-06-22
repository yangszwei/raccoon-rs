use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::TryStreamExt;
use raccoon_contract_object_store::{ByteStream, Bytes, ObjectKey, ObjectStoreError, Stream};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::{IngestError, IngestObjectId};

const STAGED_READ_CHUNK_BYTES: usize = 1 << 20;

pub(crate) struct StagedIngestBody {
    path: PathBuf,
    pub(crate) content_length: u64,
    pub(crate) sha256: String,
}

impl StagedIngestBody {
    /// Spools the one-shot request stream so DICOM parsing and object storage can
    /// both read from a file without buffering the full object in memory.
    pub(crate) async fn write(
        ingest_object_id: IngestObjectId,
        error_object_key: ObjectKey,
        body: ByteStream,
        staging_dir: &Path,
        max_object_size: Option<u64>,
    ) -> Result<Self, IngestError> {
        tokio::fs::create_dir_all(staging_dir)
            .await
            .map_err(|source| IngestError::ObjectStore {
                ingest_object_id: ingest_object_id.clone(),
                object_key: error_object_key.clone(),
                source: ObjectStoreError::backend_with_source(
                    "failed to create ingest staging directory",
                    source,
                ),
            })?;
        let (path, mut file) =
            create_staging_file(staging_dir, &ingest_object_id, &error_object_key).await?;

        let write_result = async {
            let mut content_length = 0_u64;
            let mut hasher = Sha256::new();
            let mut stream = body.into_stream();
            let mut chunk_count = 0_u64;
            while let Some(chunk) =
                stream
                    .try_next()
                    .await
                    .map_err(|source| IngestError::ObjectStore {
                        ingest_object_id: ingest_object_id.clone(),
                        object_key: error_object_key.clone(),
                        source: ObjectStoreError::backend_with_source(
                            "failed to read ingest request body",
                            source,
                        ),
                    })?
            {
                chunk_count = chunk_count.saturating_add(1);
                let next_content_length = content_length + chunk.len() as u64;
                if let Some(max_content_length) = max_object_size
                    && next_content_length > max_content_length
                {
                    return Err(IngestError::ContentTooLarge {
                        ingest_object_id: ingest_object_id.clone(),
                        max_content_length,
                    });
                }

                hasher.update(&chunk);
                content_length = next_content_length;
                file.write_all(&chunk)
                    .await
                    .map_err(|source| IngestError::ObjectStore {
                        ingest_object_id: ingest_object_id.clone(),
                        object_key: error_object_key.clone(),
                        source: ObjectStoreError::backend_with_source(
                            "failed to write ingest staging file",
                            source,
                        ),
                    })?;
            }
            let span = tracing::Span::current();
            span.record("ingest.staging_chunk_count", chunk_count);
            span.record("ingest.content_length", content_length);
            Ok((content_length, hex_lower(&hasher.finalize())))
        }
        .await;
        drop(file);
        let (content_length, sha256) = match write_result {
            Ok(staged) => staged,
            Err(error) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(error);
            }
        };

        Ok(Self {
            path,
            content_length,
            sha256,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) async fn body_stream(
        &self,
        object_key: &ObjectKey,
    ) -> Result<ByteStream, ObjectStoreError> {
        let file = tokio::fs::File::open(&self.path).await.map_err(|source| {
            ObjectStoreError::backend_with_source(
                format!("failed to open staged ingest body for {object_key}"),
                source,
            )
        })?;
        Ok(ByteStream::new(FileByteStream {
            inner: ReaderStream::with_capacity(file, STAGED_READ_CHUNK_BYTES),
        }))
    }
}

async fn create_staging_file(
    staging_dir: &Path,
    ingest_object_id: &IngestObjectId,
    error_object_key: &ObjectKey,
) -> Result<(PathBuf, tokio::fs::File), IngestError> {
    for _ in 0..16 {
        let suffix = uuid::Uuid::now_v7();
        let path = staging_dir.join(format!("{ingest_object_id}-{suffix}.part"));
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(IngestError::ObjectStore {
                    ingest_object_id: ingest_object_id.clone(),
                    object_key: error_object_key.clone(),
                    source: ObjectStoreError::backend_with_source(
                        "failed to create ingest staging file",
                        source,
                    ),
                });
            }
        }
    }

    Err(IngestError::ObjectStore {
        ingest_object_id: ingest_object_id.clone(),
        object_key: error_object_key.clone(),
        source: ObjectStoreError::backend("failed to create unique ingest staging file"),
    })
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

impl Drop for StagedIngestBody {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct FileByteStream {
    inner: ReaderStream<tokio::fs::File>,
}

impl Stream for FileByteStream {
    type Item = raccoon_contract_object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(source))) => Poll::Ready(Some(Err(
                ObjectStoreError::backend_with_source("failed to read staged ingest body", source),
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
