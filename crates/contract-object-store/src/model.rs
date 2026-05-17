use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use bytes::Bytes;
use thiserror::Error;

use crate::{Result, Stream};

type BoxByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

/// Streaming object body used by the object store contract.
///
/// Bodies are consumed once as chunks of [`Bytes`]. This shape keeps large
/// objects out of memory and maps cleanly to Tokio IO adapters, S3/GCS response
/// bodies, and gRPC streaming messages.
#[must_use = "byte streams must be consumed to observe body errors"]
pub struct ByteStream {
    inner: BoxByteStream,
}

impl ByteStream {
    /// Creates a byte stream from any sendable stream of byte chunks.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Creates an empty byte stream.
    pub fn empty() -> Self {
        Self::from_chunks([])
    }

    /// Creates a byte stream containing one chunk.
    pub fn once(bytes: impl Into<Bytes>) -> Self {
        Self::from_chunks([bytes.into()])
    }

    /// Creates a byte stream from in-memory chunks.
    pub fn from_chunks(chunks: impl IntoIterator<Item = Bytes>) -> Self {
        Self::new(ChunkByteStream {
            chunks: chunks.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
        })
    }

    /// Consumes this body into its boxed stream.
    pub fn into_stream(self) -> BoxByteStream {
        self.inner
    }
}

struct ChunkByteStream {
    chunks: std::vec::IntoIter<Result<Bytes>>,
}

impl Stream for ChunkByteStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.chunks.next())
    }
}

impl From<Bytes> for ByteStream {
    fn from(bytes: Bytes) -> Self {
        Self::once(bytes)
    }
}

impl From<Vec<u8>> for ByteStream {
    fn from(bytes: Vec<u8>) -> Self {
        Self::once(bytes)
    }
}

impl From<&'static [u8]> for ByteStream {
    fn from(bytes: &'static [u8]) -> Self {
        Self::once(Bytes::from_static(bytes))
    }
}

impl From<String> for ByteStream {
    fn from(body: String) -> Self {
        Self::once(body)
    }
}

impl From<&'static str> for ByteStream {
    fn from(body: &'static str) -> Self {
        Self::once(body)
    }
}

impl fmt::Debug for ByteStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ByteStream").finish_non_exhaustive()
    }
}

impl Stream for ByteStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(context)
    }
}

/// Storage key for an object.
///
/// Keys are relative object names. They must not be empty, absolute paths, or
/// contain empty, current-directory, traversal, NUL, or backslash segments.
/// This keeps the contract portable across local filesystem and cloud object
/// stores.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates a validated object key.
    pub fn new(key: impl Into<String>) -> Result<Self, ObjectKeyError> {
        let key = key.into();
        validate_object_key(&key)?;
        Ok(Self(key))
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the key into its string representation.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ObjectKey {
    type Err = ObjectKeyError;

    fn from_str(key: &str) -> Result<Self, Self::Err> {
        Self::new(key)
    }
}

fn validate_object_key(key: &str) -> Result<(), ObjectKeyError> {
    if key.is_empty() {
        return Err(ObjectKeyError::Empty);
    }

    if key.starts_with('/') {
        return Err(ObjectKeyError::Absolute);
    }

    if key.contains('\0') || key.contains('\\') {
        return Err(ObjectKeyError::InvalidCharacter);
    }

    for segment in key.split('/') {
        if segment.is_empty() {
            return Err(ObjectKeyError::EmptySegment);
        }

        if matches!(segment, "." | "..") {
            return Err(ObjectKeyError::Traversal);
        }
    }

    Ok(())
}

/// Object key validation failures.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ObjectKeyError {
    /// Object keys must not be empty.
    #[error("object key must not be empty")]
    Empty,
    /// Object keys must be relative.
    #[error("object key must not be absolute")]
    Absolute,
    /// Object key segments must not be empty.
    #[error("object key segment must not be empty")]
    EmptySegment,
    /// Object keys must not contain current-directory or traversal segments.
    #[error("object key must not contain current-directory or traversal segments")]
    Traversal,
    /// Object keys must not contain NUL or backslash characters.
    #[error("object key must not contain NUL or backslash characters")]
    InvalidCharacter,
}

/// Stored object plus metadata.
#[derive(Debug)]
pub struct Object {
    /// Object metadata.
    pub metadata: ObjectMetadata,
    /// Object body.
    pub body: ByteStream,
}

impl Object {
    /// Creates an object from metadata and body.
    pub fn new(metadata: ObjectMetadata, body: impl Into<ByteStream>) -> Self {
        Self {
            metadata,
            body: body.into(),
        }
    }
}

/// Metadata returned by `head` and `get`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    /// Storage key for the object.
    pub key: ObjectKey,
    /// Object size in bytes.
    pub content_length: u64,
    /// Entity tag or content hash when the backing store provides one.
    pub etag: Option<String>,
}

impl ObjectMetadata {
    /// Creates metadata with the required primitive fields.
    pub fn new(key: ObjectKey, content_length: u64) -> Self {
        Self {
            key,
            content_length,
            etag: None,
        }
    }

    /// Sets the entity tag.
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }
}

/// Result returned after storing an object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutResult {
    /// Metadata for the stored object.
    pub metadata: ObjectMetadata,
}

impl PutResult {
    /// Creates a put result from metadata.
    pub fn new(metadata: ObjectMetadata) -> Self {
        Self { metadata }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use super::*;
    use crate::ObjectStoreError;

    #[test]
    fn object_key_accepts_relative_keys() {
        let key = ObjectKey::new("studies/1/payload.dcm").expect("valid key");

        assert_eq!(key.as_str(), "studies/1/payload.dcm");
    }

    #[test]
    fn object_key_rejects_empty_keys() {
        let err = ObjectKey::new("").expect_err("invalid key");

        assert_eq!(err, ObjectKeyError::Empty);
    }

    #[test]
    fn object_key_rejects_absolute_keys() {
        let err = ObjectKey::new("/studies/1").expect_err("invalid key");

        assert_eq!(err, ObjectKeyError::Absolute);
    }

    #[test]
    fn object_key_rejects_traversal_segments() {
        let err = ObjectKey::new("studies/../payload").expect_err("invalid key");

        assert_eq!(err, ObjectKeyError::Traversal);
    }

    #[test]
    fn object_key_rejects_current_directory_segments() {
        let err = ObjectKey::new("studies/./payload").expect_err("invalid key");

        assert_eq!(err, ObjectKeyError::Traversal);
    }

    #[test]
    fn object_key_rejects_empty_segments() {
        let err = ObjectKey::new("studies//payload").expect_err("invalid key");

        assert_eq!(err, ObjectKeyError::EmptySegment);
    }

    #[test]
    fn object_key_rejects_invalid_characters() {
        let backslash_err = ObjectKey::new("studies\\payload").expect_err("invalid key");
        let nul_err = ObjectKey::new("studies/\0/payload").expect_err("invalid key");

        assert_eq!(backslash_err, ObjectKeyError::InvalidCharacter);
        assert_eq!(nul_err, ObjectKeyError::InvalidCharacter);
    }

    #[tokio::test]
    async fn byte_stream_preserves_bytes() {
        let body = ByteStream::from("payload");

        let chunks = collect_stream(body).await.expect("stream should succeed");

        assert_eq!(chunks, vec![Bytes::from_static(b"payload")]);
    }

    #[tokio::test]
    async fn byte_stream_preserves_chunks() {
        let body =
            ByteStream::from_chunks([Bytes::from_static(b"pay"), Bytes::from_static(b"load")]);

        let chunks = collect_stream(body).await.expect("stream should succeed");

        assert_eq!(
            chunks,
            vec![Bytes::from_static(b"pay"), Bytes::from_static(b"load")]
        );
    }

    #[tokio::test]
    async fn byte_stream_preserves_stream_errors() {
        let body = ByteStream::new(FailingByteStream {
            chunks: vec![
                Ok(Bytes::from_static(b"pay")),
                Err(ObjectStoreError::backend("read failed")),
            ]
            .into_iter(),
        });

        let err = collect_stream(body).await.expect_err("stream should fail");

        assert_eq!(err.to_string(), "object store backend error: read failed");
    }

    async fn collect_stream(body: ByteStream) -> Result<Vec<Bytes>> {
        let mut stream = body.into_stream();
        let mut chunks = Vec::new();

        while let Some(chunk) = poll_fn(|context| stream.as_mut().poll_next(context)).await {
            chunks.push(chunk?);
        }

        Ok(chunks)
    }

    struct FailingByteStream {
        chunks: std::vec::IntoIter<Result<Bytes>>,
    }

    impl Stream for FailingByteStream {
        type Item = Result<Bytes>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.chunks.next())
        }
    }
}
