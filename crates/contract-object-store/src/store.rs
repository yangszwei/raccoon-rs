use async_trait::async_trait;

use crate::Result;
use crate::model::{ByteStream, Object, ObjectKey, ObjectMetadata, PutResult};

/// Low-level object storage operations.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Stores an object body at the given key.
    async fn put(&self, key: ObjectKey, body: ByteStream) -> Result<PutResult>;

    /// Retrieves an object body and metadata by key.
    async fn get(&self, key: &ObjectKey) -> Result<Object>;

    /// Retrieves object metadata by key.
    async fn head(&self, key: &ObjectKey) -> Result<ObjectMetadata>;

    /// Deletes an object by key.
    ///
    /// Deleting a missing object succeeds.
    async fn delete(&self, key: &ObjectKey) -> Result<()>;
}
