use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use raccoon_adapter_object_store_filesystem::FsObjectStore;
use raccoon_contract_object_store::{
    ByteStream, Bytes, ObjectKey, ObjectStore, ObjectStoreError, Result, Stream,
};
use tempfile::TempDir;
use tokio::fs;

fn key(value: &str) -> ObjectKey {
    ObjectKey::new(value).expect("valid key")
}

fn store() -> (TempDir, FsObjectStore) {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = FsObjectStore::new(temp_dir.path());
    (temp_dir, store)
}

async fn collect_body(body: ByteStream) -> Result<Vec<u8>> {
    let mut stream = body.into_stream();
    let mut bytes = Vec::new();

    while let Some(chunk) = poll_fn(|context| stream.as_mut().poll_next(context)).await {
        bytes.extend_from_slice(&chunk?);
    }

    Ok(bytes)
}

#[tokio::test]
async fn put_creates_parent_directories_and_streams_body() {
    let (_temp_dir, store) = store();
    let key = key("studies/one/payload.dcm");

    let result = store
        .put(
            key.clone(),
            ByteStream::from_chunks([Bytes::from_static(b"pay"), Bytes::from_static(b"load")]),
        )
        .await
        .expect("put succeeds");

    assert_eq!(result.metadata.key, key);
    assert_eq!(result.metadata.content_length, 7);
    assert_eq!(
        fs::read(store.root().join("studies/one/payload.dcm"))
            .await
            .expect("object file"),
        b"payload"
    );
}

#[tokio::test]
async fn put_creates_missing_store_root() {
    let temp_dir = TempDir::new().expect("temp dir");
    let root = temp_dir.path().join("objects/ingest");
    let store = FsObjectStore::new(&root);
    let key = key("studies/one/payload.dcm");

    store
        .put(key.clone(), ByteStream::from("payload"))
        .await
        .expect("put succeeds");

    assert_eq!(
        fs::read(root.join("studies/one/payload.dcm"))
            .await
            .expect("object file"),
        b"payload"
    );
}

#[tokio::test]
async fn get_returns_metadata_and_streaming_body() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    store
        .put(key.clone(), ByteStream::from("payload"))
        .await
        .expect("put succeeds");

    let object = store.get(&key).await.expect("get succeeds");

    assert_eq!(object.metadata.content_length, 7);
    assert_eq!(
        collect_body(object.body).await.expect("body stream"),
        b"payload"
    );
}

#[tokio::test]
async fn head_returns_object_metadata_without_body() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    store
        .put(key.clone(), ByteStream::from("payload"))
        .await
        .expect("put succeeds");

    let metadata = store.head(&key).await.expect("head succeeds");

    assert_eq!(metadata.key, key);
    assert_eq!(metadata.content_length, 7);
    assert_eq!(metadata.etag, None);
}

#[tokio::test]
async fn delete_removes_object() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    store
        .put(key.clone(), ByteStream::from("payload"))
        .await
        .expect("put succeeds");
    store.delete(&key).await.expect("delete succeeds");

    assert!(matches!(
        store.head(&key).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
}

#[tokio::test]
async fn missing_objects_return_not_found_for_read_operations() {
    let (_temp_dir, store) = store();
    let key = key("missing");

    assert!(matches!(
        store.get(&key).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.head(&key).await,
        Err(ObjectStoreError::NotFound { .. })
    ));
}

#[tokio::test]
async fn delete_is_idempotent_for_missing_objects() {
    let (_temp_dir, store) = store();
    let key = key("missing");

    store.delete(&key).await.expect("missing delete succeeds");
}

#[tokio::test]
async fn put_replaces_existing_object() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    store
        .put(key.clone(), ByteStream::from("old"))
        .await
        .expect("first put succeeds");
    store
        .put(key.clone(), ByteStream::from("new payload"))
        .await
        .expect("second put succeeds");

    let object = store.get(&key).await.expect("get succeeds");

    assert_eq!(
        collect_body(object.body).await.expect("body stream"),
        b"new payload"
    );
}

#[tokio::test]
async fn failed_put_does_not_leave_object_or_temp_file() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    let err = store
        .put(
            key.clone(),
            ByteStream::new(FailingByteStream {
                chunks: vec![
                    Ok(Bytes::from_static(b"partial")),
                    Err(ObjectStoreError::backend("stream failed")),
                ]
                .into_iter(),
            }),
        )
        .await
        .expect_err("put should fail");

    assert_eq!(err.to_string(), "object store backend error: stream failed");
    assert!(matches!(
        store.head(&key).await,
        Err(ObjectStoreError::NotFound { .. })
    ));

    let mut entries = fs::read_dir(store.root().join("objects"))
        .await
        .expect("objects dir exists");
    assert!(entries.next_entry().await.expect("read entry").is_none());
}

#[tokio::test]
async fn cleanup_temporary_files_recursively_removes_stale_leftovers() {
    let (_temp_dir, store) = store();
    let root_temp_file = store.root().join(".raccoon-object-root.tmp");
    let nested_dir = store.root().join("studies/one");
    let nested_temp_file = nested_dir.join(".raccoon-object-nested.tmp");
    let fresh_temp_file = nested_dir.join(".raccoon-object-fresh.tmp");
    let regular_file = nested_dir.join("regular.tmp");
    fs::create_dir_all(&nested_dir).await.expect("nested dir");
    fs::write(&root_temp_file, b"temp")
        .await
        .expect("root temp file");
    fs::write(&nested_temp_file, b"temp")
        .await
        .expect("nested temp file");
    fs::write(&fresh_temp_file, b"fresh")
        .await
        .expect("fresh temp file");
    fs::write(&regular_file, b"regular")
        .await
        .expect("regular file");
    make_stale(&root_temp_file);
    make_stale(&nested_temp_file);

    let removed = store
        .cleanup_temporary_files()
        .await
        .expect("cleanup succeeds");

    assert_eq!(removed, 2);
    assert!(!root_temp_file.exists());
    assert!(!nested_temp_file.exists());
    assert!(fresh_temp_file.exists());
    assert!(regular_file.exists());
}

#[tokio::test]
async fn failed_replacement_keeps_existing_object() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    store
        .put(key.clone(), ByteStream::from("original"))
        .await
        .expect("initial put succeeds");

    let err = store
        .put(
            key.clone(),
            ByteStream::new(FailingByteStream {
                chunks: vec![
                    Ok(Bytes::from_static(b"replacement")),
                    Err(ObjectStoreError::backend("stream failed")),
                ]
                .into_iter(),
            }),
        )
        .await
        .expect_err("replacement should fail");

    assert_eq!(err.to_string(), "object store backend error: stream failed");

    let object = store.get(&key).await.expect("existing object remains");
    assert_eq!(
        collect_body(object.body).await.expect("body stream"),
        b"original"
    );
}

#[tokio::test]
async fn directory_at_object_path_is_integrity_error() {
    let (_temp_dir, store) = store();
    let key = key("objects/payload.bin");

    fs::create_dir_all(store.root().join("objects/payload.bin"))
        .await
        .expect("object directory");

    assert!(matches!(
        store.head(&key).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
    assert!(matches!(
        store.put(key.clone(), ByteStream::from("payload")).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
    assert!(matches!(
        store.delete(&key).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_root_is_rejected() {
    let temp_dir = TempDir::new().expect("temp dir");
    let target = temp_dir.path().join("target");
    let root_link = temp_dir.path().join("root-link");
    std::fs::create_dir(&target).expect("target dir");
    std::os::unix::fs::symlink(&target, &root_link).expect("root symlink");
    let store = FsObjectStore::new(root_link);

    let err = store
        .put(key("objects/payload.bin"), ByteStream::from("payload"))
        .await
        .expect_err("symlink root should fail");

    assert!(matches!(err, ObjectStoreError::Integrity { .. }));
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_parent_is_rejected_before_writing_through_it() {
    let (_temp_dir, store) = store();
    let outside = TempDir::new().expect("outside dir");
    fs::create_dir_all(store.root().join("objects"))
        .await
        .expect("objects dir");
    std::os::unix::fs::symlink(outside.path(), store.root().join("objects/link"))
        .expect("parent symlink");

    let err = store
        .put(key("objects/link/payload.bin"), ByteStream::from("payload"))
        .await
        .expect_err("symlink parent should fail");

    assert!(matches!(err, ObjectStoreError::Integrity { .. }));
    assert!(!outside.path().join("payload.bin").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_object_is_rejected() {
    let (_temp_dir, store) = store();
    let outside = TempDir::new().expect("outside dir");
    let outside_file = outside.path().join("payload.bin");
    std::fs::write(&outside_file, b"outside").expect("outside file");
    fs::create_dir_all(store.root().join("objects"))
        .await
        .expect("objects dir");
    std::os::unix::fs::symlink(&outside_file, store.root().join("objects/payload.bin"))
        .expect("object symlink");
    let key = key("objects/payload.bin");

    assert!(matches!(
        store.get(&key).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
    assert!(matches!(
        store.put(key.clone(), ByteStream::from("payload")).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
    assert!(matches!(
        store.delete(&key).await,
        Err(ObjectStoreError::Integrity { .. })
    ));
    assert_eq!(
        std::fs::read(&outside_file).expect("outside file remains"),
        b"outside"
    );
}

struct FailingByteStream {
    chunks: std::vec::IntoIter<Result<Bytes>>,
}

impl Stream for FailingByteStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.chunks.next())
    }
}

fn make_stale(path: &std::path::Path) {
    let stale_time = SystemTime::now() - Duration::from_secs(48 * 60 * 60);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open stale file");
    let times = std::fs::FileTimes::new().set_modified(stale_time);
    file.set_times(times).expect("set stale file time");
}
