use raccoon_contract_object_store::{ObjectKey, ObjectStoreError};

pub(crate) fn map_io_error(err: std::io::Error, key: Option<&ObjectKey>) -> ObjectStoreError {
    map_io_error_with_message("filesystem object store operation failed", err, key)
}

pub(crate) fn map_io_error_with_message(
    message: impl Into<String>,
    err: std::io::Error,
    key: Option<&ObjectKey>,
) -> ObjectStoreError {
    if err.kind() == std::io::ErrorKind::NotFound
        && let Some(key) = key
    {
        return ObjectStoreError::not_found(key.clone());
    }

    match err.kind() {
        std::io::ErrorKind::PermissionDenied => {
            ObjectStoreError::permission_denied_with_source(message, err)
        }
        std::io::ErrorKind::OutOfMemory
        | std::io::ErrorKind::StorageFull
        | std::io::ErrorKind::QuotaExceeded
        | std::io::ErrorKind::ResourceBusy
        | std::io::ErrorKind::ExecutableFileBusy
        | std::io::ErrorKind::Deadlock => {
            ObjectStoreError::unavailable_with_source(message, false, err)
        }
        std::io::ErrorKind::Interrupted | std::io::ErrorKind::TimedOut => {
            ObjectStoreError::unavailable_with_source(message, true, err)
        }
        _ => ObjectStoreError::backend_with_source(message, err),
    }
}
