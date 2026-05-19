use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use raccoon_contract_object_store::Result;
use tokio::fs;

use crate::error::map_io_error_with_message;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
const TEMP_FILE_PREFIX: &str = ".raccoon-object-";
const TEMP_FILE_SUFFIX: &str = ".tmp";
const STALE_TEMP_FILE_AGE: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) fn create_temp_path(parent: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    parent.join(format!(
        "{TEMP_FILE_PREFIX}{pid}-{now}-{counter}{TEMP_FILE_SUFFIX}",
        pid = process::id()
    ))
}

pub(crate) async fn cleanup_temporary_files_under(root: &Path) -> Result<usize> {
    let mut directories = vec![root.to_path_buf()];
    let mut removed = 0;

    while let Some(directory) = directories.pop() {
        let mut entries = match fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(map_io_error_with_message(
                    "failed to scan object directory",
                    err,
                    None,
                ));
            }
        };

        while let Some(entry) = entries.next_entry().await.map_err(|err| {
            map_io_error_with_message("failed to scan object directory", err, None)
        })? {
            let file_type = entry.file_type().await.map_err(|err| {
                map_io_error_with_message("failed to inspect object directory entry", err, None)
            })?;
            let path = entry.path();

            if file_type.is_dir() {
                directories.push(path);
                continue;
            }

            if !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if !file_name.starts_with(TEMP_FILE_PREFIX) || !file_name.ends_with(TEMP_FILE_SUFFIX) {
                continue;
            }

            let metadata = entry.metadata().await.map_err(|err| {
                map_io_error_with_message("failed to inspect temporary object file", err, None)
            })?;
            let Ok(modified) = metadata.modified() else {
                continue;
            };
            let Ok(age) = SystemTime::now().duration_since(modified) else {
                continue;
            };
            if age < STALE_TEMP_FILE_AGE {
                continue;
            }

            fs::remove_file(path).await.map_err(|err| {
                map_io_error_with_message("failed to remove temporary object file", err, None)
            })?;
            removed += 1;
        }
    }

    Ok(removed)
}
