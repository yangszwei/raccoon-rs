use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use raccoon_contract_dicom::{SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};

const DEFAULT_CAPACITY: usize = 1024;
const DEFAULT_TTL: Duration = Duration::from_secs(60);
const DEFAULT_REVISION_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub(crate) struct QidoJsonCache {
    inner: Arc<Mutex<CacheInner>>,
    capacity: usize,
    ttl: Duration,
    revision_check_interval: Duration,
}

#[derive(Debug)]
struct CacheInner {
    entries: HashMap<QidoJsonCacheKey, CacheEntry>,
    order: VecDeque<QidoJsonCacheKey>,
    read_model_revision: Option<u64>,
    revision_checked_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    bytes: Vec<u8>,
    inserted_at: Instant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QidoJsonCacheKey {
    route: &'static str,
    study_instance_uid: String,
    series_instance_uid: Option<String>,
    sop_instance_uid: Option<String>,
    url_origin: String,
    url_base_path: String,
}

impl Default for QidoJsonCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CacheInner {
                entries: HashMap::new(),
                order: VecDeque::new(),
                read_model_revision: None,
                revision_checked_at: None,
            })),
            capacity: DEFAULT_CAPACITY,
            ttl: DEFAULT_TTL,
            revision_check_interval: DEFAULT_REVISION_CHECK_INTERVAL,
        }
    }
}

impl QidoJsonCache {
    pub(crate) fn with_revision_check_interval(revision_check_interval: Duration) -> Self {
        Self {
            revision_check_interval,
            ..Self::default()
        }
    }

    pub(crate) fn revision_check_due(&self) -> bool {
        let inner = self.inner.lock().expect("QIDO JSON cache lock");
        inner
            .revision_checked_at
            .is_none_or(|checked_at| checked_at.elapsed() >= self.revision_check_interval)
    }

    pub(crate) fn record_read_model_revision(&self, revision: u64) {
        let mut inner = self.inner.lock().expect("QIDO JSON cache lock");
        if inner
            .read_model_revision
            .is_some_and(|known| known != revision)
        {
            inner.entries.clear();
            inner.order.clear();
        }
        inner.read_model_revision = Some(revision);
        inner.revision_checked_at = Some(Instant::now());
    }

    pub(crate) fn get(&self, key: &QidoJsonCacheKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock().expect("QIDO JSON cache lock");
        let entry = inner.entries.get(key)?;
        if entry.inserted_at.elapsed() > self.ttl {
            inner.entries.remove(key);
            return None;
        }
        Some(entry.bytes.clone())
    }

    pub(crate) fn insert(&self, key: QidoJsonCacheKey, bytes: Vec<u8>) {
        let mut inner = self.inner.lock().expect("QIDO JSON cache lock");
        if !inner.entries.contains_key(&key) {
            inner.order.push_back(key.clone());
        }
        inner.entries.insert(
            key,
            CacheEntry {
                bytes,
                inserted_at: Instant::now(),
            },
        );
        while inner.entries.len() > self.capacity {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            inner.entries.remove(&oldest);
        }
    }
}

impl QidoJsonCacheKey {
    pub(crate) fn study(
        route: &'static str,
        study: &StudyInstanceUid,
        url_origin: impl Into<String>,
        url_base_path: impl Into<String>,
    ) -> Self {
        Self {
            route,
            study_instance_uid: study.as_str().to_string(),
            series_instance_uid: None,
            sop_instance_uid: None,
            url_origin: url_origin.into(),
            url_base_path: url_base_path.into(),
        }
    }

    pub(crate) fn series(
        route: &'static str,
        study: &StudyInstanceUid,
        series: &SeriesInstanceUid,
        url_origin: impl Into<String>,
        url_base_path: impl Into<String>,
    ) -> Self {
        Self {
            route,
            study_instance_uid: study.as_str().to_string(),
            series_instance_uid: Some(series.as_str().to_string()),
            sop_instance_uid: None,
            url_origin: url_origin.into(),
            url_base_path: url_base_path.into(),
        }
    }

    pub(crate) fn instance(
        route: &'static str,
        study: &StudyInstanceUid,
        series: &SeriesInstanceUid,
        sop: &SopInstanceUid,
        url_origin: impl Into<String>,
        url_base_path: impl Into<String>,
    ) -> Self {
        Self {
            route,
            study_instance_uid: study.as_str().to_string(),
            series_instance_uid: Some(series.as_str().to_string()),
            sop_instance_uid: Some(sop.as_str().to_string()),
            url_origin: url_origin.into(),
            url_base_path: url_base_path.into(),
        }
    }
}
