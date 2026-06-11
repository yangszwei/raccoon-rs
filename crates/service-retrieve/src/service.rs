use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use raccoon_contract_object_store::ObjectStore;
use tracing::instrument;

use crate::error::{RetrieveError, RetrieveRepositoryError};
use crate::model::{InstanceRef, RetrieveResult, RetrievedInstance};
use crate::repository::RetrieveRepository;
use crate::scope::{RetrieveRequest, RetrieveScope};

/// Protocol-neutral DICOM Retrieve service behavior.
#[async_trait]
pub trait RetrieveService: Send + Sync {
    /// Resolves the retrieve scope and returns the instance count plus a lazy
    /// body stream.
    ///
    /// The scope is resolved eagerly against the repository so that
    /// [`RetrieveResult::instance_count`] is available before any object bytes
    /// are fetched. This lets bridges pre-announce sub-operation totals
    /// (C-MOVE Pending response, gRPC stream header) before streaming begins.
    ///
    /// A [`RetrieveError::Repository`] returned here means the scope could not
    /// be resolved; the stream was never started. Per-instance object store
    /// failures surface as [`RetrieveError::ObjectStore`] items within
    /// [`RetrieveResult::stream`].
    async fn retrieve(&self, request: RetrieveRequest) -> Result<RetrieveResult, RetrieveError>;
}

/// Object-store-backed implementation of [`RetrieveService`].
pub struct StandardRetrieveService {
    repository: Arc<dyn RetrieveRepository>,
    object_store: Arc<dyn ObjectStore>,
}

impl StandardRetrieveService {
    pub fn new(
        repository: Arc<dyn RetrieveRepository>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            repository,
            object_store,
        }
    }
}

#[async_trait]
impl RetrieveService for StandardRetrieveService {
    #[instrument(
        skip(self, request),
        fields(
            retrieve.scope = request.scope.label(),
            retrieve.instance_count = tracing::field::Empty,
            retrieve.total_content_length = tracing::field::Empty,
        )
    )]
    async fn retrieve(&self, request: RetrieveRequest) -> Result<RetrieveResult, RetrieveError> {
        let refs = resolve_scope(self.repository.as_ref(), &request.scope).await?;
        let instance_count = refs.len();
        let total_content_length = refs.iter().try_fold(0u64, |sum, r| {
            r.content_length.and_then(|len| sum.checked_add(len))
        });
        let span = tracing::Span::current();
        span.record("retrieve.instance_count", instance_count);
        if let Some(total) = total_content_length {
            span.record("retrieve.total_content_length", total);
        }
        let object_store = Arc::clone(&self.object_store);
        Ok(RetrieveResult {
            instance_count,
            total_content_length,
            stream: Box::pin(stream::iter(refs).then(move |ref_| {
                let store = Arc::clone(&object_store);
                async move {
                    let sop_instance_uid = ref_.identity.sop_instance_uid.clone();
                    store
                        .get(&ref_.object_key)
                        .await
                        .map(|obj| RetrievedInstance {
                            identity: ref_.identity,
                            transfer_syntax_uid: ref_.transfer_syntax_uid,
                            content_length: obj.metadata.content_length,
                            body: obj.body,
                        })
                        .map_err(|source| RetrieveError::ObjectStoreForInstance {
                            sop_instance_uid,
                            source,
                        })
                }
            })),
        })
    }
}

async fn resolve_scope(
    repository: &dyn RetrieveRepository,
    scope: &RetrieveScope,
) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
    match scope {
        RetrieveScope::Patient { patient_id } => {
            repository.find_instances_for_patient(patient_id).await
        }
        RetrieveScope::Study { study_instance_uid } => {
            repository
                .find_instances_for_study(study_instance_uid)
                .await
        }
        RetrieveScope::Series {
            study_instance_uid,
            series_instance_uid,
        } => match study_instance_uid {
            Some(study_uid) => {
                repository
                    .find_instances_for_study_series(study_uid, series_instance_uid)
                    .await
            }
            None => {
                repository
                    .find_instances_for_series(series_instance_uid)
                    .await
            }
        },
        RetrieveScope::Instance {
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        } => repository
            .find_instance_in_scope(
                study_instance_uid.as_ref(),
                series_instance_uid.as_ref(),
                sop_instance_uid,
            )
            .await
            .map(|opt| opt.into_iter().collect()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use futures_util::StreamExt;
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, PatientId, SeriesInstanceUid, SopClassUid, SopInstanceUid,
        StudyInstanceUid, TransferSyntaxUid,
    };
    use raccoon_contract_object_store::{
        ByteStream, Bytes, Object, ObjectKey, ObjectMetadata, ObjectStore, ObjectStoreError,
        PutResult,
    };

    use super::*;
    use crate::error::RetrieveRepositoryError;

    // — Fakes ——————————————————————————————————————————————————————————————

    #[derive(Default)]
    struct FakeRepository {
        patient_refs: Vec<InstanceRef>,
        study_refs: Vec<InstanceRef>,
        series_refs: Vec<InstanceRef>,
        instance_ref: Option<InstanceRef>,
        fail: Option<&'static str>,
    }

    #[async_trait]
    impl RetrieveRepository for FakeRepository {
        async fn find_instances_for_patient(
            &self,
            _patient_id: &PatientId,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            self.check_fail()?;
            Ok(self.patient_refs.clone())
        }

        async fn find_instances_for_study(
            &self,
            _uid: &StudyInstanceUid,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            self.check_fail()?;
            Ok(self.study_refs.clone())
        }

        async fn find_instances_for_series(
            &self,
            _uid: &SeriesInstanceUid,
        ) -> Result<Vec<InstanceRef>, RetrieveRepositoryError> {
            self.check_fail()?;
            Ok(self.series_refs.clone())
        }

        async fn find_instance(
            &self,
            _uid: &SopInstanceUid,
        ) -> Result<Option<InstanceRef>, RetrieveRepositoryError> {
            self.check_fail()?;
            Ok(self.instance_ref.clone())
        }
    }

    impl FakeRepository {
        fn check_fail(&self) -> Result<(), RetrieveRepositoryError> {
            match self.fail {
                Some(msg) => Err(RetrieveRepositoryError::new(msg)),
                None => Ok(()),
            }
        }
    }

    struct FakeObjectStore {
        bodies: HashMap<ObjectKey, Bytes>,
        fail_key: Option<ObjectKey>,
    }

    impl FakeObjectStore {
        fn new(bodies: impl IntoIterator<Item = (ObjectKey, Bytes)>) -> Self {
            Self {
                bodies: bodies.into_iter().collect(),
                fail_key: None,
            }
        }

        fn failing_on(mut self, key: ObjectKey) -> Self {
            self.fail_key = Some(key);
            self
        }
    }

    #[async_trait]
    impl ObjectStore for FakeObjectStore {
        async fn put(
            &self,
            _key: ObjectKey,
            _body: ByteStream,
        ) -> raccoon_contract_object_store::Result<PutResult> {
            unimplemented!("retrieve tests do not put objects")
        }

        async fn get(&self, key: &ObjectKey) -> raccoon_contract_object_store::Result<Object> {
            if self.fail_key.as_ref() == Some(key) {
                return Err(ObjectStoreError::backend("simulated read failure"));
            }
            let body = self
                .bodies
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            let content_length = body.len() as u64;
            Ok(Object::new(
                ObjectMetadata::new(key.clone(), content_length),
                ByteStream::once(body),
            ))
        }

        async fn head(
            &self,
            key: &ObjectKey,
        ) -> raccoon_contract_object_store::Result<ObjectMetadata> {
            let body = self
                .bodies
                .get(key)
                .ok_or_else(|| ObjectStoreError::not_found(key.clone()))?;
            Ok(ObjectMetadata::new(key.clone(), body.len() as u64))
        }

        async fn delete(&self, _key: &ObjectKey) -> raccoon_contract_object_store::Result<()> {
            unimplemented!("retrieve tests do not delete objects")
        }
    }

    // — Helpers ————————————————————————————————————————————————————————————

    fn uid(suffix: &str) -> StudyInstanceUid {
        StudyInstanceUid::new(format!("1.2.3.{suffix}")).expect("valid UID")
    }

    fn series_uid(suffix: &str) -> SeriesInstanceUid {
        SeriesInstanceUid::new(format!("1.2.3.4.{suffix}")).expect("valid UID")
    }

    fn sop_uid(suffix: &str) -> SopInstanceUid {
        SopInstanceUid::new(format!("1.2.3.4.5.{suffix}")).expect("valid UID")
    }

    fn sop_class() -> SopClassUid {
        SopClassUid::new("1.2.840.10008.5.1.4.1.1.4").expect("valid UID")
    }

    fn ts_uid() -> TransferSyntaxUid {
        TransferSyntaxUid::new("1.2.840.10008.1.2.1").expect("valid UID")
    }

    fn instance_ref(n: u8) -> InstanceRef {
        let identity = DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.4").unwrap(),
            sop_uid(&n.to_string()),
            sop_class(),
        );
        let key = ObjectKey::new(format!("instances/{n}")).unwrap();
        InstanceRef::new(identity, key)
            .with_transfer_syntax(ts_uid())
            .with_content_length(512)
    }

    fn object_store_with(refs: &[InstanceRef]) -> FakeObjectStore {
        let bodies = refs.iter().map(|r| {
            let payload = Bytes::from(format!("payload-{}", r.object_key));
            (r.object_key.clone(), payload)
        });
        FakeObjectStore::new(bodies)
    }

    fn service(repo: FakeRepository, store: FakeObjectStore) -> StandardRetrieveService {
        StandardRetrieveService::new(Arc::new(repo), Arc::new(store))
    }

    async fn collect_stream(
        result: RetrieveResult,
    ) -> Vec<Result<RetrievedInstance, RetrieveError>> {
        result.stream.collect().await
    }

    // — Tests ——————————————————————————————————————————————————————————————

    #[tokio::test]
    async fn study_scope_streams_all_instances() {
        let refs = vec![instance_ref(1), instance_ref(2), instance_ref(3)];
        let repo = FakeRepository {
            study_refs: refs.clone(),
            ..Default::default()
        };
        let store = object_store_with(&refs);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 3);
        // All three refs carry content_length = 512 (set by instance_ref helper).
        assert_eq!(result.total_content_length, Some(3 * 512));
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 3);
        assert!(items.iter().all(|i| i.is_ok()));
    }

    #[tokio::test]
    async fn series_scope_streams_instances_for_series() {
        let refs = vec![instance_ref(1), instance_ref(2)];
        let repo = FakeRepository {
            series_refs: refs.clone(),
            ..Default::default()
        };
        let store = object_store_with(&refs);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Series {
                study_instance_uid: None,
                series_instance_uid: series_uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 2);
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn scoped_series_filters_by_parent_study_before_streaming() {
        let matching = instance_ref(1);
        let mut other_study = instance_ref(2);
        other_study.identity.study_instance_uid = uid("99");
        let repo = FakeRepository {
            series_refs: vec![matching.clone(), other_study.clone()],
            ..Default::default()
        };
        let store = object_store_with(&[matching.clone(), other_study]);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Series {
                study_instance_uid: Some(matching.identity.study_instance_uid.clone()),
                series_instance_uid: matching.identity.series_instance_uid.clone(),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 1);
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].as_ref().unwrap().identity.sop_instance_uid,
            matching.identity.sop_instance_uid
        );
    }

    #[tokio::test]
    async fn instance_scope_streams_single_instance() {
        let ref_ = instance_ref(1);
        let repo = FakeRepository {
            instance_ref: Some(ref_.clone()),
            ..Default::default()
        };
        let store = object_store_with(&[ref_]);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid: sop_uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 1);
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 1);
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn scoped_instance_rejects_mismatched_parent_before_streaming() {
        let ref_ = instance_ref(1);
        let repo = FakeRepository {
            instance_ref: Some(ref_.clone()),
            ..Default::default()
        };
        let store = object_store_with(std::slice::from_ref(&ref_));
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Instance {
                study_instance_uid: Some(uid("99")),
                series_instance_uid: Some(ref_.identity.series_instance_uid.clone()),
                sop_instance_uid: ref_.identity.sop_instance_uid.clone(),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 0);
        assert!(collect_stream(result).await.is_empty());
    }

    #[tokio::test]
    async fn patient_scope_streams_all_patient_instances() {
        let refs = vec![instance_ref(1), instance_ref(2)];
        let repo = FakeRepository {
            patient_refs: refs.clone(),
            ..Default::default()
        };
        let store = object_store_with(&refs);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Patient {
                patient_id: PatientId::new("PAT-001").unwrap(),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 2);
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn retrieved_instance_carries_correct_metadata() {
        let ref_ = instance_ref(1);
        let repo = FakeRepository {
            instance_ref: Some(ref_.clone()),
            ..Default::default()
        };
        let store = object_store_with(std::slice::from_ref(&ref_));
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid: sop_uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        let items = collect_stream(result).await;
        let instance = items.into_iter().next().unwrap().expect("ok");
        assert_eq!(instance.identity, ref_.identity);
        assert_eq!(instance.transfer_syntax_uid, ref_.transfer_syntax_uid);
        assert!(instance.content_length > 0);
    }

    #[tokio::test]
    async fn empty_scope_returns_zero_count_and_empty_stream() {
        let repo = FakeRepository::default();
        let store = FakeObjectStore::new([]);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("999"),
            }))
            .await
            .expect("retrieve succeeds even when scope is empty");

        assert_eq!(result.instance_count, 0);
        assert_eq!(result.total_content_length, Some(0));
        let items = collect_stream(result).await;
        assert!(items.is_empty());
    }

    // Blank and whitespace patient IDs are rejected by PatientId::new() at
    // construction time (see contract-dicom tests). No service-level guard needed.

    #[tokio::test]
    async fn total_content_length_is_none_when_any_ref_lacks_size() {
        // Build refs where only some have content_length set.
        let ref_with_size = instance_ref(1); // has content_length = 512
        let mut ref_without_size = instance_ref(2);
        ref_without_size.content_length = None;

        let refs = vec![ref_with_size, ref_without_size];
        let repo = FakeRepository {
            study_refs: refs.clone(),
            ..Default::default()
        };
        let store = object_store_with(&refs);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        assert_eq!(result.instance_count, 2);
        assert_eq!(result.total_content_length, None);
    }

    #[tokio::test]
    async fn instance_scope_missing_returns_zero_count_and_empty_stream() {
        let repo = FakeRepository {
            instance_ref: None,
            ..Default::default()
        };
        let store = FakeObjectStore::new([]);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid: sop_uid("999"),
            }))
            .await
            .expect("retrieve succeeds even when instance is absent");

        assert_eq!(result.instance_count, 0);
        let items = collect_stream(result).await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn repository_failure_surfaces_before_stream() {
        let repo = FakeRepository {
            fail: Some("catalog offline"),
            ..Default::default()
        };
        let store = FakeObjectStore::new([]);
        let svc = service(repo, store);

        let error = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("1"),
            }))
            .await
            .expect_err("repository failure propagates from retrieve()");

        assert!(matches!(error, RetrieveError::Repository(_)));
        assert!(error.to_string().contains("catalog offline"));
    }

    #[tokio::test]
    async fn object_store_failure_surfaces_as_stream_item_error() {
        let refs = vec![instance_ref(1), instance_ref(2)];
        let failing_key = refs[0].object_key.clone();
        let repo = FakeRepository {
            study_refs: refs.clone(),
            ..Default::default()
        };
        // Only refs[1] is in the store; refs[0] will trigger a failure.
        let store =
            FakeObjectStore::new([(refs[1].object_key.clone(), Bytes::from_static(b"payload"))])
                .failing_on(failing_key);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("1"),
            }))
            .await
            .expect("retrieve() itself succeeds");

        assert_eq!(result.instance_count, 2);
        let items = collect_stream(result).await;
        assert_eq!(items.len(), 2);
        // First item fails with its SOP Instance UID preserved, second succeeds.
        assert!(matches!(
            &items[0],
            Err(RetrieveError::ObjectStoreForInstance {
                sop_instance_uid,
                ..
            }) if sop_instance_uid.as_str() == refs[0].identity.sop_instance_uid.as_str()
        ));
        assert!(items[1].is_ok());
    }

    #[tokio::test]
    async fn stream_delivers_body_bytes() {
        let ref_ = instance_ref(1);
        let payload = Bytes::from_static(b"dicom-payload");
        let repo = FakeRepository {
            instance_ref: Some(ref_.clone()),
            ..Default::default()
        };
        let store = FakeObjectStore::new([(ref_.object_key.clone(), payload.clone())]);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Instance {
                study_instance_uid: None,
                series_instance_uid: None,
                sop_instance_uid: sop_uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        let mut items = collect_stream(result).await;
        let mut instance = items.remove(0).expect("ok");

        let mut collected = Vec::new();
        use futures_util::StreamExt as _;
        while let Some(chunk) = instance.body.next().await {
            collected.extend_from_slice(&chunk.expect("no chunk error"));
        }
        assert_eq!(collected, b"dicom-payload");
    }

    #[tokio::test]
    async fn instance_count_matches_stream_length_for_multiple_instances() {
        let refs: Vec<InstanceRef> = (1..=5).map(instance_ref).collect();
        let repo = FakeRepository {
            study_refs: refs.clone(),
            ..Default::default()
        };
        let store = object_store_with(&refs);
        let svc = service(repo, store);

        let result = svc
            .retrieve(RetrieveRequest::new(RetrieveScope::Study {
                study_instance_uid: uid("1"),
            }))
            .await
            .expect("retrieve succeeds");

        let count = result.instance_count;
        let items = collect_stream(result).await;
        assert_eq!(count, items.len());
        assert!(items.iter().all(|i| i.is_ok()));
    }
}
