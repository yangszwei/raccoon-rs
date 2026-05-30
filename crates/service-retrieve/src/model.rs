use std::pin::Pin;

use futures_util::stream::Stream;
use raccoon_contract_dicom::{DicomInstanceIdentity, TransferSyntaxUid};
use raccoon_contract_object_store::{ByteStream, ObjectKey};

use crate::error::RetrieveError;

/// Location and identity of one stored DICOM instance.
///
/// Returned by [`RetrieveRepository`] for each instance matching a retrieve
/// scope. The service fetches the object body using [`object_key`]; the
/// protocol bridge uses [`transfer_syntax_uid`] to evaluate transfer syntax
/// compatibility before the fetch, and [`content_length`] to report
/// sub-operation totals (e.g. C-MOVE Pending responses) before streaming
/// begins.
///
/// [`RetrieveRepository`]: crate::RetrieveRepository
/// [`object_key`]: InstanceRef::object_key
/// [`transfer_syntax_uid`]: InstanceRef::transfer_syntax_uid
/// [`content_length`]: InstanceRef::content_length
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceRef {
    /// Full instance identity (study, series, SOP instance, SOP class UIDs).
    pub identity: DicomInstanceIdentity,
    /// Transfer syntax in which the object is stored.
    ///
    /// `None` when the transfer syntax was not recorded at ingest time. The
    /// bridge must treat this as unknown and apply negotiation fallback logic.
    pub transfer_syntax_uid: Option<TransferSyntaxUid>,
    /// Object store key used to fetch the raw bytes.
    pub object_key: ObjectKey,
    /// Stored byte count, when known.
    ///
    /// Used to pre-announce C-MOVE/C-GET sub-operation totals. `None` when
    /// the size was not recorded at ingest time.
    pub content_length: Option<u64>,
}

impl InstanceRef {
    pub fn new(identity: DicomInstanceIdentity, object_key: ObjectKey) -> Self {
        Self {
            identity,
            transfer_syntax_uid: None,
            object_key,
            content_length: None,
        }
    }

    pub fn with_transfer_syntax(mut self, uid: TransferSyntaxUid) -> Self {
        self.transfer_syntax_uid = Some(uid);
        self
    }

    pub fn with_content_length(mut self, length: u64) -> Self {
        self.content_length = Some(length);
        self
    }
}

/// One DICOM instance delivered as a streaming body.
///
/// The [`body`][RetrievedInstance::body] must be fully consumed before the
/// enclosing [`RetrieveStream`] is polled for the next instance. The service
/// never pre-fetches; bytes are requested from the object store one instance
/// at a time, preserving the stream-only contract.
#[derive(Debug)]
pub struct RetrievedInstance {
    /// Full instance identity.
    pub identity: DicomInstanceIdentity,
    /// Transfer syntax in which the body bytes are encoded.
    pub transfer_syntax_uid: Option<TransferSyntaxUid>,
    /// Exact byte count of the body as reported by the object store.
    pub content_length: u64,
    /// Raw object bytes as a lazy async stream of chunks.
    ///
    /// Bytes are delivered exactly as stored; no transcoding is applied.
    pub body: ByteStream,
}

/// A lazy stream of retrieved DICOM instances.
///
/// Each item is `Ok(`[`RetrievedInstance`]`)` on success, or
/// `Err(`[`RetrieveError::ObjectStore`]`)` for a per-instance object store
/// failure. A repository failure during scope resolution surfaces as
/// `Err(`[`RetrieveError::Repository`]`)` from
/// [`RetrieveService::retrieve`] before this stream is returned.
///
/// [`RetrieveError::ObjectStore`]: crate::RetrieveError::ObjectStore
/// [`RetrieveError::Repository`]: crate::RetrieveError::Repository
/// [`RetrieveService::retrieve`]: crate::RetrieveService::retrieve
pub type RetrieveStream =
    Pin<Box<dyn Stream<Item = Result<RetrievedInstance, RetrieveError>> + Send>>;

/// Result of a retrieve operation: the resolved instance count and the lazy
/// body stream.
///
/// The [`instance_count`] is known before streaming begins because the service
/// resolves the full [`InstanceRef`] list eagerly from the repository. This
/// lets the protocol bridge pre-announce sub-operation totals (e.g. C-MOVE
/// Pending responses, gRPC stream header) before any object bytes are fetched.
///
/// [`instance_count`]: RetrieveResult::instance_count
pub struct RetrieveResult {
    /// Number of instances the service will attempt to retrieve — including
    /// any that produce [`RetrieveError::ObjectStore`] error items in the
    /// stream. Use this for C-MOVE/C-GET sub-operation totals (PS3.4
    /// C.4.1.2.1) and gRPC stream headers; it is the count of attempts, not
    /// the count of successes.
    ///
    /// [`RetrieveError::ObjectStore`]: crate::RetrieveError::ObjectStore
    pub instance_count: usize,
    /// Sum of all pre-known instance byte counts, computed from ingest-time
    /// metadata before any object bytes are fetched.
    ///
    /// `Some(total)` when every resolved [`InstanceRef`] had a recorded
    /// [`content_length`][InstanceRef::content_length]; `None` when at least
    /// one was unknown at ingest time or when the running total would overflow
    /// [`u64`].
    ///
    /// **Note:** this value reflects sizes recorded at ingest time and may
    /// diverge from the actual bytes delivered through the stream if objects
    /// were re-stored after initial ingest. Use it only for pre-announcing
    /// sub-operation totals (e.g. C-MOVE Pending responses, gRPC stream
    /// headers); do not treat it as an authoritative byte count.
    pub total_content_length: Option<u64>,
    /// Lazy stream of instance bodies.
    pub stream: RetrieveStream,
}

impl std::fmt::Debug for RetrieveResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrieveResult")
            .field("instance_count", &self.instance_count)
            .field("total_content_length", &self.total_content_length)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use raccoon_contract_dicom::{
        DicomInstanceIdentity, SeriesInstanceUid, SopClassUid, SopInstanceUid, StudyInstanceUid,
        TransferSyntaxUid,
    };
    use raccoon_contract_object_store::ObjectKey;

    use super::*;

    fn identity() -> DicomInstanceIdentity {
        DicomInstanceIdentity::new(
            StudyInstanceUid::new("1.2.3").unwrap(),
            SeriesInstanceUid::new("1.2.3.4").unwrap(),
            SopInstanceUid::new("1.2.3.4.5").unwrap(),
            SopClassUid::new("1.2.840.10008.5.1.4.1.1.4").unwrap(),
        )
    }

    fn object_key() -> ObjectKey {
        ObjectKey::new("instances/1.2.3.4.5").unwrap()
    }

    #[test]
    fn instance_ref_new_sets_required_fields() {
        let ref_ = InstanceRef::new(identity(), object_key());

        assert_eq!(ref_.identity, identity());
        assert_eq!(ref_.object_key, object_key());
        assert!(ref_.transfer_syntax_uid.is_none());
        assert!(ref_.content_length.is_none());
    }

    #[test]
    fn instance_ref_builder_methods_set_optional_fields() {
        let ts = TransferSyntaxUid::new("1.2.840.10008.1.2.1").unwrap();
        let ref_ = InstanceRef::new(identity(), object_key())
            .with_transfer_syntax(ts.clone())
            .with_content_length(1024);

        assert_eq!(ref_.transfer_syntax_uid, Some(ts));
        assert_eq!(ref_.content_length, Some(1024));
    }
}
