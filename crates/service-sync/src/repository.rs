use std::time::Duration;

use async_trait::async_trait;

use crate::error::SyncRepositoryError;
use crate::model::{
    ClaimedSyncObject, QuarantineRecord, SyncClaimToken, SyncWorkerId, SyncedReadModelObject,
};

/// Source-side repository port for concurrent sync claims.
///
/// Implementations must claim atomically. Returned records must be accepted
/// ingest objects that are not already synced or quarantined and are either
/// unclaimed or have an expired claim lease.
#[async_trait]
pub trait SyncSourceRepository: Send + Sync {
    async fn claim_pending_objects(
        &self,
        worker_id: &SyncWorkerId,
        batch_size: usize,
        claim_ttl: Duration,
    ) -> Result<Vec<ClaimedSyncObject>, SyncRepositoryError>;

    async fn mark_synced(&self, claim_token: &SyncClaimToken) -> Result<(), SyncRepositoryError>;

    async fn release_claim(&self, claim_token: &SyncClaimToken) -> Result<(), SyncRepositoryError>;
}

/// Read-side repository port for idempotent read-model upserts.
#[async_trait]
pub trait SyncReadModelWriter: Send + Sync {
    async fn upsert_synced_object(
        &self,
        object: &SyncedReadModelObject,
    ) -> Result<(), SyncRepositoryError>;
}

/// Repository port for terminal sync quarantine.
///
/// Implementations must verify the claim token still owns the source record
/// before marking it quarantined and updating the stored/retrieve object key.
#[async_trait]
pub trait SyncQuarantineRepository: Send + Sync {
    async fn mark_quarantined(&self, record: &QuarantineRecord) -> Result<(), SyncRepositoryError>;
}
