use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tracing::instrument;

use crate::application_entity::{
    AeTitle, ApplicationEntityError, LocalApplicationEntity, PeerApplicationEntity,
};

/// Errors while building or updating the Application Entity registry.
#[derive(Debug, Eq, PartialEq, Error)]
pub enum ApplicationEntityRegistryError {
    #[error("invalid application entity: {0}")]
    InvalidAe(#[from] ApplicationEntityError),

    #[error("duplicate application entity: {0}")]
    DuplicateAe(AeTitle),

    #[cfg(feature = "grpc")]
    #[error("gRPC registry request failed ({code:?}): {message}")]
    GrpcStatus { code: tonic::Code, message: String },

    #[cfg(feature = "grpc")]
    #[error("missing application entity in gRPC request")]
    MissingApplicationEntity,

    #[cfg(feature = "grpc")]
    #[error("max concurrent associations is too large for this platform: {0}")]
    MaxConcurrentAssociationsTooLarge(u64),
}

#[cfg(feature = "grpc")]
impl From<tonic::Status> for ApplicationEntityRegistryError {
    fn from(status: tonic::Status) -> Self {
        Self::GrpcStatus {
            code: status.code(),
            message: status.message().to_string(),
        }
    }
}

/// Read interface for local and peer Application Entities.
#[async_trait]
pub trait ApplicationEntityRegistry {
    /// List registered local Application Entities.
    async fn list_locals(
        &self,
    ) -> Result<Vec<LocalApplicationEntity>, ApplicationEntityRegistryError>;

    /// List registered peer Application Entities.
    async fn list_peers(
        &self,
    ) -> Result<Vec<PeerApplicationEntity>, ApplicationEntityRegistryError>;

    /// Look up a local Application Entity by AE title.
    async fn get_local(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<LocalApplicationEntity>, ApplicationEntityRegistryError>;

    /// Look up a peer Application Entity by AE title.
    async fn get_peer(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<PeerApplicationEntity>, ApplicationEntityRegistryError>;
}

/// Write interface for local and peer Application Entities.
#[async_trait]
pub trait ApplicationEntityRegistryWriter {
    /// Insert a local Application Entity.
    async fn insert_local(
        &mut self,
        local_ae: LocalApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError>;

    /// Insert a peer Application Entity.
    async fn insert_peer(
        &mut self,
        peer_ae: PeerApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError>;
}

/// Registry of local and peer Application Entities by AE title.
#[derive(Debug, Default)]
pub struct InMemoryApplicationEntityRegistry {
    locals: HashMap<AeTitle, LocalApplicationEntity>,
    peers: HashMap<AeTitle, PeerApplicationEntity>,
}

impl InMemoryApplicationEntityRegistry {
    /// Build a registry and reject duplicate AE titles.
    #[instrument(skip(local_aes, peer_aes), fields(local.count = local_aes.len(), peer.count = peer_aes.len()))]
    pub fn try_new(
        local_aes: Vec<LocalApplicationEntity>,
        peer_aes: Vec<PeerApplicationEntity>,
    ) -> Result<Self, ApplicationEntityRegistryError> {
        let mut registry = Self::default();

        for local_ae in local_aes {
            registry.insert_local(local_ae)?;
        }

        for peer_ae in peer_aes {
            registry.insert_peer(peer_ae)?;
        }

        Ok(registry)
    }

    /// Iterate over registered local Application Entities.
    pub fn locals(&self) -> impl ExactSizeIterator<Item = &LocalApplicationEntity> + '_ {
        self.locals.values()
    }

    /// Iterate over registered peer Application Entities.
    pub fn peers(&self) -> impl ExactSizeIterator<Item = &PeerApplicationEntity> + '_ {
        self.peers.values()
    }

    /// Insert a local Application Entity.
    #[instrument(skip(self, local_ae), fields(ae.title = %local_ae.title()))]
    pub fn insert_local(
        &mut self,
        local_ae: LocalApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        let ae_title = local_ae.title().clone();
        if self.contains_title(&ae_title) {
            return Err(ApplicationEntityRegistryError::DuplicateAe(ae_title));
        }
        self.locals.insert(ae_title, local_ae);
        Ok(())
    }

    /// Insert a peer Application Entity.
    #[instrument(skip(self, peer_ae), fields(ae.title = %peer_ae.title()))]
    pub fn insert_peer(
        &mut self,
        peer_ae: PeerApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        let ae_title = peer_ae.title().clone();
        if self.contains_title(&ae_title) {
            return Err(ApplicationEntityRegistryError::DuplicateAe(ae_title));
        }
        self.peers.insert(ae_title, peer_ae);
        Ok(())
    }

    /// Look up a local Application Entity by AE title.
    pub fn get_local(&self, ae_title: &AeTitle) -> Option<&LocalApplicationEntity> {
        self.locals.get(ae_title)
    }

    /// Look up a peer Application Entity by AE title.
    pub fn get_peer(&self, ae_title: &AeTitle) -> Option<&PeerApplicationEntity> {
        self.peers.get(ae_title)
    }

    fn contains_title(&self, title: &AeTitle) -> bool {
        self.locals.contains_key(title) || self.peers.contains_key(title)
    }
}

#[async_trait]
impl ApplicationEntityRegistryWriter for InMemoryApplicationEntityRegistry {
    async fn insert_local(
        &mut self,
        local_ae: LocalApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        InMemoryApplicationEntityRegistry::insert_local(self, local_ae)
    }

    async fn insert_peer(
        &mut self,
        peer_ae: PeerApplicationEntity,
    ) -> Result<(), ApplicationEntityRegistryError> {
        InMemoryApplicationEntityRegistry::insert_peer(self, peer_ae)
    }
}

#[async_trait]
impl ApplicationEntityRegistry for InMemoryApplicationEntityRegistry {
    #[instrument(skip(self), fields(local.count = self.locals.len()))]
    async fn list_locals(
        &self,
    ) -> Result<Vec<LocalApplicationEntity>, ApplicationEntityRegistryError> {
        Ok(self.locals.values().cloned().collect())
    }

    #[instrument(skip(self), fields(peer.count = self.peers.len()))]
    async fn list_peers(
        &self,
    ) -> Result<Vec<PeerApplicationEntity>, ApplicationEntityRegistryError> {
        Ok(self.peers.values().cloned().collect())
    }

    #[instrument(skip(self), fields(ae.title = %ae_title))]
    async fn get_local(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<LocalApplicationEntity>, ApplicationEntityRegistryError> {
        Ok(self.locals.get(ae_title).cloned())
    }

    #[instrument(skip(self), fields(ae.title = %ae_title))]
    async fn get_peer(
        &self,
        ae_title: &AeTitle,
    ) -> Result<Option<PeerApplicationEntity>, ApplicationEntityRegistryError> {
        Ok(self.peers.get(ae_title).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        ApplicationEntityRegistry, ApplicationEntityRegistryError, ApplicationEntityRegistryWriter,
        InMemoryApplicationEntityRegistry,
    };
    use crate::application_entity::{AeTitle, local_ae, peer_ae};

    #[test]
    fn try_new_stores_local_and_peer_entities() {
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");

        let registry = InMemoryApplicationEntityRegistry::try_new(
            vec![local_ae.clone()],
            vec![peer_ae.clone()],
        )
        .expect("registry should build");

        assert_eq!(registry.locals().len(), 1);
        assert_eq!(registry.peers().len(), 1);
        assert_eq!(registry.get_local(local_ae.title()), Some(&local_ae));
        assert_eq!(registry.get_peer(peer_ae.title()), Some(&peer_ae));
    }

    #[test]
    fn try_new_rejects_duplicate_titles_across_local_and_peer() {
        const DUPLICATE_AE: &str = "DUPLICATE_AE";

        let error = InMemoryApplicationEntityRegistry::try_new(
            vec![local_ae(DUPLICATE_AE, "127.0.0.1:11112")],
            vec![peer_ae(DUPLICATE_AE, "192.0.2.10:104")],
        )
        .unwrap_err();

        assert_eq!(
            error,
            ApplicationEntityRegistryError::DuplicateAe(DUPLICATE_AE.parse().unwrap())
        );
    }

    #[test]
    fn insert_local_accepts_new_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");

        registry
            .insert_local(local_ae.clone())
            .expect("insert should succeed");

        assert!(registry.locals().any(|stored| stored == &local_ae));
        assert_eq!(registry.peers().len(), 0);
    }

    #[test]
    fn insert_peer_accepts_new_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");

        registry
            .insert_peer(peer_ae.clone())
            .expect("insert should succeed");

        assert!(registry.peers().any(|stored| stored == &peer_ae));
        assert_eq!(registry.locals().len(), 0);
    }

    #[test]
    fn insert_local_rejects_duplicate_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");

        registry
            .insert_local(local_ae.clone())
            .expect("first insert should succeed");

        let error = registry.insert_local(local_ae.clone()).unwrap_err();

        assert_eq!(
            error,
            ApplicationEntityRegistryError::DuplicateAe(local_ae.title().clone())
        );
    }

    #[test]
    fn insert_local_rejects_duplicate_padded_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();

        registry
            .insert_local(local_ae("LOCAL_AE", "127.0.0.1:11112"))
            .expect("first insert should succeed");

        let error = registry
            .insert_local(local_ae(" LOCAL_AE ", "127.0.0.1:11113"))
            .unwrap_err();

        assert_eq!(
            error,
            ApplicationEntityRegistryError::DuplicateAe("LOCAL_AE".parse().unwrap())
        );
    }

    #[test]
    fn get_local_accepts_padded_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let padded_title = AeTitle::from_str(" LOCAL_AE ").expect("valid padded title");

        registry
            .insert_local(local_ae.clone())
            .expect("insert should succeed");

        assert_eq!(registry.get_local(&padded_title), Some(&local_ae));
    }

    #[test]
    fn insert_peer_rejects_duplicate_title() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");

        registry
            .insert_peer(peer_ae.clone())
            .expect("first insert should succeed");

        let error = registry.insert_peer(peer_ae.clone()).unwrap_err();

        assert_eq!(
            error,
            ApplicationEntityRegistryError::DuplicateAe(peer_ae.title().clone())
        );
    }

    #[tokio::test]
    async fn trait_methods_return_owned_entities() {
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");
        let registry = InMemoryApplicationEntityRegistry::try_new(
            vec![local_ae.clone()],
            vec![peer_ae.clone()],
        )
        .expect("registry should build");

        assert_eq!(
            registry.list_locals().await.unwrap(),
            vec![local_ae.clone()]
        );
        assert_eq!(registry.list_peers().await.unwrap(), vec![peer_ae.clone()]);
        assert_eq!(
            ApplicationEntityRegistry::get_local(&registry, local_ae.title())
                .await
                .unwrap(),
            Some(local_ae)
        );
        assert_eq!(
            ApplicationEntityRegistry::get_peer(&registry, peer_ae.title())
                .await
                .unwrap(),
            Some(peer_ae)
        );
    }

    #[tokio::test]
    async fn writer_trait_insert_methods_preserve_duplicate_checks() {
        let mut registry = InMemoryApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");

        ApplicationEntityRegistryWriter::insert_local(&mut registry, local_ae.clone())
            .await
            .expect("insert should succeed");

        let error = ApplicationEntityRegistryWriter::insert_peer(
            &mut registry,
            peer_ae("LOCAL_AE", "192.0.2.10:104"),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            ApplicationEntityRegistryError::DuplicateAe(local_ae.title().clone())
        );
    }
}
