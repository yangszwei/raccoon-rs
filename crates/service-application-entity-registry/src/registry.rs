use std::collections::HashMap;

use thiserror::Error;

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
}

/// Registry of local and peer Application Entities by AE title.
#[derive(Debug, Default)]
pub struct ApplicationEntityRegistry {
    locals: HashMap<AeTitle, LocalApplicationEntity>,
    peers: HashMap<AeTitle, PeerApplicationEntity>,
}

impl ApplicationEntityRegistry {
    /// Build a registry and reject duplicate AE titles.
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{ApplicationEntityRegistry, ApplicationEntityRegistryError};
    use crate::application_entity::{AeTitle, local_ae, peer_ae};

    #[test]
    fn try_new_stores_local_and_peer_entities() {
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");

        let registry =
            ApplicationEntityRegistry::try_new(vec![local_ae.clone()], vec![peer_ae.clone()])
                .expect("registry should build");

        assert_eq!(registry.locals().len(), 1);
        assert_eq!(registry.peers().len(), 1);
        assert_eq!(registry.get_local(local_ae.title()), Some(&local_ae));
        assert_eq!(registry.get_peer(peer_ae.title()), Some(&peer_ae));
    }

    #[test]
    fn try_new_rejects_duplicate_titles_across_local_and_peer() {
        const DUPLICATE_AE: &str = "DUPLICATE_AE";

        let error = ApplicationEntityRegistry::try_new(
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
        let mut registry = ApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");

        registry
            .insert_local(local_ae.clone())
            .expect("insert should succeed");

        assert!(registry.locals().any(|stored| stored == &local_ae));
        assert_eq!(registry.peers().len(), 0);
    }

    #[test]
    fn insert_peer_accepts_new_title() {
        let mut registry = ApplicationEntityRegistry::default();
        let peer_ae = peer_ae("PEER_AE", "192.0.2.10:104");

        registry
            .insert_peer(peer_ae.clone())
            .expect("insert should succeed");

        assert!(registry.peers().any(|stored| stored == &peer_ae));
        assert_eq!(registry.locals().len(), 0);
    }

    #[test]
    fn insert_local_rejects_duplicate_title() {
        let mut registry = ApplicationEntityRegistry::default();
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
        let mut registry = ApplicationEntityRegistry::default();

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
        let mut registry = ApplicationEntityRegistry::default();
        let local_ae = local_ae("LOCAL_AE", "127.0.0.1:11112");
        let padded_title = AeTitle::from_str(" LOCAL_AE ").expect("valid padded title");

        registry
            .insert_local(local_ae.clone())
            .expect("insert should succeed");

        assert_eq!(registry.get_local(&padded_title), Some(&local_ae));
    }

    #[test]
    fn insert_peer_rejects_duplicate_title() {
        let mut registry = ApplicationEntityRegistry::default();
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
}
