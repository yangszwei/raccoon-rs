//! DICOM application entity configuration.

use std::net::{Ipv4Addr, SocketAddrV4};

use serde::Deserialize;

const DEFAULT_LOCAL_AE_TITLE: &str = "RACCOON";
const DEFAULT_LOCAL_AE_BIND_ADDRESS: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 11112);
const DEFAULT_MAX_CONCURRENT_ASSOCIATIONS: usize = 64;
const DEFAULT_MAX_PDU_LENGTH: u32 = 65_536;

const fn default_max_pdu_length() -> u32 {
    DEFAULT_MAX_PDU_LENGTH
}

/// Local DICOM application entity listener settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LocalApplicationEntityConfig {
    /// AE title advertised by the local service.
    pub title: String,

    /// Socket address where the local AE accepts associations.
    pub bind_address: String,

    /// Maximum associations accepted concurrently by this AE.
    pub max_concurrent_associations: usize,

    /// Optional socket read timeout in seconds.
    pub read_timeout_seconds: Option<u64>,

    /// Optional socket write timeout in seconds.
    pub write_timeout_seconds: Option<u64>,

    /// Maximum PDU size negotiated for associations.
    pub max_pdu_length: u32,
}

impl Default for LocalApplicationEntityConfig {
    fn default() -> Self {
        Self {
            title: DEFAULT_LOCAL_AE_TITLE.to_string(),
            bind_address: DEFAULT_LOCAL_AE_BIND_ADDRESS.to_string(),
            max_concurrent_associations: DEFAULT_MAX_CONCURRENT_ASSOCIATIONS,
            read_timeout_seconds: None,
            write_timeout_seconds: None,
            max_pdu_length: DEFAULT_MAX_PDU_LENGTH,
        }
    }
}

/// Remote DICOM application entity connection settings.
#[derive(Debug, Deserialize)]
pub struct PeerApplicationEntityConfig {
    /// AE title expected for the peer.
    pub title: String,

    /// Socket address used to connect to the peer.
    pub address: String,

    /// Optional association connect timeout in seconds.
    pub connect_timeout_seconds: Option<u64>,

    /// Optional socket read timeout in seconds.
    pub read_timeout_seconds: Option<u64>,

    /// Optional socket write timeout in seconds.
    pub write_timeout_seconds: Option<u64>,

    /// Maximum PDU size negotiated for associations.
    #[serde(default = "default_max_pdu_length")]
    pub max_pdu_length: u32,
}

/// Configured local and peer DICOM application entities.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ApplicationEntitiesConfig {
    /// Local AEs exposed by this process.
    pub local: Vec<LocalApplicationEntityConfig>,

    /// Peer AEs this process may initiate associations with.
    pub peer: Vec<PeerApplicationEntityConfig>,
}

impl Default for ApplicationEntitiesConfig {
    fn default() -> Self {
        Self {
            local: vec![LocalApplicationEntityConfig::default()],
            peer: Vec::new(),
        }
    }
}
