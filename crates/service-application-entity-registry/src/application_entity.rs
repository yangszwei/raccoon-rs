use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;

use thiserror::Error;
use tracing::warn;

const MAX_AE_TITLE_LENGTH: usize = 16;

const PRINTABLE_ASCII: std::ops::RangeInclusive<u8> = b' '..=b'~';

const MIN_RECOMMENDED_MAX_PDU_LENGTH: u32 = 65_536;

/// Errors while parsing or validating a DICOM AE title.
#[derive(Debug, Eq, PartialEq, Error)]
pub enum AeTitleError {
    #[error("AE title must not be empty or all spaces")]
    Empty,

    #[error("AE title must contain DICOM AE VR characters only")]
    InvalidCharacter,

    #[error("AE title must be at most 16 characters")]
    TooLong,
}

/// Errors while building an Application Entity.
#[derive(Debug, Eq, PartialEq, Error)]
pub enum ApplicationEntityError {
    #[error("invalid title: {0}")]
    InvalidTitle(#[from] AeTitleError),

    #[error("invalid address: {0}")]
    InvalidAddress(#[from] AddrParseError),

    #[error("invalid peer address: expected host:port")]
    InvalidPeerAddress,
}

/// DICOM AE title.
///
/// Accepts AE VR text up to 16 bytes, rejects backslashes and control
/// characters, and stores the title without space padding.
///
/// <https://dicom.nema.org/medical/dicom/2025b/output/html/part05.html#table_6.2-1>
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AeTitle(String);

impl AeTitle {
    /// Return the canonical AE title.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AeTitle {
    type Err = AeTitleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let canonical_value = value.trim_matches(' ');

        if canonical_value.is_empty() {
            return Err(AeTitleError::Empty);
        }

        if value
            .bytes()
            .any(|b| !PRINTABLE_ASCII.contains(&b) || b == b'\\')
        {
            return Err(AeTitleError::InvalidCharacter);
        }

        if value.len() > MAX_AE_TITLE_LENGTH {
            return Err(AeTitleError::TooLong);
        }

        Ok(Self(canonical_value.to_string()))
    }
}

impl std::fmt::Display for AeTitle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Local DICOM Application Entity served by this process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalApplicationEntity {
    title: AeTitle,
    bind_addr: SocketAddr,
    max_concurrent_associations: usize,
    read_timeout_seconds: Option<u64>,
    write_timeout_seconds: Option<u64>,
    max_pdu_length: u32,
}

impl LocalApplicationEntity {
    /// Build a local Application Entity from its title and bind address.
    pub fn try_new(
        title: &str,
        bind_addr: &str,
        max_concurrent_associations: usize,
        read_timeout_seconds: Option<u64>,
        write_timeout_seconds: Option<u64>,
        max_pdu_length: u32,
    ) -> Result<Self, ApplicationEntityError> {
        if max_pdu_length < MIN_RECOMMENDED_MAX_PDU_LENGTH {
            warn!(
                ae.kind = "local",
                ae.title = title,
                ae.max_pdu_length = max_pdu_length,
                ae.recommended_min = MIN_RECOMMENDED_MAX_PDU_LENGTH,
                "Max PDU length of local AE \"{title}\" ({max_pdu_length}) is below the recommended default ({MIN_RECOMMENDED_MAX_PDU_LENGTH}) and may reduce interoperability",
            );
        }

        Ok(LocalApplicationEntity {
            title: AeTitle::from_str(title)?,
            bind_addr: SocketAddr::from_str(bind_addr)?,
            max_concurrent_associations,
            read_timeout_seconds,
            write_timeout_seconds,
            max_pdu_length,
        })
    }

    /// Return the local AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Return the socket address this AE binds to.
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub fn max_concurrent_associations(&self) -> usize {
        self.max_concurrent_associations
    }

    pub fn read_timeout_seconds(&self) -> Option<u64> {
        self.read_timeout_seconds
    }

    pub fn write_timeout_seconds(&self) -> Option<u64> {
        self.write_timeout_seconds
    }

    pub fn max_pdu_length(&self) -> u32 {
        self.max_pdu_length
    }
}

#[cfg(any(test, feature = "test-support"))]
/// Build a test local AE with common defaults.
pub fn local_ae(title: &'static str, bind_addr: &'static str) -> LocalApplicationEntity {
    LocalApplicationEntity::try_new(title, bind_addr, 64, Some(5), Some(5), 65_536)
        .expect("invalid local application entity")
}

/// Peer DICOM Application Entity reached by this process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerApplicationEntity {
    title: AeTitle,
    addr: String,
    connect_timeout_seconds: Option<u64>,
    read_timeout_seconds: Option<u64>,
    write_timeout_seconds: Option<u64>,
    max_pdu_length: u32,
}

impl PeerApplicationEntity {
    /// Build a peer Application Entity from its title and address.
    pub fn try_new(
        title: &str,
        addr: &str,
        connect_timeout_seconds: Option<u64>,
        read_timeout_seconds: Option<u64>,
        write_timeout_seconds: Option<u64>,
        max_pdu_length: u32,
    ) -> Result<Self, ApplicationEntityError> {
        validate_peer_addr(addr)?;

        Ok(Self {
            title: AeTitle::from_str(title)?,
            addr: addr.to_string(),
            connect_timeout_seconds,
            read_timeout_seconds,
            write_timeout_seconds,
            max_pdu_length,
        })
    }

    /// Return the peer AE title.
    pub fn title(&self) -> &AeTitle {
        &self.title
    }

    /// Return the peer socket address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn connect_timeout_seconds(&self) -> Option<u64> {
        self.connect_timeout_seconds
    }

    pub fn read_timeout_seconds(&self) -> Option<u64> {
        self.read_timeout_seconds
    }

    pub fn write_timeout_seconds(&self) -> Option<u64> {
        self.write_timeout_seconds
    }

    pub fn max_pdu_length(&self) -> u32 {
        self.max_pdu_length
    }
}

fn validate_peer_addr(addr: &str) -> Result<(), ApplicationEntityError> {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return Err(ApplicationEntityError::InvalidPeerAddress);
    };

    if host.is_empty() || port.parse::<u16>().is_err() {
        return Err(ApplicationEntityError::InvalidPeerAddress);
    }

    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
/// Build a test peer AE with common defaults.
pub fn peer_ae(title: &'static str, addr: &'static str) -> PeerApplicationEntity {
    PeerApplicationEntity::try_new(title, addr, Some(5), Some(5), Some(5), 65_536)
        .expect("invalid peer application entity")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        AeTitle, AeTitleError, ApplicationEntityError, LocalApplicationEntity,
        PeerApplicationEntity,
    };

    #[test]
    fn ae_title_accepts_valid_title() {
        AeTitle::from_str("RACCOON").expect("valid title should parse");
    }

    #[test]
    fn ae_title_accepts_leading_and_trailing_spaces_if_not_all_spaces() {
        let title = AeTitle::from_str(" RACCOON ").expect("spaces are valid chars");

        assert_eq!(title.as_str(), "RACCOON");
    }

    #[test]
    fn ae_title_rejects_empty() {
        assert_eq!(AeTitle::from_str("").unwrap_err(), AeTitleError::Empty);
    }

    #[test]
    fn ae_title_rejects_all_spaces() {
        assert_eq!(AeTitle::from_str("   ").unwrap_err(), AeTitleError::Empty);
    }

    #[test]
    fn ae_title_rejects_backslashes() {
        assert_eq!(
            AeTitle::from_str("RUST\\COON").unwrap_err(),
            AeTitleError::InvalidCharacter
        );
    }

    #[test]
    fn ae_title_rejects_control_characters() {
        assert_eq!(
            AeTitle::from_str("RUST\tCOON").unwrap_err(),
            AeTitleError::InvalidCharacter
        );
    }

    #[test]
    fn ae_title_accepts_exactly_sixteen_characters() {
        AeTitle::from_str("ABCDEFGHIJKLMNOP").expect("16-char title should parse");
    }

    #[test]
    fn ae_title_rejects_too_long() {
        assert_eq!(
            AeTitle::from_str("ABCDEFGHIJKLMNOPQ").unwrap_err(),
            AeTitleError::TooLong
        );
    }

    #[test]
    fn try_new_builds_local_entity() {
        let local = LocalApplicationEntity::try_new(
            "LOCAL_AE",
            "127.0.0.1:11112",
            64,
            Some(30),
            Some(45),
            65_536,
        )
        .expect("valid local entity");

        assert_eq!(local.title().as_str(), "LOCAL_AE");
        assert_eq!(local.bind_addr().to_string(), "127.0.0.1:11112");
        assert_eq!(local.max_concurrent_associations(), 64);
        assert_eq!(local.read_timeout_seconds(), Some(30));
        assert_eq!(local.write_timeout_seconds(), Some(45));
        assert_eq!(local.max_pdu_length(), 65_536);
    }

    #[test]
    fn local_try_new_rejects_invalid_socket_address() {
        let err = LocalApplicationEntity::try_new(
            "LOCAL_AE",
            "not-an-address",
            64,
            Some(5),
            Some(5),
            65_536,
        )
        .unwrap_err();

        assert!(matches!(err, ApplicationEntityError::InvalidAddress(_)));
    }

    #[test]
    fn try_new_builds_peer_entity() {
        let peer = PeerApplicationEntity::try_new(
            "REMOTE_AE",
            "192.0.2.10:104",
            Some(5),
            Some(30),
            Some(45),
            65_536,
        )
        .expect("valid peer entity");

        assert_eq!(peer.title().as_str(), "REMOTE_AE");
        assert_eq!(peer.addr(), "192.0.2.10:104");
        assert_eq!(peer.connect_timeout_seconds(), Some(5));
        assert_eq!(peer.read_timeout_seconds(), Some(30));
        assert_eq!(peer.write_timeout_seconds(), Some(45));
        assert_eq!(peer.max_pdu_length(), 65_536);
    }

    #[test]
    fn try_new_builds_peer_entity_with_dns_name() {
        let peer = PeerApplicationEntity::try_new(
            "REMOTE_AE",
            "c-move-destination:11113",
            Some(5),
            Some(30),
            Some(45),
            65_536,
        )
        .expect("valid peer entity");

        assert_eq!(peer.addr(), "c-move-destination:11113");
    }

    #[test]
    fn peer_try_new_rejects_invalid_address() {
        let err = PeerApplicationEntity::try_new(
            "REMOTE_AE",
            "not-an-address",
            Some(5),
            Some(5),
            Some(5),
            65_536,
        )
        .unwrap_err();

        assert_eq!(err, ApplicationEntityError::InvalidPeerAddress);
    }
}
