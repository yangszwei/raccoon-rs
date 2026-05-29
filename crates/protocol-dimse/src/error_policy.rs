use crate::error::DimseError;

/// Control flow decision after one association-level service error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPolicyAction {
    /// Continue serving this association.
    Continue,
    /// Stop without sending any additional PDU.
    Stop,
    /// Send A-RELEASE-RP then stop.
    SendReleaseRpAndStop,
    /// Send A-ABORT then return the error.
    AbortAndStop,
}

/// Error handling strategy for the DIMSE association serve loop.
pub trait AssociationErrorPolicy: Send + Sync {
    fn on_error(&self, error: &DimseError) -> ErrorPolicyAction;
}

/// Default association error policy.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultAssociationErrorPolicy;

impl AssociationErrorPolicy for DefaultAssociationErrorPolicy {
    fn on_error(&self, error: &DimseError) -> ErrorPolicyAction {
        match error {
            DimseError::PeerReleaseRequested => ErrorPolicyAction::SendReleaseRpAndStop,
            DimseError::PeerAborted | DimseError::ConnectionClosed | DimseError::Ul(_) => {
                ErrorPolicyAction::Stop
            }
            _ => ErrorPolicyAction::AbortAndStop,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AssociationErrorPolicy, DefaultAssociationErrorPolicy, ErrorPolicyAction};
    use crate::DimseError;

    #[test]
    fn default_policy_replies_release_when_peer_requests_release() {
        let policy = DefaultAssociationErrorPolicy;
        let action = policy.on_error(&DimseError::PeerReleaseRequested);
        assert_eq!(action, ErrorPolicyAction::SendReleaseRpAndStop);
    }

    #[test]
    fn default_policy_stops_on_peer_aborted_or_connection_closed() {
        let policy = DefaultAssociationErrorPolicy;
        assert_eq!(
            policy.on_error(&DimseError::PeerAborted),
            ErrorPolicyAction::Stop
        );
        assert_eq!(
            policy.on_error(&DimseError::ConnectionClosed),
            ErrorPolicyAction::Stop
        );
    }

    #[test]
    fn default_policy_aborts_on_protocol_errors() {
        let policy = DefaultAssociationErrorPolicy;
        let action = policy.on_error(&DimseError::Protocol("unexpected state".to_string()));
        assert_eq!(action, ErrorPolicyAction::AbortAndStop);
    }
}
