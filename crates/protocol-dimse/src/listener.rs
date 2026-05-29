use std::net::SocketAddr;
use std::time::Duration;

use dicom_ul::association::server::ServerAssociationOptions;
use dicom_ul::pdu::Pdu;
use raccoon_service_application_entity_registry::{AeTitle, LocalApplicationEntity};
use tokio::net::{TcpListener, TcpStream};

use crate::association::{AssociationContext, DimseAssociation};
use crate::error::DimseError;
use crate::error_policy::{
    AssociationErrorPolicy, DefaultAssociationErrorPolicy, ErrorPolicyAction,
};
use crate::instrumentation::next_association_id;
use crate::registry::ServiceClassProvider;

/// Inbound DIMSE listener bound to one local Application Entity.
pub struct DimseListener {
    tcp_listener: TcpListener,
    local_ae_title: AeTitle,
    abstract_syntax_uids: Vec<String>,
    max_pdu_length: u32,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl DimseListener {
    /// Bind a DIMSE listener from a `LocalApplicationEntity`.
    pub async fn bind(local_ae: &LocalApplicationEntity) -> Result<Self, DimseError> {
        let tcp_listener = TcpListener::bind(local_ae.bind_addr()).await?;
        Ok(Self {
            tcp_listener,
            local_ae_title: local_ae.title().clone(),
            abstract_syntax_uids: Vec::new(),
            max_pdu_length: local_ae.max_pdu_length(),
            read_timeout: local_ae.read_timeout_seconds().map(Duration::from_secs),
            write_timeout: local_ae.write_timeout_seconds().map(Duration::from_secs),
        })
    }

    /// Add one abstract syntax UID to the association negotiation.
    pub fn with_abstract_syntax(mut self, uid: impl Into<String>) -> Self {
        self.abstract_syntax_uids.push(uid.into());
        self
    }

    /// Add all abstract syntax UIDs from a service registry.
    pub fn with_registry_syntaxes(self, registry: &crate::registry::ServiceClassRegistry) -> Self {
        registry
            .supported_abstract_syntax_uids()
            .into_iter()
            .fold(self, |l, uid| l.with_abstract_syntax(uid))
    }

    /// Return the bound local socket address.
    pub fn local_addr(&self) -> Result<SocketAddr, DimseError> {
        self.tcp_listener.local_addr().map_err(DimseError::Io)
    }

    /// Accept one inbound TCP connection and negotiate a DIMSE association.
    #[tracing::instrument(skip(self), fields(ae.title = %self.local_ae_title))]
    pub async fn accept(&self) -> Result<(AssociationContext, SocketAddr), DimseError> {
        let (socket, peer_addr) = self.tcp_listener.accept().await?;
        let ctx = self
            .establish_association(socket, next_association_id())
            .await?;
        Ok((ctx, peer_addr))
    }

    /// Accept one association and run the serve loop with `DefaultAssociationErrorPolicy`.
    pub async fn accept_and_serve(
        &self,
        provider: &dyn ServiceClassProvider,
    ) -> Result<(), DimseError> {
        self.accept_and_serve_with_policy(provider, &DefaultAssociationErrorPolicy)
            .await
    }

    /// Accept one association and run the serve loop with a custom error policy.
    pub async fn accept_and_serve_with_policy(
        &self,
        provider: &dyn ServiceClassProvider,
        policy: &dyn AssociationErrorPolicy,
    ) -> Result<(), DimseError> {
        let association_id = next_association_id();
        let (socket, peer_addr) = self.tcp_listener.accept().await?;
        let ctx = self.establish_association(socket, association_id).await?;
        serve_association(ctx, peer_addr, provider, policy).await
    }

    async fn establish_association(
        &self,
        socket: TcpStream,
        association_id: u64,
    ) -> Result<AssociationContext, DimseError> {
        let mut options = ServerAssociationOptions::new()
            .accept_any()
            .ae_title(self.local_ae_title.as_str())
            .max_pdu_length(self.max_pdu_length);

        for uid in &self.abstract_syntax_uids {
            options = options.with_abstract_syntax(uid.as_str());
        }

        if let Some(timeout) = self.read_timeout {
            options = options.read_timeout(timeout);
        }
        if let Some(timeout) = self.write_timeout {
            options = options.write_timeout(timeout);
        }

        let inner = options.establish_async(socket).await?;
        let assoc = DimseAssociation::new(inner, self.local_ae_title.clone(), association_id);
        Ok(AssociationContext::new(assoc))
    }
}

/// Drive the association serve loop for one established context.
async fn serve_association(
    mut ctx: AssociationContext,
    peer_addr: SocketAddr,
    provider: &dyn ServiceClassProvider,
    policy: &dyn AssociationErrorPolicy,
) -> Result<(), DimseError> {
    let association_id = ctx.association_id;
    let called_ae = ctx.association().called_ae_title().as_str().to_string();
    let calling_ae = ctx
        .association()
        .peer_ae_title()
        .map(|t| t.as_str().to_string())
        .unwrap_or_else(|| "UNKNOWN".to_string());

    tracing::info!(
        association.id = association_id,
        ae.called = %called_ae,
        ae.calling = %calling_ae,
        peer.addr = %peer_addr,
        "DIMSE association accepted"
    );

    loop {
        let bytes_in_before = ctx.bytes_in();
        let bytes_out_before = ctx.bytes_out();
        let _request_id = ctx.next_request_id();

        let result = provider.handle(&mut ctx).await;

        match result {
            Ok(()) => {
                if let Err(cycle_err) = ctx.complete_message_cycle() {
                    tracing::warn!(
                        association.id = association_id,
                        error = %cycle_err,
                        "message cycle incomplete after provider returned"
                    );
                }
                tracing::debug!(
                    association.id = association_id,
                    bytes_in = ctx.bytes_in().saturating_sub(bytes_in_before),
                    bytes_out = ctx.bytes_out().saturating_sub(bytes_out_before),
                    status = ?ctx.response_status(),
                    "DIMSE request completed"
                );
            }
            Err(error) => {
                tracing::debug!(
                    association.id = association_id,
                    command = ?ctx.cached_command().map(|c| c.command_field.to_string()),
                    error = %error,
                    "DIMSE request failed"
                );
                match policy.on_error(&error) {
                    ErrorPolicyAction::Continue => {
                        continue;
                    }
                    ErrorPolicyAction::Stop => {
                        tracing::info!(
                            association.id = association_id,
                            "DIMSE association closing (stop)"
                        );
                        break;
                    }
                    ErrorPolicyAction::SendReleaseRpAndStop => {
                        let _ = ctx.association_mut().send_pdu(&Pdu::ReleaseRP).await;
                        tracing::info!(
                            association.id = association_id,
                            "DIMSE association released"
                        );
                        break;
                    }
                    ErrorPolicyAction::AbortAndStop => {
                        tracing::warn!(
                            association.id = association_id,
                            error = %error,
                            "DIMSE association aborting"
                        );
                        let assoc = ctx.into_dimse_association();
                        let _ = assoc.abort().await;
                        return Err(error);
                    }
                }
            }
        }
    }

    Ok(())
}
