use std::sync::Arc;

use async_trait::async_trait;
use dicom_dictionary_std::uids;

use super::message::{CEchoRequest, CEchoResponse};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::CommandField;
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

/// Verification Service Class (C-ECHO SCP) provider.
#[derive(Debug, Default)]
pub struct VerificationServiceProvider;

impl VerificationServiceProvider {
    pub const SOP_CLASS_UID: &'static str = uids::VERIFICATION;

    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ServiceClassProvider for VerificationServiceProvider {
    #[tracing::instrument(skip(self, ctx), fields(command = "C-ECHO"))]
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CEchoRequest::from_command(&ctx.read_command().await?)?;
        tracing::debug!(stage = "validate", "C-ECHO request validated");
        let response = CEchoResponse::success_for(&request).to_command_object();
        ctx.send_command_object(request.presentation_context_id, &response)
            .await?;
        ctx.record_response_status(0x0000, None);
        tracing::debug!(
            stage = "response",
            status = "0x0000",
            "C-ECHO response sent"
        );
        Ok(())
    }
}

impl DescribedServiceClassProvider for VerificationServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        static BINDINGS: [ServiceBinding; 1] = [ServiceBinding::new(
            CommandField::CEchoRq,
            VerificationServiceProvider::SOP_CLASS_UID,
        )];
        &BINDINGS
    }
}

/// Convenience constructor to register with `Arc`.
pub fn verification_provider() -> Arc<VerificationServiceProvider> {
    Arc::new(VerificationServiceProvider::new())
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::uids;

    use super::VerificationServiceProvider;
    use crate::message::CommandField;
    use crate::registry::DescribedServiceClassProvider;

    #[test]
    fn bindings_declare_c_echo_for_verification_uid() {
        let provider = VerificationServiceProvider;
        let bindings = provider.bindings();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].command_field, CommandField::CEchoRq);
        assert_eq!(bindings[0].sop_class_uid.as_ref(), uids::VERIFICATION);
    }
}
