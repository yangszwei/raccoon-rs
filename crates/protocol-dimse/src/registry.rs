use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::CommandField;

const ANY_SOP_CLASS_UID: &str = "*";

/// DIMSE service-class provider for one association message cycle.
#[async_trait]
pub trait ServiceClassProvider: Send + Sync {
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError>;
}

/// One registry routing key for a DIMSE service provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceBinding {
    pub command_field: CommandField,
    pub sop_class_uid: Cow<'static, str>,
}

impl ServiceBinding {
    pub const fn new(command_field: CommandField, sop_class_uid: &'static str) -> Self {
        Self {
            command_field,
            sop_class_uid: Cow::Borrowed(sop_class_uid),
        }
    }

    pub fn owned(command_field: CommandField, sop_class_uid: impl Into<String>) -> Self {
        Self {
            command_field,
            sop_class_uid: Cow::Owned(sop_class_uid.into()),
        }
    }
}

/// Optional descriptor for providers that can declare their registry bindings.
pub trait DescribedServiceClassProvider: ServiceClassProvider {
    fn bindings(&self) -> &[ServiceBinding];
}

/// Routing registry for DIMSE service-class providers keyed by `(CommandField, SOP Class UID)`.
#[derive(Default)]
pub struct ServiceClassRegistry {
    providers: HashMap<(CommandField, String), Arc<dyn ServiceClassProvider>>,
}

impl ServiceClassRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        command_field: CommandField,
        sop_class_uid: impl Into<String>,
        provider: Arc<dyn ServiceClassProvider>,
    ) -> &mut Self {
        self.providers
            .insert((command_field, sop_class_uid.into()), provider);
        self
    }

    pub fn register_described<P>(&mut self, provider: Arc<P>) -> &mut Self
    where
        P: DescribedServiceClassProvider + 'static,
    {
        let bindings = provider.bindings().to_vec();
        let provider: Arc<dyn ServiceClassProvider> = provider;
        for binding in &bindings {
            self.register(
                binding.command_field,
                binding.sop_class_uid.as_ref(),
                provider.clone(),
            );
        }
        self
    }

    pub fn supported_abstract_syntax_uids(&self) -> Vec<String> {
        let mut values = self
            .providers
            .keys()
            .map(|(_, uid)| uid.as_str())
            .filter(|uid| *uid != ANY_SOP_CLASS_UID)
            .map(str::to_string)
            .collect::<Vec<_>>();
        values.sort();
        values.dedup();
        values
    }

    pub(crate) fn provider_for(
        &self,
        command_field: CommandField,
        sop_class_uid: Option<&str>,
    ) -> Option<&Arc<dyn ServiceClassProvider>> {
        if let Some(uid) = sop_class_uid {
            return self
                .providers
                .get(&(command_field, uid.to_string()))
                .or_else(|| {
                    self.providers
                        .get(&(command_field, ANY_SOP_CLASS_UID.to_string()))
                });
        }

        let wildcard = self
            .providers
            .get(&(command_field, ANY_SOP_CLASS_UID.to_string()));
        if wildcard.is_some() {
            return wildcard;
        }

        let mut matches = self
            .providers
            .iter()
            .filter_map(|((field, uid), provider)| {
                if *field == command_field && uid != ANY_SOP_CLASS_UID {
                    Some(provider)
                } else {
                    None
                }
            });
        let first = matches.next()?;
        if matches.next().is_some() {
            None
        } else {
            Some(first)
        }
    }
}

#[async_trait]
impl ServiceClassProvider for ServiceClassRegistry {
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command().await?;

        tracing::info!(
            association.id = ctx.association_id,
            request.id = ctx.current_request_id(),
            command = %command.command_field,
            sop_class_uid = command.sop_class_uid.as_deref(),
            message_id = command.message_id,
            "DIMSE request received"
        );

        let provider = self
            .provider_for(command.command_field, command.sop_class_uid.as_deref())
            .ok_or_else(|| match command.sop_class_uid.as_deref() {
                Some(uid) => DimseError::protocol(format!(
                    "no provider for command {} and SOP Class UID {}",
                    command.command_field, uid
                )),
                None => DimseError::protocol(format!(
                    "no provider for command {} without SOP Class UID",
                    command.command_field
                )),
            })?;

        provider.handle(ctx).await?;
        ctx.complete_message_cycle()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{
        CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
        ServiceClassRegistry,
    };
    use crate::association::AssociationContext;
    use crate::error::DimseError;

    struct NoopProvider;

    #[async_trait]
    impl ServiceClassProvider for NoopProvider {
        async fn handle(&self, _ctx: &mut AssociationContext) -> Result<(), DimseError> {
            Ok(())
        }
    }

    struct MultiBindingProvider;

    #[async_trait]
    impl ServiceClassProvider for MultiBindingProvider {
        async fn handle(&self, _ctx: &mut AssociationContext) -> Result<(), DimseError> {
            Ok(())
        }
    }

    impl DescribedServiceClassProvider for MultiBindingProvider {
        fn bindings(&self) -> &[ServiceBinding] {
            static BINDINGS: [ServiceBinding; 2] = [
                ServiceBinding::new(CommandField::CFindRq, "1.2.3"),
                ServiceBinding::new(CommandField::CGetRq, "1.2.4"),
            ];
            &BINDINGS
        }
    }

    #[test]
    fn provider_lookup_uses_exact_then_wildcard() {
        let exact: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let wildcard: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CStoreRq, "1.2.3", exact.clone());
        registry.register(CommandField::CStoreRq, "*", wildcard.clone());

        let selected_exact = registry
            .provider_for(CommandField::CStoreRq, Some("1.2.3"))
            .expect("exact provider");
        let selected_fallback = registry
            .provider_for(CommandField::CStoreRq, Some("9.9.9"))
            .expect("wildcard provider");

        assert!(Arc::ptr_eq(selected_exact, &exact));
        assert!(Arc::ptr_eq(selected_fallback, &wildcard));
    }

    #[test]
    fn provider_lookup_without_sop_uid_selects_single_bound_provider() {
        let single: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CEchoRq, "1.2.840.10008.1.1", single.clone());

        let selected = registry
            .provider_for(CommandField::CEchoRq, None)
            .expect("single bound provider");
        assert!(Arc::ptr_eq(selected, &single));
    }

    #[test]
    fn register_described_registers_all_bindings() {
        let provider = Arc::new(MultiBindingProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register_described(provider);

        assert!(
            registry
                .provider_for(CommandField::CFindRq, Some("1.2.3"))
                .is_some()
        );
        assert!(
            registry
                .provider_for(CommandField::CGetRq, Some("1.2.4"))
                .is_some()
        );
    }

    #[test]
    fn supported_abstract_syntax_uids_are_sorted_unique_and_skip_wildcard() {
        let any: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let exact_1: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let exact_2: Arc<dyn ServiceClassProvider> = Arc::new(NoopProvider);
        let mut registry = ServiceClassRegistry::new();
        registry.register(CommandField::CStoreRq, "*", any);
        registry.register(CommandField::CEchoRq, "1.2.840.10008.1.1", exact_1);
        registry.register(CommandField::CFindRq, "1.2.840.10008.1.1", exact_2);

        assert_eq!(
            registry.supported_abstract_syntax_uids(),
            vec!["1.2.840.10008.1.1".to_string()]
        );
    }
}
