use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_ul::association::AsyncClientAssociation;
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu, PresentationContextResultReason};
use futures_util::StreamExt;
use raccoon_service_application_entity_registry::{
    AeTitle, ApplicationEntityRegistry, LocalApplicationEntity, PeerApplicationEntity,
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use super::super::dataset::prepare_dimse_dataset;
use super::provider::{
    MoveDestinationError, MoveDestinationStore, MoveStoreOutcome, MoveStoreRequest,
};
use crate::error::DimseError;
use crate::message::{CommandField, CommandObject, DimseCommand, Priority};
use crate::service::storage::StorageServiceProvider;

const DEFAULT_MAX_TOTAL_OUTBOUND_ASSOCIATIONS: usize = 16;
const DEFAULT_MAX_IDLE_ASSOCIATIONS_PER_PEER_PROFILE: usize = 1;
const DEFAULT_MAX_IDLE_ASSOCIATION_AGE: Duration = Duration::from_secs(60);
/// Runtime limits for AE-registry backed C-MOVE destination stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryMoveDestinationStoreConfig {
    pub max_total_associations: usize,
    pub max_idle_per_peer_profile: usize,
    pub max_idle_association_age: Duration,
}

impl Default for RegistryMoveDestinationStoreConfig {
    fn default() -> Self {
        Self {
            max_total_associations: DEFAULT_MAX_TOTAL_OUTBOUND_ASSOCIATIONS,
            max_idle_per_peer_profile: DEFAULT_MAX_IDLE_ASSOCIATIONS_PER_PEER_PROFILE,
            max_idle_association_age: DEFAULT_MAX_IDLE_ASSOCIATION_AGE,
        }
    }
}

/// C-MOVE destination store backed by the Application Entity registry.
pub struct RegistryMoveDestinationStore {
    registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
    local_ae: LocalApplicationEntity,
    config: RegistryMoveDestinationStoreConfig,
    pool: OutboundStoreAssociationPool,
}

impl RegistryMoveDestinationStore {
    pub fn new(
        registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
        local_ae: LocalApplicationEntity,
    ) -> Self {
        Self::with_config(
            registry,
            local_ae,
            RegistryMoveDestinationStoreConfig::default(),
        )
    }

    pub fn with_config(
        registry: Arc<dyn ApplicationEntityRegistry + Send + Sync>,
        local_ae: LocalApplicationEntity,
        config: RegistryMoveDestinationStoreConfig,
    ) -> Self {
        Self {
            registry,
            local_ae,
            config,
            pool: OutboundStoreAssociationPool::new(config),
        }
    }

    pub fn config(&self) -> RegistryMoveDestinationStoreConfig {
        self.config
    }

    async fn resolve_peer(
        &self,
        ae_title: &str,
    ) -> Result<PeerApplicationEntity, MoveDestinationError> {
        let ae_title =
            AeTitle::from_str(ae_title).map_err(|_| MoveDestinationError::UnknownDestination)?;
        self.registry
            .get_peer(&ae_title)
            .await
            .map_err(|error| MoveDestinationError::StoreFailed(error.to_string()))?
            .ok_or(MoveDestinationError::UnknownDestination)
    }
}

#[async_trait]
impl MoveDestinationStore for RegistryMoveDestinationStore {
    async fn validate_destination(&self, ae_title: &str) -> Result<(), MoveDestinationError> {
        self.resolve_peer(ae_title).await.map(|_| ())
    }

    async fn store(
        &self,
        request: MoveStoreRequest,
    ) -> Result<MoveStoreOutcome, MoveDestinationError> {
        let peer = self.resolve_peer(&request.destination_ae_title).await?;
        self.pool.store(&self.local_ae, peer, request).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OutboundPoolKey {
    ae_title: String,
    addr: String,
    transfer_syntax_uid: String,
}

impl OutboundPoolKey {
    fn new(peer: &PeerApplicationEntity, transfer_syntax_uid: String) -> Self {
        Self {
            ae_title: peer.title().as_str().to_string(),
            addr: peer.addr().to_string(),
            transfer_syntax_uid,
        }
    }
}

struct PooledOutboundAssociation {
    association: AsyncClientAssociation<TcpStream>,
    permit: OwnedSemaphorePermit,
    idle_since: Instant,
}

struct CheckedOutAssociation {
    key: OutboundPoolKey,
    association: AsyncClientAssociation<TcpStream>,
    permit: OwnedSemaphorePermit,
}

struct OutboundStoreAssociationPool {
    config: RegistryMoveDestinationStoreConfig,
    permits: Arc<Semaphore>,
    idle: Mutex<HashMap<OutboundPoolKey, Vec<PooledOutboundAssociation>>>,
}

impl OutboundStoreAssociationPool {
    fn new(config: RegistryMoveDestinationStoreConfig) -> Self {
        let permits = config.max_total_associations.max(1);
        Self {
            config,
            permits: Arc::new(Semaphore::new(permits)),
            idle: Mutex::new(HashMap::new()),
        }
    }

    async fn store(
        &self,
        local_ae: &LocalApplicationEntity,
        peer: PeerApplicationEntity,
        request: MoveStoreRequest,
    ) -> Result<MoveStoreOutcome, MoveDestinationError> {
        let transfer_syntax_uid = request
            .instance
            .transfer_syntax_uid
            .as_ref()
            .map(|uid| uid.as_str().to_string())
            .ok_or_else(|| {
                MoveDestinationError::StoreFailed(
                    "retrieved instance is missing Transfer Syntax UID; cannot send raw bytes without transcoding".to_string(),
                )
            })?;
        let key = OutboundPoolKey::new(&peer, transfer_syntax_uid.clone());
        let mut checked_out = self
            .checkout(local_ae, &peer, key, &transfer_syntax_uid)
            .await?;

        let outcome = send_outbound_store(&mut checked_out.association, request).await;
        match outcome {
            Ok(outcome) => {
                self.checkin(checked_out).await;
                Ok(outcome)
            }
            Err(error) => {
                let _ = checked_out.association.abort().await;
                Err(MoveDestinationError::StoreFailed(error.to_string()))
            }
        }
    }

    async fn checkout(
        &self,
        local_ae: &LocalApplicationEntity,
        peer: &PeerApplicationEntity,
        key: OutboundPoolKey,
        transfer_syntax_uid: &str,
    ) -> Result<CheckedOutAssociation, MoveDestinationError> {
        while let Some(pooled) = self.idle.lock().await.get_mut(&key).and_then(Vec::pop) {
            if pooled.idle_since.elapsed() <= self.config.max_idle_association_age {
                return Ok(CheckedOutAssociation {
                    key,
                    association: pooled.association,
                    permit: pooled.permit,
                });
            }
            let _ = pooled.association.release().await;
        }

        let permit = self.permits.clone().acquire_owned().await.map_err(|_| {
            MoveDestinationError::OutOfResources("outbound association pool is closed".to_string())
        })?;
        let association = open_outbound_association(local_ae, peer, transfer_syntax_uid).await?;
        Ok(CheckedOutAssociation {
            key,
            association,
            permit,
        })
    }

    async fn checkin(&self, checked_out: CheckedOutAssociation) {
        let mut idle = self.idle.lock().await;
        let entries = idle.entry(checked_out.key).or_default();
        if entries.len() >= self.config.max_idle_per_peer_profile {
            drop(idle);
            let _ = checked_out.association.release().await;
            return;
        }
        entries.push(PooledOutboundAssociation {
            association: checked_out.association,
            permit: checked_out.permit,
            idle_since: Instant::now(),
        });
    }
}

async fn open_outbound_association(
    local_ae: &LocalApplicationEntity,
    peer: &PeerApplicationEntity,
    transfer_syntax_uid: &str,
) -> Result<AsyncClientAssociation<TcpStream>, MoveDestinationError> {
    let mut options = ClientAssociationOptions::new()
        .calling_ae_title(local_ae.title().as_str())
        .called_ae_title(peer.title().as_str())
        .max_pdu_length(peer.max_pdu_length());

    if let Some(seconds) = peer.connect_timeout_seconds() {
        options = options.connection_timeout(Duration::from_secs(seconds));
    }
    if let Some(seconds) = peer.read_timeout_seconds() {
        options = options.read_timeout(Duration::from_secs(seconds));
    }
    if let Some(seconds) = peer.write_timeout_seconds() {
        options = options.write_timeout(Duration::from_secs(seconds));
    }

    for sop_class_uid in StorageServiceProvider::DEFAULT_STORAGE_SOP_CLASS_UIDS {
        options = options.with_presentation_context(*sop_class_uid, vec![transfer_syntax_uid]);
    }

    options
        .establish_async(peer.addr())
        .await
        .map_err(|error| MoveDestinationError::OutOfResources(error.to_string()))
}

async fn send_outbound_store(
    association: &mut AsyncClientAssociation<TcpStream>,
    request: MoveStoreRequest,
) -> Result<MoveStoreOutcome, DimseError> {
    let sop_class_uid = request.instance.identity.sop_class_uid.as_str();
    let transfer_syntax_uid = request
        .instance
        .transfer_syntax_uid
        .as_ref()
        .map(|uid| uid.as_str())
        .ok_or_else(|| {
            DimseError::protocol(
                "retrieved instance is missing Transfer Syntax UID; cannot send raw bytes without transcoding",
            )
        })?;
    let presentation_context_id =
        accepted_store_presentation_context(association, sop_class_uid, transfer_syntax_uid)?;

    let mut body = request.instance.body;
    let buffered_chunks = prepare_dimse_dataset(&mut body).await?;

    let command = outbound_store_request_command(
        request.message_id,
        sop_class_uid,
        request.instance.identity.sop_instance_uid.as_str(),
        &request.originator_ae_title,
        request.originator_message_id,
        request.priority,
    );
    send_command_object(association, presentation_context_id, &command).await?;

    send_data_set(association, presentation_context_id, buffered_chunks, body).await?;

    let response = read_command(association).await?;
    validate_store_response(&response, presentation_context_id, request.message_id)?;
    match response
        .status
        .ok_or_else(|| DimseError::protocol("C-STORE-RSP missing Status"))?
    {
        0x0000 => Ok(MoveStoreOutcome::Success),
        0xB000..=0xBFFF => Ok(MoveStoreOutcome::Warning),
        status => Err(DimseError::protocol(format!(
            "C-STORE sub-operation failed with status 0x{status:04X}"
        ))),
    }
}

fn validate_store_response(
    response: &DimseCommand,
    presentation_context_id: u8,
    message_id: u16,
) -> Result<(), DimseError> {
    if response.command_field != CommandField::CStoreRsp {
        return Err(DimseError::protocol(format!(
            "expected C-STORE-RSP, got {}",
            response.command_field
        )));
    }
    if response.presentation_context_id != presentation_context_id {
        return Err(DimseError::protocol(format!(
            "C-STORE-RSP presentation context {} does not match request context {}",
            response.presentation_context_id, presentation_context_id
        )));
    }
    if response.has_data_set {
        return Err(DimseError::protocol(
            "C-STORE-RSP must not include a data set",
        ));
    }
    if response.message_id_being_responded_to != Some(message_id) {
        return Err(DimseError::protocol(format!(
            "C-STORE-RSP Message ID Being Responded To {:?} does not match request Message ID {}",
            response.message_id_being_responded_to, message_id
        )));
    }
    Ok(())
}

fn accepted_store_presentation_context(
    association: &AsyncClientAssociation<TcpStream>,
    sop_class_uid: &str,
    transfer_syntax_uid: &str,
) -> Result<u8, DimseError> {
    association
        .presentation_contexts()
        .iter()
        .find(|pc| {
            pc.abstract_syntax == sop_class_uid
                && pc.reason == PresentationContextResultReason::Acceptance
                && pc.transfer_syntax == transfer_syntax_uid
        })
        .map(|pc| pc.id)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "no accepted presentation context for retrieved SOP Class UID {sop_class_uid} with Transfer Syntax UID {transfer_syntax_uid}"
            ))
        })
}

fn outbound_store_request_command(
    message_id: u16,
    sop_class_uid: &str,
    sop_instance_uid: &str,
    move_originator_ae_title: &str,
    move_originator_message_id: u16,
    priority: Priority,
) -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::AFFECTED_SOP_CLASS_UID,
        VR::UI,
        sop_class_uid,
    ));
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(0x0001_u16),
    ));
    command.put(DataElement::new(
        tags::MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(message_id),
    ));
    command.put(DataElement::new(
        tags::PRIORITY,
        VR::US,
        priority_code(priority),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(0x0000_u16),
    ));
    command.put(DataElement::new(
        tags::AFFECTED_SOP_INSTANCE_UID,
        VR::UI,
        sop_instance_uid,
    ));
    command.put(DataElement::new(
        tags::MOVE_ORIGINATOR_APPLICATION_ENTITY_TITLE,
        VR::AE,
        move_originator_ae_title,
    ));
    command.put(DataElement::new(
        tags::MOVE_ORIGINATOR_MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(move_originator_message_id),
    ));
    command
}

async fn send_command_object(
    association: &mut AsyncClientAssociation<TcpStream>,
    presentation_context_id: u8,
    command: &InMemDicomObject,
) -> Result<(), DimseError> {
    let mut bytes = Vec::new();
    command.write_dataset_with_ts(&mut bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased())?;
    association
        .send(&Pdu::PData {
            data: vec![PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Command,
                is_last: true,
                data: bytes,
            }],
        })
        .await
        .map_err(DimseError::Ul)
}

async fn send_data_set(
    association: &mut AsyncClientAssociation<TcpStream>,
    presentation_context_id: u8,
    buffered_chunks: VecDeque<Bytes>,
    mut body: raccoon_contract_object_store::ByteStream,
) -> Result<(), DimseError> {
    let mut pending_chunk = None;
    for chunk in buffered_chunks {
        if let Some(previous) = pending_chunk.replace(chunk) {
            send_data_bytes(association, presentation_context_id, previous, false).await?;
        }
    }

    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|error| DimseError::protocol(error.to_string()))?;
        if let Some(previous) = pending_chunk.replace(chunk) {
            send_data_bytes(association, presentation_context_id, previous, false).await?;
        }
    }

    if let Some(last) = pending_chunk {
        send_data_bytes(association, presentation_context_id, last, true).await
    } else {
        send_data_pdv(
            association,
            PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Data,
                is_last: true,
                data: Vec::new(),
            },
        )
        .await
    }
}

async fn send_data_bytes(
    association: &mut AsyncClientAssociation<TcpStream>,
    presentation_context_id: u8,
    data: Bytes,
    is_last_chunk: bool,
) -> Result<(), DimseError> {
    if data.is_empty() {
        if is_last_chunk {
            send_data_pdv(
                association,
                PDataValue {
                    presentation_context_id,
                    value_type: PDataValueType::Data,
                    is_last: true,
                    data: Vec::new(),
                },
            )
            .await?;
        }
        return Ok(());
    }

    let max_data_len = max_pdv_data_len(association.acceptor_max_pdu_length())?;
    let mut offset = 0;
    while offset < data.len() {
        let end = offset.saturating_add(max_data_len).min(data.len());
        let is_last = is_last_chunk && end == data.len();
        send_data_pdv(
            association,
            PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Data,
                is_last,
                data: data.slice(offset..end).to_vec(),
            },
        )
        .await?;
        offset = end;
    }
    Ok(())
}

async fn send_data_pdv(
    association: &mut AsyncClientAssociation<TcpStream>,
    pdv: PDataValue,
) -> Result<(), DimseError> {
    association
        .send(&Pdu::PData { data: vec![pdv] })
        .await
        .map_err(DimseError::Ul)
}

fn max_pdv_data_len(peer_max_pdu_length: u32) -> Result<usize, DimseError> {
    const PDATA_PDU_HEADER_BYTES: usize = 6;
    const PDV_ITEM_OVERHEAD_BYTES: usize = 6;

    if peer_max_pdu_length == 0 {
        return Ok(usize::MAX);
    }

    (peer_max_pdu_length as usize)
        .checked_sub(PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "peer max PDU length {} too small for PDV payload",
                peer_max_pdu_length
            ))
        })
}

async fn read_command(
    association: &mut AsyncClientAssociation<TcpStream>,
) -> Result<DimseCommand, DimseError> {
    let (presentation_context_id, command_bytes) = read_command_fragments(association).await?;
    let command = InMemDicomObject::read_dataset_with_ts(
        Cursor::new(command_bytes),
        &IMPLICIT_VR_LITTLE_ENDIAN.erased(),
    )?;
    let command_object = CommandObject {
        presentation_context_id,
        command,
    };
    DimseCommand::from_command_object(&command_object)
}

async fn read_command_fragments(
    association: &mut AsyncClientAssociation<TcpStream>,
) -> Result<(u8, Vec<u8>), DimseError> {
    let mut command_bytes = Vec::new();
    let mut context_id: Option<u8> = None;
    let mut pending_pdvs = VecDeque::new();

    loop {
        let pdv = read_next_pdv(association, &mut pending_pdvs).await?;
        match pdv.value_type {
            PDataValueType::Command => {
                match context_id {
                    Some(id) if id != pdv.presentation_context_id => {
                        return Err(DimseError::protocol(
                            "command fragments use multiple presentation contexts",
                        ));
                    }
                    Some(_) => {}
                    None => context_id = Some(pdv.presentation_context_id),
                }
                command_bytes.extend_from_slice(&pdv.data);
                if pdv.is_last {
                    if !pending_pdvs.is_empty() {
                        return Err(DimseError::protocol(
                            "received extra PDVs after command was complete",
                        ));
                    }
                    return context_id
                        .map(|id| (id, command_bytes))
                        .ok_or_else(|| DimseError::protocol("missing presentation context"));
                }
            }
            PDataValueType::Data => {
                return Err(DimseError::protocol(
                    "received data PDV before command was complete",
                ));
            }
        }
    }
}

async fn read_next_pdv(
    association: &mut AsyncClientAssociation<TcpStream>,
    pending_pdvs: &mut VecDeque<PDataValue>,
) -> Result<PDataValue, DimseError> {
    if let Some(pdv) = pending_pdvs.pop_front() {
        return Ok(pdv);
    }

    loop {
        match association.receive().await.map_err(DimseError::Ul)? {
            Pdu::PData { data } => {
                pending_pdvs.extend(data);
                if let Some(pdv) = pending_pdvs.pop_front() {
                    return Ok(pdv);
                }
            }
            Pdu::AbortRQ { .. } => return Err(DimseError::PeerAborted),
            Pdu::ReleaseRQ => return Err(DimseError::PeerReleaseRequested),
            Pdu::ReleaseRP => return Err(DimseError::ConnectionClosed),
            other => {
                return Err(DimseError::protocol(format!(
                    "unexpected PDU during DIMSE read: {:?}",
                    other
                )));
            }
        }
    }
}

fn priority_code(priority: Priority) -> PrimitiveValue {
    PrimitiveValue::from(match priority {
        Priority::Medium => 0x0000_u16,
        Priority::High => 0x0001_u16,
        Priority::Low => 0x0002_u16,
        Priority::Unknown(raw) => raw,
    })
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use raccoon_service_application_entity_registry::{
        ApplicationEntityRegistryError, InMemoryApplicationEntityRegistry,
    };

    use super::*;

    fn local_ae() -> LocalApplicationEntity {
        LocalApplicationEntity::try_new("LOCAL_AE", "127.0.0.1:11112", 64, None, None, 65_536)
            .expect("valid local AE")
    }

    fn peer_ae() -> PeerApplicationEntity {
        PeerApplicationEntity::try_new("PEER_AE", "127.0.0.1:11113", None, None, None, 65_536)
            .expect("valid peer AE")
    }

    #[test]
    fn registry_move_destination_store_config_defaults_to_bounded_pool() {
        let config = RegistryMoveDestinationStoreConfig::default();

        assert_eq!(config.max_total_associations, 16);
        assert_eq!(config.max_idle_per_peer_profile, 1);
        assert_eq!(config.max_idle_association_age, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn validate_destination_accepts_registered_peer() {
        let registry =
            InMemoryApplicationEntityRegistry::try_new(vec![local_ae()], vec![peer_ae()])
                .expect("valid registry");
        let store = RegistryMoveDestinationStore::new(Arc::new(registry), local_ae());

        store
            .validate_destination("PEER_AE")
            .await
            .expect("peer should resolve");
    }

    #[tokio::test]
    async fn validate_destination_rejects_unknown_peer() {
        let registry = InMemoryApplicationEntityRegistry::try_new(vec![local_ae()], Vec::new())
            .expect("valid registry");
        let store = RegistryMoveDestinationStore::new(Arc::new(registry), local_ae());

        let error = store
            .validate_destination("MISSING_AE")
            .await
            .expect_err("peer should be unknown");

        assert!(matches!(error, MoveDestinationError::UnknownDestination));
    }

    #[tokio::test]
    async fn validate_destination_rejects_invalid_ae_title_as_unknown() {
        let registry = InMemoryApplicationEntityRegistry::try_new(vec![local_ae()], Vec::new())
            .expect("valid registry");
        let store = RegistryMoveDestinationStore::new(Arc::new(registry), local_ae());

        let error = store
            .validate_destination("THIS_AE_TITLE_IS_TOO_LONG")
            .await
            .expect_err("invalid AE should be unknown");

        assert!(matches!(error, MoveDestinationError::UnknownDestination));
    }

    struct FailingRegistry;

    #[async_trait]
    impl ApplicationEntityRegistry for FailingRegistry {
        async fn list_locals(
            &self,
        ) -> Result<Vec<LocalApplicationEntity>, ApplicationEntityRegistryError> {
            Ok(Vec::new())
        }

        async fn list_peers(
            &self,
        ) -> Result<Vec<PeerApplicationEntity>, ApplicationEntityRegistryError> {
            Ok(Vec::new())
        }

        async fn get_local(
            &self,
            _ae_title: &AeTitle,
        ) -> Result<Option<LocalApplicationEntity>, ApplicationEntityRegistryError> {
            Ok(None)
        }

        async fn get_peer(
            &self,
            ae_title: &AeTitle,
        ) -> Result<Option<PeerApplicationEntity>, ApplicationEntityRegistryError> {
            Err(ApplicationEntityRegistryError::DuplicateAe(
                ae_title.clone(),
            ))
        }
    }

    #[tokio::test]
    async fn validate_destination_maps_registry_errors_to_store_failed() {
        let store = RegistryMoveDestinationStore::new(Arc::new(FailingRegistry), local_ae());

        let error = store
            .validate_destination("PEER_AE")
            .await
            .expect_err("registry error should fail validation");

        assert!(matches!(error, MoveDestinationError::StoreFailed(_)));
    }

    fn store_response(message_id: u16) -> DimseCommand {
        DimseCommand {
            presentation_context_id: 3,
            command_field: CommandField::CStoreRsp,
            sop_class_uid: None,
            sop_instance_uid: None,
            message_id: None,
            message_id_being_responded_to: Some(message_id),
            priority: None,
            status: Some(0x0000),
            move_destination: None,
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: false,
        }
    }

    #[test]
    fn validate_store_response_accepts_matching_command_only_response() {
        validate_store_response(&store_response(7), 3, 7).expect("valid response");
    }

    #[test]
    fn validate_store_response_rejects_message_id_mismatch() {
        let error = validate_store_response(&store_response(8), 3, 7)
            .expect_err("message ID mismatch should fail");

        assert!(matches!(error, DimseError::Protocol(_)));
    }

    #[test]
    fn validate_store_response_rejects_response_dataset() {
        let mut response = store_response(7);
        response.has_data_set = true;

        let error =
            validate_store_response(&response, 3, 7).expect_err("C-STORE-RSP dataset should fail");

        assert!(matches!(error, DimseError::Protocol(_)));
    }

    #[test]
    fn max_pdv_data_len_accounts_for_pdata_and_pdv_overhead() {
        assert_eq!(max_pdv_data_len(65_536).expect("valid max pdu"), 65_524);
        assert_eq!(max_pdv_data_len(0).expect("unlimited max pdu"), usize::MAX);
        assert!(matches!(
            max_pdv_data_len(12).expect_err("no payload room"),
            DimseError::Protocol(_)
        ));
    }
}
