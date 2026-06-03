use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_ul::association::Association;
use dicom_ul::pdu::{PDataValue, Pdu};
use raccoon_service_application_entity_registry::AeTitle;

use crate::error::DimseError;
use crate::message::{CommandObject, DimseCommand, DimseReader, DimseWriter};

/// An established inbound DIMSE association wrapping a `dicom-ul` async server connection.
pub struct DimseAssociation {
    inner: dicom_ul::association::AsyncServerAssociation<tokio::net::TcpStream>,
    peer_ae_title: Option<AeTitle>,
    called_ae_title: AeTitle,
    pub(crate) association_id: u64,
}

impl DimseAssociation {
    pub(crate) fn new(
        inner: dicom_ul::association::AsyncServerAssociation<tokio::net::TcpStream>,
        called_ae_title: AeTitle,
        association_id: u64,
    ) -> Self {
        let peer_ae_title = inner.peer_ae_title().parse::<AeTitle>().ok();
        Self {
            inner,
            peer_ae_title,
            called_ae_title,
            association_id,
        }
    }

    pub fn peer_ae_title(&self) -> Option<&AeTitle> {
        self.peer_ae_title.as_ref()
    }

    pub fn called_ae_title(&self) -> &AeTitle {
        &self.called_ae_title
    }

    pub fn presentation_contexts(&self) -> &[dicom_ul::pdu::PresentationContextNegotiated] {
        self.inner.presentation_contexts()
    }

    pub fn peer_max_pdu_length(&self) -> u32 {
        self.inner.peer_max_pdu_length()
    }

    pub(crate) async fn receive_pdu(&mut self) -> Result<Pdu, DimseError> {
        self.inner.receive().await.map_err(DimseError::Ul)
    }

    pub(crate) async fn send_pdu(&mut self, pdu: &Pdu) -> Result<(), DimseError> {
        self.inner.send(pdu).await.map_err(DimseError::Ul)
    }

    pub async fn abort(self) -> Result<(), DimseError> {
        self.inner.abort().await.map_err(DimseError::Ul)
    }
}

/// DIMSE I/O context scoped to one established association.
///
/// Bundles the association connection, reader/writer state, and command caches
/// for use by service-class providers.
pub struct AssociationContext {
    association: DimseAssociation,
    pub(crate) association_id: u64,
    next_request_id: u64,
    current_request_id: Option<u64>,
    response_status: Option<u16>,
    response_reason: Option<String>,
    request_command: Option<DimseCommand>,
    response_command: Option<DimseCommand>,
    reader: DimseReader,
    writer: DimseWriter,
    cached_command_object: Option<CommandObject>,
    cached_command: Option<DimseCommand>,
}

impl AssociationContext {
    pub(crate) fn new(association: DimseAssociation) -> Self {
        let association_id = association.association_id;
        Self {
            association,
            association_id,
            next_request_id: 1,
            current_request_id: None,
            response_status: None,
            response_reason: None,
            request_command: None,
            response_command: None,
            reader: DimseReader::new(),
            writer: DimseWriter::new(),
            cached_command_object: None,
            cached_command: None,
        }
    }

    pub fn association(&self) -> &DimseAssociation {
        &self.association
    }

    pub(crate) fn association_mut(&mut self) -> &mut DimseAssociation {
        &mut self.association
    }

    pub(crate) fn begin_message_cycle(&mut self) {
        self.current_request_id = None;
        self.response_status = None;
        self.response_reason = None;
        self.request_command = None;
        self.response_command = None;
        self.cached_command_object = None;
        self.cached_command = None;
    }

    pub(crate) fn current_request_id(&self) -> Option<u64> {
        self.current_request_id
    }

    fn start_request_cycle(&mut self) -> u64 {
        if let Some(id) = self.current_request_id {
            return id;
        }

        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.current_request_id = Some(id);
        id
    }

    pub(crate) fn record_response_status(&mut self, status: u16, reason: Option<String>) {
        self.response_status = Some(status);
        self.response_reason = reason;
    }

    pub(crate) fn response_status(&self) -> Option<u16> {
        self.response_status
    }

    pub(crate) fn response_reason(&self) -> Option<&str> {
        self.response_reason.as_deref()
    }

    pub(crate) fn request_command(&self) -> Option<&DimseCommand> {
        self.request_command.as_ref()
    }

    pub(crate) fn response_command(&self) -> Option<&DimseCommand> {
        self.response_command.as_ref()
    }

    pub(crate) fn cached_command(&self) -> Option<&DimseCommand> {
        self.cached_command.as_ref()
    }

    /// Read and cache the command object for this message cycle.
    pub async fn read_command_object(&mut self) -> Result<CommandObject, DimseError> {
        if let Some(command_object) = &self.cached_command_object {
            return Ok(command_object.clone());
        }

        let command_object = self
            .reader
            .read_command_object(&mut self.association)
            .await?;
        self.cached_command_object = Some(command_object.clone());
        Ok(command_object)
    }

    /// Read and cache a parsed `DimseCommand`.
    pub async fn read_command(&mut self) -> Result<DimseCommand, DimseError> {
        if let Some(command) = &self.cached_command {
            return Ok(command.clone());
        }

        let command = DimseCommand::from_command_object(&self.read_command_object().await?)?;
        if self.request_command.is_none() {
            self.start_request_cycle();
            self.request_command = Some(command.clone());
        }
        self.cached_command = Some(command.clone());
        Ok(command)
    }

    /// Read a command that is nested inside the active DIMSE operation.
    ///
    /// C-GET invokes C-STORE sub-operations on the same association, so the
    /// provider must read the C-STORE-RSP instead of reusing the cached
    /// C-GET-RQ for the outer message cycle.
    pub(crate) async fn read_suboperation_command(&mut self) -> Result<DimseCommand, DimseError> {
        self.cached_command_object = None;
        self.cached_command = None;
        self.read_command().await
    }

    /// Read one dataset PDV fragment for the active command.
    pub async fn read_data_pdv(&mut self) -> Result<Option<PDataValue>, DimseError> {
        self.reader.read_data_pdv(&mut self.association).await
    }

    /// Serialize and send one DIMSE command set.
    pub async fn send_command_object(
        &mut self,
        presentation_context_id: u8,
        command: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        let response_command = DimseCommand::from_command_object(&CommandObject {
            presentation_context_id,
            command: command.clone(),
        })?;

        self.writer
            .send_command_object(&mut self.association, presentation_context_id, command)
            .await?;
        self.response_command = Some(response_command);
        Ok(())
    }

    /// Send one dataset PDV fragment.
    pub async fn send_data_pdv(&mut self, pdv: PDataValue) -> Result<(), DimseError> {
        self.writer.send_data_pdv(&mut self.association, pdv).await
    }

    /// Serialize and send one DIMSE data set using the negotiated transfer syntax.
    pub async fn send_data_set_object(
        &mut self,
        presentation_context_id: u8,
        data_set: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        let transfer_syntax_uid = self
            .association
            .presentation_contexts()
            .iter()
            .find(|pc| pc.id == presentation_context_id)
            .map(|pc| pc.transfer_syntax.as_str())
            .ok_or_else(|| {
                DimseError::protocol(format!(
                    "presentation context {} was not negotiated",
                    presentation_context_id
                ))
            })?;
        let transfer_syntax = TransferSyntaxRegistry
            .get(transfer_syntax_uid)
            .ok_or_else(|| {
                DimseError::protocol(format!(
                    "unsupported transfer syntax {}",
                    transfer_syntax_uid
                ))
            })?;

        self.writer
            .send_data_set_object(
                &mut self.association,
                presentation_context_id,
                data_set,
                transfer_syntax,
            )
            .await
    }

    pub fn has_unfinished_data_set(&self) -> bool {
        self.reader.has_unfinished_data_set()
    }

    pub(crate) fn complete_message_cycle(&mut self) -> Result<(), DimseError> {
        if self.has_unfinished_data_set() {
            return Err(DimseError::protocol(
                "provider returned before consuming the full DIMSE data set",
            ));
        }
        self.cached_command_object = None;
        self.cached_command = None;
        Ok(())
    }

    pub fn bytes_in(&self) -> u64 {
        self.reader.bytes_in()
    }

    pub fn bytes_out(&self) -> u64 {
        self.writer.bytes_out()
    }

    pub(crate) fn into_dimse_association(self) -> DimseAssociation {
        self.association
    }
}
