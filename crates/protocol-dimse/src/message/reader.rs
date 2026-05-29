use std::collections::VecDeque;
use std::io::Cursor;

use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu, PresentationContextResultReason};

use crate::association::DimseAssociation;
use crate::error::DimseError;
use crate::message::CommandObject;

#[derive(Debug)]
struct ActiveDataSet {
    presentation_context_id: u8,
    finished: bool,
}

/// Incremental DIMSE reader over UL P-DATA PDUs.
/// Reads full commands while keeping datasets streamable PDV-by-PDV.
#[derive(Debug, Default)]
pub struct DimseReader {
    pending_pdvs: VecDeque<PDataValue>,
    active_data_set: Option<ActiveDataSet>,
    bytes_in: u64,
}

impl DimseReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn read_command_object(
        &mut self,
        association: &mut DimseAssociation,
    ) -> Result<CommandObject, DimseError> {
        if self.active_data_set.is_some() {
            return Err(DimseError::protocol(
                "cannot read next command before data set is finished",
            ));
        }

        let (context_id, command_bytes) = self.read_command_fragments(association).await?;
        validate_presentation_context(association, context_id)?;

        let command = InMemDicomObject::read_dataset_with_ts(
            Cursor::new(command_bytes),
            &IMPLICIT_VR_LITTLE_ENDIAN.erased(),
        )?;

        validate_command_object(&command)?;

        let has_data_set =
            element_u16(&command, tags::COMMAND_DATA_SET_TYPE)? != COMMAND_DATA_SET_MISSING;

        self.active_data_set = if has_data_set {
            Some(ActiveDataSet {
                presentation_context_id: context_id,
                finished: false,
            })
        } else {
            None
        };

        Ok(CommandObject {
            presentation_context_id: context_id,
            command,
        })
    }

    pub async fn read_data_pdv(
        &mut self,
        association: &mut DimseAssociation,
    ) -> Result<Option<PDataValue>, DimseError> {
        let (expected_context_id, finished) = match self.active_data_set.as_ref() {
            Some(active) => (active.presentation_context_id, active.finished),
            None => return Ok(None),
        };

        if finished {
            self.active_data_set = None;
            return Ok(None);
        }

        let pdv = self.next_pdv(association).await?;
        if pdv.value_type != PDataValueType::Data {
            return Err(DimseError::protocol(
                "received command PDV while data set was expected",
            ));
        }
        if pdv.presentation_context_id != expected_context_id {
            return Err(DimseError::protocol(
                "data set fragments use multiple presentation contexts",
            ));
        }
        if pdv.is_last
            && let Some(active) = &mut self.active_data_set
        {
            active.finished = true;
        }
        Ok(Some(pdv))
    }

    pub fn has_unfinished_data_set(&self) -> bool {
        self.active_data_set
            .as_ref()
            .map(|active| !active.finished)
            .unwrap_or(false)
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in
    }

    async fn read_command_fragments(
        &mut self,
        association: &mut DimseAssociation,
    ) -> Result<(u8, Vec<u8>), DimseError> {
        let mut command_bytes = Vec::new();
        let mut context_id: Option<u8> = None;

        loop {
            let pdv = self.next_pdv(association).await?;
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
                        let id = context_id
                            .ok_or_else(|| DimseError::protocol("missing presentation context"))?;
                        return Ok((id, command_bytes));
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

    async fn next_pdv(
        &mut self,
        association: &mut DimseAssociation,
    ) -> Result<PDataValue, DimseError> {
        if let Some(pdv) = self.pending_pdvs.pop_front() {
            return Ok(pdv);
        }

        loop {
            match association.receive_pdu().await? {
                Pdu::PData { data } => {
                    if data.is_empty() {
                        continue;
                    }
                    const PDATA_PDU_HEADER_BYTES: u64 = 6;
                    const PDV_ITEM_OVERHEAD_BYTES: u64 = 6;
                    let payload_len = data.iter().map(|pdv| pdv.data.len() as u64).sum::<u64>();
                    self.bytes_in = self
                        .bytes_in
                        .saturating_add(PDATA_PDU_HEADER_BYTES)
                        .saturating_add(PDV_ITEM_OVERHEAD_BYTES.saturating_mul(data.len() as u64))
                        .saturating_add(payload_len);
                    self.pending_pdvs.extend(data);
                    return self
                        .pending_pdvs
                        .pop_front()
                        .ok_or_else(|| DimseError::protocol("missing PDV in P-DATA"));
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
}

const COMMAND_DATA_SET_MISSING: u16 = 0x0101;

fn validate_presentation_context(
    association: &DimseAssociation,
    presentation_context_id: u8,
) -> Result<(), DimseError> {
    let negotiated = association
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "presentation context {} was not negotiated",
                presentation_context_id
            ))
        })?;

    if negotiated.reason != PresentationContextResultReason::Acceptance {
        return Err(DimseError::protocol(format!(
            "presentation context {} is not accepted",
            presentation_context_id
        )));
    }

    Ok(())
}

fn validate_command_object(command: &InMemDicomObject) -> Result<(), DimseError> {
    for tag in command.tags() {
        if tag.group() != 0x0000 {
            return Err(DimseError::protocol(format!(
                "command set contains non-command element {}",
                tag
            )));
        }
    }

    let _ = element_u16(command, tags::COMMAND_FIELD)?;
    let _ = element_u16(command, tags::COMMAND_DATA_SET_TYPE)?;
    Ok(())
}

fn element_u16(command: &InMemDicomObject, tag: dicom_core::Tag) -> Result<u16, DimseError> {
    command
        .element(tag)
        .map_err(|_| DimseError::protocol(format!("missing {}", tag)))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol(format!("invalid {}", tag)))
}
