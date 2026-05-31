use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::TransferSyntax;
use dicom_transfer_syntax_registry::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_ul::pdu::{PDataValue, PDataValueType, Pdu, PresentationContextResultReason};

use crate::association::DimseAssociation;
use crate::error::DimseError;

const PDV_ITEM_OVERHEAD_BYTES: usize = 6;
const PDATA_PDU_HEADER_BYTES: usize = 6;

/// Stateless DIMSE writer; fragments PDVs to fit within the peer's max PDU length.
#[derive(Debug, Default)]
pub struct DimseWriter {
    bytes_out: u64,
}

impl DimseWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn send_command_object(
        &mut self,
        association: &mut DimseAssociation,
        presentation_context_id: u8,
        command: &InMemDicomObject,
    ) -> Result<(), DimseError> {
        validate_presentation_context(association, presentation_context_id)?;
        validate_command_object(command)?;

        let mut bytes = Vec::new();
        command.write_dataset_with_ts(&mut bytes, &IMPLICIT_VR_LITTLE_ENDIAN.erased())?;

        self.send_pdv(
            association,
            PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Command,
                is_last: true,
                data: bytes,
            },
        )
        .await
    }

    pub async fn send_data_pdv(
        &mut self,
        association: &mut DimseAssociation,
        pdv: PDataValue,
    ) -> Result<(), DimseError> {
        if pdv.value_type != PDataValueType::Data {
            return Err(DimseError::protocol("send_data_pdv expects a data PDV"));
        }
        self.send_pdv(association, pdv).await
    }

    pub async fn send_data_set_object(
        &mut self,
        association: &mut DimseAssociation,
        presentation_context_id: u8,
        data_set: &InMemDicomObject,
        transfer_syntax: &TransferSyntax,
    ) -> Result<(), DimseError> {
        let mut bytes = Vec::new();
        data_set.write_dataset_with_ts(&mut bytes, transfer_syntax)?;
        self.send_pdv(
            association,
            PDataValue {
                presentation_context_id,
                value_type: PDataValueType::Data,
                is_last: true,
                data: bytes,
            },
        )
        .await
    }

    async fn send_pdv(
        &mut self,
        association: &mut DimseAssociation,
        pdv: PDataValue,
    ) -> Result<(), DimseError> {
        validate_presentation_context(association, pdv.presentation_context_id)?;

        let peer_max_pdu_length = association.peer_max_pdu_length() as usize;
        let max_pdv_data_len = max_pdv_data_len_for_peer(peer_max_pdu_length)?;

        if pdv.data.is_empty() {
            if !pdv.is_last {
                return Err(DimseError::protocol(
                    "empty PDV payload must be the last fragment",
                ));
            }

            association
                .send_pdu(&Pdu::PData { data: vec![pdv] })
                .await?;
            self.bytes_out = self
                .bytes_out
                .saturating_add((PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES) as u64);
            return Ok(());
        }

        let total_len = pdv.data.len();
        let mut offset = 0;
        while offset < total_len {
            let end = offset.saturating_add(max_pdv_data_len).min(total_len);
            let is_fragment_last = end == total_len && pdv.is_last;
            let fragment_data = pdv.data[offset..end].to_vec();

            association
                .send_pdu(&Pdu::PData {
                    data: vec![PDataValue {
                        presentation_context_id: pdv.presentation_context_id,
                        value_type: pdv.value_type.clone(),
                        is_last: is_fragment_last,
                        data: fragment_data,
                    }],
                })
                .await?;
            self.bytes_out = self
                .bytes_out
                .saturating_add((PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES) as u64)
                .saturating_add((end - offset) as u64);

            offset = end;
        }

        Ok(())
    }

    pub fn bytes_out(&self) -> u64 {
        self.bytes_out
    }
}

fn max_pdv_data_len_for_peer(peer_max_pdu_length: usize) -> Result<usize, DimseError> {
    if peer_max_pdu_length == 0 {
        return Ok(usize::MAX);
    }

    peer_max_pdu_length
        .checked_sub(PDATA_PDU_HEADER_BYTES + PDV_ITEM_OVERHEAD_BYTES)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            DimseError::protocol(format!(
                "peer max PDU length {} too small for PDV payload",
                peer_max_pdu_length
            ))
        })
}

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

    let _ = command
        .element(tags::COMMAND_FIELD)
        .map_err(|_| DimseError::protocol("missing Command Field"))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol("invalid Command Field"))?;

    let _ = command
        .element(tags::COMMAND_DATA_SET_TYPE)
        .map_err(|_| DimseError::protocol("missing Command Data Set Type"))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol("invalid Command Data Set Type"))?;

    Ok(())
}
