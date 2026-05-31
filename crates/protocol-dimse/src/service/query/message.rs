use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;

use crate::error::DimseError;
use crate::message::{CommandField, DimseCommand, Priority};

/// Parsed C-FIND-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CFindRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
}

impl CFindRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CFindRq {
            return Err(DimseError::protocol(format!(
                "expected C-FIND-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol("C-FIND-RQ must include a data set"));
        }

        let affected_sop_class_uid = command
            .sop_class_uid
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Affected SOP Class UID in C-FIND-RQ"))
            .and_then(|uid| {
                if is_valid_uid(&uid) {
                    Ok(uid)
                } else {
                    Err(DimseError::protocol(
                        "invalid Affected SOP Class UID in C-FIND-RQ",
                    ))
                }
            })?;

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-FIND-RQ"))?,
            priority: command
                .priority
                .ok_or_else(|| DimseError::protocol("missing Priority in C-FIND-RQ"))
                .and_then(|priority| match priority {
                    Priority::Medium | Priority::High | Priority::Low => Ok(priority),
                    Priority::Unknown(raw) => Err(DimseError::protocol(format!(
                        "invalid Priority in C-FIND-RQ: 0x{raw:04X}"
                    ))),
                })?,
            affected_sop_class_uid,
        })
    }
}

fn is_valid_uid(uid: &str) -> bool {
    !uid.is_empty()
        && uid.len() <= 64
        && uid.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && !uid.starts_with('.')
        && !uid.ends_with('.')
        && !uid.contains("..")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CFindStatus {
    Success,
    Pending,
    PendingOptionalKeysNotSupported,
    RefusedOutOfResources,
    IdentifierDoesNotMatchSopClass,
    UnableToProcess,
}

impl CFindStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Success => 0x0000,
            Self::Pending => 0xFF00,
            Self::PendingOptionalKeysNotSupported => 0xFF01,
            Self::RefusedOutOfResources => 0xA700,
            Self::IdentifierDoesNotMatchSopClass => 0xA900,
            Self::UnableToProcess => 0xC000,
        }
    }

    pub fn has_identifier(self) -> bool {
        matches!(self, Self::Pending | Self::PendingOptionalKeysNotSupported)
    }
}

/// C-FIND-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CFindResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: CFindStatus,
    pub error_comment: Option<String>,
}

impl CFindResponse {
    pub fn for_request(request: &CFindRequest, status: CFindStatus) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            status,
            error_comment: None,
        }
    }

    pub fn with_error_comment(mut self, comment: impl Into<String>) -> Self {
        self.error_comment = Some(comment.into().chars().take(64).collect());
        self
    }

    pub fn to_command_object(&self) -> InMemDicomObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::AFFECTED_SOP_CLASS_UID,
            VR::UI,
            self.affected_sop_class_uid.as_str(),
        ));
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(0x8020_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(if self.status.has_identifier() {
                0x0000_u16
            } else {
                0x0101_u16
            }),
        ));
        command.put(DataElement::new(
            tags::STATUS,
            VR::US,
            PrimitiveValue::from(self.status.code()),
        ));
        if let Some(comment) = &self.error_comment {
            command.put(DataElement::new(
                tags::ERROR_COMMENT,
                VR::LO,
                comment.as_str(),
            ));
        }
        command
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::uids;

    use super::{CFindRequest, CFindResponse, CFindStatus};
    use crate::message::{CommandField, DimseCommand, Priority};

    fn find_command() -> DimseCommand {
        DimseCommand {
            presentation_context_id: 5,
            command_field: CommandField::CFindRq,
            sop_class_uid: Some(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND.to_string()),
            sop_instance_uid: None,
            message_id: Some(7),
            message_id_being_responded_to: None,
            priority: Some(Priority::Medium),
            status: None,
            move_destination: None,
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: true,
        }
    }

    #[test]
    fn parses_find_request_and_builds_pending_response() {
        let request = CFindRequest::from_command(&find_command()).expect("valid C-FIND-RQ");
        assert_eq!(request.message_id, 7);

        let response =
            CFindResponse::for_request(&request, CFindStatus::Pending).to_command_object();
        assert_eq!(
            response
                .element(dicom_dictionary_std::tags::COMMAND_DATA_SET_TYPE)
                .unwrap()
                .to_int::<u16>()
                .unwrap(),
            0x0000
        );
    }
}
