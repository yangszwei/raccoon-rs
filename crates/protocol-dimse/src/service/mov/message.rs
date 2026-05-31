use std::str::FromStr;

use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use raccoon_service_application_entity_registry::AeTitle;

use crate::error::DimseError;
use crate::message::{CommandField, DimseCommand, Priority, is_valid_uid};

/// Parsed C-MOVE-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CMoveRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
    pub move_destination: String,
}

impl CMoveRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CMoveRq {
            return Err(DimseError::protocol(format!(
                "expected C-MOVE-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol("C-MOVE-RQ must include a data set"));
        }
        let affected_sop_class_uid = command
            .sop_class_uid
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Affected SOP Class UID in C-MOVE-RQ"))
            .and_then(|uid| {
                if is_valid_uid(&uid) {
                    Ok(uid)
                } else {
                    Err(DimseError::protocol(
                        "invalid Affected SOP Class UID in C-MOVE-RQ",
                    ))
                }
            })?;
        let move_destination = command
            .move_destination
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Move Destination in C-MOVE-RQ"))?;
        let move_destination = AeTitle::from_str(&move_destination)?.to_string();

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-MOVE-RQ"))?,
            priority: command
                .priority
                .ok_or_else(|| DimseError::protocol("missing Priority in C-MOVE-RQ"))
                .and_then(|priority| match priority {
                    Priority::Medium | Priority::High | Priority::Low => Ok(priority),
                    Priority::Unknown(raw) => Err(DimseError::protocol(format!(
                        "invalid Priority in C-MOVE-RQ: 0x{raw:04X}"
                    ))),
                })?,
            affected_sop_class_uid,
            move_destination,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CMoveStatus {
    Success,
    Pending,
    RefusedOutOfResourcesUnableToCalculate,
    RefusedOutOfResourcesUnableToPerform,
    MoveDestinationUnknown,
    IdentifierDoesNotMatchSopClass,
    UnableToProcess,
    SubOperationsCompleteOneOrMoreFailures,
}

impl CMoveStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Success => 0x0000,
            Self::Pending => 0xFF00,
            Self::RefusedOutOfResourcesUnableToCalculate => 0xA701,
            Self::RefusedOutOfResourcesUnableToPerform => 0xA702,
            Self::MoveDestinationUnknown => 0xA801,
            Self::IdentifierDoesNotMatchSopClass => 0xA900,
            Self::UnableToProcess => 0xC000,
            Self::SubOperationsCompleteOneOrMoreFailures => 0xB000,
        }
    }
}

/// C-MOVE-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CMoveResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: CMoveStatus,
    pub has_identifier: bool,
    pub remaining: Option<u16>,
    pub completed: Option<u16>,
    pub failed: Option<u16>,
    pub warning: Option<u16>,
    pub error_comment: Option<String>,
}

impl CMoveResponse {
    pub fn for_request(request: &CMoveRequest, status: CMoveStatus) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            status,
            has_identifier: false,
            remaining: None,
            completed: None,
            failed: None,
            warning: None,
            error_comment: None,
        }
    }

    pub fn with_counts(
        mut self,
        remaining: Option<u16>,
        completed: u16,
        failed: u16,
        warning: u16,
    ) -> Self {
        self.remaining = remaining;
        self.completed = Some(completed);
        self.failed = Some(failed);
        self.warning = Some(warning);
        self
    }

    pub fn with_identifier(mut self) -> Self {
        self.has_identifier = true;
        self
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
            PrimitiveValue::from(0x8021_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(if self.has_identifier {
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
        put_optional_u16(
            &mut command,
            tags::NUMBER_OF_REMAINING_SUBOPERATIONS,
            self.remaining,
        );
        put_optional_u16(
            &mut command,
            tags::NUMBER_OF_COMPLETED_SUBOPERATIONS,
            self.completed,
        );
        put_optional_u16(
            &mut command,
            tags::NUMBER_OF_FAILED_SUBOPERATIONS,
            self.failed,
        );
        put_optional_u16(
            &mut command,
            tags::NUMBER_OF_WARNING_SUBOPERATIONS,
            self.warning,
        );
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

fn put_optional_u16(object: &mut InMemDicomObject, tag: dicom_core::Tag, value: Option<u16>) {
    if let Some(value) = value {
        object.put(DataElement::new(tag, VR::US, PrimitiveValue::from(value)));
    }
}

#[cfg(test)]
mod tests {
    use super::{CMoveRequest, CMoveResponse, CMoveStatus};
    use crate::message::{CommandField, DimseCommand, Priority};

    fn move_command() -> DimseCommand {
        DimseCommand {
            presentation_context_id: 5,
            command_field: CommandField::CMoveRq,
            sop_class_uid: Some("1.2.840.10008.5.1.4.1.2.2.2".to_string()),
            sop_instance_uid: None,
            message_id: Some(19),
            message_id_being_responded_to: None,
            priority: Some(Priority::High),
            status: None,
            move_destination: Some(" DEST_AE ".to_string()),
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: true,
        }
    }

    #[test]
    fn parses_c_move_request() {
        let request = CMoveRequest::from_command(&move_command()).expect("valid C-MOVE-RQ");

        assert_eq!(request.presentation_context_id, 5);
        assert_eq!(request.message_id, 19);
        assert_eq!(request.priority, Priority::High);
        assert_eq!(request.move_destination, "DEST_AE");
    }

    #[test]
    fn c_move_response_writes_command_fields_and_counts() {
        let request = CMoveRequest::from_command(&move_command()).expect("valid C-MOVE-RQ");
        let response = CMoveResponse::for_request(&request, CMoveStatus::Pending)
            .with_counts(Some(3), 1, 2, 4)
            .to_command_object();

        assert_eq!(
            response
                .element(dicom_dictionary_std::tags::COMMAND_FIELD)
                .expect("command field")
                .to_int::<u16>()
                .expect("US"),
            0x8021
        );
        assert_eq!(
            response
                .element(dicom_dictionary_std::tags::MESSAGE_ID_BEING_RESPONDED_TO)
                .expect("message id being responded to")
                .to_int::<u16>()
                .expect("US"),
            19
        );
        assert_eq!(
            response
                .element(dicom_dictionary_std::tags::STATUS)
                .expect("status")
                .to_int::<u16>()
                .expect("US"),
            0xFF00
        );
        assert_eq!(
            response
                .element(dicom_dictionary_std::tags::NUMBER_OF_REMAINING_SUBOPERATIONS)
                .expect("remaining")
                .to_int::<u16>()
                .expect("US"),
            3
        );
    }
}
