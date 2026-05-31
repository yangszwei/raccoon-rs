use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;

use crate::error::DimseError;
use crate::message::{CommandField, DimseCommand, Priority};

/// Parsed C-GET-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CGetRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
    pub originator_ae_title: Option<String>,
}

impl CGetRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CGetRq {
            return Err(DimseError::protocol(format!(
                "expected C-GET-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol("C-GET-RQ must include a data set"));
        }
        let affected_sop_class_uid = command
            .sop_class_uid
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Affected SOP Class UID in C-GET-RQ"))
            .and_then(|uid| {
                if is_valid_uid(&uid) {
                    Ok(uid)
                } else {
                    Err(DimseError::protocol(
                        "invalid Affected SOP Class UID in C-GET-RQ",
                    ))
                }
            })?;

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-GET-RQ"))?,
            priority: command
                .priority
                .ok_or_else(|| DimseError::protocol("missing Priority in C-GET-RQ"))
                .and_then(|priority| match priority {
                    Priority::Medium | Priority::High | Priority::Low => Ok(priority),
                    Priority::Unknown(raw) => Err(DimseError::protocol(format!(
                        "invalid Priority in C-GET-RQ: 0x{raw:04X}"
                    ))),
                })?,
            affected_sop_class_uid,
            originator_ae_title: command.move_originator_ae_title.clone(),
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
pub enum CGetStatus {
    Success,
    Pending,
    RefusedOutOfResourcesUnableToCalculate,
    RefusedOutOfResourcesUnableToPerform,
    IdentifierDoesNotMatchSopClass,
    UnableToProcess,
    SubOperationsCompleteOneOrMoreFailures,
}

impl CGetStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Success => 0x0000,
            Self::Pending => 0xFF00,
            Self::RefusedOutOfResourcesUnableToCalculate => 0xA701,
            Self::RefusedOutOfResourcesUnableToPerform => 0xA702,
            Self::IdentifierDoesNotMatchSopClass => 0xA900,
            Self::UnableToProcess => 0xC000,
            Self::SubOperationsCompleteOneOrMoreFailures => 0xB000,
        }
    }
}

/// C-GET-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CGetResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: CGetStatus,
    pub has_identifier: bool,
    pub remaining: Option<u16>,
    pub completed: Option<u16>,
    pub failed: Option<u16>,
    pub warning: Option<u16>,
    pub error_comment: Option<String>,
}

impl CGetResponse {
    pub fn for_request(request: &CGetRequest, status: CGetStatus) -> Self {
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
            PrimitiveValue::from(0x8010_u16),
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
