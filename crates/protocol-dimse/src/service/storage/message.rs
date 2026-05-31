use std::str::FromStr;

use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_object::InMemDicomObject;
use raccoon_service_application_entity_registry::AeTitle;

use crate::error::DimseError;
use crate::message::{CommandField, DimseCommand, Priority, is_valid_uid};

const MAX_ERROR_COMMENT_CHARS: usize = 64;

/// Parsed C-STORE-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CStoreRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub priority: Priority,
    pub affected_sop_class_uid: String,
    pub affected_sop_instance_uid: String,
    pub move_originator_ae_title: Option<String>,
    pub move_originator_message_id: Option<u16>,
}

impl CStoreRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CStoreRq {
            return Err(DimseError::protocol(format!(
                "expected C-STORE-RQ, got {}",
                command.command_field
            )));
        }
        if !command.has_data_set {
            return Err(DimseError::protocol("C-STORE-RQ must include a data set"));
        }
        if command.move_originator_ae_title.is_some()
            != command.move_originator_message_id.is_some()
        {
            return Err(DimseError::protocol(
                "Move Originator AE Title and Move Originator Message ID must both be present in C-STORE-RQ",
            ));
        }
        if let Some(title) = &command.move_originator_ae_title {
            AeTitle::from_str(title).map_err(|_| {
                DimseError::protocol("invalid Move Originator AE Title in C-STORE-RQ")
            })?;
        }

        let affected_sop_class_uid = command
            .sop_class_uid
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Affected SOP Class UID in C-STORE-RQ"))
            .and_then(|uid| {
                if is_valid_uid(&uid) {
                    Ok(uid)
                } else {
                    Err(DimseError::protocol(
                        "invalid Affected SOP Class UID in C-STORE-RQ",
                    ))
                }
            })?;

        let affected_sop_instance_uid = command
            .sop_instance_uid
            .clone()
            .ok_or_else(|| DimseError::protocol("missing Affected SOP Instance UID in C-STORE-RQ"))
            .and_then(|uid| {
                if is_valid_uid(&uid) {
                    Ok(uid)
                } else {
                    Err(DimseError::protocol(
                        "invalid Affected SOP Instance UID in C-STORE-RQ",
                    ))
                }
            })?;

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-STORE-RQ"))?,
            priority: command
                .priority
                .ok_or_else(|| DimseError::protocol("missing Priority in C-STORE-RQ"))
                .and_then(|priority| match priority {
                    Priority::Medium | Priority::High | Priority::Low => Ok(priority),
                    Priority::Unknown(raw) => Err(DimseError::protocol(format!(
                        "invalid Priority in C-STORE-RQ: 0x{raw:04X}"
                    ))),
                })?,
            affected_sop_class_uid,
            affected_sop_instance_uid,
            move_originator_ae_title: command.move_originator_ae_title.clone(),
            move_originator_message_id: command.move_originator_message_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CStoreStatus {
    /// 0x0000 — operation completed successfully.
    Success,
    /// 0xA700 — local resource exhaustion while receiving or persisting the instance.
    OutOfResources,
    /// 0xA900 — the received data set does not match the requested SOP Class.
    DataSetDoesNotMatchSopClass,
    /// 0xC000 — command or data set could not be interpreted as a valid C-STORE request.
    CannotUnderstand,
}

impl CStoreStatus {
    pub fn code(self) -> u16 {
        match self {
            Self::Success => 0x0000,
            Self::OutOfResources => 0xA700,
            Self::DataSetDoesNotMatchSopClass => 0xA900,
            Self::CannotUnderstand => 0xC000,
        }
    }
}

/// C-STORE-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CStoreResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub affected_sop_instance_uid: String,
    pub status: CStoreStatus,
    pub offending_elements: Vec<Tag>,
    pub error_comment: Option<String>,
}

impl CStoreResponse {
    pub fn success_for(request: &CStoreRequest) -> Self {
        Self::for_request(request, CStoreStatus::Success)
    }

    pub fn for_request(request: &CStoreRequest, status: CStoreStatus) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            affected_sop_instance_uid: request.affected_sop_instance_uid.clone(),
            status,
            offending_elements: Vec::new(),
            error_comment: None,
        }
    }

    pub fn with_offending_element(mut self, tag: Tag) -> Self {
        self.offending_elements.push(tag);
        self
    }

    pub fn with_error_comment(mut self, comment: impl Into<String>) -> Self {
        self.error_comment = Some(
            comment
                .into()
                .chars()
                .take(MAX_ERROR_COMMENT_CHARS)
                .collect(),
        );
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
            PrimitiveValue::from(0x8001_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::AFFECTED_SOP_INSTANCE_UID,
            VR::UI,
            self.affected_sop_instance_uid.as_str(),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0101_u16),
        ));
        command.put(DataElement::new(
            tags::STATUS,
            VR::US,
            PrimitiveValue::from(self.status.code()),
        ));
        if !self.offending_elements.is_empty() {
            command.put(DataElement::new(
                tags::OFFENDING_ELEMENT,
                VR::AT,
                PrimitiveValue::Tags(self.offending_elements.clone().into()),
            ));
        }
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

    use super::{CStoreRequest, CStoreResponse, CStoreStatus};
    use crate::message::{CommandField, DimseCommand, Priority};

    fn store_command() -> DimseCommand {
        DimseCommand {
            presentation_context_id: 3,
            command_field: CommandField::CStoreRq,
            sop_class_uid: Some(uids::CT_IMAGE_STORAGE.to_string()),
            sop_instance_uid: Some("1.2.3.4".to_string()),
            message_id: Some(7),
            message_id_being_responded_to: None,
            priority: Some(Priority::Medium),
            status: None,
            move_destination: None,
            move_originator_ae_title: Some("MOVE_SCU".to_string()),
            move_originator_message_id: Some(99),
            has_data_set: true,
        }
    }

    #[test]
    fn parses_store_request_and_builds_success_response() {
        let request = CStoreRequest::from_command(&store_command()).expect("valid store request");
        assert_eq!(request.presentation_context_id, 3);
        assert_eq!(request.message_id, 7);
        assert_eq!(request.priority, Priority::Medium);
        assert_eq!(
            request.move_originator_ae_title.as_deref(),
            Some("MOVE_SCU")
        );
        assert_eq!(request.move_originator_message_id, Some(99));

        let response = CStoreResponse::success_for(&request).to_command_object();
        let status = response
            .element(dicom_dictionary_std::tags::STATUS)
            .expect("status element")
            .to_int::<u16>()
            .expect("u16 status");
        assert_eq!(status, 0x0000);
    }

    #[test]
    fn rejects_invalid_store_requests() {
        let mut command = store_command();
        command.command_field = CommandField::CEchoRq;
        let error = CStoreRequest::from_command(&command).expect_err("wrong command field");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("expected C-STORE-RQ, got C-ECHO-RQ")
        ));

        let mut command = store_command();
        command.has_data_set = false;
        let error = CStoreRequest::from_command(&command).expect_err("dataset required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("C-STORE-RQ must include a data set")
        ));
    }

    #[test]
    fn rejects_missing_required_store_request_fields() {
        let mut command = store_command();
        command.message_id = None;
        let error = CStoreRequest::from_command(&command).expect_err("message id required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("missing Message ID")
        ));

        let mut command = store_command();
        command.sop_class_uid = None;
        let error = CStoreRequest::from_command(&command).expect_err("sop class uid required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("missing Affected SOP Class UID")
        ));

        let mut command = store_command();
        command.priority = Some(Priority::Unknown(0x0003));
        let error = CStoreRequest::from_command(&command).expect_err("priority value required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("invalid Priority in C-STORE-RQ: 0x0003")
        ));
    }

    #[test]
    fn rejects_invalid_uids() {
        let mut command = store_command();
        command.sop_class_uid = Some("not-a-valid-uid".to_string());
        let error =
            CStoreRequest::from_command(&command).expect_err("sop class uid syntax required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("invalid Affected SOP Class UID")
        ));

        let mut command = store_command();
        command.sop_instance_uid = Some("not-a-valid-uid".to_string());
        let error =
            CStoreRequest::from_command(&command).expect_err("sop instance uid syntax required");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("invalid Affected SOP Instance UID")
        ));
    }

    #[test]
    fn status_codes_match_expected_values() {
        assert_eq!(CStoreStatus::Success.code(), 0x0000);
        assert_eq!(CStoreStatus::OutOfResources.code(), 0xA700);
        assert_eq!(CStoreStatus::DataSetDoesNotMatchSopClass.code(), 0xA900);
        assert_eq!(CStoreStatus::CannotUnderstand.code(), 0xC000);
    }

    #[test]
    fn error_comment_is_truncated_to_lo_length() {
        let request = CStoreRequest::from_command(&store_command()).expect("valid store request");
        let response = CStoreResponse::for_request(&request, CStoreStatus::CannotUnderstand)
            .with_error_comment(
                "presentation context abstract syntax does not match command Affected SOP Class UID",
            )
            .to_command_object();
        let comment = response
            .element(dicom_dictionary_std::tags::ERROR_COMMENT)
            .expect("error comment")
            .to_str()
            .expect("lo string");

        assert_eq!(comment.chars().count(), 64);
    }
}
