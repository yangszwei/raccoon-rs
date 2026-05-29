use dicom_core::{DataElement, PrimitiveValue, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::InMemDicomObject;

use crate::error::DimseError;
use crate::message::{CommandField, DimseCommand};

/// Parsed C-ECHO-RQ command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CEchoRequest {
    pub presentation_context_id: u8,
    pub message_id: u16,
    pub affected_sop_class_uid: String,
}

impl CEchoRequest {
    pub fn from_command(command: &DimseCommand) -> Result<Self, DimseError> {
        if command.command_field != CommandField::CEchoRq {
            return Err(DimseError::protocol(format!(
                "expected C-ECHO-RQ, got {}",
                command.command_field
            )));
        }
        if command.has_data_set {
            return Err(DimseError::protocol(
                "C-ECHO-RQ must not include a data set",
            ));
        }

        Ok(Self {
            presentation_context_id: command.presentation_context_id,
            message_id: command
                .message_id
                .ok_or_else(|| DimseError::protocol("missing Message ID in C-ECHO-RQ"))?,
            affected_sop_class_uid: command
                .sop_class_uid
                .clone()
                .unwrap_or_else(|| uids::VERIFICATION.to_string()),
        })
    }
}

/// C-ECHO-RSP command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CEchoResponse {
    pub message_id_being_responded_to: u16,
    pub affected_sop_class_uid: String,
    pub status: u16,
}

impl CEchoResponse {
    pub fn success_for(request: &CEchoRequest) -> Self {
        Self {
            message_id_being_responded_to: request.message_id,
            affected_sop_class_uid: request.affected_sop_class_uid.clone(),
            status: 0x0000,
        }
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
            PrimitiveValue::from(0x8030_u16),
        ));
        command.put(DataElement::new(
            tags::MESSAGE_ID_BEING_RESPONDED_TO,
            VR::US,
            PrimitiveValue::from(self.message_id_being_responded_to),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0101_u16),
        ));
        command.put(DataElement::new(
            tags::STATUS,
            VR::US,
            PrimitiveValue::from(self.status),
        ));
        command
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::uids;

    use super::{CEchoRequest, CEchoResponse};
    use crate::message::{CommandField, DimseCommand};

    fn echo_command() -> DimseCommand {
        DimseCommand {
            presentation_context_id: 1,
            command_field: CommandField::CEchoRq,
            sop_class_uid: Some(uids::VERIFICATION.to_string()),
            sop_instance_uid: None,
            message_id: Some(7),
            message_id_being_responded_to: None,
            priority: None,
            status: None,
            move_destination: None,
            move_originator_ae_title: None,
            move_originator_message_id: None,
            has_data_set: false,
        }
    }

    #[test]
    fn parses_echo_request_and_builds_success_response() {
        let request = CEchoRequest::from_command(&echo_command()).expect("valid C-ECHO-RQ");
        assert_eq!(request.message_id, 7);
        assert_eq!(request.affected_sop_class_uid, uids::VERIFICATION);

        let response = CEchoResponse::success_for(&request).to_command_object();
        let status = response
            .element(dicom_dictionary_std::tags::STATUS)
            .expect("status element")
            .to_int::<u16>()
            .expect("u16 status");
        assert_eq!(status, 0x0000);
    }

    #[test]
    fn rejects_non_echo_command_field() {
        let mut command = echo_command();
        command.command_field = CommandField::CStoreRq;
        let error = CEchoRequest::from_command(&command).expect_err("wrong command field");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("expected C-ECHO-RQ, got C-STORE-RQ")
        ));
    }

    #[test]
    fn rejects_echo_request_with_data_set() {
        let mut command = echo_command();
        command.has_data_set = true;
        let error = CEchoRequest::from_command(&command).expect_err("dataset should be rejected");
        assert!(matches!(
            error,
            crate::error::DimseError::Protocol(message)
                if message.contains("C-ECHO-RQ must not include a data set")
        ));
    }

    #[test]
    fn echoes_default_verification_uid_when_command_omits_it() {
        let mut command = echo_command();
        command.sop_class_uid = None;
        let request = CEchoRequest::from_command(&command).expect("verification uid defaults");
        assert_eq!(request.affected_sop_class_uid, uids::VERIFICATION);
    }
}
