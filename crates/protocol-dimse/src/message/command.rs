use std::fmt;

use dicom_dictionary_std::tags;

use crate::error::DimseError;
use crate::message::CommandObject;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandField {
    CStoreRq,
    CStoreRsp,
    CGetRq,
    CGetRsp,
    CFindRq,
    CFindRsp,
    CMoveRq,
    CMoveRsp,
    CEchoRq,
    CEchoRsp,
    Unknown(u16),
}

impl CommandField {
    pub fn from_raw(value: u16) -> Self {
        match value {
            0x0001 => Self::CStoreRq,
            0x8001 => Self::CStoreRsp,
            0x0010 => Self::CGetRq,
            0x8010 => Self::CGetRsp,
            0x0020 => Self::CFindRq,
            0x8020 => Self::CFindRsp,
            0x0021 => Self::CMoveRq,
            0x8021 => Self::CMoveRsp,
            0x0030 => Self::CEchoRq,
            0x8030 => Self::CEchoRsp,
            other => Self::Unknown(other),
        }
    }
}

impl fmt::Display for CommandField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CStoreRq => write!(f, "C-STORE-RQ"),
            Self::CStoreRsp => write!(f, "C-STORE-RSP"),
            Self::CGetRq => write!(f, "C-GET-RQ"),
            Self::CGetRsp => write!(f, "C-GET-RSP"),
            Self::CFindRq => write!(f, "C-FIND-RQ"),
            Self::CFindRsp => write!(f, "C-FIND-RSP"),
            Self::CMoveRq => write!(f, "C-MOVE-RQ"),
            Self::CMoveRsp => write!(f, "C-MOVE-RSP"),
            Self::CEchoRq => write!(f, "C-ECHO-RQ"),
            Self::CEchoRsp => write!(f, "C-ECHO-RSP"),
            Self::Unknown(raw) => write!(f, "UNKNOWN(0x{raw:04X})"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    Medium,
    High,
    Low,
    Unknown(u16),
}

impl Priority {
    fn from_raw(value: u16) -> Self {
        match value {
            0x0000 => Self::Medium,
            0x0001 => Self::High,
            0x0002 => Self::Low,
            other => Self::Unknown(other),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DimseCommand {
    pub presentation_context_id: u8,
    pub command_field: CommandField,
    pub sop_class_uid: Option<String>,
    pub sop_instance_uid: Option<String>,
    pub message_id: Option<u16>,
    pub message_id_being_responded_to: Option<u16>,
    pub priority: Option<Priority>,
    pub status: Option<u16>,
    pub move_destination: Option<String>,
    pub move_originator_ae_title: Option<String>,
    pub move_originator_message_id: Option<u16>,
    pub has_data_set: bool,
}

impl DimseCommand {
    pub fn from_command_object(command_object: &CommandObject) -> Result<Self, DimseError> {
        let raw_command_field = required_u16(command_object, tags::COMMAND_FIELD, "Command Field")?;
        let command_field = CommandField::from_raw(raw_command_field);
        let has_data_set = required_u16(
            command_object,
            tags::COMMAND_DATA_SET_TYPE,
            "Command Data Set Type",
        )? != 0x0101;

        let sop_class_uid = match command_field {
            CommandField::CStoreRq | CommandField::CStoreRsp => {
                optional_string(command_object, tags::AFFECTED_SOP_CLASS_UID)?
            }
            _ => optional_string(command_object, tags::AFFECTED_SOP_CLASS_UID)?.or(
                optional_string(command_object, tags::REQUESTED_SOP_CLASS_UID)?,
            ),
        };
        let sop_instance_uid = match command_field {
            CommandField::CStoreRq | CommandField::CStoreRsp => {
                optional_string(command_object, tags::AFFECTED_SOP_INSTANCE_UID)?
            }
            _ => optional_string(command_object, tags::AFFECTED_SOP_INSTANCE_UID)?.or(
                optional_string(command_object, tags::REQUESTED_SOP_INSTANCE_UID)?,
            ),
        };

        Ok(Self {
            presentation_context_id: command_object.presentation_context_id,
            command_field,
            sop_class_uid,
            sop_instance_uid,
            message_id: optional_u16(command_object, tags::MESSAGE_ID)?,
            message_id_being_responded_to: optional_u16(
                command_object,
                tags::MESSAGE_ID_BEING_RESPONDED_TO,
            )?,
            priority: optional_u16(command_object, tags::PRIORITY)?.map(Priority::from_raw),
            status: optional_u16(command_object, tags::STATUS)?,
            move_destination: optional_string(command_object, tags::MOVE_DESTINATION)?,
            move_originator_ae_title: optional_string(
                command_object,
                tags::MOVE_ORIGINATOR_APPLICATION_ENTITY_TITLE,
            )?,
            move_originator_message_id: optional_u16(
                command_object,
                tags::MOVE_ORIGINATOR_MESSAGE_ID,
            )?,
            has_data_set,
        })
    }
}

fn required_u16(
    command_object: &CommandObject,
    tag: dicom_core::Tag,
    name: &str,
) -> Result<u16, DimseError> {
    command_object
        .command
        .element(tag)
        .map_err(|_| DimseError::protocol(format!("missing {}", name)))?
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol(format!("invalid {}", name)))
}

fn optional_u16(
    command_object: &CommandObject,
    tag: dicom_core::Tag,
) -> Result<Option<u16>, DimseError> {
    let element = match command_object.command.element(tag) {
        Ok(element) => element,
        Err(_) => return Ok(None),
    };

    let value = element
        .to_int::<u16>()
        .map_err(|_| DimseError::protocol(format!("invalid {}", tag)))?;
    Ok(Some(value))
}

fn optional_string(
    command_object: &CommandObject,
    tag: dicom_core::Tag,
) -> Result<Option<String>, DimseError> {
    let element = match command_object.command.element(tag) {
        Ok(element) => element,
        Err(_) => return Ok(None),
    };

    let value = element
        .to_str()
        .map_err(|_| DimseError::protocol(format!("invalid {}", tag)))?;
    Ok(Some(value.to_string()))
}

#[cfg(test)]
mod tests {
    use dicom_core::{DataElement, Length, PrimitiveValue, VR, value::Value};
    use dicom_dictionary_std::tags;
    use dicom_object::InMemDicomObject;

    use super::{CommandField, DimseCommand, Priority};
    use crate::{CommandObject, DimseError};

    fn base_command_object(command_field: u16, command_data_set_type: u16) -> CommandObject {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::COMMAND_FIELD,
            VR::US,
            PrimitiveValue::from(command_field),
        ));
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(command_data_set_type),
        ));

        CommandObject {
            presentation_context_id: 3,
            command,
        }
    }

    #[test]
    fn command_field_from_raw_maps_known_and_unknown_values() {
        assert_eq!(CommandField::from_raw(0x0001), CommandField::CStoreRq);
        assert_eq!(CommandField::from_raw(0x8030), CommandField::CEchoRsp);
        assert_eq!(
            CommandField::from_raw(0x9999),
            CommandField::Unknown(0x9999)
        );
    }

    #[test]
    fn parses_minimal_command_with_optional_fields_absent() {
        let command_object = base_command_object(0x0030, 0x0101);
        let parsed = DimseCommand::from_command_object(&command_object).unwrap();

        assert_eq!(parsed.presentation_context_id, 3);
        assert_eq!(parsed.command_field, CommandField::CEchoRq);
        assert!(!parsed.has_data_set);
        assert!(parsed.sop_class_uid.is_none());
        assert!(parsed.message_id.is_none());
        assert!(parsed.priority.is_none());
    }

    #[test]
    fn display_formats_known_and_unknown_command_fields() {
        assert_eq!(CommandField::CFindRsp.to_string(), "C-FIND-RSP");
        assert_eq!(CommandField::Unknown(0x2222).to_string(), "UNKNOWN(0x2222)");
    }

    #[test]
    fn rejects_missing_required_command_field() {
        let mut command = InMemDicomObject::new_empty();
        command.put(DataElement::new(
            tags::COMMAND_DATA_SET_TYPE,
            VR::US,
            PrimitiveValue::from(0x0101_u16),
        ));
        let command_object = CommandObject {
            presentation_context_id: 1,
            command,
        };

        let result = DimseCommand::from_command_object(&command_object);
        assert!(matches!(result, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn rejects_invalid_optional_string_fields() {
        let mut command_object = base_command_object(0x0030, 0x0101);
        command_object.command.put(DataElement::new(
            tags::MOVE_DESTINATION,
            VR::SQ,
            Value::new_sequence(Vec::<InMemDicomObject>::new(), Length::UNDEFINED),
        ));

        let result = DimseCommand::from_command_object(&command_object);
        assert!(matches!(result, Err(DimseError::Protocol(_))));
    }

    #[test]
    fn c_store_does_not_fallback_to_requested_sop_uids() {
        let mut command_object = base_command_object(0x0001, 0x0000);
        command_object.command.put(DataElement::new(
            tags::REQUESTED_SOP_CLASS_UID,
            VR::UI,
            "1.2.840.10008.5.1.4.1.1.2",
        ));
        command_object.command.put(DataElement::new(
            tags::REQUESTED_SOP_INSTANCE_UID,
            VR::UI,
            "1.2.3.4",
        ));

        let parsed = DimseCommand::from_command_object(&command_object).unwrap();
        assert_eq!(parsed.command_field, CommandField::CStoreRq);
        assert!(parsed.sop_class_uid.is_none());
        assert!(parsed.sop_instance_uid.is_none());
    }

    #[test]
    fn priority_maps_unknown_values() {
        let mut command_object = base_command_object(0x0020, 0x0101);
        command_object.command.put(DataElement::new(
            tags::PRIORITY,
            VR::US,
            PrimitiveValue::from(0x1234_u16),
        ));

        let parsed = DimseCommand::from_command_object(&command_object).unwrap();
        assert_eq!(parsed.priority, Some(Priority::Unknown(0x1234)));
    }
}
