mod command;
mod reader;
mod writer;

pub use command::{CommandField, DimseCommand, Priority};
use dicom_object::InMemDicomObject;
pub(crate) use reader::DimseReader;
pub(crate) use writer::DimseWriter;

/// A fully-buffered DIMSE command set bound to its presentation context.
#[derive(Clone, Debug)]
pub struct CommandObject {
    pub presentation_context_id: u8,
    pub command: InMemDicomObject,
}

pub(crate) fn is_valid_uid(uid: &str) -> bool {
    !uid.is_empty()
        && uid.len() <= 64
        && uid.split('.').all(|component| {
            !component.is_empty()
                && component.bytes().all(|b| b.is_ascii_digit())
                && (component.len() == 1 || !component.starts_with('0'))
        })
}

#[cfg(test)]
mod tests {
    use super::is_valid_uid;

    #[test]
    fn uid_validation_rejects_leading_zero_components() {
        assert!(is_valid_uid("1.2.840.10008"));
        assert!(is_valid_uid("0.1.2"));
        assert!(!is_valid_uid("01.2.3"));
        assert!(!is_valid_uid("1.02.3"));
        assert!(!is_valid_uid("1..2"));
        assert!(!is_valid_uid("1.2."));
    }
}
