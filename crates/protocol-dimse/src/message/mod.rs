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
