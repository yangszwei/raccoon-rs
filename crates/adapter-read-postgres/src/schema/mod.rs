mod attributes;
mod tables;

use std::collections::HashMap;

pub(crate) use attributes::{ATTRIBUTE_MAPPINGS, AttributeMapping, VrClass};
use dicom_core::Tag;
pub(crate) use tables::TableId;

/// O(1) tag → [`AttributeMapping`] lookup, built once from [`ATTRIBUTE_MAPPINGS`].
#[derive(Debug)]
pub(crate) struct AttributeRegistry {
    by_tag: HashMap<Tag, &'static AttributeMapping>,
}

impl AttributeRegistry {
    pub(crate) fn new() -> Self {
        let by_tag = ATTRIBUTE_MAPPINGS.iter().map(|m| (m.tag, m)).collect();
        Self { by_tag }
    }

    pub(crate) fn get(&self, tag: Tag) -> Option<&'static AttributeMapping> {
        self.by_tag.get(&tag).copied()
    }
}
