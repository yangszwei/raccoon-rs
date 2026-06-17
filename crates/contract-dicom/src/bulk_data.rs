/// Returns true when a DICOM JSON element should be represented as bulk data.
///
/// `parent_sequence` is the enclosing sequence tag for sequence item contents.
/// It is required for Waveform Data (`5400,1010`), which is bulk data only
/// inside Waveform Sequence (`5400,0100`).
pub fn is_bulk_data_element(tag: &str, parent_sequence: Option<&str>) -> bool {
    if parent_sequence == Some("54000100") && tag == "54001010" {
        return true;
    }
    match tag {
        "00281201" | "00281202" | "00281203" | "00281204" | "00281211" | "00281212"
        | "00281213" | "00281221" | "00281222" | "00281223" | "00281224" | "00283006"
        | "00287FE0" | "00420011" | "56000020" | "7FE00008" | "7FE00009" | "7FE00010" => true,
        _ => is_repeating_group_bulk_data_tag(tag),
    }
}

fn is_repeating_group_bulk_data_tag(tag: &str) -> bool {
    let Some((group, element)) = tag_group_element(tag) else {
        return false;
    };
    matches!(
        (group, element),
        (0x5000..=0x50FF, 0x200C | 0x3000) | (0x6000..=0x60FF, 0x3000)
    )
}

fn tag_group_element(tag: &str) -> Option<(u16, u16)> {
    if tag.len() != 8 {
        return None;
    }
    Some((
        u16::from_str_radix(&tag[..4], 16).ok()?,
        u16::from_str_radix(&tag[4..], 16).ok()?,
    ))
}
