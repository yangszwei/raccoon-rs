use dicom_core::Tag;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::StandardDataDictionary;
use serde_json::Value;

use crate::DicomWebError;

pub(crate) fn native_dicom_model_xml(dataset: &Value) -> Result<String, DicomWebError> {
    let object = dataset
        .as_object()
        .ok_or_else(|| DicomWebError::Internal("DICOM XML root is not an object".to_string()))?;

    let mut output = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    output
        .push_str(r#"<NativeDicomModel xmlns="http://dicom.nema.org/PS3.19/models/NativeDICOM">"#);
    for (tag, element) in object {
        write_attribute(&mut output, tag, element)?;
    }
    output.push_str("</NativeDicomModel>");
    Ok(output)
}

fn write_attribute(output: &mut String, tag: &str, element: &Value) -> Result<(), DicomWebError> {
    let object = element.as_object().ok_or_else(|| {
        DicomWebError::Internal(format!("DICOM XML element {tag} is not an object"))
    })?;
    let vr = object.get("vr").and_then(Value::as_str).unwrap_or("UN");

    output.push_str(r#"<DicomAttribute tag=""#);
    output.push_str(&escape_attr(tag));
    output.push('"');
    output.push_str(r#" vr=""#);
    output.push_str(&escape_attr(vr));
    output.push('"');
    if let Some(keyword) = keyword_for_tag(tag) {
        output.push_str(r#" keyword=""#);
        output.push_str(&escape_attr(keyword));
        output.push('"');
    }
    output.push('>');

    if let Some(uri) = object.get("BulkDataURI").and_then(Value::as_str) {
        output.push_str(r#"<BulkData uri=""#);
        output.push_str(&escape_attr(uri));
        output.push_str(r#""/>"#);
    } else if let Some(inline_binary) = object.get("InlineBinary").and_then(Value::as_str) {
        output.push_str("<InlineBinary>");
        output.push_str(&escape_text(inline_binary));
        output.push_str("</InlineBinary>");
    } else if let Some(values) = object.get("Value").and_then(Value::as_array) {
        if vr == "SQ" {
            for (index, item) in values.iter().enumerate() {
                write_item(output, index + 1, item)?;
            }
        } else if vr == "PN" {
            for (index, value) in values.iter().enumerate() {
                write_person_name(output, index + 1, value)?;
            }
        } else {
            for (index, value) in values.iter().enumerate() {
                write_value(output, index + 1, value);
            }
        }
    }

    output.push_str("</DicomAttribute>");
    Ok(())
}

fn write_item(output: &mut String, number: usize, item: &Value) -> Result<(), DicomWebError> {
    let object = item.as_object().ok_or_else(|| {
        DicomWebError::Internal("DICOM XML sequence item is not an object".to_string())
    })?;
    output.push_str(r#"<Item number=""#);
    output.push_str(&number.to_string());
    output.push_str(r#"">"#);
    for (tag, element) in object {
        write_attribute(output, tag, element)?;
    }
    output.push_str("</Item>");
    Ok(())
}

fn write_person_name(
    output: &mut String,
    number: usize,
    value: &Value,
) -> Result<(), DicomWebError> {
    let object = value.as_object().ok_or_else(|| {
        DicomWebError::Internal("DICOM XML person name value is not an object".to_string())
    })?;
    output.push_str(r#"<PersonName number=""#);
    output.push_str(&number.to_string());
    output.push_str(r#"">"#);
    for component in ["Alphabetic", "Ideographic", "Phonetic"] {
        if let Some(text) = object.get(component).and_then(Value::as_str) {
            output.push('<');
            output.push_str(component);
            output.push('>');
            output.push_str(&escape_text(text));
            output.push_str("</");
            output.push_str(component);
            output.push('>');
        }
    }
    output.push_str("</PersonName>");
    Ok(())
}

fn write_value(output: &mut String, number: usize, value: &Value) {
    output.push_str(r#"<Value number=""#);
    output.push_str(&number.to_string());
    output.push_str(r#"">"#);
    output.push_str(&escape_text(&value_text(value)));
    output.push_str("</Value>");
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn keyword_for_tag(tag: &str) -> Option<&'static str> {
    let tag = parse_tag(tag)?;
    StandardDataDictionary
        .by_tag(tag)
        .map(|entry| entry.alias())
}

fn parse_tag(tag: &str) -> Option<Tag> {
    if tag.len() != 8 {
        return None;
    }
    Some(Tag(
        u16::from_str_radix(&tag[..4], 16).ok()?,
        u16::from_str_radix(&tag[4..], 16).ok()?,
    ))
}

fn escape_attr(value: &str) -> String {
    escape_xml(value)
}

fn escape_text(value: &str) -> String {
    escape_xml(value)
}

fn escape_xml(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(ch),
        }
    }
    output
}
