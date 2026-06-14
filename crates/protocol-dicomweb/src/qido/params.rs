use dicom_core::Tag;
use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::{StandardDataDictionary, tags};
use raccoon_contract_dicom::{SpecificCharacterSet, SpecificCharacterSetError};
use raccoon_service_query::{
    AttributePath, MatchingRule, Predicate, Projection, QueryPaging, RangeMatching,
};

use crate::DicomWebError;

#[derive(Debug)]
pub(crate) struct QidoQueryParams {
    pub projection: Projection,
    pub predicates: Vec<Predicate>,
    pub paging: Option<QueryPaging>,
    pub fuzzy_matching: bool,
    pub timezone_offset: Option<String>,
    pub specific_character_set: Option<Vec<String>>,
    pub limit_for_span: Option<u64>,
    pub offset_for_span: u64,
}

impl QidoQueryParams {
    pub fn parse(raw: Option<&str>) -> Result<Self, DicomWebError> {
        let pairs = parse_raw_pairs(raw.unwrap_or_default())?;
        let charset_terms = specific_character_set_terms(&pairs)?;
        let charset = match charset_terms.as_ref() {
            Some(terms) => Some(parse_specific_character_set(terms)?),
            None => None,
        };

        let mut includefields = Vec::new();
        let mut predicates = Vec::new();
        let mut limit = None;
        let mut offset = 0;
        let mut fuzzy_matching = false;
        let mut timezone_offset = None;

        for pair in pairs {
            match pair.key.as_str() {
                "includefield" => {
                    includefields.push(decode_utf8_control("includefield", &pair.value)?)
                }
                "limit" => {
                    limit = Some(parse_u64(
                        "limit",
                        &decode_utf8_control("limit", &pair.value)?,
                    )?)
                }
                "offset" => {
                    offset = parse_u64("offset", &decode_utf8_control("offset", &pair.value)?)?
                }
                "fuzzymatching" => {
                    fuzzy_matching = parse_bool(
                        "fuzzymatching",
                        &decode_utf8_control("fuzzymatching", &pair.value)?,
                    )?
                }
                "timezoneoffset" => {
                    timezone_offset = Some(decode_utf8_control("timezoneoffset", &pair.value)?)
                }
                key => {
                    let tag = parse_tag(key)?;
                    if tag == tags::SPECIFIC_CHARACTER_SET {
                        continue;
                    }
                    let value = decode_query_value(tag, &pair.value, charset.as_ref())?;
                    predicates.push(predicate_for_value(tag, value)?);
                }
            }
        }

        let projection = projection(includefields)?;
        let paging = limit
            .map(|limit| QueryPaging::new(offset, limit))
            .transpose()
            .map_err(|error| DicomWebError::bad_request(error.to_string()))?;

        Ok(Self {
            projection,
            predicates,
            paging,
            fuzzy_matching,
            timezone_offset,
            specific_character_set: charset_terms,
            limit_for_span: limit,
            offset_for_span: offset,
        })
    }
}

#[derive(Debug)]
struct RawPair {
    key: String,
    value: Vec<u8>,
}

fn parse_raw_pairs(raw: &str) -> Result<Vec<RawPair>, DicomWebError> {
    raw.split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            let key = decode_form_component(key.as_bytes())?;
            let key = std::str::from_utf8(&key)
                .map_err(|_| {
                    DicomWebError::bad_request("QIDO-RS query parameter name is not UTF-8")
                })?
                .to_string();
            let value = decode_form_component(value.as_bytes())?;
            Ok(RawPair { key, value })
        })
        .collect()
}

fn decode_form_component(input: &[u8]) -> Result<Vec<u8>, DicomWebError> {
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= input.len() {
                    return Err(DicomWebError::bad_request(
                        "invalid percent-encoding in QIDO-RS query",
                    ));
                }
                let hi = hex_value(input[index + 1]).ok_or_else(|| {
                    DicomWebError::bad_request("invalid percent-encoding in QIDO-RS query")
                })?;
                let lo = hex_value(input[index + 2]).ok_or_else(|| {
                    DicomWebError::bad_request("invalid percent-encoding in QIDO-RS query")
                })?;
                output.push((hi << 4) | lo);
                index += 3;
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    Ok(output)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn specific_character_set_terms(pairs: &[RawPair]) -> Result<Option<Vec<String>>, DicomWebError> {
    let mut terms = Vec::new();
    for pair in pairs {
        let is_charset = pair.key == "SpecificCharacterSet"
            || parse_tag(&pair.key).is_ok_and(|tag| tag == tags::SPECIFIC_CHARACTER_SET);
        if !is_charset {
            continue;
        }
        let value = decode_utf8_control("SpecificCharacterSet", &pair.value)?;
        terms.extend(value.split('\\').map(|part| part.trim().to_string()));
    }
    Ok((!terms.is_empty()).then_some(terms))
}

fn parse_specific_character_set(terms: &[String]) -> Result<SpecificCharacterSet, DicomWebError> {
    let charset = SpecificCharacterSet::parse_terms(terms.iter().cloned())
        .map_err(specific_character_set_bad_request)?;
    Ok(charset)
}

fn decode_query_value(
    tag: Tag,
    bytes: &[u8],
    charset: Option<&SpecificCharacterSet>,
) -> Result<String, DicomWebError> {
    if !is_character_set_affected_vr(tag) {
        return decode_utf8_control("QIDO-RS query value", bytes);
    }
    match charset {
        Some(charset) if charset.is_supported() => charset
            .decode_bytes(bytes)
            .map_err(specific_character_set_bad_request),
        Some(charset) if !bytes.iter().all(u8::is_ascii) || bytes.contains(&0x1b) => {
            Err(DicomWebError::not_acceptable(format!(
                "unsupported Specific Character Set {}",
                charset.label()
            )))
        }
        Some(_) | None => {
            if bytes.iter().all(u8::is_ascii) {
                Ok(String::from_utf8(bytes.to_vec()).expect("ASCII is valid UTF-8"))
            } else {
                Err(DicomWebError::bad_request(
                    "QIDO-RS text query value contains non-ASCII bytes without a supported Specific Character Set",
                ))
            }
        }
    }
}

fn decode_utf8_control(name: &str, bytes: &[u8]) -> Result<String, DicomWebError> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| DicomWebError::bad_request(format!("{name} is not valid UTF-8")))
}

fn specific_character_set_bad_request(error: SpecificCharacterSetError) -> DicomWebError {
    DicomWebError::bad_request(error.to_string())
}

pub(crate) fn uid_predicate(tag: Tag, value: &str) -> Predicate {
    Predicate::Attribute(
        AttributePath::from_tag(tag),
        MatchingRule::SingleValue(value.to_string()),
    )
}

fn projection(values: Vec<String>) -> Result<Projection, DicomWebError> {
    if values.is_empty() {
        return Ok(Projection::Default);
    }
    if values.iter().any(|value| value.eq_ignore_ascii_case("all")) {
        return Ok(Projection::All);
    }

    let mut fields = Vec::new();
    for value in values {
        for part in value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            fields.push(AttributePath::from_tag(parse_tag(part)?));
        }
    }
    Ok(Projection::Fields(fields))
}

fn predicate_for_value(tag: Tag, value: String) -> Result<Predicate, DicomWebError> {
    if vr_for_tag(tag) == VR::SQ {
        return Err(DicomWebError::bad_request(
            "QIDO-RS sequence matching is not supported",
        ));
    }

    let rule = if value.is_empty() {
        MatchingRule::Universal
    } else if is_uid_tag(tag) {
        MatchingRule::UidList(value.split('\\').map(str::to_string).collect())
    } else if is_range_vr(tag) {
        range_rule(tag, &value).unwrap_or_else(|| string_rule(value))
    } else {
        string_rule(value)
    };
    Ok(Predicate::Attribute(AttributePath::from_tag(tag), rule))
}

fn string_rule(value: String) -> MatchingRule {
    if value.contains('\\') {
        MatchingRule::MultipleValues(value.split('\\').map(str::to_string).collect())
    } else if value.contains('*') || value.contains('?') {
        MatchingRule::Wildcard(value)
    } else {
        MatchingRule::SingleValue(value)
    }
}

fn range_rule(tag: Tag, value: &str) -> Option<MatchingRule> {
    let (start, end) = range_bounds(tag, value)?;
    let range = match (start.is_empty(), end.is_empty()) {
        (true, true) => return None,
        (true, false) => RangeMatching::until_end(end),
        (false, true) => RangeMatching::from_start(start),
        (false, false) => RangeMatching::closed(start, end),
    };
    Some(if vr_for_tag(tag) == VR::DT {
        MatchingRule::DateTimeRange(range)
    } else {
        MatchingRule::Range(range)
    })
}

fn range_bounds(tag: Tag, value: &str) -> Option<(&str, &str)> {
    if vr_for_tag(tag) == VR::DT {
        return datetime_range_bounds(value);
    }
    value
        .find('-')
        .map(|index| (&value[..index], &value[index + 1..]))
}

fn datetime_range_bounds(value: &str) -> Option<(&str, &str)> {
    value.match_indices('-').find_map(|(index, _)| {
        let start = &value[..index];
        let end = &value[index + 1..];
        (valid_datetime_range_bound(start) && valid_datetime_range_bound(end))
            .then_some((start, end))
    })
}

fn valid_datetime_range_bound(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    let value = strip_datetime_timezone(value);
    let (date_time, fraction) = value.split_once('.').unwrap_or((value, ""));
    !date_time.is_empty()
        && date_time.len() <= 14
        && date_time.bytes().all(|byte| byte.is_ascii_digit())
        && fraction.bytes().all(|byte| byte.is_ascii_digit())
}

fn strip_datetime_timezone(value: &str) -> &str {
    if value.len() > 5 {
        let offset = &value[value.len() - 5..];
        if matches!(offset.as_bytes()[0], b'+' | b'-')
            && offset.as_bytes()[1..]
                .iter()
                .all(|byte| byte.is_ascii_digit())
        {
            return &value[..value.len() - 5];
        }
    }
    value
}

fn is_uid_tag(tag: Tag) -> bool {
    matches!(
        tag,
        tags::SOP_INSTANCE_UID
            | tags::SERIES_INSTANCE_UID
            | tags::STUDY_INSTANCE_UID
            | tags::SOP_CLASS_UID
    )
}

fn is_range_vr(tag: Tag) -> bool {
    matches!(vr_for_tag(tag), VR::DA | VR::DT | VR::TM)
}

fn is_character_set_affected_vr(tag: Tag) -> bool {
    matches!(
        vr_for_tag(tag),
        VR::AE | VR::LO | VR::LT | VR::PN | VR::SH | VR::ST | VR::UC | VR::UT
    )
}

fn vr_for_tag(tag: Tag) -> VR {
    StandardDataDictionary
        .by_tag(tag)
        .and_then(|entry| entry.vr().exact())
        .unwrap_or(VR::LO)
}

fn parse_tag(value: &str) -> Result<Tag, DicomWebError> {
    let normalized = value.trim();
    if let Some(entry) = StandardDataDictionary.by_name(normalized) {
        return Ok(entry.tag());
    }
    normalized.parse().map_err(|_| {
        DicomWebError::bad_request(format!("expected DICOM keyword or tag, got {value:?}"))
    })
}

fn parse_u64(name: &'static str, value: &str) -> Result<u64, DicomWebError> {
    value
        .parse()
        .map_err(|_| DicomWebError::bad_request(format!("{name} must be an unsigned integer")))
}

fn parse_bool(name: &'static str, value: &str) -> Result<bool, DicomWebError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(DicomWebError::bad_request(format!(
            "{name} must be true or false"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use dicom_dictionary_std::tags;
    use raccoon_service_query::{AttributePathSegment, MatchingRule};

    use super::*;

    #[test]
    fn specific_character_set_is_not_a_predicate() {
        let params = QidoQueryParams::parse(Some("00080005=ISO_IR+192&PatientName=Doe"))
            .expect("query parses");

        assert_eq!(
            params.specific_character_set.as_deref(),
            Some(&["ISO_IR 192".to_string()][..])
        );
        assert_eq!(params.predicates.len(), 1);
    }

    #[test]
    fn latin1_query_value_decodes_before_matching() {
        let params =
            QidoQueryParams::parse(Some("SpecificCharacterSet=ISO_IR+100&PatientName=Caf%E9"))
                .expect("Latin-1 query parses");
        let Predicate::Attribute(path, MatchingRule::SingleValue(value)) = &params.predicates[0]
        else {
            panic!("expected single-value PatientName predicate");
        };

        assert_eq!(
            path.segments(),
            &[AttributePathSegment::Tag(tags::PATIENT_NAME)]
        );
        assert_eq!(value, "Café");
    }

    #[test]
    fn non_ascii_without_charset_is_bad_request() {
        let error = QidoQueryParams::parse(Some("PatientName=Caf%E9"))
            .expect_err("non-ASCII default repertoire is invalid");

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn unsupported_charset_with_non_ascii_query_text_is_not_acceptable() {
        let error = QidoQueryParams::parse(Some(
            "SpecificCharacterSet=ISO+2022+IR+159&PatientName=%1B%24%42",
        ))
        .expect_err("unsupported non-ASCII charset is not acceptable");

        assert_eq!(error.status_code(), StatusCode::NOT_ACCEPTABLE);
    }

    #[test]
    fn iso2022_jis_query_value_decodes_before_matching() {
        let params = QidoQueryParams::parse(Some(
            "SpecificCharacterSet=ISO+2022+IR+87&StudyDescription=%1B%24%428%21%3A%3A",
        ))
        .expect("ISO 2022 IR 87 query parses");
        let Predicate::Attribute(path, MatchingRule::SingleValue(value)) = &params.predicates[0]
        else {
            panic!("expected single-value StudyDescription predicate");
        };

        assert_eq!(
            path.segments(),
            &[AttributePathSegment::Tag(tags::STUDY_DESCRIPTION)]
        );
        assert_eq!(value, "検査");
    }
}
