use dicom_core::Tag;
use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::{StandardDataDictionary, tags};
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
    pub limit_for_span: Option<u64>,
    pub offset_for_span: u64,
}

impl QidoQueryParams {
    pub fn parse(raw: Option<&str>) -> Result<Self, DicomWebError> {
        let mut includefields = Vec::new();
        let mut predicates = Vec::new();
        let mut limit = None;
        let mut offset = 0;
        let mut fuzzy_matching = false;
        let mut timezone_offset = None;

        for (key, value) in form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
            match key.as_ref() {
                "includefield" => includefields.push(value.into_owned()),
                "limit" => limit = Some(parse_u64("limit", &value)?),
                "offset" => offset = parse_u64("offset", &value)?,
                "fuzzymatching" => fuzzy_matching = parse_bool("fuzzymatching", &value)?,
                "timezoneoffset" => timezone_offset = Some(value.into_owned()),
                key => {
                    let tag = parse_tag(key)?;
                    predicates.push(predicate_for_value(tag, value.into_owned())?);
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
            limit_for_span: limit,
            offset_for_span: offset,
        })
    }
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
