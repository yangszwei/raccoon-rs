use std::fmt;

use dicom_core::Tag;
use thiserror::Error;

/// One segment of an [`AttributePath`]: either a tag or a sequence item selector.
///
/// The only item selector is "any" — DICOM sequence matching always applies
/// the predicate to all items and reports a match if any item satisfies it.
/// Indexed item access has no representation in DIMSE C-FIND or QIDO-RS.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttributePathSegment {
    Tag(Tag),
    /// Any sequence item (`[*]`).
    AnyItem,
}

/// Validation failures for [`AttributePath`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AttributePathError {
    #[error("attribute path must not be empty")]
    Empty,
    #[error("attribute path cannot start with an item selector")]
    StartsWithItemSelector,
    #[error("attribute path cannot end with an item selector")]
    EndsWithItemSelector,
    #[error("attribute path cannot contain consecutive item selectors")]
    ConsecutiveItemSelectors,
    #[error(
        "attribute path cannot contain consecutive tag segments; use an item selector to navigate into a sequence"
    )]
    ConsecutiveTagSegments,
    #[error(
        "attribute path contains an item selector, which is only valid in filter predicates, not response attribute paths"
    )]
    ContainsItemSelector,
}

/// A path into a DICOM attribute tree, supporting nested sequence navigation.
///
/// A valid path starts with a tag and alternates between tags and item
/// selectors — e.g. `(0040,0100)[*].(0040,0009)` navigates into a sequence
/// and selects a nested attribute. Consecutive item selectors are not valid.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttributePath {
    segments: Vec<AttributePathSegment>,
}

impl AttributePath {
    /// Creates a single-tag path. Always valid.
    #[must_use]
    pub fn from_tag(tag: Tag) -> Self {
        Self {
            segments: vec![AttributePathSegment::Tag(tag)],
        }
    }

    /// Appends a tag segment.
    #[must_use]
    pub fn push_tag(mut self, tag: Tag) -> Self {
        self.segments.push(AttributePathSegment::Tag(tag));
        self
    }

    /// Appends an any-item selector, stepping into a sequence attribute.
    #[must_use]
    pub fn push_any(mut self) -> Self {
        self.segments.push(AttributePathSegment::AnyItem);
        self
    }

    pub fn segments(&self) -> &[AttributePathSegment] {
        &self.segments
    }

    pub fn contains_item_selector(&self) -> bool {
        self.segments
            .iter()
            .any(|segment| matches!(segment, AttributePathSegment::AnyItem))
    }

    /// Validates structural constraints: non-empty, starts with a tag, ends
    /// with a tag, and contains no consecutive item selectors.
    pub fn validate(&self) -> Result<(), AttributePathError> {
        if self.segments.is_empty() {
            return Err(AttributePathError::Empty);
        }
        if matches!(self.segments.first(), Some(AttributePathSegment::AnyItem)) {
            return Err(AttributePathError::StartsWithItemSelector);
        }
        if matches!(self.segments.last(), Some(AttributePathSegment::AnyItem)) {
            return Err(AttributePathError::EndsWithItemSelector);
        }
        if self.segments.windows(2).any(|w| {
            matches!(
                w,
                [AttributePathSegment::AnyItem, AttributePathSegment::AnyItem]
            )
        }) {
            return Err(AttributePathError::ConsecutiveItemSelectors);
        }
        if self.segments.windows(2).any(|w| {
            matches!(
                w,
                [AttributePathSegment::Tag(_), AttributePathSegment::Tag(_)]
            )
        }) {
            return Err(AttributePathError::ConsecutiveTagSegments);
        }
        Ok(())
    }
}

impl fmt::Display for AttributePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                AttributePathSegment::Tag(tag) => {
                    if i > 0 {
                        write!(f, ".")?;
                    }
                    write!(f, "{tag}")?;
                }
                AttributePathSegment::AnyItem => write!(f, "[*]")?,
            }
        }
        Ok(())
    }
}

/// A lower and/or upper bound for range matching.
///
/// Used with DA, TM, and DT attributes. Either bound may be omitted for an
/// open-ended range. Values are the raw DICOM string representations as sent
/// in the identifier.
///
/// This layer does not parse DA/TM/DT strings, so it cannot enforce that
/// `start <= end` for closed ranges, or that TM ranges that cross midnight are
/// meaningful. The bridge is responsible for validating ordering before emitting
/// the request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMatching {
    start: Option<String>,
    end: Option<String>,
}

impl RangeMatching {
    pub fn closed(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            start: Some(start.into()),
            end: Some(end.into()),
        }
    }

    pub fn from_start(start: impl Into<String>) -> Self {
        Self {
            start: Some(start.into()),
            end: None,
        }
    }

    pub fn until_end(end: impl Into<String>) -> Self {
        Self {
            start: None,
            end: Some(end.into()),
        }
    }

    pub fn start(&self) -> Option<&str> {
        self.start.as_deref()
    }

    pub fn end(&self) -> Option<&str> {
        self.end.as_deref()
    }
}

/// Matching predicate applied to each item within a sequence attribute.
///
/// Per the DICOM standard, sequence matching uses the first (and only) item of
/// the request SQ as a template. A stored sequence item matches if the nested
/// predicate holds for that item. The item selector is implicitly "any".
#[derive(Clone, Debug, PartialEq)]
pub struct SequenceMatching {
    pub predicate: Box<Predicate>,
}

/// A single attribute matching rule, corresponding to the DICOM PS3.4 C-FIND
/// matching rules.
#[derive(Clone, Debug, PartialEq)]
pub enum MatchingRule {
    /// Exact match for the given value.
    SingleValue(String),
    /// Zero-length: match all (universal matching).
    Universal,
    /// Wildcard match with `*` (any chars) and `?` (one char); valid only for
    /// string VRs: AE, CS, LO, LT, PN, SH, ST, UC, UR, UT.
    Wildcard(String),
    /// Range match for DA or TM attributes.
    Range(RangeMatching),
    /// Range match for DT (datetime) attributes.
    DateTimeRange(RangeMatching),
    /// OR match across multiple UID values; valid only for UI VR (baseline).
    UidList(Vec<String>),
    /// Each listed value must appear among the entity attribute's values
    /// (PS3.4 C.2.2.2.8). Requires extended negotiation (QIDO-RS
    /// `multiplevaluematching`, or C-FIND extended negotiation). Distinct from
    /// [`MatchingRule::UidList`] so bridges can enforce the negotiation
    /// precondition before emitting the request.
    ///
    /// The bridge is responsible for verifying that the target attribute's VR
    /// is one of AE/AS/AT/CS/LO/PN/SH/UC with VM > 1; this layer does not
    /// have access to the tag dictionary at validation time.
    MultipleValues(Vec<String>),
    /// Match attributes that are absent or zero-length (extended negotiation).
    EmptyValue,
    /// Sequence matching: predicate applied to any item of the stored sequence.
    Sequence(SequenceMatching),
}

/// Validation failures for a [`Predicate`] tree.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PredicateError {
    #[error("invalid attribute path: {0}")]
    InvalidPath(#[from] AttributePathError),
    #[error("invalid matching rule: {0}")]
    InvalidRule(String),
}

/// A filter predicate over DICOM attribute trees.
///
/// DICOM query matching is implicitly conjunctive: all attributes in the
/// identifier must match. Neither C-FIND nor QIDO-RS can express disjunction
/// or negation between attributes, so only [`Predicate::All`] (conjunction)
/// and [`Predicate::Attribute`] (leaf matching rule) are supported.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// All the inner predicates must hold (conjunction).
    All(Vec<Predicate>),
    /// A matching rule applied to a specific attribute.
    Attribute(AttributePath, MatchingRule),
}

fn is_valid_uid(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    if !value.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return false;
    }
    // Each component must be non-empty and must not have a leading zero unless
    // the component is exactly "0" (PS3.5 UID rules).
    value
        .split('.')
        .all(|c| !c.is_empty() && (c == "0" || !c.starts_with('0')))
}

impl Predicate {
    /// Validates all attribute paths and matching rule invariants within the
    /// predicate tree.
    pub fn validate(&self) -> Result<(), PredicateError> {
        match self {
            Self::All(items) => {
                for item in items {
                    item.validate()?;
                }
            }
            Self::Attribute(path, rule) => {
                path.validate()?;
                rule.validate_for_path(path)?;
            }
        }
        Ok(())
    }
}

impl MatchingRule {
    fn validate_for_path(&self, path: &AttributePath) -> Result<(), PredicateError> {
        match self {
            Self::SingleValue(value) => require_non_empty(
                value,
                "single value matching requires a non-empty value; use Universal for zero-length matching",
            ),
            Self::Wildcard(pattern) => require_non_empty(
                pattern,
                "wildcard matching requires a non-empty pattern; use Universal for zero-length matching",
            ),
            Self::Range(range) | Self::DateTimeRange(range) => validate_range(range),
            Self::UidList(values) => validate_uid_list(values),
            Self::MultipleValues(values) => validate_multiple_values(values),
            Self::Sequence(sequence) => validate_sequence(path, sequence),
            Self::Universal | Self::EmptyValue => Ok(()),
        }
    }
}

fn invalid_rule(message: impl Into<String>) -> PredicateError {
    PredicateError::InvalidRule(message.into())
}

fn require_non_empty(value: &str, message: &'static str) -> Result<(), PredicateError> {
    if value.is_empty() {
        Err(invalid_rule(message))
    } else {
        Ok(())
    }
}

fn validate_range(range: &RangeMatching) -> Result<(), PredicateError> {
    if range.start().is_some_and(str::is_empty) {
        return Err(invalid_rule("range start bound must not be empty"));
    }
    if range.end().is_some_and(str::is_empty) {
        return Err(invalid_rule("range end bound must not be empty"));
    }
    Ok(())
}

fn validate_uid_list(values: &[String]) -> Result<(), PredicateError> {
    if values.is_empty() {
        return Err(invalid_rule("UID list must contain at least one value"));
    }
    for uid in values {
        if !is_valid_uid(uid) {
            return Err(invalid_rule(format!("invalid UID in list: {uid:?}")));
        }
    }
    Ok(())
}

fn validate_multiple_values(values: &[String]) -> Result<(), PredicateError> {
    if values.is_empty() {
        return Err(invalid_rule(
            "multiple-value list must contain at least one value",
        ));
    }
    for value in values {
        if value.is_empty() {
            return Err(invalid_rule(
                "multiple-value list must not contain empty entries; use EmptyValue for zero-length matching",
            ));
        }
        if value.contains('*') || value.contains('?') {
            return Err(invalid_rule(format!(
                "multiple-value matching does not allow wildcards: {value:?}"
            )));
        }
    }
    Ok(())
}

fn validate_sequence(
    path: &AttributePath,
    sequence: &SequenceMatching,
) -> Result<(), PredicateError> {
    if path.contains_item_selector() {
        return Err(invalid_rule(
            "sequence matching rule cannot be applied to a path that navigates through sequence items",
        ));
    }
    sequence.predicate.validate()
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;

    use super::*;

    #[test]
    fn tag_displays_as_uppercase_hex() {
        assert_eq!(tags::PATIENT_ID.to_string(), "(0010,0020)");
        assert_eq!(tags::QUERY_RETRIEVE_LEVEL.to_string(), "(0008,0052)");
    }

    #[test]
    fn attribute_path_from_tag_has_one_segment() {
        let path = AttributePath::from_tag(tags::PATIENT_ID);

        assert_eq!(
            path.segments(),
            &[AttributePathSegment::Tag(tags::PATIENT_ID)]
        );
        path.validate().expect("single-tag path is valid");
    }

    #[test]
    fn attribute_path_push_tag_extends_segments() {
        // push_tag on a path that already entered a sequence item via push_any is valid.
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);

        assert_eq!(path.segments().len(), 3);
        path.validate().expect("tag-item-tag path is valid");
    }

    #[test]
    fn attribute_path_rejects_consecutive_tag_segments() {
        let path =
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID).push_tag(tags::SERIES_INSTANCE_UID);

        assert_eq!(
            path.validate().unwrap_err(),
            AttributePathError::ConsecutiveTagSegments
        );
    }

    #[test]
    fn attribute_path_with_any_item_selector_is_valid() {
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);

        assert_eq!(path.segments().len(), 3);
        path.validate().expect("tag.any.tag path is valid");
    }

    #[test]
    fn attribute_path_rejects_empty_segments() {
        let path = AttributePath { segments: vec![] };

        assert_eq!(path.validate().unwrap_err(), AttributePathError::Empty);
    }

    #[test]
    fn attribute_path_rejects_leading_item_selector() {
        let path = AttributePath {
            segments: vec![AttributePathSegment::AnyItem],
        };

        assert_eq!(
            path.validate().unwrap_err(),
            AttributePathError::StartsWithItemSelector
        );
    }

    #[test]
    fn attribute_path_rejects_trailing_item_selector() {
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE).push_any();

        assert_eq!(
            path.validate().unwrap_err(),
            AttributePathError::EndsWithItemSelector
        );
    }

    #[test]
    fn attribute_path_rejects_consecutive_item_selectors() {
        // Ends with a tag so EndsWithItemSelector does not fire first.
        let path = AttributePath {
            segments: vec![
                AttributePathSegment::Tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
                AttributePathSegment::AnyItem,
                AttributePathSegment::AnyItem,
                AttributePathSegment::Tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            ],
        };

        assert_eq!(
            path.validate().unwrap_err(),
            AttributePathError::ConsecutiveItemSelectors
        );
    }

    #[test]
    fn attribute_path_ordering_follows_tag_value() {
        // MODALITY (0008,0060) < PATIENT_ID (0010,0020)
        let modality = AttributePath::from_tag(tags::MODALITY);
        let patient_id = AttributePath::from_tag(tags::PATIENT_ID);

        assert!(modality < patient_id);
    }

    #[test]
    fn attribute_path_displays_simple_tag() {
        let path = AttributePath::from_tag(tags::PATIENT_ID);

        assert_eq!(path.to_string(), "(0010,0020)");
    }

    #[test]
    fn attribute_path_displays_nested_sequence_path() {
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);

        assert_eq!(path.to_string(), "(0040,0275)[*].(0040,0009)");
    }

    #[test]
    fn range_matching_builders_set_expected_bounds() {
        let closed = RangeMatching::closed("20260101", "20261231");
        assert_eq!(closed.start(), Some("20260101"));
        assert_eq!(closed.end(), Some("20261231"));

        let from = RangeMatching::from_start("20260101");
        assert_eq!(from.start(), Some("20260101"));
        assert!(from.end().is_none());

        let until = RangeMatching::until_end("20261231");
        assert!(until.start().is_none());
        assert_eq!(until.end(), Some("20261231"));
    }

    #[test]
    fn predicate_all_composes_multiple_attribute_rules() {
        let predicate = Predicate::All(vec![
            Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("PAT-001".to_string()),
            ),
            Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_DATE),
                MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
            ),
        ]);

        predicate.validate().expect("valid predicate tree");
        assert!(matches!(predicate, Predicate::All(ref items) if items.len() == 2));
    }

    #[test]
    fn predicate_with_sequence_matching_validates_nested_path() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
            MatchingRule::Sequence(SequenceMatching {
                predicate: Box::new(Predicate::Attribute(
                    AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
                    MatchingRule::SingleValue("STEP-1".to_string()),
                )),
            }),
        );

        predicate.validate().expect("sequence predicate is valid");
    }

    #[test]
    fn predicate_validate_propagates_invalid_path_error() {
        let bad_path = AttributePath {
            segments: vec![AttributePathSegment::AnyItem],
        };
        let predicate = Predicate::All(vec![Predicate::Attribute(
            bad_path,
            MatchingRule::Universal,
        )]);

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidPath(AttributePathError::StartsWithItemSelector)
        ));
    }

    #[test]
    fn predicate_rejects_empty_uid_list() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            MatchingRule::UidList(vec![]),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_empty_multiple_values_list() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::MultipleValues(vec![]),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_malformed_uid_in_uid_list() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            MatchingRule::UidList(vec!["1.2.840".to_string(), "not-a-uid".to_string()]),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_uid_with_leading_zero_component() {
        // "1.02.840" has component "02" — leading zero on non-zero component is invalid.
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            MatchingRule::UidList(vec!["1.02.840".to_string()]),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_empty_wildcard_pattern() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::PATIENT_NAME),
            MatchingRule::Wildcard(String::new()),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_accepts_valid_uid_list() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
            MatchingRule::UidList(vec![
                "1.2.840.10008.5.1.4.1.1.4".to_string(),
                "1.2.840.10008.5.1.4.1.1.2".to_string(),
            ]),
        );

        predicate.validate().expect("valid UID list");
    }

    #[test]
    fn predicate_rejects_empty_single_value() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::SingleValue(String::new()),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_sequence_rule_on_path_with_item_selector() {
        // A structurally-valid path that contains AnyItem: tag.AnyItem.tag.
        // Sequence matching must target the SQ tag directly, not a path that
        // already navigates through sequence items.
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
                .push_any()
                .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            MatchingRule::Sequence(SequenceMatching {
                predicate: Box::new(Predicate::Attribute(
                    AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
                    MatchingRule::Universal,
                )),
            }),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_empty_range_bound() {
        // Empty start bound in a closed Range.
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_DATE),
            MatchingRule::Range(RangeMatching::closed("", "20261231")),
        );
        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));

        // Empty end bound in a DateTimeRange.
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::STUDY_DATE),
            MatchingRule::DateTimeRange(RangeMatching::closed("20260101", "")),
        );
        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_empty_entry_in_multiple_values() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::MultipleValues(vec!["CT".to_string(), String::new()]),
        );

        assert!(matches!(
            predicate.validate().unwrap_err(),
            PredicateError::InvalidRule(_)
        ));
    }

    #[test]
    fn predicate_rejects_wildcard_in_multiple_values() {
        for wildcard_value in ["CT*", "?T", "C*T", "PT?"] {
            let predicate = Predicate::Attribute(
                AttributePath::from_tag(tags::MODALITY),
                MatchingRule::MultipleValues(vec![wildcard_value.to_string()]),
            );
            assert!(
                matches!(
                    predicate.validate().unwrap_err(),
                    PredicateError::InvalidRule(_)
                ),
                "expected InvalidRule for {wildcard_value:?}"
            );
        }
    }

    #[test]
    fn multiple_values_matching_rule_holds_list_of_strings() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::MultipleValues(vec!["CT".to_string(), "PT".to_string()]),
        );

        predicate.validate().expect("valid predicate");
        assert!(
            matches!(predicate, Predicate::Attribute(_, MatchingRule::MultipleValues(ref values)) if values.len() == 2)
        );
    }
}
