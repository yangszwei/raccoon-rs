use raccoon_contract_dicom::{PatientRootQueryRetrieveLevel, StudyRootQueryRetrieveLevel};

use crate::error::QueryError;
use crate::filter::{AttributePath, Predicate, PredicateError};

/// Which instance view applies when Enhanced Multi-Frame Image Conversion is
/// negotiated (0008,0053). Controls whether the C-FIND SCU sees the classic
/// single-frame view or the enhanced multi-frame view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryRetrieveView {
    Classic,
    Enhanced,
}

/// Which DICOM information model and level a [`DicomQuery`] targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryScope {
    PatientRoot(PatientRootQueryRetrieveLevel),
    StudyRoot(StudyRootQueryRetrieveLevel),
}

/// Paging parameters for a [`DicomQuery`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPaging {
    offset: u64,
    limit: u64,
}

impl QueryPaging {
    /// Creates paging parameters. Fails if `limit` is zero.
    pub fn new(offset: u64, limit: u64) -> Result<Self, QueryError> {
        if limit == 0 {
            return Err(QueryError::invalid_query("paging limit must not be zero"));
        }
        Ok(Self { offset, limit })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }
}

/// Which attributes to include in each result row.
///
/// `Fields` requests a specific set (deduplicated, non-empty). `All` corresponds
/// to QIDO-RS `includefield=all`. `Default` corresponds to QIDO-RS with no
/// `includefield` parameter, or C-FIND with no explicitly requested optional
/// attributes — the repository returns only the mandatory attributes for the
/// query level.
#[derive(Clone, Debug)]
pub enum Projection {
    /// Return only the specified attributes (QIDO-RS `includefield=tag,...`).
    Fields(Vec<AttributePath>),
    /// Return all stored attributes (QIDO-RS `includefield=all`).
    All,
    /// Return the mandatory attributes for the query level (no `includefield`).
    /// The bridge is responsible for injecting mandatory C-FIND Identifier
    /// attributes (e.g. Query/Retrieve Level (0008,0052)) into each response
    /// [`QueryMatch`]; this layer does not enforce their presence.
    Default,
}

impl Projection {
    /// Returns the explicit field list, or `None` for [`Projection::All`] and
    /// [`Projection::Default`].
    pub fn fields(&self) -> Option<&[AttributePath]> {
        match self {
            Self::Fields(paths) => Some(paths),
            Self::All | Self::Default => None,
        }
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    fn validate(self) -> Result<Self, QueryError> {
        match self {
            Self::All | Self::Default => Ok(self),
            Self::Fields(paths) => normalize_projection_fields(paths).map(Self::Fields),
        }
    }
}

/// Protocol-neutral DICOM query request.
///
/// Carries the information model scope, an optional predicate tree, a
/// projection, optional paging, and query-level flags. Constructed via
/// [`DicomQuery::new`]; the predicate is validated in
/// [`DicomQuery::with_predicate`].
#[derive(Clone, Debug)]
pub struct DicomQuery {
    pub(crate) scope: QueryScope,
    pub(crate) predicate: Option<Predicate>,
    pub(crate) projection: Projection,
    pub(crate) paging: Option<QueryPaging>,
    /// Whether fuzzy semantic matching applies to all PN attributes in this
    /// query (QIDO-RS `fuzzymatching=true`, PS3.18 §8.3.4.2).
    pub(crate) fuzzy_matching: bool,
    /// Timezone Offset From UTC (0008,0201) in `+HHMM`/`-HHMM` form.
    /// Repositories use this to adjust DA/TM/DT range matching when timezone
    /// query adjustment is negotiated (PS3.4 C.2.2.2.5).
    pub(crate) timezone_offset_from_utc: Option<String>,
    /// Specific Character Set (0008,0005) values from the request identifier.
    /// Repositories need this to interpret PN/LO/SH/LT/ST/UT attribute values
    /// when the SCU declares a non-default character set.
    pub(crate) specific_character_set: Option<Vec<String>>,
    /// Query/Retrieve View (0008,0053) when Enhanced Multi-Frame Image
    /// Conversion is negotiated.
    pub(crate) query_retrieve_view: Option<QueryRetrieveView>,
    /// Whether the query uses relational (extended negotiation) mode.
    /// In baseline hierarchical mode the bridge is responsible for ensuring
    /// unique-key presence and above-level key constraints; this flag signals
    /// that those constraints are relaxed by explicit SCU/SCP negotiation.
    pub(crate) relational_query: bool,
}

impl DicomQuery {
    /// Creates a query for the given scope and projection.
    ///
    /// For [`Projection::Fields`]: fails if the list is empty or contains an
    /// invalid [`AttributePath`]; duplicates are silently removed in order of
    /// first appearance. For [`Projection::All`]: no field-level validation.
    pub fn new(scope: QueryScope, projection: Projection) -> Result<Self, QueryError> {
        Ok(Self {
            scope,
            predicate: None,
            projection: projection.validate()?,
            paging: None,
            fuzzy_matching: false,
            timezone_offset_from_utc: None,
            specific_character_set: None,
            query_retrieve_view: None,
            relational_query: false,
        })
    }

    /// Attaches a filter predicate. Fails if any attribute path in the
    /// predicate tree is structurally invalid or any matching rule invariant
    /// is violated (e.g. an empty UID list or a malformed UID).
    pub fn with_predicate(mut self, predicate: Predicate) -> Result<Self, QueryError> {
        predicate
            .validate()
            .map_err(|e: PredicateError| QueryError::invalid_query(e.to_string()))?;
        self.predicate = Some(predicate);
        Ok(self)
    }

    /// Attaches paging parameters.
    pub fn with_paging(mut self, paging: QueryPaging) -> Self {
        self.paging = Some(paging);
        self
    }

    /// Enables fuzzy semantic matching for all PN attributes (QIDO-RS
    /// `fuzzymatching=true`). The repository is responsible for applying
    /// fuzzy PN matching when this flag is set.
    pub fn with_fuzzy_matching(mut self) -> Self {
        self.fuzzy_matching = true;
        self
    }

    pub fn scope(&self) -> QueryScope {
        self.scope
    }

    pub fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    pub fn paging(&self) -> Option<QueryPaging> {
        self.paging
    }

    pub fn fuzzy_matching(&self) -> bool {
        self.fuzzy_matching
    }

    /// Attaches a Timezone Offset From UTC (0008,0201). The value must be in
    /// `+HHMM` or `-HHMM` form with HH in 00–14 and MM in 00–59 (and MM = 00
    /// when HH = 14). Repositories apply this when evaluating timezone-sensitive
    /// DA/TM/DT range matching.
    pub fn with_timezone_offset(mut self, offset: impl Into<String>) -> Result<Self, QueryError> {
        let offset = offset.into();
        validate_timezone_offset(&offset)?;
        self.timezone_offset_from_utc = Some(offset);
        Ok(self)
    }

    pub fn timezone_offset_from_utc(&self) -> Option<&str> {
        self.timezone_offset_from_utc.as_deref()
    }

    /// Attaches Specific Character Set (0008,0005) values from the request
    /// identifier. Repositories use these to correctly interpret and encode
    /// attribute values with non-default character sets.
    ///
    /// Hard rules enforced here:
    /// - `ISO_IR 192` (UTF-8) must be the sole value; it cannot be combined.
    /// - An empty first value is the ISO 2022 extension form (PS3.3 C.7.6.3.1.2);
    ///   all subsequent values must be ISO 2022 extension defined terms (their
    ///   names start with `"ISO 2022"`).
    ///
    /// Full charset-term validation (e.g. checking defined terms against the
    /// registry) is the bridge's responsibility.
    pub fn with_specific_character_set(
        mut self,
        charsets: Vec<String>,
    ) -> Result<Self, QueryError> {
        validate_specific_character_set(&charsets)?;
        self.specific_character_set = Some(charsets);
        Ok(self)
    }

    pub fn specific_character_set(&self) -> Option<&[String]> {
        self.specific_character_set.as_deref()
    }

    /// Sets the Query/Retrieve View (0008,0053) when Enhanced Multi-Frame
    /// Image Conversion is negotiated.
    pub fn with_query_retrieve_view(mut self, view: QueryRetrieveView) -> Self {
        self.query_retrieve_view = Some(view);
        self
    }

    pub fn query_retrieve_view(&self) -> Option<QueryRetrieveView> {
        self.query_retrieve_view
    }

    /// Marks this as a relational query (extended negotiation). In baseline
    /// hierarchical mode the bridge must ensure unique-key presence and
    /// above-level key constraints; setting this flag signals that those
    /// constraints are explicitly relaxed by SCU/SCP negotiation.
    pub fn with_relational_query(mut self) -> Self {
        self.relational_query = true;
        self
    }

    pub fn relational_query(&self) -> bool {
        self.relational_query
    }
}

/// A protocol-neutral DICOM attribute value as returned by the repository.
///
/// All string values are Unicode (UTF-8). DA/TM/DT values retain their DICOM
/// string form (e.g. `"20260101"`, `"143000.000000"`); text VRs (PN, LO, SH,
/// LT, ST, UT, etc.) are transcoded by the repository from the stored character
/// encoding into Unicode before being placed here. Bridges must re-encode from
/// Unicode into the negotiated `SpecificCharacterSet` when serializing to the
/// DICOM wire format.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    /// A single string value, covering most VRs.
    Text(String),
    /// Multiple string values (VM > 1), covering multi-valued attributes.
    Texts(Vec<String>),
    /// Sequence items; each item is itself a set of projected attributes.
    Sequence(Vec<QueryMatch>),
    /// Raw bytes for binary VRs (OB, OW, UN, etc.).
    Binary(Vec<u8>),
}

/// Presence or value of a projected attribute in a [`QueryMatch`].
///
/// Distinguishes three states that DICOM C-FIND responses must encode
/// differently: [`Present`] when the attribute has a value, [`ZeroLength`]
/// when the attribute is supported but has no stored value (encoded as
/// zero-length on the wire), and [`Absent`] when the attribute is not
/// supported by the repository and will be omitted from the response entirely.
///
/// [`Present`]: ResponseValue::Present
/// [`ZeroLength`]: ResponseValue::ZeroLength
/// [`Absent`]: ResponseValue::Absent
#[derive(Clone, Debug, PartialEq)]
pub enum ResponseValue {
    Present(AttributeValue),
    ZeroLength,
    Absent,
}

impl ResponseValue {
    fn validate(&self) -> Result<(), QueryError> {
        let Self::Present(value) = self else {
            return Ok(());
        };

        match value {
            AttributeValue::Text(s) if s.is_empty() => Err(QueryError::invalid_query(
                "Present(Text(\"\")) is zero-length; use ResponseValue::ZeroLength instead",
            )),
            AttributeValue::Texts(v) if v.is_empty() => Err(QueryError::invalid_query(
                "AttributeValue::Texts must contain at least one value; use ResponseValue::ZeroLength instead",
            )),
            AttributeValue::Binary(b) if b.is_empty() => Err(QueryError::invalid_query(
                "Present(Binary([])) is zero-length; use ResponseValue::ZeroLength instead",
            )),
            _ => Ok(()),
        }
    }
}

/// A single projected attribute within a [`QueryMatch`].
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedAttribute {
    pub path: AttributePath,
    pub value: ResponseValue,
}

/// One result row returned by the repository.
///
/// Attributes are stored in ascending tag order with duplicates removed, matching
/// the invariant required by PS3.5 §7.1 for DICOM datasets. Mandatory C-FIND
/// Identifier attributes (e.g. Query/Retrieve Level (0008,0052)) must be
/// injected by the DIMSE bridge; this model does not enforce their presence.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryMatch {
    attributes: Vec<ProjectedAttribute>,
}

impl QueryMatch {
    /// Creates a match, validating each path and value, then sorting by path
    /// and removing duplicates (first occurrence wins).
    ///
    /// Fails if any path is structurally invalid, contains an item selector
    /// (response attributes must be direct tag paths; use
    /// [`AttributeValue::Sequence`] for sequence data), or if any
    /// [`AttributeValue::Texts`] is empty (use [`ResponseValue::ZeroLength`]
    /// for absent values instead).
    pub fn new(mut attributes: Vec<ProjectedAttribute>) -> Result<Self, QueryError> {
        for attr in &attributes {
            validate_response_path(&attr.path)?;
            attr.value.validate()?;
        }
        attributes.sort_by(|a, b| a.path.cmp(&b.path));
        attributes.dedup_by_key(|a| a.path.clone());
        Ok(Self { attributes })
    }

    pub fn attributes(&self) -> &[ProjectedAttribute] {
        &self.attributes
    }

    /// Decomposes the match, returning the sorted, deduplicated attribute list.
    ///
    /// Useful for serialization bridges that need to move the attributes
    /// without cloning — the ordering and deduplication invariants established
    /// by [`QueryMatch::new`] are preserved in the returned `Vec`.
    pub fn into_attributes(self) -> Vec<ProjectedAttribute> {
        self.attributes
    }
}

fn normalize_projection_fields(
    paths: Vec<AttributePath>,
) -> Result<Vec<AttributePath>, QueryError> {
    if paths.is_empty() {
        return Err(QueryError::invalid_query("projection must not be empty"));
    }

    let mut seen = std::collections::HashSet::new();
    let mut deduped = Vec::with_capacity(paths.len());
    for path in paths {
        validate_path(&path)?;
        if seen.insert(path.clone()) {
            deduped.push(path);
        }
    }
    Ok(deduped)
}

fn validate_path(path: &AttributePath) -> Result<(), QueryError> {
    path.validate()
        .map_err(|error| QueryError::invalid_query(error.to_string()))
}

fn validate_response_path(path: &AttributePath) -> Result<(), QueryError> {
    validate_path(path)?;
    if path.contains_item_selector() {
        return Err(QueryError::invalid_query(
            "response attribute path must not contain item selectors; use AttributeValue::Sequence for sequence data",
        ));
    }
    Ok(())
}

fn validate_timezone_offset(offset: &str) -> Result<(), QueryError> {
    let shape_ok = offset.len() == 5
        && matches!(offset.as_bytes().first(), Some(b'+') | Some(b'-'))
        && offset[1..].bytes().all(|b| b.is_ascii_digit());
    if !shape_ok {
        return Err(QueryError::invalid_query(format!(
            "timezone offset must be in +HHMM or -HHMM form, got: {offset:?}"
        )));
    }

    let hh: u8 = offset[1..3].parse().unwrap(); // safe: 2 ASCII digits
    let mm: u8 = offset[3..5].parse().unwrap(); // safe: 2 ASCII digits
    if hh > 14 || mm > 59 || (hh == 14 && mm != 0) {
        return Err(QueryError::invalid_query(format!(
            "timezone offset out of range (±00:00 to ±14:00), got: {offset:?}"
        )));
    }
    Ok(())
}

fn validate_specific_character_set(charsets: &[String]) -> Result<(), QueryError> {
    if charsets.is_empty() {
        return Err(QueryError::invalid_query(
            "specific character set list must not be empty",
        ));
    }
    if charsets.len() == 1 && charsets[0].is_empty() {
        return Err(QueryError::invalid_query(
            "single-value specific character set must not be empty",
        ));
    }
    if charsets.iter().any(|charset| charset == "ISO_IR 192") && charsets.len() > 1 {
        return Err(QueryError::invalid_query(
            "ISO_IR 192 (UTF-8) must be the sole SpecificCharacterSet value",
        ));
    }

    if charsets[0].is_empty() {
        validate_iso2022_extensions(charsets)
    } else {
        validate_non_initial_charsets(charsets)
    }
}

fn validate_iso2022_extensions(charsets: &[String]) -> Result<(), QueryError> {
    for charset in charsets.iter().skip(1) {
        if charset.is_empty() || !charset.starts_with("ISO 2022") {
            return Err(QueryError::invalid_query(format!(
                "when the first value is empty, subsequent values must be ISO 2022 extension terms; got: {charset:?}"
            )));
        }
    }
    Ok(())
}

fn validate_non_initial_charsets(charsets: &[String]) -> Result<(), QueryError> {
    for charset in charsets.iter().skip(1) {
        if charset.is_empty() {
            return Err(QueryError::invalid_query(
                "specific character set values after the first must not be empty",
            ));
        }
    }
    Ok(())
}

/// A page of [`QueryMatch`] results.
///
/// All string values within each [`QueryMatch`] are Unicode (UTF-8). The bridge
/// is responsible for re-encoding them into the negotiated `SpecificCharacterSet`
/// when serializing to the wire.
#[derive(Clone, Debug)]
pub struct QueryPage {
    pub items: Vec<QueryMatch>,
    pub offset: u64,
    pub limit: u64,
    /// Total number of matching rows, when the repository supports counting.
    pub total: Option<u64>,
}

impl QueryPage {
    pub fn new(items: Vec<QueryMatch>, offset: u64, limit: u64, total: Option<u64>) -> Self {
        Self {
            items,
            offset,
            limit,
            total,
        }
    }

    pub fn empty(offset: u64, limit: u64) -> Self {
        Self {
            items: Vec::new(),
            offset,
            limit,
            total: Some(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;
    use raccoon_contract_dicom::{PatientRootQueryRetrieveLevel, StudyRootQueryRetrieveLevel};

    use super::*;
    use crate::error::QueryError;
    use crate::filter::{AttributePath, MatchingRule, Predicate};

    fn fields(paths: Vec<AttributePath>) -> Projection {
        Projection::Fields(paths)
    }

    fn study_scope() -> QueryScope {
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study)
    }

    #[test]
    fn query_scope_wraps_patient_root_level() {
        let scope = QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study);

        assert!(matches!(
            scope,
            QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study)
        ));
    }

    #[test]
    fn query_scope_wraps_study_root_level() {
        let scope = QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series);

        assert!(matches!(
            scope,
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)
        ));
    }

    #[test]
    fn query_paging_stores_offset_and_limit() {
        let paging = QueryPaging::new(20, 10).expect("valid paging");

        assert_eq!(paging.offset(), 20);
        assert_eq!(paging.limit(), 10);
    }

    #[test]
    fn query_paging_rejects_zero_limit() {
        let error = QueryPaging::new(0, 0).expect_err("zero limit is invalid");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
        assert!(error.to_string().contains("zero"));
    }

    #[test]
    fn dicom_query_rejects_empty_fields_projection() {
        let error = DicomQuery::new(study_scope(), fields(vec![]))
            .expect_err("empty projection is invalid");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn dicom_query_stores_scope_and_projection() {
        let scope = study_scope();
        let path = AttributePath::from_tag(tags::STUDY_INSTANCE_UID);
        let query = DicomQuery::new(scope, fields(vec![path.clone()])).expect("valid query");

        assert_eq!(query.scope(), scope);
        assert_eq!(
            query.projection().fields().expect("fields projection"),
            &[path]
        );
        assert!(query.predicate().is_none());
        assert!(query.paging().is_none());
        assert!(!query.fuzzy_matching());
    }

    #[test]
    fn dicom_query_all_projection_accepts_all_attributes() {
        let query = DicomQuery::new(study_scope(), Projection::All).expect("valid query");

        assert!(query.projection().is_all());
        assert!(query.projection().fields().is_none());
    }

    #[test]
    fn dicom_query_deduplicates_fields_projection_preserving_order() {
        let modality = AttributePath::from_tag(tags::MODALITY);
        let query = DicomQuery::new(
            study_scope(),
            fields(vec![
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                modality.clone(),
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                modality.clone(),
                AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
            ]),
        )
        .expect("valid query with duplicates");

        let paths = query.projection().fields().expect("fields");
        assert_eq!(paths.len(), 3);
        assert_eq!(paths[0], AttributePath::from_tag(tags::STUDY_INSTANCE_UID));
        assert_eq!(paths[1], modality);
        assert_eq!(paths[2], AttributePath::from_tag(tags::SERIES_INSTANCE_UID));
    }

    #[test]
    fn dicom_query_with_predicate_validates_attribute_paths() {
        let query = DicomQuery::new(
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
            fields(vec![
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
            ]),
        )
        .expect("valid query")
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::SingleValue("CT".to_string()),
        ))
        .expect("valid predicate");

        assert!(query.predicate().is_some());
    }

    #[test]
    fn dicom_query_with_paging_stores_parameters() {
        let paging = QueryPaging::new(40, 20).expect("valid paging");
        let query = DicomQuery::new(
            QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
            fields(vec![AttributePath::from_tag(tags::PATIENT_ID)]),
        )
        .expect("valid query")
        .with_paging(paging);

        let stored = query.paging().expect("paging present");
        assert_eq!(stored.offset(), 40);
        assert_eq!(stored.limit(), 20);
    }

    #[test]
    fn dicom_query_fuzzy_matching_defaults_to_false() {
        let query = DicomQuery::new(
            study_scope(),
            fields(vec![AttributePath::from_tag(tags::STUDY_INSTANCE_UID)]),
        )
        .expect("valid query");

        assert!(!query.fuzzy_matching());
    }

    #[test]
    fn dicom_query_with_timezone_offset_stores_valid_offset() {
        let query = DicomQuery::new(
            study_scope(),
            fields(vec![AttributePath::from_tag(tags::STUDY_DATE)]),
        )
        .expect("valid query")
        .with_timezone_offset("+0530")
        .expect("valid timezone offset");

        assert_eq!(query.timezone_offset_from_utc(), Some("+0530"));
    }

    #[test]
    fn dicom_query_rejects_malformed_timezone_offset() {
        let base = DicomQuery::new(
            study_scope(),
            fields(vec![AttributePath::from_tag(tags::STUDY_DATE)]),
        )
        .expect("valid query");

        for bad in [
            "UTC", "+05:30", "0530", "+05300", "", "+9999", "-1260", "+1401",
        ] {
            base.clone()
                .with_timezone_offset(bad)
                .expect_err(&format!("should reject {bad:?}"));
        }
    }

    #[test]
    fn dicom_query_with_specific_character_set_stores_values() {
        let charsets = vec!["ISO_IR 192".to_string()];
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(charsets.clone())
            .expect("valid charset list");

        assert_eq!(query.specific_character_set(), Some(charsets.as_slice()));
    }

    #[test]
    fn dicom_query_accepts_empty_first_charset_in_multi_value_form() {
        // ISO 2022 extended character sets may have an empty first value.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec![String::new(), "ISO 2022 IR 87".to_string()])
            .expect("ISO 2022 multi-value form is valid");

        let charsets = query.specific_character_set().expect("charsets present");
        assert_eq!(charsets[0], "");
        assert_eq!(charsets[1], "ISO 2022 IR 87");
    }

    #[test]
    fn dicom_query_rejects_single_empty_charset() {
        let error = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec![String::new()])
            .expect_err("single empty charset is invalid");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn dicom_query_rejects_empty_charset_after_first() {
        let error = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec!["ISO_IR 192".to_string(), String::new()])
            .expect_err("empty non-first charset is invalid");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn dicom_query_allows_utf8_as_single_charset() {
        DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec!["ISO_IR 192".to_string()])
            .expect("ISO_IR 192 alone is valid");
    }

    #[test]
    fn dicom_query_rejects_utf8_combined_with_other_charsets() {
        for combo in [
            vec!["ISO_IR 192".to_string(), "ISO 2022 IR 87".to_string()],
            vec!["ISO_IR 100".to_string(), "ISO_IR 192".to_string()],
        ] {
            DicomQuery::new(study_scope(), Projection::Default)
                .expect("valid query")
                .with_specific_character_set(combo)
                .expect_err("ISO_IR 192 cannot be combined");
        }
    }

    #[test]
    fn dicom_query_rejects_non_iso2022_after_empty_first_charset() {
        // ["", "ISO_IR 192"] is invalid — ISO_IR 192 is not an ISO 2022 term.
        let error = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec![String::new(), "ISO_IR 192".to_string()])
            .expect_err("non-ISO 2022 term after empty first must be rejected");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn dicom_query_rejects_empty_specific_character_set() {
        let error = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_specific_character_set(vec![])
            .expect_err("empty charset list is invalid");

        assert!(matches!(error, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn dicom_query_with_query_retrieve_view_stores_view() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_query_retrieve_view(QueryRetrieveView::Enhanced);

        assert_eq!(
            query.query_retrieve_view(),
            Some(QueryRetrieveView::Enhanced)
        );
    }

    #[test]
    fn dicom_query_relational_defaults_to_false_and_can_be_set() {
        let baseline = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        assert!(!baseline.relational_query());

        let relational = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_relational_query();
        assert!(relational.relational_query());
    }

    #[test]
    fn dicom_query_with_fuzzy_matching_enables_flag() {
        let query = DicomQuery::new(
            study_scope(),
            fields(vec![AttributePath::from_tag(tags::PATIENT_NAME)]),
        )
        .expect("valid query")
        .with_fuzzy_matching();

        assert!(query.fuzzy_matching());
    }

    #[test]
    fn query_page_new_stores_all_fields() {
        let items = vec![QueryMatch::default(), QueryMatch::default()];
        let page = QueryPage::new(items, 0, 10, Some(2));

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.offset, 0);
        assert_eq!(page.limit, 10);
        assert_eq!(page.total, Some(2));
    }

    #[test]
    fn query_page_empty_has_zero_total() {
        let page = QueryPage::empty(0, 50);

        assert!(page.items.is_empty());
        assert_eq!(page.total, Some(0));
    }

    #[test]
    fn projected_attribute_distinguishes_absent_and_zero_length() {
        let absent = ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_ID),
            value: ResponseValue::Absent,
        };
        let zero_length = ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_ID),
            value: ResponseValue::ZeroLength,
        };
        let present = ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_ID),
            value: ResponseValue::Present(AttributeValue::Text("P1".to_string())),
        };

        assert!(matches!(absent.value, ResponseValue::Absent));
        assert!(matches!(zero_length.value, ResponseValue::ZeroLength));
        assert!(matches!(present.value, ResponseValue::Present(_)));
    }

    #[test]
    fn attribute_value_sequence_holds_nested_matches() {
        let inner = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            value: ResponseValue::Present(AttributeValue::Text("STEP-1".to_string())),
        }])
        .expect("valid path");
        let value = AttributeValue::Sequence(vec![inner]);

        assert!(matches!(value, AttributeValue::Sequence(ref items) if items.len() == 1));
    }

    #[test]
    fn dicom_query_default_projection_is_accepted() {
        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");

        assert!(query.projection().is_default());
        assert!(!query.projection().is_all());
        assert!(query.projection().fields().is_none());
    }

    #[test]
    fn query_match_sorts_attributes_by_tag() {
        // MODALITY (0008,0060) < PATIENT_ID (0010,0020) — insert in reverse order
        let qm = QueryMatch::new(vec![
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::PATIENT_ID),
                value: ResponseValue::Absent,
            },
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::MODALITY),
                value: ResponseValue::Absent,
            },
        ])
        .expect("valid paths");

        let attrs = qm.attributes();
        assert_eq!(attrs[0].path, AttributePath::from_tag(tags::MODALITY));
        assert_eq!(attrs[1].path, AttributePath::from_tag(tags::PATIENT_ID));
    }

    #[test]
    fn query_match_rejects_path_containing_item_selector() {
        // tag.AnyItem.tag is structurally valid (passes path.validate()) but
        // must not appear in a response match — sequence data must be nested
        // via AttributeValue::Sequence, not carried via AnyItem navigation.
        let path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);
        let err = QueryMatch::new(vec![ProjectedAttribute {
            path,
            value: ResponseValue::Absent,
        }])
        .expect_err("path with AnyItem must be rejected in response");

        assert!(matches!(err, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn query_match_rejects_path_ending_with_item_selector() {
        // A path that ends with AnyItem cannot be serialized as a DICOM dataset element.
        let bad_path = AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE).push_any();
        let err = QueryMatch::new(vec![ProjectedAttribute {
            path: bad_path,
            value: ResponseValue::Absent,
        }])
        .expect_err("invalid path must be rejected");

        assert!(matches!(err, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn query_match_rejects_zero_length_text_as_present() {
        let err = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_NAME),
            value: ResponseValue::Present(AttributeValue::Text(String::new())),
        }])
        .expect_err("Present(Text(\"\")) must use ZeroLength");

        assert!(matches!(err, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn query_match_rejects_zero_length_binary_as_present() {
        let err = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::PATIENT_ID),
            value: ResponseValue::Present(AttributeValue::Binary(vec![])),
        }])
        .expect_err("Present(Binary([])) must use ZeroLength");

        assert!(matches!(err, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn query_match_allows_empty_sequence_as_present() {
        // An SQ attribute with zero items is a valid DICOM value distinct from
        // zero-length; it must not be rejected.
        QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
            value: ResponseValue::Present(AttributeValue::Sequence(vec![])),
        }])
        .expect("empty Sequence(vec![]) is a valid zero-item SQ");
    }

    #[test]
    fn query_match_rejects_empty_texts_value() {
        let err = QueryMatch::new(vec![ProjectedAttribute {
            path: AttributePath::from_tag(tags::MODALITY),
            value: ResponseValue::Present(AttributeValue::Texts(vec![])),
        }])
        .expect_err("Texts(vec![]) must be rejected; use ResponseValue::ZeroLength instead");

        assert!(matches!(err, QueryError::InvalidQuery(_)));
    }

    #[test]
    fn query_match_deduplicates_duplicate_paths() {
        let qm = QueryMatch::new(vec![
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::PATIENT_ID),
                value: ResponseValue::Present(AttributeValue::Text("P1".to_string())),
            },
            ProjectedAttribute {
                path: AttributePath::from_tag(tags::PATIENT_ID),
                value: ResponseValue::ZeroLength,
            },
        ])
        .expect("valid paths");

        assert_eq!(qm.attributes().len(), 1);
    }
}
