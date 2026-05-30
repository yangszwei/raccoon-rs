//! Translates a [`DicomQuery`] into a SQLite statement.
//!
//! Every query joins all three tables (`instances i`, `series se`, `studies s`)
//! and always SELECTs every indexed column plus the `attributes` blob.
//! Projection is applied on the Rust side in [`crate::project`]; the SQL side
//! only handles filtering (WHERE) and deduplication (ROW_NUMBER).
//!
//! # Deduplication
//!
//! DICOM queries at Study/Series/Patient scope must return one row per distinct
//! entity even though the underlying join may produce one row per instance.
//! We use `ROW_NUMBER() OVER (PARTITION BY <unique key> ORDER BY <unique key>)`
//! inside a CTE and then filter to `rn = 1`.
//!
//! # Total count
//!
//! When paging is requested we also compute `COUNT(*) OVER ()` on the
//! deduplicated set, giving the total without a second round-trip to SQLite.

use dicom_core::Tag;
use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::StandardDataDictionary;
use raccoon_service_query::{
    AttributePath, AttributePathSegment, MatchingRule, PatientRootQueryRetrieveLevel, Predicate,
    QueryScope, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};
use raccoon_service_query::{DicomQuery, QueryPaging};

use crate::error::SqliteReadRepositoryError;
use crate::schema::{ATTRIBUTE_MAPPINGS, AttributeRegistry};

#[derive(Debug, Clone)]
pub(crate) enum BindValue {
    Text(String),
    Int(i64),
}

#[derive(Debug)]
pub(crate) struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<BindValue>,
    /// Whether the result rows include a `_total` column (present only when
    /// paging was requested).
    pub has_total_count: bool,
    /// Paging parameters echoed back for the page-summary in the response.
    pub paging: Option<QueryPaging>,
}

pub(crate) fn compile(
    query: &DicomQuery,
    registry: &AttributeRegistry,
) -> Result<CompiledQuery, SqliteReadRepositoryError> {
    let scope = query.scope();
    let mut binds: Vec<BindValue> = Vec::new();
    let mut seq_counter: usize = 0;

    let tz_offset_minutes = query
        .timezone_offset_from_utc()
        .and_then(parse_timezone_offset_minutes);

    // WHERE clause
    let where_sql = query
        .predicate()
        .map(|p| {
            compile_predicate(
                p,
                &CompileContext::root(query.fuzzy_matching(), tz_offset_minutes),
                registry,
                &mut binds,
                &mut seq_counter,
            )
        })
        .transpose()?;

    let paging = query.paging();
    let has_paging = paging.is_some();
    let sql = build_sql(
        scope,
        where_sql.as_deref(),
        query.sort_keys(),
        registry,
        has_paging,
        &mut binds,
        paging,
    );

    Ok(CompiledQuery {
        sql,
        binds,
        has_total_count: has_paging,
        paging,
    })
}

/// The SELECT list is always the same: every indexed column in
/// [`ATTRIBUTE_MAPPINGS`] order, then `i.attributes`, then the three
/// aliased sync timestamps used for ORDER BY stability.
fn select_list() -> String {
    let mut cols: Vec<String> = ATTRIBUTE_MAPPINGS
        .iter()
        .map(|m| format!("{}.{}", m.table.alias(), m.column))
        .collect();
    cols.push("i.attributes".to_string());
    // Aliased so all three can be SELECTed without name collisions.
    // These are infrastructure columns (not DICOM attributes) so they are not
    // in ATTRIBUTE_MAPPINGS and are never included in QueryMatch output.
    cols.push("s.synced_at_unix_ms  AS s_synced_at".to_string());
    cols.push("se.synced_at_unix_ms AS se_synced_at".to_string());
    cols.push("i.synced_at_unix_ms  AS i_synced_at".to_string());
    cols.join(", ")
}

fn build_sql(
    scope: QueryScope,
    where_sql: Option<&str>,
    sort_keys: &[SortKey],
    registry: &AttributeRegistry,
    has_paging: bool,
    binds: &mut Vec<BindValue>,
    paging: Option<QueryPaging>,
) -> String {
    let select = select_list();
    let where_clause = where_sql.map(|w| format!(" WHERE {w}")).unwrap_or_default();

    let (partition_col, default_order) = dedup_info_for_scope(scope);

    // When the caller supplies sort keys, honour them first; the default
    // (newest-synced-first + unique key) acts as a stable tiebreaker.
    let order_col = if sort_keys.is_empty() {
        default_order
    } else {
        let user_order = compile_sort_keys(sort_keys, registry);
        format!("{user_order}, {default_order}")
    };

    // Build the `base` CTE: full three-table join with optional dedup column.
    // The ROW_NUMBER inner ORDER BY uses `i.synced_at_unix_ms DESC` so we
    // pick the most recently synced instance as the representative row for
    // each entity (study/series/patient).  `i.sop_instance_uid` is the
    // tiebreaker to make the choice stable when timestamps are equal.
    let dedup_col = partition_col.as_deref().map(|p| {
        format!(
            ", ROW_NUMBER() OVER (PARTITION BY {p} ORDER BY i.synced_at_unix_ms DESC, i.sop_instance_uid) AS _rn"
        )
    });
    let dedup_expr = dedup_col.as_deref().unwrap_or("");

    let base_cte = format!(
        "WITH _base AS (SELECT {select}{dedup_expr} \
         FROM instances i \
         JOIN series se ON se.series_instance_uid = i.series_instance_uid \
         JOIN studies s ON s.study_instance_uid = se.study_instance_uid\
         {where_clause})"
    );

    // Dedup CTE (only for Study/Series/Patient scope).
    let (from_cte, deduped_cte) = if partition_col.is_some() {
        (
            "_deduped".to_string(),
            ", _deduped AS (SELECT * FROM _base WHERE _rn = 1)".to_string(),
        )
    } else {
        ("_base".to_string(), String::new())
    };

    // Final SELECT: optionally add COUNT(*) OVER () for paging.
    let final_select = if has_paging {
        format!(
            "SELECT *, COUNT(*) OVER () AS _total FROM {from_cte} ORDER BY {order_col} LIMIT ? OFFSET ?"
        )
    } else {
        format!("SELECT * FROM {from_cte} ORDER BY {order_col}")
    };

    if has_paging {
        let p = paging.expect("has_paging implies paging is Some");
        binds.push(BindValue::Int(p.limit() as i64));
        binds.push(BindValue::Int(p.offset() as i64));
    }

    format!("{base_cte}{deduped_cte} {final_select}")
}

/// Compiles a list of [`SortKey`]s into a comma-separated SQL ORDER BY fragment.
///
/// Indexed attributes use their bare column name (table aliases are not in
/// scope outside the base CTE).  Non-indexed attributes use `json_extract`
/// on the `attributes` blob column.
fn compile_sort_keys(sort_keys: &[SortKey], registry: &AttributeRegistry) -> String {
    sort_keys
        .iter()
        .filter_map(|key| {
            let tag = key.path.segments().iter().find_map(|s| {
                if let AttributePathSegment::Tag(t) = s {
                    Some(*t)
                } else {
                    None
                }
            })?;
            let direction = match key.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            };
            let expr = if let Some(m) = registry.get(tag) {
                // Indexed column: bare name — table aliases are no longer in scope.
                m.column.to_string()
            } else {
                // Blob attribute: json_extract from the `attributes` column.
                format!("json_extract(attributes, {})", blob_scalar_json_path(tag))
            };
            Some(format!("{expr} {direction}"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns `(partition_expr, order_cols)`.
///
/// `partition_expr` uses the table-qualified column (valid inside the base CTE
/// where table aliases are in scope).  It is `None` for Image-level queries
/// (SOP Instance UID is already unique per row; no dedup needed).
///
/// `order_cols` is the multi-column `ORDER BY` expression for the **outer**
/// `SELECT`, where table aliases are no longer in scope.  Results are ordered
/// newest-synced-first (`DESC`) within the scope entity, with the scope's
/// unique key as a tiebreaker so that pagination is stable when multiple
/// entities share the same `synced_at_unix_ms`.
fn dedup_info_for_scope(scope: QueryScope) -> (Option<String>, String) {
    match scope {
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient) => (
            // COALESCE so that studies without a PatientID each form their own
            // partition rather than all being grouped into one "null patient".
            // PatientID is a required unique key (PS3.4 C.2.2.1.1), so this is
            // a safety net for non-conformant data only.
            Some("COALESCE(s.patient_id, s.study_instance_uid)".to_string()),
            "s_synced_at DESC, COALESCE(patient_id, study_instance_uid)".to_string(),
        ),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study) => (
            Some("s.study_instance_uid".to_string()),
            "s_synced_at DESC, study_instance_uid".to_string(),
        ),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Series)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series) => (
            Some("se.series_instance_uid".to_string()),
            "se_synced_at DESC, series_instance_uid".to_string(),
        ),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image) => {
            (None, "i_synced_at DESC, sop_instance_uid".to_string())
        }
    }
}

/// Tracks the JSON source expression for blob-attribute access.
///
/// In the root context, mapped indexed columns take precedence over the blob.
/// Inside a `json_each` sequence loop, only the loop's `value` JSON is available.
struct CompileContext {
    /// Expression for the DICOM JSON blob document.
    /// Root: `i.attributes`. Inside a sequence: `_seqN.value`.
    blob_expr: String,
    /// Whether mapped (indexed) columns can be used for this context level.
    /// False inside `json_each` loops.
    use_mapped_columns: bool,
    /// Whether fuzzy semantic matching is active (PS3.4 C.2.2.2.3).
    /// When true, PN attribute predicates use SOUNDEX-based phonetic matching.
    fuzzy_matching: bool,
    /// Signed timezone offset in minutes parsed from `timezone_offset_from_utc`
    /// (e.g. `"+0800"` → `480`).  `None` when no timezone is declared.
    /// Used to shift DT range bounds from the SCU's timezone to UTC before
    /// emitting the SQL parameter.
    tz_offset_minutes: Option<i32>,
}

impl CompileContext {
    fn root(fuzzy_matching: bool, tz_offset_minutes: Option<i32>) -> Self {
        Self {
            blob_expr: "i.attributes".to_string(),
            use_mapped_columns: true,
            fuzzy_matching,
            tz_offset_minutes,
        }
    }

    /// Creates a nested context for use inside a `json_each` sequence loop.
    /// Inherits both `fuzzy_matching` and `tz_offset_minutes` from the parent.
    fn nested(&self, seq_alias: &str) -> Self {
        Self {
            blob_expr: format!("{seq_alias}.value"),
            use_mapped_columns: false,
            fuzzy_matching: self.fuzzy_matching,
            tz_offset_minutes: self.tz_offset_minutes,
        }
    }
}

fn compile_predicate(
    predicate: &Predicate,
    ctx: &CompileContext,
    registry: &AttributeRegistry,
    binds: &mut Vec<BindValue>,
    seq_counter: &mut usize,
) -> Result<String, SqliteReadRepositoryError> {
    match predicate {
        Predicate::All(items) => {
            if items.is_empty() {
                return Ok("TRUE".to_string());
            }
            let parts = items
                .iter()
                .map(|p| compile_predicate(p, ctx, registry, binds, seq_counter))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        Predicate::Attribute(path, MatchingRule::Sequence(seq)) => {
            let tag = single_tag(path)?;
            compile_sequence_predicate(tag, &seq.predicate, ctx, registry, binds, seq_counter)
        }
        Predicate::Attribute(path, rule) => {
            let tag = single_tag(path)?;
            let value_sql = scalar_value_sql(tag, ctx, registry);
            if ctx.fuzzy_matching && is_pn_tag(tag) {
                compile_fuzzy_pn_rule(&value_sql, rule, binds)
            } else {
                compile_matching_rule(&value_sql, rule, ctx.tz_offset_minutes, binds)
            }
        }
    }
}

fn compile_sequence_predicate(
    sq_tag: Tag,
    inner_predicate: &Predicate,
    ctx: &CompileContext,
    registry: &AttributeRegistry,
    binds: &mut Vec<BindValue>,
    seq_counter: &mut usize,
) -> Result<String, SqliteReadRepositoryError> {
    let alias = format!("_seq{}", *seq_counter);
    *seq_counter += 1;

    // In any context, SQ attributes live in the JSON blob (never in indexed columns).
    let array_sql = format!(
        "COALESCE(json_extract({blob}, '$.\"{tag}\".Value'), json('[]'))",
        blob = ctx.blob_expr,
        tag = tag_key(sq_tag),
    );

    let nested_ctx = ctx.nested(&alias);
    let inner_sql = compile_predicate(inner_predicate, &nested_ctx, registry, binds, seq_counter)?;

    Ok(format!(
        "EXISTS (SELECT 1 FROM json_each({array_sql}) AS {alias} WHERE {inner_sql})"
    ))
}

fn compile_matching_rule(
    value_sql: &str,
    rule: &MatchingRule,
    tz_offset_minutes: Option<i32>,
    binds: &mut Vec<BindValue>,
) -> Result<String, SqliteReadRepositoryError> {
    match rule {
        MatchingRule::Universal => Ok("TRUE".to_string()),

        MatchingRule::EmptyValue => Ok(format!("({value_sql} IS NULL OR {value_sql} = '')")),

        MatchingRule::SingleValue(v) => {
            binds.push(BindValue::Text(v.clone()));
            Ok(format!("{value_sql} = ?"))
        }

        MatchingRule::Wildcard(pattern) => {
            // DICOM wildcard: `*` → SQL `%`, `?` → SQL `_`.
            // We use ESCAPE '\' so literal `%` and `_` in DICOM values are safe.
            let like_pattern = pattern
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
                .replace('*', "%")
                .replace('?', "_");
            binds.push(BindValue::Text(like_pattern));
            Ok(format!("{value_sql} LIKE ? ESCAPE '\\'"))
        }

        MatchingRule::Range(r) => compile_range(value_sql, r.start(), r.end(), false, None, binds),
        MatchingRule::DateTimeRange(r) => {
            // Shift DT bounds from the SCU's declared timezone to UTC.
            compile_range(
                value_sql,
                r.start(),
                r.end(),
                true,
                tz_offset_minutes,
                binds,
            )
        }

        MatchingRule::UidList(uids) | MatchingRule::MultipleValues(uids) => {
            let placeholders: Vec<&str> = uids
                .iter()
                .map(|uid| {
                    binds.push(BindValue::Text(uid.clone()));
                    "?"
                })
                .collect();
            Ok(format!("{value_sql} IN ({})", placeholders.join(", ")))
        }

        MatchingRule::Sequence(_) => {
            // Handled one level up in compile_predicate; should never reach here.
            Err(SqliteReadRepositoryError::InternalError(
                "Sequence matching rule must be handled at predicate dispatch".to_string(),
            ))
        }
    }
}

fn compile_range(
    value_sql: &str,
    start: Option<&str>,
    end: Option<&str>,
    is_datetime: bool,
    tz_offset_minutes: Option<i32>,
    binds: &mut Vec<BindValue>,
) -> Result<String, SqliteReadRepositoryError> {
    let mut clauses: Vec<String> = Vec::new();
    if is_datetime {
        // DT range matching requires that the stored value is not NULL so that
        // lexicographic comparison is meaningful.
        clauses.push(format!("{value_sql} IS NOT NULL"));
    }
    if let Some(s) = start {
        let adjusted = if is_datetime {
            tz_offset_minutes
                .and_then(|off| shift_dt_to_utc(s, off))
                .unwrap_or_else(|| s.to_string())
        } else {
            s.to_string()
        };
        binds.push(BindValue::Text(adjusted));
        clauses.push(format!("{value_sql} >= ?"));
    }
    if let Some(e) = end {
        let adjusted = if is_datetime {
            tz_offset_minutes
                .and_then(|off| shift_dt_to_utc(e, off))
                .unwrap_or_else(|| e.to_string())
        } else {
            e.to_string()
        };
        binds.push(BindValue::Text(adjusted));
        clauses.push(format!("{value_sql} <= ?"));
    }
    if clauses.is_empty() {
        Ok("TRUE".to_string())
    } else {
        Ok(format!("({})", clauses.join(" AND ")))
    }
}

/// Parses a timezone offset string in `+HHMM` / `-HHMM` form into signed minutes.
/// Returns `None` for any input that does not match the expected format.
fn parse_timezone_offset_minutes(offset: &str) -> Option<i32> {
    if offset.len() != 5 {
        return None;
    }
    let sign: i32 = match offset.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hh: i32 = offset[1..3].parse().ok()?;
    let mm: i32 = offset[3..5].parse().ok()?;
    Some(sign * (hh * 60 + mm))
}

/// Shifts a DICOM DT string by `offset_minutes` to convert from the SCU's
/// declared timezone to UTC (which is what the repository stores).
///
/// The standard (PS3.4 C.2.2.2.5) requires the SCP to adjust DT values in
/// the identifier from the SCU's timezone to its own timezone.  We assume
/// the repository stores DT values in UTC.
///
/// Handles partial DT forms (`YYYY`, `YYYYMM`, `YYYYMMDD`, …) up to
/// `YYYYMMDDHHMMSS`.  The fractional-seconds component is preserved but not
/// adjusted.  Returns `None` if the DT cannot be parsed (caller falls back
/// to the original string unchanged).
///
/// The maximum timezone offset is ±840 minutes (±14 hours), so the shift
/// can span at most one calendar day — a full epoch conversion is not needed.
fn shift_dt_to_utc(dt: &str, offset_minutes: i32) -> Option<String> {
    // Strip trailing timezone suffix ("+HHMM"/"-HHMM") if present.
    let (dt, tz_suffix) = if dt.len() >= 5 {
        let last5 = &dt[dt.len() - 5..];
        if matches!(last5.as_bytes()[0], b'+' | b'-')
            && last5[1..].bytes().all(|b| b.is_ascii_digit())
        {
            (&dt[..dt.len() - 5], last5)
        } else {
            (dt, "")
        }
    } else {
        (dt, "")
    };
    // Strip fractional seconds (".FFFFFF") if present.
    let (dt, frac) = dt
        .find('.')
        .map(|i| (&dt[..i], &dt[i..]))
        .unwrap_or((dt, ""));

    if !matches!(dt.len(), 4 | 6 | 8 | 10 | 12 | 14) || !dt.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    let orig_len = dt.len();
    let year: i64 = dt[0..4].parse().ok()?;
    let month: i64 = if orig_len >= 6 {
        dt[4..6].parse().ok()?
    } else {
        1
    };
    let day: i64 = if orig_len >= 8 {
        dt[6..8].parse().ok()?
    } else {
        1
    };
    let hour: i64 = if orig_len >= 10 {
        dt[8..10].parse().ok()?
    } else {
        0
    };
    let minute: i64 = if orig_len >= 12 {
        dt[10..12].parse().ok()?
    } else {
        0
    };
    let second: i64 = if orig_len == 14 {
        dt[12..14].parse().ok()?
    } else {
        0
    };

    // The only components affected by the timezone shift are hour and minute;
    // the carry/borrow propagates at most one day (offset ≤ 840 min < 1440).
    let adj_day_mins = (hour * 60 + minute) - i64::from(offset_minutes);
    let day_delta: i64 = if adj_day_mins < 0 {
        -1
    } else if adj_day_mins >= 1440 {
        1
    } else {
        0
    };
    let norm_mins = adj_day_mins - day_delta * 1440;
    let new_hour = norm_mins / 60;
    let new_minute = norm_mins % 60;
    let (new_year, new_month, new_day) = adjust_calendar_day(year, month, day + day_delta)?;

    let core = match orig_len {
        4 => format!("{new_year:04}"),
        6 => format!("{new_year:04}{new_month:02}"),
        8 => format!("{new_year:04}{new_month:02}{new_day:02}"),
        10 => format!("{new_year:04}{new_month:02}{new_day:02}{new_hour:02}"),
        12 => format!("{new_year:04}{new_month:02}{new_day:02}{new_hour:02}{new_minute:02}"),
        14 => format!(
            "{new_year:04}{new_month:02}{new_day:02}{new_hour:02}{new_minute:02}{second:02}"
        ),
        _ => return None,
    };
    Some(format!("{core}{frac}{tz_suffix}"))
}

/// Normalises a (year, month, day) triple after a ±1-day adjustment,
/// handling month-boundary rollovers and year-boundary rollovers.
fn adjust_calendar_day(year: i64, month: i64, day: i64) -> Option<(i64, i64, i64)> {
    if day < 1 {
        let (prev_year, prev_month) = if month == 1 {
            (year - 1, 12)
        } else {
            (year, month - 1)
        };
        if prev_year < 1 {
            return None;
        }
        Some((
            prev_year,
            prev_month,
            days_in_month(prev_year, prev_month) + day,
        ))
    } else {
        let dim = days_in_month(year, month);
        if day > dim {
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            if next_year > 9999 {
                return None;
            }
            Some((next_year, next_month, day - dim))
        } else {
            Some((year, month, day))
        }
    }
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 30,
    }
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Compiles a matching rule using SOUNDEX-based phonetic approximation.
///
/// Called only when `fuzzy_matching` is active **and** the attribute's VR is
/// PN (PS3.4 C.2.2.2.3).  SQLite's built-in `SOUNDEX()` provides English-
/// language phonetic encoding.
///
/// Known limitations:
/// - Only the Alphabetic PN component is matched; Ideographic and Phonetic
///   components are not consulted.
/// - SOUNDEX is English-only and does not implement the full DICOM fuzzy
///   semantic matching specification.
/// - Wildcard patterns have their wildcard characters stripped before phonetic
///   encoding.  A pattern composed entirely of wildcards degenerates to
///   `TRUE` (universal match).
fn compile_fuzzy_pn_rule(
    value_sql: &str,
    rule: &MatchingRule,
    binds: &mut Vec<BindValue>,
) -> Result<String, SqliteReadRepositoryError> {
    match rule {
        MatchingRule::Universal => Ok("TRUE".to_string()),
        MatchingRule::EmptyValue => Ok(format!("({value_sql} IS NULL OR {value_sql} = '')")),
        MatchingRule::SingleValue(v) => {
            binds.push(BindValue::Text(v.clone()));
            Ok(format!("SOUNDEX({value_sql}) = SOUNDEX(?)"))
        }
        MatchingRule::Wildcard(pattern) => {
            // Strip DICOM wildcard characters before phonetic encoding.
            // Replace `*` with a space so multi-word names like "JOHN MICHAEL"
            // can still degrade gracefully; strip `?` entirely.
            let literal = pattern.replace('*', " ").replace('?', "");
            let literal = literal.split_whitespace().collect::<Vec<_>>().join(" ");
            if literal.is_empty() {
                return Ok("TRUE".to_string());
            }
            binds.push(BindValue::Text(literal));
            Ok(format!("SOUNDEX({value_sql}) = SOUNDEX(?)"))
        }
        // Range, UID list, multiple values, and sequence matching are not
        // PN-specific in practice — fall through to standard matching.
        // No timezone adjustment needed: PN attributes are never DT VR.
        other => compile_matching_rule(value_sql, other, None, binds),
    }
}

/// Returns the SQL expression for the scalar string value of a DICOM attribute.
///
/// Indexed (mapped) columns take precedence in the root context; all other
/// attributes are read from the JSON blob via `json_extract`.
///
/// PN attributes receive special treatment: DICOM JSON encodes them as objects
/// (`{"Alphabetic":"DOE^JOHN","Ideographic":"...","Phonetic":"..."}`), so we
/// extract the `.Alphabetic` sub-field rather than the object itself.  Matching
/// on the raw JSON object representation would break all wildcard and
/// single-value predicates against non-indexed PN attributes.
fn scalar_value_sql(tag: Tag, ctx: &CompileContext, registry: &AttributeRegistry) -> String {
    if ctx.use_mapped_columns
        && let Some(m) = registry.get(tag)
    {
        return format!("CAST({}.{} AS TEXT)", m.table.alias(), m.column);
    }
    format!(
        "CAST(json_extract({blob}, {path}) AS TEXT)",
        blob = ctx.blob_expr,
        path = blob_scalar_json_path(tag),
    )
}

/// Returns the JSONPath string (including surrounding single quotes) for
/// extracting the scalar query-matching value of `tag` from the DICOM JSON blob.
///
/// For PN VRs the standard C-FIND matching applies to the Alphabetic component
/// (PS3.4 C.2.2.2.1).  All other VRs use the first element of the `Value` array.
/// Returns `true` if the tag's VR in the standard data dictionary is PN.
///
/// Unknown or private tags return `false`; they are not treated as PN for
/// either JSON path extraction or fuzzy matching.
fn is_pn_tag(tag: Tag) -> bool {
    StandardDataDictionary
        .by_tag(tag)
        .and_then(|entry| entry.vr().exact())
        == Some(VR::PN)
}

fn blob_scalar_json_path(tag: Tag) -> String {
    if is_pn_tag(tag) {
        format!("'$.\"{}\".Value[0].Alphabetic'", tag_key(tag))
    } else {
        format!("'$.\"{}\".Value[0]'", tag_key(tag))
    }
}

/// Formats a DICOM [`Tag`] as the 8-character uppercase hex key used in
/// DICOM JSON (PS3.18): e.g. `Tag(0x0010, 0x0020)` → `"00100020"`.
pub(crate) fn tag_key(tag: Tag) -> String {
    format!("{:04X}{:04X}", tag.group(), tag.element())
}

/// Extracts the single [`Tag`] from a predicate path.
///
/// In our [`Predicate`] model, non-sequence attribute paths are always
/// single-tag (`Tag(X)` with no item selectors). Sequence paths target
/// the SQ tag directly and the inner predicate is compiled recursively.
fn single_tag(path: &AttributePath) -> Result<Tag, SqliteReadRepositoryError> {
    path.segments()
        .iter()
        .find_map(|s| {
            if let AttributePathSegment::Tag(t) = s {
                Some(*t)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            SqliteReadRepositoryError::InternalError(format!(
                "predicate path contains no tag segment: {path}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;
    use raccoon_service_query::{
        AttributePath, MatchingRule, PatientRootQueryRetrieveLevel, Predicate, Projection,
        QueryPaging, QueryScope, RangeMatching, SequenceMatching, StudyRootQueryRetrieveLevel,
    };

    use super::*;
    use crate::schema::AttributeRegistry;

    fn registry() -> AttributeRegistry {
        AttributeRegistry::new()
    }

    fn study_scope() -> QueryScope {
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study)
    }

    fn compile_ok(query: &DicomQuery, registry: &AttributeRegistry) -> CompiledQuery {
        compile(query, registry).expect("compile should succeed")
    }

    #[test]
    fn no_predicate_compiles_to_no_where_clause() {
        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        let compiled = compile_ok(&query, &registry());

        // Without a predicate the only WHERE must be the dedup filter, not a user predicate.
        let sql_without_dedup_filter = compiled.sql.replace("WHERE _rn = 1", "");
        assert!(!sql_without_dedup_filter.contains("WHERE"));
        assert!(compiled.sql.contains("ROW_NUMBER()"));
        assert!(compiled.sql.contains("PARTITION BY s.study_instance_uid"));
        assert!(compiled.sql.contains("s_synced_at DESC"));
        assert!(!compiled.has_total_count);
    }

    #[test]
    fn single_value_predicate_on_mapped_column_uses_column_expression() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("PAT-001".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("CAST(s.patient_id AS TEXT) = ?"));
        assert_eq!(compiled.binds.len(), 1);
        assert!(matches!(&compiled.binds[0], BindValue::Text(v) if v == "PAT-001"));
    }

    #[test]
    fn wildcard_on_patient_name_uses_like() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_NAME),
                MatchingRule::Wildcard("DOE*".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        // `*` → `%`, no literal `%` or `_` to escape
        assert!(compiled.sql.contains("CAST(s.patient_name AS TEXT) LIKE ?"));
        assert!(matches!(&compiled.binds[0], BindValue::Text(v) if v == "DOE%"));
    }

    #[test]
    fn wildcard_escapes_literal_sql_wildcards() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_NAME),
                MatchingRule::Wildcard("50%_off*".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        // literal `%` → `\%`, literal `_` → `\_`, DICOM `*` → SQL `%`
        assert!(matches!(
            &compiled.binds[0],
            BindValue::Text(v) if v == "50\\%\\_off%"
        ));
        assert!(compiled.sql.contains("ESCAPE '\\'"));
    }

    #[test]
    fn uid_list_compiles_to_in_expression() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                MatchingRule::UidList(vec!["1.2.3".to_string(), "1.2.4".to_string()]),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("IN (?, ?)"));
        assert_eq!(compiled.binds.len(), 2);
    }

    #[test]
    fn date_range_compiles_to_between_clauses() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_DATE),
                MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains(">= ?") && compiled.sql.contains("<= ?"));
        assert_eq!(compiled.binds.len(), 2);
    }

    #[test]
    fn non_indexed_attribute_falls_back_to_json_extract() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                // MANUFACTURER (0008,0070) is not a mapped column
                AttributePath::from_tag(tags::MANUFACTURER),
                MatchingRule::SingleValue("ACME".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("json_extract(i.attributes"));
        assert!(compiled.sql.contains("00080070"));
    }

    #[test]
    fn sequence_predicate_compiles_to_exists_with_json_each() {
        let inner = Predicate::Attribute(
            AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
            MatchingRule::SingleValue("STEP-1".to_string()),
        );
        let query = DicomQuery::new(
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            Projection::Default,
        )
        .expect("valid query")
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
            MatchingRule::Sequence(SequenceMatching {
                predicate: Box::new(inner),
            }),
        ))
        .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("EXISTS"));
        assert!(compiled.sql.contains("json_each"));
        assert!(compiled.sql.contains("00400275")); // REQUEST_ATTRIBUTES_SEQUENCE
        assert!(compiled.sql.contains("00400009")); // SCHEDULED_PROCEDURE_STEP_ID
        // Inner predicate must use the seq alias, not i.attributes
        assert!(compiled.sql.contains("_seq0.value"));
    }

    #[test]
    fn paging_adds_limit_offset_and_total_count_column() {
        let paging = QueryPaging::new(20, 10).expect("valid paging");
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_paging(paging);
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("COUNT(*) OVER ()"));
        assert!(compiled.sql.contains("LIMIT ?"));
        assert!(compiled.sql.contains("OFFSET ?"));
        assert!(compiled.has_total_count);
        // Last two binds must be limit=10 and offset=20
        let n = compiled.binds.len();
        assert!(matches!(&compiled.binds[n - 2], BindValue::Int(10)));
        assert!(matches!(&compiled.binds[n - 1], BindValue::Int(20)));
    }

    #[test]
    fn image_scope_has_no_row_number_dedup() {
        let query = DicomQuery::new(
            QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image),
            Projection::Default,
        )
        .expect("valid query");
        let compiled = compile_ok(&query, &registry());

        assert!(!compiled.sql.contains("ROW_NUMBER()"));
        assert!(!compiled.sql.contains("_deduped"));
    }

    #[test]
    fn patient_root_patient_scope_partitions_by_coalesced_patient_id() {
        let query = DicomQuery::new(
            QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
            Projection::Default,
        )
        .expect("valid query");
        let compiled = compile_ok(&query, &registry());

        // NULL PatientID rows each get their own partition via the COALESCE fallback.
        assert!(
            compiled
                .sql
                .contains("PARTITION BY COALESCE(s.patient_id, s.study_instance_uid)")
        );
    }

    #[test]
    fn non_indexed_pn_attribute_extracts_alphabetic_component() {
        // PerformingPhysicianName (0008,1048) is PN but not an indexed column.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PERFORMING_PHYSICIAN_NAME),
                MatchingRule::Wildcard("DOE*".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        // Must use the .Alphabetic sub-path, not the raw Value[0] JSON object.
        assert!(compiled.sql.contains(".Alphabetic'"));
        assert!(!compiled.sql.contains(".Value[0]'"));
    }

    #[test]
    fn indexed_pn_attribute_uses_column_not_json_extract() {
        // PatientName (0010,0010) is PN and IS an indexed column — must use
        // the column directly, not json_extract at all.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_NAME),
                MatchingRule::Wildcard("DOE*".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("CAST(s.patient_name AS TEXT)"));
        assert!(!compiled.sql.contains("json_extract"));
    }

    #[test]
    fn non_indexed_non_pn_attribute_uses_value_array_not_alphabetic() {
        // Manufacturer (0008,0070) is LO (not PN) — must use Value[0], not .Alphabetic.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::MANUFACTURER),
                MatchingRule::SingleValue("ACME".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains(".Value[0]'"));
        assert!(!compiled.sql.contains(".Alphabetic"));
    }

    #[test]
    fn conjunction_of_predicates_joins_with_and() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::All(vec![
                Predicate::Attribute(
                    AttributePath::from_tag(tags::PATIENT_ID),
                    MatchingRule::SingleValue("PAT-001".to_string()),
                ),
                Predicate::Attribute(
                    AttributePath::from_tag(tags::STUDY_DATE),
                    MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
                ),
            ]))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains(" AND "));
        assert_eq!(compiled.binds.len(), 3); // 1 + 2 range bounds
    }

    #[test]
    fn empty_value_matching_checks_null_and_empty_string() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::ACCESSION_NUMBER),
                MatchingRule::EmptyValue,
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("IS NULL OR") && compiled.sql.contains("= ''"));
        assert!(compiled.binds.is_empty());
    }

    #[test]
    fn datetime_range_includes_not_null_guard() {
        let query = DicomQuery::new(
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            Projection::Default,
        )
        .expect("valid query")
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::ACQUISITION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching::from_start("20260101120000+0000")),
        ))
        .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("IS NOT NULL"));
        assert!(compiled.sql.contains(">= ?"));
    }

    #[test]
    fn sort_keys_prepend_user_columns_before_default_order() {
        use raccoon_service_query::{SortDirection, SortKey};

        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_sort_keys(vec![
                SortKey {
                    path: AttributePath::from_tag(tags::STUDY_DATE),
                    direction: SortDirection::Descending,
                },
                SortKey {
                    path: AttributePath::from_tag(tags::PATIENT_NAME),
                    direction: SortDirection::Ascending,
                },
            ])
            .expect("valid sort keys");
        let compiled = compile_ok(&query, &registry());

        // User sort columns appear before the default stability tiebreaker.
        assert!(compiled.sql.contains("study_date DESC"));
        assert!(compiled.sql.contains("patient_name ASC"));
        // Default tiebreaker still present after user columns.
        assert!(compiled.sql.contains("s_synced_at DESC"));
    }

    #[test]
    fn sort_key_on_non_indexed_attribute_uses_json_extract() {
        use raccoon_service_query::{SortDirection, SortKey};

        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_sort_keys(vec![SortKey {
                path: AttributePath::from_tag(tags::MANUFACTURER),
                direction: SortDirection::Ascending,
            }])
            .expect("valid sort keys");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("json_extract(attributes"));
        assert!(compiled.sql.contains("00080070")); // MANUFACTURER tag key
        assert!(compiled.sql.contains("ASC"));
    }

    #[test]
    fn fuzzy_matching_on_pn_attribute_emits_soundex() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_fuzzy_matching()
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_NAME),
                MatchingRule::SingleValue("DOE^JOHN".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("SOUNDEX"));
        // The bind must be the original value (not modified for fuzzy).
        assert!(matches!(&compiled.binds[0], BindValue::Text(v) if v == "DOE^JOHN"));
    }

    #[test]
    fn fuzzy_matching_does_not_affect_non_pn_attributes() {
        // PatientID (0010,0020) is LO — fuzzy matching must not apply.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_fuzzy_matching()
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("PAT-001".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(!compiled.sql.contains("SOUNDEX"));
        assert!(compiled.sql.contains("= ?"));
    }

    #[test]
    fn timezone_offset_shifts_datetime_range_bounds() {
        // SCU is at UTC+0800; repository stores in UTC.
        // Query: acquisition >= 20260101T080000 local (+0800) → UTC 20260101T000000.
        let query = DicomQuery::new(
            QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image),
            Projection::Default,
        )
        .expect("valid query")
        .with_timezone_offset("+0800")
        .expect("valid offset")
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::ACQUISITION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching::from_start("20260101080000")),
        ))
        .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        // The bind value must be shifted 8 hours back (08:00 → 00:00 UTC).
        // Non-paging binds: just the one range bound.
        let dt_bind = compiled
            .binds
            .iter()
            .find_map(|b| {
                if let BindValue::Text(v) = b {
                    Some(v.as_str())
                } else {
                    None
                }
            })
            .expect("one text bind");
        assert_eq!(dt_bind, "20260101000000");
    }

    #[test]
    fn timezone_offset_without_dt_predicate_does_not_affect_date_range() {
        // DA ranges (StudyDate) must not be shifted — timezone adjustment only
        // applies to DT attributes per PS3.4 C.2.2.2.5.
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_timezone_offset("+0800")
            .expect("valid offset")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_DATE),
                MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        // DA range binds must be unchanged.
        let texts: Vec<&str> = compiled
            .binds
            .iter()
            .filter_map(|b| {
                if let BindValue::Text(v) = b {
                    Some(v.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(texts.contains(&"20260101"));
        assert!(texts.contains(&"20261231"));
    }
}
