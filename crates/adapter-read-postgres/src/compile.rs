//! Translates a [`DicomQuery`] into a Postgres statement.
//!
//! Generic queries join all three tables (`instances i`, `series se`,
//! `studies s`). Projection is applied on the Rust side in [`crate::project`];
//! the SQL side handles filtering (WHERE), derived relationship counts, and
//! deduplication (ROW_NUMBER) where needed. Narrow series-level queries can use
//! a direct `series JOIN studies` scan when no predicate or projection needs
//! instance rows.
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
//! deduplicated set, giving the total without a second round-trip to Postgres.

use std::collections::HashSet;

use dicom_core::Tag;
use dicom_core::VR;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_dictionary_std::StandardDataDictionary;
use dicom_dictionary_std::tags;
use raccoon_service_query::{
    AttributePath, AttributePathSegment, MatchingRule, PatientRootQueryRetrieveLevel, Predicate,
    Projection, QueryScope, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
};
use raccoon_service_query::{DicomQuery, QueryPaging};

use crate::error::PostgresReadRepositoryError;
use crate::schema::{ATTRIBUTE_MAPPINGS, AttributeMapping, AttributeRegistry, TableId};

#[derive(Debug, Clone)]
pub(crate) enum BindValue {
    Text(String),
    Int(i64),
}

fn push_text_bind(value: String, binds: &mut Vec<BindValue>) -> String {
    binds.push(BindValue::Text(value));
    format!("${}", binds.len())
}

fn push_int_bind(value: i64, binds: &mut Vec<BindValue>) -> String {
    binds.push(BindValue::Int(value));
    format!("${}", binds.len())
}

#[derive(Debug)]
pub(crate) struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<BindValue>,
    /// Indexed mappings actually selected by SQL. Projection materialization
    /// uses this to avoid reading columns that were intentionally omitted.
    pub selected_mappings: Vec<&'static AttributeMapping>,
    /// Whether the result rows include `attributes`.
    pub includes_attributes: bool,
    /// Whether the result rows include a `_total` column (present only when
    /// paging was requested).
    pub has_total_count: bool,
    /// Paging parameters echoed back for the page-summary in the response.
    pub paging: Option<QueryPaging>,
}

pub(crate) fn compile(
    query: &DicomQuery,
    registry: &AttributeRegistry,
) -> Result<CompiledQuery, PostgresReadRepositoryError> {
    let scope = query.scope();
    let mut binds: Vec<BindValue> = Vec::new();
    let mut seq_counter: usize = 0;
    let selection = QuerySelection::for_query(query, registry);
    let use_series_table_scan = supports_series_table_scan(query, registry, &selection);
    let use_image_table_scan = supports_image_table_scan(query);

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
    let sql = if use_series_table_scan {
        build_series_table_sql(
            where_sql.as_deref(),
            query.sort_keys(),
            registry,
            &selection,
            has_paging,
            &mut binds,
            paging,
        )
    } else if use_image_table_scan {
        build_image_table_sql(
            where_sql.as_deref(),
            query.sort_keys(),
            registry,
            &selection,
            has_paging,
            &mut binds,
            paging,
        )
    } else {
        build_sql(
            scope,
            where_sql.as_deref(),
            query.sort_keys(),
            registry,
            &selection,
            &mut binds,
            paging,
        )
    };

    Ok(CompiledQuery {
        sql,
        binds,
        selected_mappings: selection.mappings,
        includes_attributes: selection.include_attributes,
        has_total_count: has_paging,
        paging,
    })
}

#[derive(Debug)]
struct QuerySelection {
    mappings: Vec<&'static AttributeMapping>,
    include_attributes: bool,
    sort_scalars: Vec<JsonSortScalar>,
}

#[derive(Debug)]
struct JsonSortScalar {
    tag: Tag,
    alias: String,
}

impl QuerySelection {
    fn for_query(query: &DicomQuery, registry: &AttributeRegistry) -> Self {
        let mut tags = HashSet::new();
        let mut include_attributes = false;

        match query.projection() {
            Projection::All => {
                tags.extend(ATTRIBUTE_MAPPINGS.iter().map(|m| m.tag));
                include_attributes = true;
            }
            Projection::Default => {
                tags.extend(mandatory_tags_for_scope(query.scope()));
            }
            Projection::Fields(paths) => {
                for path in paths {
                    if let Some(tag) = tag_of_path(path) {
                        if registry.get(tag).is_some() {
                            tags.insert(tag);
                        } else {
                            include_attributes = true;
                        }
                    }
                }
            }
        }

        for tag in order_key_tags_for_scope(query.scope()) {
            tags.insert(tag);
        }
        let mut sort_scalars = Vec::new();
        for (index, key) in query.sort_keys().iter().enumerate() {
            if let Some(tag) = tag_of_path(&key.path) {
                if registry.get(tag).is_some() {
                    tags.insert(tag);
                } else {
                    sort_scalars.push(JsonSortScalar {
                        tag,
                        alias: format!("_sort_{index}"),
                    });
                }
            }
        }

        let mappings = ATTRIBUTE_MAPPINGS
            .iter()
            .filter(|mapping| tags.contains(&mapping.tag))
            .collect();

        Self {
            mappings,
            include_attributes,
            sort_scalars,
        }
    }

    fn selects_tag(&self, tag: Tag) -> bool {
        self.mappings.iter().any(|mapping| mapping.tag == tag)
    }

    fn needs_study_counts(&self) -> bool {
        self.selects_tag(tags::NUMBER_OF_STUDY_RELATED_SERIES)
            || self.selects_tag(tags::NUMBER_OF_STUDY_RELATED_INSTANCES)
    }

    fn needs_series_counts(&self) -> bool {
        self.selects_tag(tags::NUMBER_OF_SERIES_RELATED_INSTANCES)
    }

    fn needs_study_modalities(&self) -> bool {
        self.selects_tag(tags::MODALITIES_IN_STUDY)
    }
}

fn supports_image_table_scan(query: &DicomQuery) -> bool {
    matches!(
        query.scope(),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image)
            | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image)
    )
}

fn supports_series_table_scan(
    query: &DicomQuery,
    registry: &AttributeRegistry,
    selection: &QuerySelection,
) -> bool {
    matches!(
        query.scope(),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Series)
            | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)
    ) && !selection.include_attributes
        && selection
            .mappings
            .iter()
            .all(|mapping| mapping.table != TableId::Instance)
        && query
            .sort_keys()
            .iter()
            .all(|key| series_join_direct_tag(&key.path, registry))
        && query
            .predicate()
            .is_none_or(|predicate| predicate_uses_only_series_join(predicate, registry))
}

fn predicate_uses_only_series_join(predicate: &Predicate, registry: &AttributeRegistry) -> bool {
    match predicate {
        Predicate::All(items) => items
            .iter()
            .all(|item| predicate_uses_only_series_join(item, registry)),
        Predicate::Attribute(_, MatchingRule::Sequence(_)) => false,
        Predicate::Attribute(path, _) => series_join_direct_tag(path, registry),
    }
}

fn series_join_direct_tag(path: &AttributePath, registry: &AttributeRegistry) -> bool {
    tag_of_path(path).is_some_and(|tag| {
        registry
            .get(tag)
            .is_some_and(|mapping| mapping.table != TableId::Instance && is_real_column_tag(tag))
    })
}

fn is_real_column_tag(tag: Tag) -> bool {
    !matches!(
        tag,
        tags::MODALITIES_IN_STUDY
            | tags::NUMBER_OF_STUDY_RELATED_SERIES
            | tags::NUMBER_OF_STUDY_RELATED_INSTANCES
            | tags::NUMBER_OF_SERIES_RELATED_INSTANCES
    )
}

/// Builds the SELECT list from only the columns observable by the projection,
/// sort order, and stable ordering tiebreakers.
fn select_list(selection: &QuerySelection) -> String {
    let mut cols: Vec<String> = selection.mappings.iter().map(|m| select_expr(m)).collect();
    if selection.include_attributes {
        cols.push("i.attributes".to_string());
    }
    cols.extend(selection.sort_scalars.iter().map(|sort| {
        format!(
            "{} AS {}",
            jsonb_scalar_value_sql("i.attributes", sort.tag),
            sort.alias
        )
    }));
    // Aliased so all three can be SELECTed without name collisions.
    // These are infrastructure columns (not DICOM attributes) so they are not
    // in ATTRIBUTE_MAPPINGS and are never included in QueryMatch output.
    cols.push("s.synced_at_unix_ms  AS s_synced_at".to_string());
    cols.push("se.synced_at_unix_ms AS se_synced_at".to_string());
    cols.push("i.synced_at_unix_ms  AS i_synced_at".to_string());
    cols.join(", ")
}

/// Builds the SQL expression for one indexed DICOM attribute.
///
/// Most indexed attributes are stored directly on the normalized tables. The
/// DICOM relationship count attributes are derived from child rows so they stay
/// correct even though the sync pipeline does not persist denormalized counts.
fn select_expr(mapping: &AttributeMapping) -> String {
    match mapping.tag {
        tags::NUMBER_OF_STUDY_RELATED_SERIES | tags::NUMBER_OF_STUDY_RELATED_INSTANCES => {
            format!("sc.{0} AS {0}", mapping.column)
        }
        tags::MODALITIES_IN_STUDY => format!("sm.{0} AS {0}", mapping.column),
        tags::NUMBER_OF_SERIES_RELATED_INSTANCES => format!("sec.{0} AS {0}", mapping.column),
        _ => format!("{}.{}", mapping.table.alias(), mapping.column),
    }
}

fn build_sql(
    scope: QueryScope,
    where_sql: Option<&str>,
    sort_keys: &[SortKey],
    registry: &AttributeRegistry,
    selection: &QuerySelection,
    binds: &mut Vec<BindValue>,
    paging: Option<QueryPaging>,
) -> String {
    let select = select_list(selection);
    let where_clause = where_sql.map(|w| format!(" WHERE {w}")).unwrap_or_default();
    let has_paging = paging.is_some();

    let (partition_col, default_order) = dedup_info_for_scope(scope);

    // When the caller supplies sort keys, honour them first; the default
    // (newest-synced-first + unique key) acts as a stable tiebreaker.
    let order_col = if sort_keys.is_empty() {
        default_order
    } else {
        let user_order = compile_sort_keys(sort_keys, registry, selection);
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

    let mut ctes = Vec::new();
    let needs_study_scope = selection.needs_study_counts() || selection.needs_study_modalities();
    if needs_study_scope {
        ctes.push(format!(
            "_study_scope AS (\
                SELECT DISTINCT s.study_instance_uid \
                FROM instances i \
                JOIN series se ON se.series_instance_uid = i.series_instance_uid \
                JOIN studies s ON s.study_instance_uid = se.study_instance_uid \
                {where_clause}\
            )"
        ));
    }
    if selection.needs_series_counts() {
        ctes.push(format!(
            "_series_scope AS (\
                SELECT DISTINCT se.series_instance_uid \
                FROM instances i \
                JOIN series se ON se.series_instance_uid = i.series_instance_uid \
                JOIN studies s ON s.study_instance_uid = se.study_instance_uid \
                {where_clause}\
            )"
        ));
    }
    if selection.needs_study_counts() {
        ctes.push(
            "_study_counts AS (\
                SELECT ss.study_instance_uid, \
                       COUNT(DISTINCT se_count.series_instance_uid) AS number_of_study_related_series, \
                       COUNT(DISTINCT i_count.sop_instance_uid) AS number_of_study_related_instances \
                FROM _study_scope ss \
                LEFT JOIN series se_count ON se_count.study_instance_uid = ss.study_instance_uid \
                LEFT JOIN instances i_count ON i_count.study_instance_uid = ss.study_instance_uid \
                GROUP BY ss.study_instance_uid\
            )"
            .to_string(),
        );
    }
    if selection.needs_series_counts() {
        ctes.push(
            "_series_counts AS (\
                SELECT ss.series_instance_uid, \
                       COUNT(i_count.sop_instance_uid) AS number_of_series_related_instances \
                FROM _series_scope ss \
                LEFT JOIN instances i_count ON i_count.series_instance_uid = ss.series_instance_uid \
                GROUP BY ss.series_instance_uid\
            )"
            .to_string(),
        );
    }
    if selection.needs_study_modalities() {
        ctes.push(
            "_study_modalities AS (\
                SELECT ss.study_instance_uid, \
                       string_agg(DISTINCT se_mod.modality, E'\\\\' ORDER BY se_mod.modality) AS modalities_in_study \
                FROM _study_scope ss \
                JOIN series se_mod ON se_mod.study_instance_uid = ss.study_instance_uid \
                WHERE se_mod.modality IS NOT NULL AND se_mod.modality <> '' \
                GROUP BY ss.study_instance_uid\
            )"
            .to_string(),
        );
    }

    let mut derived_joins = Vec::new();
    if selection.needs_study_counts() {
        derived_joins
            .push("LEFT JOIN _study_counts sc ON sc.study_instance_uid = s.study_instance_uid");
    }
    if selection.needs_series_counts() {
        derived_joins.push(
            "LEFT JOIN _series_counts sec ON sec.series_instance_uid = se.series_instance_uid",
        );
    }
    if selection.needs_study_modalities() {
        derived_joins
            .push("LEFT JOIN _study_modalities sm ON sm.study_instance_uid = s.study_instance_uid");
    }
    let derived_joins = if derived_joins.is_empty() {
        String::new()
    } else {
        format!(" {}", derived_joins.join(" "))
    };

    ctes.push(format!(
        "_base AS (SELECT {select}{dedup_expr} \
         FROM instances i \
         JOIN series se ON se.series_instance_uid = i.series_instance_uid \
         JOIN studies s ON s.study_instance_uid = se.study_instance_uid \
         {derived_joins} \
         {where_clause})"
    ));

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
    let (limit_placeholder, offset_placeholder) = if has_paging {
        let p = paging.expect("has_paging implies paging is Some");
        (
            push_int_bind(p.limit() as i64, binds),
            push_int_bind(p.offset() as i64, binds),
        )
    } else {
        (String::new(), String::new())
    };

    let final_select = if has_paging {
        format!(
            "SELECT *, COUNT(*) OVER () AS _total FROM {from_cte} ORDER BY {order_col} LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        )
    } else {
        format!("SELECT * FROM {from_cte} ORDER BY {order_col}")
    };

    format!("WITH {}{deduped_cte} {final_select}", ctes.join(", "))
}

fn build_series_table_sql(
    where_sql: Option<&str>,
    sort_keys: &[SortKey],
    registry: &AttributeRegistry,
    selection: &QuerySelection,
    has_paging: bool,
    binds: &mut Vec<BindValue>,
    paging: Option<QueryPaging>,
) -> String {
    let mut cols: Vec<String> = selection
        .mappings
        .iter()
        .map(|mapping| series_table_select_expr(mapping))
        .collect();
    cols.push("s.synced_at_unix_ms  AS s_synced_at".to_string());
    cols.push("se.synced_at_unix_ms AS se_synced_at".to_string());
    cols.push("NULL AS i_synced_at".to_string());
    let select = cols.join(", ");
    let where_clause = where_sql.map(|w| format!(" WHERE {w}")).unwrap_or_default();

    let default_order = "se_synced_at DESC, series_instance_uid";
    let order_col = if sort_keys.is_empty() {
        default_order.to_string()
    } else {
        let user_order = compile_sort_keys(sort_keys, registry, selection);
        format!("{user_order}, {default_order}")
    };

    let from = format!(
        "FROM series se \
         JOIN studies s ON s.study_instance_uid = se.study_instance_uid \
         {where_clause}"
    );
    let (limit_placeholder, offset_placeholder) = if has_paging {
        let p = paging.expect("has_paging implies paging is Some");
        (
            push_int_bind(p.limit() as i64, binds),
            push_int_bind(p.offset() as i64, binds),
        )
    } else {
        (String::new(), String::new())
    };

    if has_paging {
        format!(
            "SELECT *, COUNT(*) OVER () AS _total FROM (SELECT {select} {from}) ORDER BY {order_col} LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        )
    } else {
        format!("SELECT {select} {from} ORDER BY {order_col}")
    }
}

fn build_image_table_sql(
    where_sql: Option<&str>,
    sort_keys: &[SortKey],
    registry: &AttributeRegistry,
    selection: &QuerySelection,
    has_paging: bool,
    binds: &mut Vec<BindValue>,
    paging: Option<QueryPaging>,
) -> String {
    let select = direct_join_select_list(selection);
    let where_clause = where_sql.map(|w| format!(" WHERE {w}")).unwrap_or_default();
    let default_order = "i_synced_at DESC, sop_instance_uid";
    let order_col = if sort_keys.is_empty() {
        default_order.to_string()
    } else {
        let user_order = compile_sort_keys(sort_keys, registry, selection);
        format!("{user_order}, {default_order}")
    };
    let from = format!(
        "FROM instances i \
         JOIN series se ON se.series_instance_uid = i.series_instance_uid \
         JOIN studies s ON s.study_instance_uid = se.study_instance_uid \
         {where_clause}"
    );
    let (limit_placeholder, offset_placeholder) = if has_paging {
        let p = paging.expect("has_paging implies paging is Some");
        (
            push_int_bind(p.limit() as i64, binds),
            push_int_bind(p.offset() as i64, binds),
        )
    } else {
        (String::new(), String::new())
    };

    if has_paging {
        format!(
            "SELECT *, COUNT(*) OVER () AS _total FROM (SELECT {select} {from}) ORDER BY {order_col} LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        )
    } else {
        format!("SELECT {select} {from} ORDER BY {order_col}")
    }
}

fn direct_join_select_list(selection: &QuerySelection) -> String {
    let mut cols: Vec<String> = selection
        .mappings
        .iter()
        .map(|mapping| direct_join_select_expr(mapping))
        .collect();
    if selection.include_attributes {
        cols.push("i.attributes".to_string());
    }
    cols.extend(selection.sort_scalars.iter().map(|sort| {
        format!(
            "{} AS {}",
            jsonb_scalar_value_sql("i.attributes", sort.tag),
            sort.alias
        )
    }));
    cols.push("s.synced_at_unix_ms  AS s_synced_at".to_string());
    cols.push("se.synced_at_unix_ms AS se_synced_at".to_string());
    cols.push("i.synced_at_unix_ms  AS i_synced_at".to_string());
    cols.join(", ")
}

fn direct_join_select_expr(mapping: &AttributeMapping) -> String {
    match mapping.tag {
        tags::NUMBER_OF_STUDY_RELATED_SERIES => "(SELECT COUNT(*) \
              FROM series se_count \
              WHERE se_count.study_instance_uid = s.study_instance_uid) \
             AS number_of_study_related_series"
            .to_string(),
        tags::NUMBER_OF_STUDY_RELATED_INSTANCES => "(SELECT COUNT(*) \
              FROM instances i_count \
              WHERE i_count.study_instance_uid = s.study_instance_uid) \
             AS number_of_study_related_instances"
            .to_string(),
        tags::MODALITIES_IN_STUDY => "(SELECT string_agg(modality, E'\\\\' ORDER BY modality) \
              FROM (SELECT DISTINCT modality \
                    FROM series sm_count \
                    WHERE sm_count.study_instance_uid = s.study_instance_uid \
                      AND modality IS NOT NULL AND modality <> '') AS modalities) \
             AS modalities_in_study"
            .to_string(),
        tags::NUMBER_OF_SERIES_RELATED_INSTANCES => "(SELECT COUNT(*) \
              FROM instances i_count \
              WHERE i_count.series_instance_uid = se.series_instance_uid) \
             AS number_of_series_related_instances"
            .to_string(),
        _ => format!("{}.{}", mapping.table.alias(), mapping.column),
    }
}

fn series_table_select_expr(mapping: &AttributeMapping) -> String {
    match mapping.tag {
        tags::NUMBER_OF_STUDY_RELATED_SERIES => "(SELECT COUNT(*) \
              FROM series se_count \
              WHERE se_count.study_instance_uid = s.study_instance_uid) \
             AS number_of_study_related_series"
            .to_string(),
        tags::NUMBER_OF_STUDY_RELATED_INSTANCES => "(SELECT COUNT(*) \
              FROM instances i_count \
              WHERE i_count.study_instance_uid = s.study_instance_uid) \
             AS number_of_study_related_instances"
            .to_string(),
        tags::MODALITIES_IN_STUDY => "(SELECT string_agg(modality, E'\\\\' ORDER BY modality) \
              FROM (SELECT DISTINCT modality \
                    FROM series sm_count \
                    WHERE sm_count.study_instance_uid = s.study_instance_uid \
                      AND modality IS NOT NULL AND modality <> '') AS modalities) \
             AS modalities_in_study"
            .to_string(),
        tags::NUMBER_OF_SERIES_RELATED_INSTANCES => "(SELECT COUNT(*) \
              FROM instances i_count \
              WHERE i_count.series_instance_uid = se.series_instance_uid) \
             AS number_of_series_related_instances"
            .to_string(),
        _ => format!("{}.{}", mapping.table.alias(), mapping.column),
    }
}

/// Compiles a list of [`SortKey`]s into a comma-separated SQL ORDER BY fragment.
///
/// Indexed attributes use their bare column name (table aliases are not in
/// scope outside the base CTE).  Non-indexed attributes use JSONB extraction
/// on the `attributes` blob column.
fn compile_sort_keys(
    sort_keys: &[SortKey],
    registry: &AttributeRegistry,
    selection: &QuerySelection,
) -> String {
    sort_keys
        .iter()
        .enumerate()
        .filter_map(|(index, key)| {
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
                selection
                    .sort_scalars
                    .iter()
                    .find(|sort| sort.tag == tag && sort.alias == format!("_sort_{index}"))
                    .map(|sort| sort.alias.clone())
                    .unwrap_or_else(|| jsonb_scalar_value_sql("attributes", tag))
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

fn order_key_tags_for_scope(scope: QueryScope) -> Vec<Tag> {
    match scope {
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient) => {
            vec![tags::PATIENT_ID, tags::STUDY_INSTANCE_UID]
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study) => {
            vec![tags::STUDY_INSTANCE_UID]
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Series)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series) => {
            vec![tags::SERIES_INSTANCE_UID]
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image) => {
            vec![tags::SOP_INSTANCE_UID]
        }
    }
}

fn mandatory_tags_for_scope(scope: QueryScope) -> Vec<Tag> {
    let patient = || {
        vec![
            tags::PATIENT_ID,
            tags::PATIENT_NAME,
            tags::PATIENT_BIRTH_DATE,
            tags::PATIENT_SEX,
        ]
    };
    let study = || {
        vec![
            tags::STUDY_INSTANCE_UID,
            tags::STUDY_DATE,
            tags::STUDY_TIME,
            tags::ACCESSION_NUMBER,
            tags::MODALITIES_IN_STUDY,
            tags::REFERRING_PHYSICIAN_NAME,
            tags::STUDY_ID,
            tags::STUDY_DESCRIPTION,
            tags::NUMBER_OF_STUDY_RELATED_SERIES,
            tags::NUMBER_OF_STUDY_RELATED_INSTANCES,
        ]
    };
    let series = || {
        vec![
            tags::SERIES_INSTANCE_UID,
            tags::MODALITY,
            tags::SERIES_NUMBER,
            tags::NUMBER_OF_SERIES_RELATED_INSTANCES,
        ]
    };
    let image = || {
        vec![
            tags::SOP_INSTANCE_UID,
            tags::SOP_CLASS_UID,
            tags::INSTANCE_NUMBER,
            tags::TRANSFER_SYNTAX_UID,
        ]
    };

    match scope {
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient) => patient(),
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study) => {
            [patient(), study()].concat()
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Series)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series) => {
            [patient(), study(), series()].concat()
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image)
        | QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image) => {
            [patient(), study(), series(), image()].concat()
        }
    }
}

/// Tracks the JSON source expression for blob-attribute access.
///
/// In the root context, mapped indexed columns take precedence over the blob.
/// Inside a `jsonb_array_elements` sequence loop, only the loop's `value` JSON
/// is available.
struct CompileContext {
    /// Expression for the DICOM JSON blob document.
    /// Root: `i.attributes`. Inside a sequence: `_seqN.value`.
    blob_expr: String,
    /// Whether mapped (indexed) columns can be used for this context level.
    /// False inside sequence item loops.
    use_mapped_columns: bool,
    /// Whether fuzzy semantic matching is active (PS3.4 C.2.2.2.3).
    /// When true, PN attribute predicates use soundex-based phonetic matching.
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

    /// Creates a nested context for use inside a sequence item loop.
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
) -> Result<String, PostgresReadRepositoryError> {
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
            if !ctx.fuzzy_matching
                && ctx.use_mapped_columns
                && registry.get(tag).is_none()
                && let Some(sql) = compile_jsonb_containment_rule(tag, rule, &ctx.blob_expr, binds)?
            {
                return Ok(sql);
            }
            let value_sql = scalar_value_sql(tag, ctx, registry);
            if ctx.fuzzy_matching && is_pn_tag(tag) {
                compile_fuzzy_pn_rule(&value_sql, rule, binds)
            } else {
                compile_matching_rule(&value_sql, rule, ctx.tz_offset_minutes, binds)
            }
        }
    }
}

fn compile_jsonb_containment_rule(
    tag: Tag,
    rule: &MatchingRule,
    blob_expr: &str,
    binds: &mut Vec<BindValue>,
) -> Result<Option<String>, PostgresReadRepositoryError> {
    match rule {
        MatchingRule::SingleValue(value) => {
            let placeholder = push_jsonb_value_match(tag, value, binds)?;
            Ok(Some(format!("{blob_expr} @> {placeholder}::jsonb")))
        }
        MatchingRule::UidList(values) | MatchingRule::MultipleValues(values) => {
            if values.is_empty() {
                return Ok(Some("FALSE".to_string()));
            }
            let clauses = values
                .iter()
                .map(|value| {
                    push_jsonb_value_match(tag, value, binds)
                        .map(|placeholder| format!("{blob_expr} @> {placeholder}::jsonb"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(format!("({})", clauses.join(" OR "))))
        }
        _ => Ok(None),
    }
}

fn push_jsonb_value_match(
    tag: Tag,
    value: &str,
    binds: &mut Vec<BindValue>,
) -> Result<String, PostgresReadRepositoryError> {
    let body = if is_pn_tag(tag) {
        serde_json::json!({
            tag_key(tag): {
                "Value": [{
                    "Alphabetic": value,
                }],
            },
        })
    } else {
        serde_json::json!({
            tag_key(tag): {
                "Value": [value],
            },
        })
    };
    serde_json::to_string(&body)
        .map(|json| push_text_bind(json, binds))
        .map_err(|err| PostgresReadRepositoryError::InternalError(err.to_string()))
}

fn compile_sequence_predicate(
    sq_tag: Tag,
    inner_predicate: &Predicate,
    ctx: &CompileContext,
    registry: &AttributeRegistry,
    binds: &mut Vec<BindValue>,
    seq_counter: &mut usize,
) -> Result<String, PostgresReadRepositoryError> {
    let alias = format!("_seq{}", *seq_counter);
    *seq_counter += 1;

    // In any context, SQ attributes live in the JSON blob (never in indexed columns).
    let array_sql = jsonb_array_value_sql(&ctx.blob_expr, sq_tag);

    let nested_ctx = ctx.nested(&alias);
    let inner_sql = compile_predicate(inner_predicate, &nested_ctx, registry, binds, seq_counter)?;

    Ok(format!(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE({array_sql}, '[]'::jsonb)) AS {alias}(value) WHERE {inner_sql})"
    ))
}

fn compile_matching_rule(
    value_sql: &str,
    rule: &MatchingRule,
    tz_offset_minutes: Option<i32>,
    binds: &mut Vec<BindValue>,
) -> Result<String, PostgresReadRepositoryError> {
    match rule {
        MatchingRule::Universal => Ok("TRUE".to_string()),

        MatchingRule::EmptyValue => Ok(format!("({value_sql} IS NULL OR {value_sql} = '')")),

        MatchingRule::SingleValue(v) => {
            let placeholder = push_text_bind(v.clone(), binds);
            Ok(format!("{value_sql} = {placeholder}"))
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
            let placeholder = push_text_bind(like_pattern, binds);
            Ok(format!("{value_sql} LIKE {placeholder} ESCAPE '\\'"))
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
            let placeholders: Vec<String> = uids
                .iter()
                .map(|uid| push_text_bind(uid.clone(), binds))
                .collect();
            Ok(format!("{value_sql} IN ({})", placeholders.join(", ")))
        }

        MatchingRule::Sequence(_) => {
            // Handled one level up in compile_predicate; should never reach here.
            Err(PostgresReadRepositoryError::InternalError(
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
) -> Result<String, PostgresReadRepositoryError> {
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
        let placeholder = push_text_bind(adjusted, binds);
        clauses.push(format!("{value_sql} >= {placeholder}"));
    }
    if let Some(e) = end {
        let adjusted = if is_datetime {
            tz_offset_minutes
                .and_then(|off| shift_dt_to_utc(e, off))
                .unwrap_or_else(|| e.to_string())
        } else {
            e.to_string()
        };
        let placeholder = push_text_bind(adjusted, binds);
        clauses.push(format!("{value_sql} <= {placeholder}"));
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

/// Compiles a matching rule using soundex-based phonetic approximation.
///
/// Called only when `fuzzy_matching` is active **and** the attribute's VR is
/// PN (PS3.4 C.2.2.2.3).  The `fuzzystrmatch` extension's `soundex()` provides
/// English-language phonetic encoding.
///
/// Known limitations:
/// - Only the Alphabetic PN component is matched; Ideographic and Phonetic
///   components are not consulted.
/// - soundex is English-only and does not implement the full DICOM fuzzy
///   semantic matching specification.
/// - Wildcard patterns have their wildcard characters stripped before phonetic
///   encoding.  A pattern composed entirely of wildcards degenerates to
///   `TRUE` (universal match).
fn compile_fuzzy_pn_rule(
    value_sql: &str,
    rule: &MatchingRule,
    binds: &mut Vec<BindValue>,
) -> Result<String, PostgresReadRepositoryError> {
    match rule {
        MatchingRule::Universal => Ok("TRUE".to_string()),
        MatchingRule::EmptyValue => Ok(format!("({value_sql} IS NULL OR {value_sql} = '')")),
        MatchingRule::SingleValue(v) => {
            let placeholder = push_text_bind(v.clone(), binds);
            Ok(format!("soundex({value_sql}) = soundex({placeholder})"))
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
            let placeholder = push_text_bind(literal, binds);
            Ok(format!("soundex({value_sql}) = soundex({placeholder})"))
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
/// attributes are read from the JSONB blob.
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
    jsonb_scalar_value_sql(&ctx.blob_expr, tag)
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

fn jsonb_scalar_value_sql(blob_expr: &str, tag: Tag) -> String {
    let tokens = blob_scalar_json_path_tokens(tag);
    let quoted = tokens
        .iter()
        .map(|token| format!("'{}'", token.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_extract_path_text({blob_expr}, {quoted})")
}

fn jsonb_array_value_sql(blob_expr: &str, tag: Tag) -> String {
    format!(
        "jsonb_extract_path({blob_expr}, '{}', 'Value')",
        tag_key(tag).replace('\'', "''")
    )
}

fn blob_scalar_json_path_tokens(tag: Tag) -> Vec<String> {
    if is_pn_tag(tag) {
        vec![
            tag_key(tag),
            "Value".to_string(),
            "0".to_string(),
            "Alphabetic".to_string(),
        ]
    } else {
        vec![tag_key(tag), "Value".to_string(), "0".to_string()]
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
fn single_tag(path: &AttributePath) -> Result<Tag, PostgresReadRepositoryError> {
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
            PostgresReadRepositoryError::InternalError(format!(
                "predicate path contains no tag segment: {path}"
            ))
        })
}

fn tag_of_path(path: &AttributePath) -> Option<Tag> {
    path.segments().iter().find_map(|s| {
        if let AttributePathSegment::Tag(t) = s {
            Some(*t)
        } else {
            None
        }
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

    fn series_scope() -> QueryScope {
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)
    }

    fn image_scope() -> QueryScope {
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image)
    }

    fn compile_ok(query: &DicomQuery, registry: &AttributeRegistry) -> CompiledQuery {
        compile(query, registry).expect("compile should succeed")
    }

    #[test]
    fn no_predicate_compiles_to_no_where_clause() {
        let query = DicomQuery::new(study_scope(), Projection::Default).expect("valid query");
        let compiled = compile_ok(&query, &registry());

        // Without a predicate the joined base query must not receive a user
        // WHERE clause. Derived fields may still use internal WHERE clauses in
        // their own CTEs.
        assert!(
            !compiled
                .sql
                .contains("JOIN studies s ON s.study_instance_uid = se.study_instance_uid WHERE")
        );
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

        assert!(compiled.sql.contains("CAST(s.patient_id AS TEXT) = $1"));
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
        assert!(
            compiled
                .sql
                .contains("CAST(s.patient_name AS TEXT) LIKE $1")
        );
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

        assert!(compiled.sql.contains("IN ($1, $2)"));
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

        assert!(compiled.sql.contains(">= $1") && compiled.sql.contains("<= $2"));
        assert_eq!(compiled.binds.len(), 2);
    }

    #[test]
    fn non_indexed_single_value_uses_jsonb_containment() {
        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                // MANUFACTURER (0008,0070) is not a mapped column
                AttributePath::from_tag(tags::MANUFACTURER),
                MatchingRule::SingleValue("ACME".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("i.attributes @> $1::jsonb"));
        assert!(
            matches!(&compiled.binds[0], BindValue::Text(v) if v.contains("00080070") && v.contains("ACME"))
        );
    }

    #[test]
    fn indexed_fields_projection_omits_attributes_blob() {
        let query = DicomQuery::new(
            study_scope(),
            Projection::Fields(vec![
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                AttributePath::from_tag(tags::PATIENT_ID),
            ]),
        )
        .expect("valid query");
        let compiled = compile_ok(&query, &registry());

        assert!(!compiled.includes_attributes);
        assert!(!compiled.sql.contains("i.attributes"));
        assert!(compiled.sql.contains("s.study_instance_uid"));
        assert!(compiled.sql.contains("s.patient_id"));
    }

    #[test]
    fn non_indexed_fields_projection_selects_attributes_blob() {
        let query = DicomQuery::new(
            study_scope(),
            Projection::Fields(vec![AttributePath::from_tag(tags::MANUFACTURER)]),
        )
        .expect("valid query");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.includes_attributes);
        assert!(compiled.sql.contains("i.attributes"));
    }

    #[test]
    fn series_scope_uses_series_table_scan_without_window_dedup() {
        let query = DicomQuery::new(series_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::All(vec![
                Predicate::Attribute(
                    AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                    MatchingRule::SingleValue("1.2.3".to_string()),
                ),
                Predicate::Attribute(
                    AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
                    MatchingRule::SingleValue("1.2.3.4".to_string()),
                ),
            ]))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("FROM series se"));
        assert!(!compiled.sql.contains("ROW_NUMBER()"));
        assert!(!compiled.sql.contains("FROM instances i JOIN"));
        assert!(
            compiled
                .sql
                .contains("CAST(s.study_instance_uid AS TEXT) = $1")
        );
        assert!(
            compiled
                .sql
                .contains("CAST(se.series_instance_uid AS TEXT) = $2")
        );
    }

    #[test]
    fn series_scope_with_blob_dependency_keeps_generic_join() {
        let query = DicomQuery::new(series_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::Attribute(
                AttributePath::from_tag(tags::MANUFACTURER),
                MatchingRule::SingleValue("ACME".to_string()),
            ))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("FROM instances i"));
        assert!(compiled.sql.contains("ROW_NUMBER()"));
        assert!(compiled.sql.contains("i.attributes @> $1::jsonb"));
    }

    #[test]
    fn image_scope_uses_direct_instance_table_scan() {
        let query = DicomQuery::new(image_scope(), Projection::Default)
            .expect("valid query")
            .with_predicate(Predicate::All(vec![
                Predicate::Attribute(
                    AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                    MatchingRule::SingleValue("1.2.3".to_string()),
                ),
                Predicate::Attribute(
                    AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
                    MatchingRule::SingleValue("1.2.3.4".to_string()),
                ),
                Predicate::Attribute(
                    AttributePath::from_tag(tags::SOP_INSTANCE_UID),
                    MatchingRule::SingleValue("1.2.3.4.5".to_string()),
                ),
            ]))
            .expect("valid predicate");
        let compiled = compile_ok(&query, &registry());

        assert!(compiled.sql.contains("FROM instances i"));
        assert!(compiled.sql.contains("JOIN series se"));
        assert!(compiled.sql.contains("JOIN studies s"));
        assert!(!compiled.sql.contains("_base AS"));
        assert!(!compiled.sql.contains("ROW_NUMBER()"));
        assert!(
            compiled
                .sql
                .contains("CAST(i.sop_instance_uid AS TEXT) = $3")
        );
    }

    #[test]
    fn sequence_predicate_compiles_to_exists_with_jsonb_array_elements() {
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
        assert!(compiled.sql.contains("jsonb_array_elements"));
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
        assert!(compiled.sql.contains("LIMIT $1"));
        assert!(compiled.sql.contains("OFFSET $2"));
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

        // Must use the Alphabetic sub-path, not the raw Value[0] JSON object.
        assert!(compiled.sql.contains("'Alphabetic'"));
        assert!(compiled.sql.contains("'Value', '0'"));
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

        assert!(compiled.sql.contains("i.attributes @> $1::jsonb"));
        assert!(
            matches!(&compiled.binds[0], BindValue::Text(v) if v.contains("\"Value\":[\"ACME\"]"))
        );
        assert!(!compiled.sql.contains("'Alphabetic'"));
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
        assert!(compiled.sql.contains(">= $1"));
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
    fn sort_key_on_non_indexed_attribute_uses_hidden_json_scalar() {
        use raccoon_service_query::{SortDirection, SortKey};

        let query = DicomQuery::new(study_scope(), Projection::Default)
            .expect("valid query")
            .with_sort_keys(vec![SortKey {
                path: AttributePath::from_tag(tags::MANUFACTURER),
                direction: SortDirection::Ascending,
            }])
            .expect("valid sort keys");
        let compiled = compile_ok(&query, &registry());

        assert!(!compiled.includes_attributes);
        assert!(
            compiled
                .sql
                .contains("jsonb_extract_path_text(i.attributes")
        );
        assert!(compiled.sql.contains("AS _sort_0"));
        assert!(compiled.sql.contains("_sort_0 ASC"));
        assert!(compiled.sql.contains("00080070")); // MANUFACTURER tag key
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

        assert!(compiled.sql.contains("soundex"));
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

        assert!(!compiled.sql.contains("soundex"));
        assert!(compiled.sql.contains("= $1"));
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
