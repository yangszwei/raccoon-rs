//! Materializes SQL result rows into [`QueryMatch`] values.
//!
//! Every query always SELECTs all indexed columns plus the `attributes` blob.
//! Projection (`Fields`, `All`, `Default`) is applied here on the Rust side
//! rather than in SQL, keeping the compiled statement shape uniform.
//!
//! # Attribute precedence
//!
//! Indexed columns always take precedence over the `attributes` blob.  When a
//! tag has a dedicated column its value comes from that column; the blob is only
//! consulted for tags that are not indexed.  This prevents double-reporting and
//! ensures that any normalization applied at sync time (e.g. trimming, type
//! conversion) is preserved in query responses.
//!
//! # `ResponseValue` semantics
//!
//! | Situation | Value |
//! |-----------|-------|
//! | Indexed column → `NULL` | `ZeroLength` (attribute is supported but absent for this entity) |
//! | Blob attribute absent from JSON | `Absent` (SCP does not know this attribute) |
//! | Blob attribute present, empty value | `ZeroLength` |
//! | Any attribute present with value | `Present(…)` |

use std::collections::HashMap;

use dicom_core::{Tag, VR, header::Header};
use dicom_object::InMemDicomObject;
use raccoon_service_query::{
    AttributePath, AttributeValue, PatientRootQueryRetrieveLevel, ProjectedAttribute, Projection,
    QueryMatch, QueryPage, QueryScope, ResponseValue, StudyRootQueryRetrieveLevel,
};
use sqlx::Row;

use crate::compile::CompiledQuery;
use crate::error::SqliteReadRepositoryError;
use crate::schema::{ATTRIBUTE_MAPPINGS, AttributeRegistry, VrClass};

/// Maximum byte size for a binary VR attribute to be inlined into a
/// [`QueryMatch`].  Binary attributes larger than this are returned as
/// [`ResponseValue::Absent`] to prevent runaway blobs (pixel data, overlays,
/// LUT tables) from consuming memory during row materialisation.
///
/// The sync process is expected to strip such attributes before writing the
/// `attributes` blob; this constant acts as a defence-in-depth guard.
const MAX_INLINE_BINARY_BYTES: usize = 64 * 1024; // 64 KiB

/// Converts a slice of raw SQLite rows into a [`QueryPage`].
pub(crate) fn materialize_page(
    rows: Vec<sqlx::sqlite::SqliteRow>,
    compiled: &CompiledQuery,
    projection: &Projection,
    scope: QueryScope,
    registry: &AttributeRegistry,
) -> Result<QueryPage, SqliteReadRepositoryError> {
    // Determine total count from first row (if paging was requested).
    let total: Option<u64> = if compiled.has_total_count {
        rows.first()
            .and_then(|row| row.try_get::<Option<i64>, _>("_total").ok().flatten())
            .map(|n| n as u64)
            .or(Some(0)) // no rows → total is 0
    } else {
        None
    };

    let items = rows
        .iter()
        .map(|row| materialize_row(row, projection, scope, registry))
        .collect::<Result<Vec<_>, _>>()?;

    let (offset, limit) = compiled
        .paging
        .map(|p| (p.offset(), p.limit()))
        .unwrap_or((0, items.len() as u64));

    Ok(QueryPage::new(items, offset, limit.max(1), total))
}

fn materialize_row(
    row: &sqlx::sqlite::SqliteRow,
    projection: &Projection,
    scope: QueryScope,
    registry: &AttributeRegistry,
) -> Result<QueryMatch, SqliteReadRepositoryError> {
    // 1. Read all indexed columns → tag → ResponseValue.
    //    Indexed attributes always use ZeroLength (not Absent) when NULL:
    //    we know the SCP supports these attributes.
    let mut tag_map: HashMap<Tag, ResponseValue> = HashMap::new();
    for mapping in ATTRIBUTE_MAPPINGS {
        let value = read_indexed_value(row, mapping)?;
        tag_map.insert(mapping.tag, value);
    }

    // 2. Parse the attributes blob and add any tags not already covered by
    //    indexed columns.  Blob tags use Absent (not ZeroLength) when missing
    //    from the JSON, because the SCP may not know those attributes.
    let needs_blob = match projection {
        Projection::All => true,
        Projection::Default => false, // all mandatory tags are indexed
        Projection::Fields(paths) => paths
            .iter()
            .any(|p| tag_of_path(p).is_some_and(|t| registry.get(t).is_none())),
    };

    if needs_blob {
        let blob_str: Option<String> = row.try_get("attributes").ok().flatten();
        if let Some(json) = blob_str.as_deref().filter(|s| !s.is_empty() && *s != "{}")
            && let Ok(obj) = dicom_json::from_str::<InMemDicomObject>(json)
        {
            for element in obj.iter() {
                let tag = element.tag();
                tag_map
                    .entry(tag)
                    .or_insert_with(|| element_to_response_value(element));
            }
        }
    }

    // 3. Apply projection: select which attributes to include in the result.
    let attrs = apply_projection(tag_map, projection, scope, registry);

    QueryMatch::new(attrs).map_err(|e| SqliteReadRepositoryError::InternalError(e.to_string()))
}

fn apply_projection(
    mut tag_map: HashMap<Tag, ResponseValue>,
    projection: &Projection,
    scope: QueryScope,
    registry: &AttributeRegistry,
) -> Vec<ProjectedAttribute> {
    match projection {
        Projection::All => tag_map
            .into_iter()
            .map(|(tag, value)| ProjectedAttribute {
                path: AttributePath::from_tag(tag),
                value,
            })
            .collect(),

        Projection::Default => mandatory_tags_for_scope(scope)
            .into_iter()
            .map(|tag| ProjectedAttribute {
                path: AttributePath::from_tag(tag),
                value: tag_map.remove(&tag).unwrap_or(ResponseValue::ZeroLength),
            })
            .collect(),

        Projection::Fields(paths) => paths
            .iter()
            .filter_map(|path| {
                let tag = tag_of_path(path)?;
                let value = if registry.get(tag).is_some() {
                    // Indexed column: supported attribute → ZeroLength if absent
                    tag_map.remove(&tag).unwrap_or(ResponseValue::ZeroLength)
                } else {
                    // Blob attribute: unknown if absent → Absent
                    tag_map.remove(&tag).unwrap_or(ResponseValue::Absent)
                };
                Some(ProjectedAttribute {
                    path: path.clone(),
                    value,
                })
            })
            .collect(),
    }
}

fn read_indexed_value(
    row: &sqlx::sqlite::SqliteRow,
    mapping: &crate::schema::AttributeMapping,
) -> Result<ResponseValue, SqliteReadRepositoryError> {
    let col = mapping.column;
    match mapping.vr_class {
        VrClass::Text => {
            let v: Option<String> = row
                .try_get(col)
                .map_err(|e| SqliteReadRepositoryError::RowRead(col.to_string(), e))?;
            Ok(match v.as_deref() {
                None | Some("") => ResponseValue::ZeroLength,
                Some(s) => ResponseValue::Present(AttributeValue::Text(s.to_string())),
            })
        }
        VrClass::Integer => {
            let v: Option<i64> = row
                .try_get(col)
                .map_err(|e| SqliteReadRepositoryError::RowRead(col.to_string(), e))?;
            Ok(match v {
                None => ResponseValue::ZeroLength,
                Some(n) => ResponseValue::Present(AttributeValue::Text(n.to_string())),
            })
        }
    }
}

fn element_to_response_value(element: &dicom_object::mem::InMemElement) -> ResponseValue {
    match element.vr() {
        VR::SQ => {
            let items = element.items().unwrap_or(&[]);
            if items.is_empty() {
                // A zero-item SQ is a valid empty sequence, not an absent attribute.
                ResponseValue::Present(AttributeValue::Sequence(vec![]))
            } else {
                let seq_items: Vec<QueryMatch> = items
                    .iter()
                    .map(object_to_query_match)
                    .collect::<Result<_, _>>()
                    .unwrap_or_default();
                ResponseValue::Present(AttributeValue::Sequence(seq_items))
            }
        }
        // Binary VRs: OB, OW, OD, OF, OL, OV, UN
        VR::OB | VR::OW | VR::OD | VR::OF | VR::OL | VR::OV | VR::UN => match element.to_bytes() {
            Ok(bytes) if bytes.is_empty() => ResponseValue::ZeroLength,
            Ok(bytes) if bytes.len() > MAX_INLINE_BINARY_BYTES => {
                // The sync process should strip large binary VRs (pixel data,
                // overlays, LUT data) before writing the attributes blob.  This
                // guard prevents a runaway blob from loading megabytes into memory
                // during row materialisation.
                ResponseValue::Absent
            }
            Ok(bytes) => ResponseValue::Present(AttributeValue::Binary(bytes.to_vec())),
            Err(_) => ResponseValue::ZeroLength,
        },
        _ => {
            // All remaining VRs are string-based.
            match element.to_multi_str() {
                Ok(values) => {
                    let trimmed: Vec<String> = values
                        .iter()
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty())
                        .collect();
                    match trimmed.len() {
                        0 => ResponseValue::ZeroLength,
                        1 => ResponseValue::Present(AttributeValue::Text(
                            trimmed.into_iter().next().unwrap(),
                        )),
                        _ => ResponseValue::Present(AttributeValue::Texts(trimmed)),
                    }
                }
                Err(_) => ResponseValue::ZeroLength,
            }
        }
    }
}

/// Converts one DICOM JSON sequence item (an [`InMemDicomObject`]) into a
/// [`QueryMatch`] for nesting inside [`AttributeValue::Sequence`].
fn object_to_query_match(obj: &InMemDicomObject) -> Result<QueryMatch, String> {
    let attrs: Vec<ProjectedAttribute> = obj
        .iter()
        .map(|element| ProjectedAttribute {
            path: AttributePath::from_tag(element.tag()),
            value: element_to_response_value(element),
        })
        .collect();
    QueryMatch::new(attrs).map_err(|e| e.to_string())
}

/// Mandatory return-key tags for [`Projection::Default`] at each scope level.
///
/// Defined per DICOM PS3.4 Table C.6-1 (Patient Root) and C.6-5 (Study Root).
/// The bridge injects protocol-level attributes (e.g. Query/Retrieve Level
/// (0008,0052)) after receiving the [`QueryMatch`]; this list covers only the
/// attributes the repository owns.
fn mandatory_tags_for_scope(scope: QueryScope) -> Vec<Tag> {
    use dicom_dictionary_std::tags;

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

/// Extracts the [`Tag`] from a single-tag [`AttributePath`], returning `None`
/// for paths that contain item selectors (which cannot appear in responses).
fn tag_of_path(path: &AttributePath) -> Option<Tag> {
    use raccoon_service_query::AttributePathSegment;
    path.segments().iter().find_map(|s| {
        if let AttributePathSegment::Tag(t) = s {
            Some(*t)
        } else {
            None
        }
    })
}
