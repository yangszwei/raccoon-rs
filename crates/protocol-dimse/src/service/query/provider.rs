use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_core::header::Header;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{StandardDataDictionary, tags, uids};
use dicom_object::InMemDicomObject;
use dicom_object::mem::InMemElement;
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};
use raccoon_contract_dicom::{PatientRootQueryRetrieveLevel, StudyRootQueryRetrieveLevel};
use raccoon_service_query::{
    AttributePath, AttributeValue, DicomQuery, MatchingRule, Predicate, ProjectedAttribute,
    Projection, QueryError, QueryMatch, QueryScope, RangeMatching, ResponseValue,
};

use super::message::{CFindRequest, CFindResponse, CFindStatus};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::CommandField;
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

/// Query/Retrieve C-FIND SCP provider backed by `QueryService`.
pub struct QueryServiceProvider {
    query: Arc<dyn raccoon_service_query::QueryService>,
    bindings: Vec<ServiceBinding>,
}

impl QueryServiceProvider {
    pub const DEFAULT_FIND_SOP_CLASS_UIDS: &[&str] = &[
        uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
    ];

    pub fn new(
        query: Arc<dyn raccoon_service_query::QueryService>,
        sop_class_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            query,
            bindings: sop_class_uids
                .into_iter()
                .map(|uid| ServiceBinding::owned(CommandField::CFindRq, uid.into()))
                .collect(),
        }
    }

    pub fn with_default_find_sop_classes(
        query: Arc<dyn raccoon_service_query::QueryService>,
    ) -> Self {
        Self::new(query, Self::DEFAULT_FIND_SOP_CLASS_UIDS.iter().copied())
    }
}

#[async_trait]
impl ServiceClassProvider for QueryServiceProvider {
    #[tracing::instrument(skip(self, ctx), fields(command = "C-FIND"))]
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command().await?;
        let request = CFindRequest::from_command(&command)?;
        let identifier = read_identifier(ctx, request.presentation_context_id).await?;
        let prepared = match build_query(&request, &identifier) {
            Ok(query) => query,
            Err(error) => {
                return send_response(
                    ctx,
                    &request,
                    CFindStatus::IdentifierDoesNotMatchSopClass,
                    Some(error.to_string()),
                    None,
                )
                .await;
            }
        };
        tracing::debug!(stage = "validate", "C-FIND request validated");

        let page = match self.query.query(prepared.query.clone()).await {
            Ok(page) => page,
            Err(error) => {
                return send_response(
                    ctx,
                    &request,
                    query_error_status(&error),
                    Some(error.to_string()),
                    None,
                )
                .await;
            }
        };

        for item in page.items {
            let pending = match query_match_to_identifier(item, &prepared) {
                Ok(identifier) => identifier,
                Err(error) => {
                    return send_response(
                        ctx,
                        &request,
                        CFindStatus::UnableToProcess,
                        Some(error.to_string()),
                        None,
                    )
                    .await;
                }
            };
            let status = if pending.unsupported_optional_keys {
                CFindStatus::PendingOptionalKeysNotSupported
            } else {
                CFindStatus::Pending
            };
            send_response(ctx, &request, status, None, Some(&pending.identifier)).await?;
        }

        send_response(ctx, &request, CFindStatus::Success, None, None).await
    }
}

impl DescribedServiceClassProvider for QueryServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
}

async fn send_response(
    ctx: &mut AssociationContext,
    request: &CFindRequest,
    status: CFindStatus,
    comment: Option<String>,
    identifier: Option<&InMemDicomObject>,
) -> Result<(), DimseError> {
    let mut response = CFindResponse::for_request(request, status);
    let response_reason = comment.clone();
    if let Some(comment) = comment {
        response = response.with_error_comment(comment);
    }
    let status_code = response.status.code();
    ctx.send_command_object(
        request.presentation_context_id,
        &response.to_command_object(),
    )
    .await?;
    if let Some(identifier) = identifier {
        ctx.send_data_set_object(request.presentation_context_id, identifier)
            .await?;
    }
    ctx.record_response_status(status_code, response_reason);
    Ok(())
}

async fn read_identifier(
    ctx: &mut AssociationContext,
    presentation_context_id: u8,
) -> Result<InMemDicomObject, DimseError> {
    let transfer_syntax_uid = ctx
        .association()
        .presentation_contexts()
        .iter()
        .find(|pc| pc.id == presentation_context_id)
        .map(|pc| pc.transfer_syntax.as_str())
        .ok_or_else(|| DimseError::protocol("missing presentation context for C-FIND"))?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(transfer_syntax_uid)
        .ok_or_else(|| DimseError::protocol("unsupported C-FIND transfer syntax"))?;

    let mut payload = Vec::new();
    while let Some(pdv) = ctx.read_data_pdv().await? {
        payload.extend_from_slice(&pdv.data);
    }
    InMemDicomObject::read_dataset_with_ts(Cursor::new(payload), transfer_syntax)
        .map_err(DimseError::from)
}

#[derive(Debug)]
struct PreparedFindQuery {
    query: DicomQuery,
    query_retrieve_level: String,
    requested_response_tags: Vec<Tag>,
}

fn build_query(
    request: &CFindRequest,
    identifier: &InMemDicomObject,
) -> Result<PreparedFindQuery, QueryError> {
    let query_retrieve_level = required_string(identifier, tags::QUERY_RETRIEVE_LEVEL)?;
    let scope = query_scope(&request.affected_sop_class_uid, &query_retrieve_level)?;
    validate_baseline_unique_keys(scope, identifier)?;
    let requested_response_tags = requested_response_tags(identifier);
    let projection = projection(&requested_response_tags);
    let predicate = predicate(identifier)?;
    let mut query = DicomQuery::new(scope, projection)?;
    if let Some(predicate) = predicate {
        query = query.with_predicate(predicate)?;
    }
    if let Some(charsets) = optional_multi_string(identifier, tags::SPECIFIC_CHARACTER_SET) {
        query = query.with_specific_character_set(charsets)?;
    }
    if let Some(offset) = optional_string(identifier, tags::TIMEZONE_OFFSET_FROM_UTC) {
        query = query.with_timezone_offset(offset)?;
    }
    Ok(PreparedFindQuery {
        query,
        query_retrieve_level,
        requested_response_tags,
    })
}

fn query_scope(sop_class_uid: &str, level: &str) -> Result<QueryScope, QueryError> {
    match sop_class_uid {
        uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND => match level {
            "PATIENT" => Ok(QueryScope::PatientRoot(
                PatientRootQueryRetrieveLevel::Patient,
            )),
            "STUDY" => Ok(QueryScope::PatientRoot(
                PatientRootQueryRetrieveLevel::Study,
            )),
            "SERIES" => Ok(QueryScope::PatientRoot(
                PatientRootQueryRetrieveLevel::Series,
            )),
            "IMAGE" => Ok(QueryScope::PatientRoot(
                PatientRootQueryRetrieveLevel::Image,
            )),
            _ => Err(QueryError::invalid_query(format!(
                "unsupported Query/Retrieve Level {level:?}"
            ))),
        },
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND => match level {
            "STUDY" => Ok(QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study)),
            "SERIES" => Ok(QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)),
            "IMAGE" => Ok(QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image)),
            _ => Err(QueryError::invalid_query(format!(
                "unsupported Study Root Query/Retrieve Level {level:?}"
            ))),
        },
        _ => Err(QueryError::invalid_query(format!(
            "unsupported C-FIND SOP Class UID {sop_class_uid}"
        ))),
    }
}

fn validate_baseline_unique_keys(
    scope: QueryScope,
    identifier: &InMemDicomObject,
) -> Result<(), QueryError> {
    let required_tags: &[Tag] = match scope {
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient) => &[tags::PATIENT_ID],
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Study) => {
            &[tags::PATIENT_ID, tags::STUDY_INSTANCE_UID]
        }
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Series) => &[
            tags::PATIENT_ID,
            tags::STUDY_INSTANCE_UID,
            tags::SERIES_INSTANCE_UID,
        ],
        QueryScope::PatientRoot(PatientRootQueryRetrieveLevel::Image) => &[
            tags::PATIENT_ID,
            tags::STUDY_INSTANCE_UID,
            tags::SERIES_INSTANCE_UID,
            tags::SOP_INSTANCE_UID,
        ],
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Study) => &[tags::STUDY_INSTANCE_UID],
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Series) => {
            &[tags::STUDY_INSTANCE_UID, tags::SERIES_INSTANCE_UID]
        }
        QueryScope::StudyRoot(StudyRootQueryRetrieveLevel::Image) => &[
            tags::STUDY_INSTANCE_UID,
            tags::SERIES_INSTANCE_UID,
            tags::SOP_INSTANCE_UID,
        ],
    };

    for tag in required_tags {
        if !baseline_unique_key_is_present(identifier, *tag) {
            return Err(QueryError::invalid_query(format!(
                "missing required unique key {tag} for baseline C-FIND"
            )));
        }
    }
    Ok(())
}

fn requested_response_tags(identifier: &InMemDicomObject) -> Vec<Tag> {
    let mut tags = identifier
        .iter()
        .map(|element| element.tag())
        .filter(|tag| {
            *tag != tags::QUERY_RETRIEVE_LEVEL
                && *tag != tags::SPECIFIC_CHARACTER_SET
                && *tag != tags::TIMEZONE_OFFSET_FROM_UTC
        })
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    tags
}

fn projection(requested_response_tags: &[Tag]) -> Projection {
    if requested_response_tags.is_empty() {
        Projection::Default
    } else {
        Projection::Fields(
            requested_response_tags
                .iter()
                .copied()
                .map(AttributePath::from_tag)
                .collect(),
        )
    }
}

fn predicate(identifier: &InMemDicomObject) -> Result<Option<Predicate>, QueryError> {
    let mut predicates = Vec::new();
    for element in identifier.iter() {
        if element.tag() == tags::QUERY_RETRIEVE_LEVEL
            || element.tag() == tags::SPECIFIC_CHARACTER_SET
            || element.tag() == tags::TIMEZONE_OFFSET_FROM_UTC
            || is_zero_length(element)
        {
            continue;
        }
        let value = element
            .to_str()
            .map_err(|_| QueryError::invalid_query(format!("invalid query key {}", element.tag())))?
            .trim()
            .to_string();
        if value.is_empty() {
            continue;
        }
        let rule = if element.vr() == VR::UI && value.contains('\\') {
            MatchingRule::UidList(value.split('\\').map(str::to_string).collect())
        } else if matches!(element.vr(), VR::DA | VR::TM) && is_date_or_time_range(&value) {
            MatchingRule::Range(range_matching(&value, range_separator_index(&value))?)
        } else if element.vr() == VR::DT && is_datetime_range(&value) {
            MatchingRule::DateTimeRange(range_matching(
                &value,
                datetime_range_separator_index(&value),
            )?)
        } else if value.contains('*') || value.contains('?') {
            MatchingRule::Wildcard(value)
        } else {
            MatchingRule::SingleValue(value)
        };
        predicates.push(Predicate::Attribute(
            AttributePath::from_tag(element.tag()),
            rule,
        ));
    }

    Ok(match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Predicate::All(predicates)),
    })
}

fn range_matching(
    value: &str,
    separator_index: Option<usize>,
) -> Result<RangeMatching, QueryError> {
    let Some(index) = separator_index else {
        return Err(QueryError::invalid_query(format!(
            "invalid range matching value {value:?}"
        )));
    };
    let (start, rest) = value.split_at(index);
    let end = &rest[1..];
    match (start.is_empty(), end.is_empty()) {
        (false, false) => Ok(RangeMatching::closed(start, end)),
        (false, true) => Ok(RangeMatching::from_start(start)),
        (true, false) => Ok(RangeMatching::until_end(end)),
        (true, true) => Err(QueryError::invalid_query(
            "range matching requires at least one bound",
        )),
    }
}

fn is_date_or_time_range(value: &str) -> bool {
    range_separator_index(value).is_some()
}

fn is_datetime_range(value: &str) -> bool {
    datetime_range_separator_index(value).is_some()
}

fn range_separator_index(value: &str) -> Option<usize> {
    value.find('-')
}

fn datetime_range_separator_index(value: &str) -> Option<usize> {
    value
        .char_indices()
        .filter_map(|(index, ch)| (ch == '-').then_some(index))
        .find(|index| !is_timezone_offset_minus(value, *index))
}

fn is_timezone_offset_minus(value: &str, index: usize) -> bool {
    if index < 6 || value.len() < index + 5 {
        return false;
    }
    let offset = &value[index + 1..index + 5];
    let offset_is_followed_by_end_or_separator =
        value.len() == index + 5 || value[index + 5..].starts_with('-');
    offset.bytes().all(|b| b.is_ascii_digit()) && offset_is_followed_by_end_or_separator
}

struct PendingIdentifier {
    identifier: InMemDicomObject,
    unsupported_optional_keys: bool,
}

fn query_match_to_identifier(
    item: QueryMatch,
    prepared: &PreparedFindQuery,
) -> Result<PendingIdentifier, DimseError> {
    let mut object = InMemDicomObject::new_empty();
    let mut unsupported_optional_keys = false;
    object.put(DataElement::new(
        tags::QUERY_RETRIEVE_LEVEL,
        VR::CS,
        prepared.query_retrieve_level.as_str(),
    ));
    for attr in item.into_attributes() {
        if let Some(element) = projected_attribute_to_element(attr)? {
            object.put(element);
        }
    }
    for tag in &prepared.requested_response_tags {
        if object.element(*tag).is_err() {
            unsupported_optional_keys = true;
            object.put(DataElement::new(*tag, vr_for_tag(*tag), ""));
        }
    }
    Ok(PendingIdentifier {
        identifier: object,
        unsupported_optional_keys,
    })
}

fn projected_attribute_to_element(
    attr: ProjectedAttribute,
) -> Result<Option<InMemElement>, DimseError> {
    let Some(tag) = single_tag(&attr.path) else {
        return Ok(None);
    };
    let vr = vr_for_tag(tag);
    match attr.value {
        ResponseValue::Absent => Ok(None),
        ResponseValue::ZeroLength => Ok(Some(DataElement::new(tag, vr, ""))),
        ResponseValue::Present(AttributeValue::Text(value)) => {
            Ok(Some(DataElement::new(tag, vr, value)))
        }
        ResponseValue::Present(AttributeValue::Texts(values)) => {
            Ok(Some(DataElement::new(tag, vr, values.join("\\"))))
        }
        ResponseValue::Present(AttributeValue::Binary(value)) => {
            Ok(Some(DataElement::new(tag, vr, PrimitiveValue::from(value))))
        }
        ResponseValue::Present(AttributeValue::Sequence(_items)) => Ok(None),
    }
}

fn vr_for_tag(tag: Tag) -> VR {
    StandardDataDictionary
        .by_tag(tag)
        .and_then(|entry| entry.vr().exact())
        .unwrap_or(VR::LO)
}

fn single_tag(path: &AttributePath) -> Option<Tag> {
    match path.segments() {
        [raccoon_service_query::AttributePathSegment::Tag(tag)] => Some(*tag),
        _ => None,
    }
}

fn required_string(object: &InMemDicomObject, tag: Tag) -> Result<String, QueryError> {
    optional_string(object, tag)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| QueryError::invalid_query(format!("missing required attribute {tag}")))
}

fn optional_string(object: &InMemDicomObject, tag: Tag) -> Option<String> {
    object
        .element(tag)
        .ok()?
        .to_str()
        .ok()
        .map(|s| s.trim().to_string())
}

fn baseline_unique_key_is_present(object: &InMemDicomObject, tag: Tag) -> bool {
    let Ok(element) = object.element(tag) else {
        return false;
    };
    if tag == tags::PATIENT_ID {
        return element
            .to_str()
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
    }
    true
}

fn optional_multi_string(object: &InMemDicomObject, tag: Tag) -> Option<Vec<String>> {
    object.element(tag).ok()?.to_multi_str().ok().map(|values| {
        values
            .iter()
            .map(|value| value.trim().to_string())
            .collect()
    })
}

fn is_zero_length(element: &dicom_object::mem::InMemElement) -> bool {
    element.header().len.0 == 0
}

fn query_error_status(error: &QueryError) -> CFindStatus {
    match error {
        QueryError::InvalidQuery(_) => CFindStatus::IdentifierDoesNotMatchSopClass,
        QueryError::Repository(_) => CFindStatus::RefusedOutOfResources,
    }
}
