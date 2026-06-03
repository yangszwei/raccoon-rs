use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::{DataElement, PrimitiveValue, Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextResultReason};
use futures_util::StreamExt;
use raccoon_contract_dicom::{PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use raccoon_service_retrieve::{
    RetrieveError, RetrieveRequest, RetrieveScope, RetrieveService, RetrievedInstance,
};

use super::message::{CGetRequest, CGetResponse, CGetStatus};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::{CommandField, Priority};
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

/// Query/Retrieve C-GET SCP provider backed by `RetrieveService`.
pub struct RetrieveServiceProvider {
    retrieve: Arc<dyn RetrieveService>,
    bindings: Vec<ServiceBinding>,
}

impl RetrieveServiceProvider {
    pub const DEFAULT_GET_SOP_CLASS_UIDS: &[&str] = &[
        uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET,
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET,
    ];

    pub fn new(
        retrieve: Arc<dyn RetrieveService>,
        sop_class_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            retrieve,
            bindings: sop_class_uids
                .into_iter()
                .map(|uid| ServiceBinding::owned(CommandField::CGetRq, uid.into()))
                .collect(),
        }
    }

    pub fn with_default_get_sop_classes(retrieve: Arc<dyn RetrieveService>) -> Self {
        Self::new(retrieve, Self::DEFAULT_GET_SOP_CLASS_UIDS.iter().copied())
    }
}

#[async_trait]
impl ServiceClassProvider for RetrieveServiceProvider {
    #[tracing::instrument(skip(self, ctx), fields(command = "C-GET"))]
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command().await?;
        let request = CGetRequest::from_command(&command)?;
        let mut suboperation_message_id = next_message_id(request.message_id);
        let identifier = read_identifier(ctx, request.presentation_context_id).await?;
        let retrieve_requests = match build_retrieve_requests(&request, &identifier) {
            Ok(requests) => requests,
            Err(error) => {
                return send_get_response(
                    ctx,
                    &request,
                    CGetStatus::IdentifierDoesNotMatchSopClass,
                    GetResponseCounts::final_error(),
                    Some(error),
                    None,
                )
                .await;
            }
        };
        tracing::debug!(stage = "validate", "C-GET request validated");

        let mut results = Vec::new();
        for retrieve_request in retrieve_requests {
            match self.retrieve.retrieve(retrieve_request).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    return send_get_response(
                        ctx,
                        &request,
                        retrieve_error_status(&error),
                        GetResponseCounts::final_error(),
                        Some(error.to_string()),
                        None,
                    )
                    .await;
                }
            }
        }

        let total = results
            .iter()
            .map(|result| result.instance_count)
            .sum::<usize>()
            .min(u16::MAX as usize) as u16;
        let mut completed = 0_u16;
        let mut failed = 0_u16;
        let mut warning = 0_u16;
        let mut failed_sop_instance_uids = Vec::new();

        for mut result in results {
            while let Some(item) = result.stream.next().await {
                match item {
                    Ok(instance) => {
                        let sop_instance_uid =
                            instance.identity.sop_instance_uid.as_str().to_string();
                        let message_id = suboperation_message_id;
                        suboperation_message_id = next_message_id(suboperation_message_id);
                        match send_store_suboperation(ctx, &request, message_id, instance).await {
                            Ok(CStoreSubOperationStatus::Success) => {
                                completed = completed.saturating_add(1)
                            }
                            Ok(CStoreSubOperationStatus::Warning) => {
                                warning = warning.saturating_add(1)
                            }
                            Err(error) => {
                                tracing::warn!(stage = "suboperation", error = %error, "C-GET C-STORE sub-operation failed");
                                failed = failed.saturating_add(1);
                                failed_sop_instance_uids.push(sop_instance_uid);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(stage = "backend_stream", error = %error, "C-GET retrieve stream failed");
                        failed = failed.saturating_add(1);
                        if let RetrieveError::ObjectStoreForInstance {
                            sop_instance_uid, ..
                        } = error
                        {
                            failed_sop_instance_uids.push(sop_instance_uid.to_string());
                        }
                    }
                }
                let done = completed.saturating_add(failed).saturating_add(warning);
                if done < total {
                    send_get_response(
                        ctx,
                        &request,
                        CGetStatus::Pending,
                        GetResponseCounts {
                            remaining: Some(total.saturating_sub(done)),
                            completed,
                            failed,
                            warning,
                        },
                        None,
                        None,
                    )
                    .await?;
                }
            }
        }

        let status = if failed == 0 && warning == 0 {
            CGetStatus::Success
        } else {
            CGetStatus::SubOperationsCompleteOneOrMoreFailures
        };
        let failed_identifier = if failed_sop_instance_uids.is_empty() {
            None
        } else {
            Some(failed_sop_instance_uid_list(&failed_sop_instance_uids))
        };
        send_get_response(
            ctx,
            &request,
            status,
            GetResponseCounts {
                remaining: None,
                completed,
                failed,
                warning,
            },
            None,
            failed_identifier.as_ref(),
        )
        .await
    }
}

impl DescribedServiceClassProvider for RetrieveServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
}

async fn send_get_response(
    ctx: &mut AssociationContext,
    request: &CGetRequest,
    status: CGetStatus,
    counts: GetResponseCounts,
    comment: Option<String>,
    identifier: Option<&InMemDicomObject>,
) -> Result<(), DimseError> {
    let remaining = if counts.remaining.is_none() && status != CGetStatus::Pending {
        Some(0)
    } else {
        counts.remaining
    };
    let response_reason = comment.clone();
    let mut response = CGetResponse::for_request(request, status).with_counts(
        remaining,
        counts.completed,
        counts.failed,
        counts.warning,
    );
    if let Some(comment) = comment {
        response = response.with_error_comment(comment);
    }
    if identifier.is_some() {
        response = response.with_identifier();
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

#[derive(Debug, Clone, Copy)]
struct GetResponseCounts {
    remaining: Option<u16>,
    completed: u16,
    failed: u16,
    warning: u16,
}

impl GetResponseCounts {
    fn final_error() -> Self {
        Self {
            remaining: None,
            completed: 0,
            failed: 0,
            warning: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CStoreSubOperationStatus {
    Success,
    Warning,
}

async fn send_store_suboperation(
    ctx: &mut AssociationContext,
    get_request: &CGetRequest,
    c_store_message_id: u16,
    instance: RetrievedInstance,
) -> Result<CStoreSubOperationStatus, DimseError> {
    let sop_class_uid = instance.identity.sop_class_uid.as_str();
    let stored_transfer_syntax_uid = instance
        .transfer_syntax_uid
        .as_ref()
        .map(|uid| uid.as_str());
    let presentation_context_id = ctx
        .association()
        .presentation_contexts()
        .iter()
        .find(|pc| {
            pc.abstract_syntax == sop_class_uid
                && pc.reason == PresentationContextResultReason::Acceptance
                && stored_transfer_syntax_uid.is_none_or(|uid| pc.transfer_syntax == uid)
        })
        .map(|pc| pc.id)
        .ok_or_else(|| {
            if let Some(uid) = stored_transfer_syntax_uid {
                DimseError::protocol(format!(
                    "no accepted presentation context for retrieved SOP Class UID {sop_class_uid} with Transfer Syntax UID {uid}"
                ))
            } else {
                DimseError::protocol(format!(
                    "no accepted presentation context for retrieved SOP Class UID {sop_class_uid}"
                ))
            }
        })?;

    let command = c_store_request_command(
        c_store_message_id,
        sop_class_uid,
        instance.identity.sop_instance_uid.as_str(),
        Some(
            get_request
                .originator_ae_title
                .as_deref()
                .or_else(|| {
                    ctx.association()
                        .peer_ae_title()
                        .map(|title| title.as_str())
                })
                .unwrap_or_else(|| ctx.association().called_ae_title().as_str()),
        ),
        get_request.message_id,
        get_request.priority,
    );
    ctx.send_command_object(presentation_context_id, &command)
        .await?;

    let mut payload = Vec::with_capacity(instance.content_length.min(usize::MAX as u64) as usize);
    let mut body = instance.body;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|error| DimseError::protocol(error.to_string()))?;
        payload.extend_from_slice(&chunk);
    }
    ctx.send_data_pdv(PDataValue {
        presentation_context_id,
        value_type: PDataValueType::Data,
        is_last: true,
        data: payload,
    })
    .await?;

    let response = ctx.read_suboperation_command().await?;
    if response.command_field != CommandField::CStoreRsp {
        return Err(DimseError::protocol(format!(
            "expected C-STORE-RSP, got {}",
            response.command_field
        )));
    }
    let status = response
        .status
        .ok_or_else(|| DimseError::protocol("C-STORE-RSP missing Status"))?;
    let outcome = match status {
        0x0000 => CStoreSubOperationStatus::Success,
        0xB000..=0xBFFF => CStoreSubOperationStatus::Warning,
        status => {
            return Err(DimseError::protocol(format!(
                "C-STORE sub-operation failed with status 0x{status:04X}"
            )));
        }
    };
    ctx.complete_message_cycle()?;
    Ok(outcome)
}

fn c_store_request_command(
    message_id: u16,
    sop_class_uid: &str,
    sop_instance_uid: &str,
    move_originator_ae_title: Option<&str>,
    move_originator_message_id: u16,
    priority: Priority,
) -> InMemDicomObject {
    let mut command = InMemDicomObject::new_empty();
    command.put(DataElement::new(
        tags::AFFECTED_SOP_CLASS_UID,
        VR::UI,
        sop_class_uid,
    ));
    command.put(DataElement::new(
        tags::COMMAND_FIELD,
        VR::US,
        PrimitiveValue::from(0x0001_u16),
    ));
    command.put(DataElement::new(
        tags::MESSAGE_ID,
        VR::US,
        PrimitiveValue::from(message_id),
    ));
    command.put(DataElement::new(
        tags::PRIORITY,
        VR::US,
        priority_code(priority),
    ));
    command.put(DataElement::new(
        tags::COMMAND_DATA_SET_TYPE,
        VR::US,
        PrimitiveValue::from(0x0000_u16),
    ));
    command.put(DataElement::new(
        tags::AFFECTED_SOP_INSTANCE_UID,
        VR::UI,
        sop_instance_uid,
    ));
    if let Some(title) = move_originator_ae_title {
        command.put(DataElement::new(
            tags::MOVE_ORIGINATOR_APPLICATION_ENTITY_TITLE,
            VR::AE,
            title,
        ));
        command.put(DataElement::new(
            tags::MOVE_ORIGINATOR_MESSAGE_ID,
            VR::US,
            PrimitiveValue::from(move_originator_message_id),
        ));
    }
    command
}

fn next_message_id(message_id: u16) -> u16 {
    let next = message_id.wrapping_add(1);
    if next == 0 { 1 } else { next }
}

fn failed_sop_instance_uid_list(sop_instance_uids: &[String]) -> InMemDicomObject {
    let mut identifier = InMemDicomObject::new_empty();
    identifier.put(DataElement::new(
        tags::FAILED_SOP_INSTANCE_UID_LIST,
        VR::UI,
        sop_instance_uids.join("\\"),
    ));
    identifier
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
        .ok_or_else(|| DimseError::protocol("missing presentation context for C-GET"))?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(transfer_syntax_uid)
        .ok_or_else(|| DimseError::protocol("unsupported C-GET transfer syntax"))?;

    let mut payload = Vec::new();
    while let Some(pdv) = ctx.read_data_pdv().await? {
        payload.extend_from_slice(&pdv.data);
    }
    InMemDicomObject::read_dataset_with_ts(Cursor::new(payload), transfer_syntax)
        .map_err(DimseError::from)
}

fn build_retrieve_requests(
    request: &CGetRequest,
    identifier: &InMemDicomObject,
) -> Result<Vec<RetrieveRequest>, String> {
    let level = required_string(identifier, tags::QUERY_RETRIEVE_LEVEL)?;
    let scopes = match (request.affected_sop_class_uid.as_str(), level.as_str()) {
        (uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET, "PATIENT") => {
            vec![RetrieveScope::Patient {
                patient_id: PatientId::new(required_string(identifier, tags::PATIENT_ID)?)
                    .map_err(|e| e.to_string())?,
            }]
        }
        (_, "STUDY") => required_strings(identifier, tags::STUDY_INSTANCE_UID)?
            .into_iter()
            .map(|uid| {
                StudyInstanceUid::new(uid)
                    .map(|study_instance_uid| RetrieveScope::Study { study_instance_uid })
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        (_, "SERIES") => required_strings(identifier, tags::SERIES_INSTANCE_UID)?
            .into_iter()
            .map(|uid| {
                SeriesInstanceUid::new(uid)
                    .map(|series_instance_uid| RetrieveScope::Series {
                        series_instance_uid,
                    })
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        (_, "IMAGE") => required_strings(identifier, tags::SOP_INSTANCE_UID)?
            .into_iter()
            .map(|uid| {
                SopInstanceUid::new(uid)
                    .map(|sop_instance_uid| RetrieveScope::Instance { sop_instance_uid })
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(format!("unsupported Query/Retrieve Level {level:?}")),
    };
    Ok(scopes.into_iter().map(RetrieveRequest::new).collect())
}

fn required_string(object: &InMemDicomObject, tag: Tag) -> Result<String, String> {
    object
        .element(tag)
        .ok()
        .and_then(|element| element.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing required attribute {tag}"))
}

fn required_strings(object: &InMemDicomObject, tag: Tag) -> Result<Vec<String>, String> {
    let values = object
        .element(tag)
        .ok()
        .and_then(|element| element.to_multi_str().ok())
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .ok_or_else(|| format!("missing required attribute {tag}"))?;
    Ok(values)
}

fn priority_code(priority: Priority) -> PrimitiveValue {
    PrimitiveValue::from(match priority {
        Priority::Medium => 0x0000_u16,
        Priority::High => 0x0001_u16,
        Priority::Low => 0x0002_u16,
        Priority::Unknown(raw) => raw,
    })
}

fn retrieve_error_status(error: &RetrieveError) -> CGetStatus {
    match error {
        RetrieveError::Repository(_) => CGetStatus::RefusedOutOfResourcesUnableToPerform,
        RetrieveError::InvalidRequest(_) => CGetStatus::IdentifierDoesNotMatchSopClass,
        RetrieveError::ObjectStore(_) | RetrieveError::ObjectStoreForInstance { .. } => {
            CGetStatus::UnableToProcess
        }
        _ => CGetStatus::UnableToProcess,
    }
}
