use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::{DataElement, Tag, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};
use futures_util::StreamExt;
use raccoon_contract_dicom::{PatientId, SeriesInstanceUid, SopInstanceUid, StudyInstanceUid};
use raccoon_service_retrieve::{
    RetrieveError, RetrieveRequest, RetrieveScope, RetrieveService, RetrievedInstance,
};
use thiserror::Error;

use super::message::{CMoveRequest, CMoveResponse, CMoveStatus};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::{CommandField, Priority};
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

/// Outcome of one C-STORE sub-operation sent to the C-MOVE destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveStoreOutcome {
    Success,
    Warning,
}

#[derive(Debug)]
pub struct MoveStoreRequest {
    pub destination_ae_title: String,
    pub originator_ae_title: String,
    pub originator_message_id: u16,
    pub message_id: u16,
    pub priority: Priority,
    pub instance: RetrievedInstance,
}

#[derive(Debug, Error)]
pub enum MoveDestinationError {
    #[error("move destination unknown")]
    UnknownDestination,

    #[error("move destination out of resources: {0}")]
    OutOfResources(String),

    #[error("move destination store failed: {0}")]
    StoreFailed(String),
}

/// C-STORE sub-operation boundary for C-MOVE destinations.
///
/// Real destination resolution, association opening, and presentation-context
/// negotiation belong behind this trait; the DIMSE provider only owns C-MOVE
/// command handling, counters, and status mapping.
#[async_trait]
pub trait MoveDestinationStore: Send + Sync {
    async fn validate_destination(&self, ae_title: &str) -> Result<(), MoveDestinationError>;

    async fn store(
        &self,
        request: MoveStoreRequest,
    ) -> Result<MoveStoreOutcome, MoveDestinationError>;
}

/// Query/Retrieve C-MOVE SCP provider backed by `RetrieveService`.
pub struct MoveServiceProvider {
    retrieve: Arc<dyn RetrieveService>,
    destination_store: Arc<dyn MoveDestinationStore>,
    bindings: Vec<ServiceBinding>,
}

impl MoveServiceProvider {
    pub const DEFAULT_MOVE_SOP_CLASS_UIDS: &[&str] = &[
        uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE,
        uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE,
    ];

    pub fn new(
        retrieve: Arc<dyn RetrieveService>,
        destination_store: Arc<dyn MoveDestinationStore>,
        sop_class_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            retrieve,
            destination_store,
            bindings: sop_class_uids
                .into_iter()
                .map(|uid| ServiceBinding::owned(CommandField::CMoveRq, uid.into()))
                .collect(),
        }
    }

    pub fn with_default_move_sop_classes(
        retrieve: Arc<dyn RetrieveService>,
        destination_store: Arc<dyn MoveDestinationStore>,
    ) -> Self {
        Self::new(
            retrieve,
            destination_store,
            Self::DEFAULT_MOVE_SOP_CLASS_UIDS.iter().copied(),
        )
    }
}

#[async_trait]
impl ServiceClassProvider for MoveServiceProvider {
    #[tracing::instrument(skip(self, ctx), fields(command = "C-MOVE"))]
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command().await?;
        let request = CMoveRequest::from_command(&command)?;
        let mut suboperation_message_id = next_message_id(request.message_id);
        let identifier = read_identifier(ctx, request.presentation_context_id).await?;
        let originator_ae_title = match ctx.association().peer_ae_title() {
            Some(title) => title.as_str().to_string(),
            None => {
                return send_move_response(
                    ctx,
                    &request,
                    CMoveStatus::UnableToProcess,
                    MoveResponseCounts::final_error(),
                    Some("missing C-MOVE originator AE title".to_string()),
                    None,
                )
                .await;
            }
        };
        if let Err(error) = self
            .destination_store
            .validate_destination(&request.move_destination)
            .await
        {
            return send_move_response(
                ctx,
                &request,
                destination_error_status(&error),
                MoveResponseCounts::final_error(),
                Some(error.to_string()),
                None,
            )
            .await;
        }
        let retrieve_requests = match build_retrieve_requests(&request, &identifier) {
            Ok(requests) => requests,
            Err(error) => {
                return send_move_response(
                    ctx,
                    &request,
                    CMoveStatus::IdentifierDoesNotMatchSopClass,
                    MoveResponseCounts::final_error(),
                    Some(error),
                    None,
                )
                .await;
            }
        };
        tracing::debug!(stage = "validate", "C-MOVE request validated");

        let mut results = Vec::new();
        for retrieve_request in retrieve_requests {
            match self.retrieve.retrieve(retrieve_request).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    return send_move_response(
                        ctx,
                        &request,
                        retrieve_error_status(&error),
                        MoveResponseCounts::final_error(),
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
                        match self
                            .destination_store
                            .store(MoveStoreRequest {
                                destination_ae_title: request.move_destination.clone(),
                                originator_ae_title: originator_ae_title.clone(),
                                originator_message_id: request.message_id,
                                message_id,
                                priority: request.priority,
                                instance,
                            })
                            .await
                        {
                            Ok(MoveStoreOutcome::Success) => {
                                completed = completed.saturating_add(1)
                            }
                            Ok(MoveStoreOutcome::Warning) => warning = warning.saturating_add(1),
                            Err(error) => {
                                tracing::warn!(stage = "suboperation", error = %error, "C-MOVE C-STORE sub-operation failed");
                                failed = failed.saturating_add(1);
                                failed_sop_instance_uids.push(sop_instance_uid);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(stage = "backend_stream", error = %error, "C-MOVE retrieve stream failed");
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
                    send_move_response(
                        ctx,
                        &request,
                        CMoveStatus::Pending,
                        MoveResponseCounts {
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
            CMoveStatus::Success
        } else {
            CMoveStatus::SubOperationsCompleteOneOrMoreFailures
        };
        let failed_identifier = if failed_sop_instance_uids.is_empty() {
            None
        } else {
            Some(failed_sop_instance_uid_list(&failed_sop_instance_uids))
        };
        send_move_response(
            ctx,
            &request,
            status,
            MoveResponseCounts {
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

impl DescribedServiceClassProvider for MoveServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
}

async fn send_move_response(
    ctx: &mut AssociationContext,
    request: &CMoveRequest,
    status: CMoveStatus,
    counts: MoveResponseCounts,
    comment: Option<String>,
    identifier: Option<&InMemDicomObject>,
) -> Result<(), DimseError> {
    let remaining = if counts.remaining.is_none() && status != CMoveStatus::Pending {
        Some(0)
    } else {
        counts.remaining
    };
    let response_reason = comment.clone();
    let mut response = CMoveResponse::for_request(request, status).with_counts(
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
struct MoveResponseCounts {
    remaining: Option<u16>,
    completed: u16,
    failed: u16,
    warning: u16,
}

impl MoveResponseCounts {
    fn final_error() -> Self {
        Self {
            remaining: None,
            completed: 0,
            failed: 0,
            warning: 0,
        }
    }
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
        .ok_or_else(|| DimseError::protocol("missing presentation context for C-MOVE"))?;
    let transfer_syntax = TransferSyntaxRegistry
        .get(transfer_syntax_uid)
        .ok_or_else(|| DimseError::protocol("unsupported C-MOVE transfer syntax"))?;

    let mut payload = Vec::new();
    while let Some(pdv) = ctx.read_data_pdv().await? {
        payload.extend_from_slice(&pdv.data);
    }
    InMemDicomObject::read_dataset_with_ts(Cursor::new(payload), transfer_syntax)
        .map_err(DimseError::from)
}

fn build_retrieve_requests(
    request: &CMoveRequest,
    identifier: &InMemDicomObject,
) -> Result<Vec<RetrieveRequest>, String> {
    let level = required_string(identifier, tags::QUERY_RETRIEVE_LEVEL)?;
    let scopes = match (request.affected_sop_class_uid.as_str(), level.as_str()) {
        (uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE, "PATIENT") => {
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
                        study_instance_uid: None,
                        series_instance_uid,
                    })
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?,
        (_, "IMAGE") => required_strings(identifier, tags::SOP_INSTANCE_UID)?
            .into_iter()
            .map(|uid| {
                SopInstanceUid::new(uid)
                    .map(|sop_instance_uid| RetrieveScope::Instance {
                        study_instance_uid: None,
                        series_instance_uid: None,
                        sop_instance_uid,
                    })
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

fn retrieve_error_status(error: &RetrieveError) -> CMoveStatus {
    match error {
        RetrieveError::Repository(_) => CMoveStatus::RefusedOutOfResourcesUnableToPerform,
        RetrieveError::InvalidRequest(_) => CMoveStatus::IdentifierDoesNotMatchSopClass,
        RetrieveError::ObjectStore(_) | RetrieveError::ObjectStoreForInstance { .. } => {
            CMoveStatus::UnableToProcess
        }
        _ => CMoveStatus::UnableToProcess,
    }
}

fn destination_error_status(error: &MoveDestinationError) -> CMoveStatus {
    match error {
        MoveDestinationError::UnknownDestination => CMoveStatus::MoveDestinationUnknown,
        MoveDestinationError::OutOfResources(_) => {
            CMoveStatus::RefusedOutOfResourcesUnableToPerform
        }
        MoveDestinationError::StoreFailed(_) => CMoveStatus::UnableToProcess,
    }
}
