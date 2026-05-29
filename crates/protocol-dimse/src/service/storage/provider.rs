use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use dicom_core::Tag;
use dicom_dictionary_std::uids;
use dicom_ul::pdu::PresentationContextResultReason;
use raccoon_contract_object_store::ByteStream;
use raccoon_service_ingest::{
    IngestError, IngestObjectIdentity, IngestObjectOutcome, IngestPayloadRepresentation,
    IngestRequest, IngestService, IngestSource, IngestUploadId,
};

use super::message::{CStoreRequest, CStoreResponse, CStoreStatus};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::CommandField;
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

/// Storage Service Class (C-STORE SCP) provider backed by `IngestService`.
pub struct StorageServiceProvider {
    ingest: Arc<dyn IngestService>,
    bindings: Vec<ServiceBinding>,
}

impl StorageServiceProvider {
    pub const DEFAULT_STORAGE_SOP_CLASS_UIDS: &[&str] = &[
        uids::COMPUTED_RADIOGRAPHY_IMAGE_STORAGE,
        uids::DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::DIGITAL_MAMMOGRAPHY_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_MAMMOGRAPHY_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::DIGITAL_INTRA_ORAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
        uids::DIGITAL_INTRA_ORAL_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
        uids::CT_IMAGE_STORAGE,
        uids::ENHANCED_CT_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_CT_IMAGE_STORAGE,
        uids::MR_IMAGE_STORAGE,
        uids::ENHANCED_MR_IMAGE_STORAGE,
        uids::ENHANCED_MR_COLOR_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_MR_IMAGE_STORAGE,
        uids::ULTRASOUND_IMAGE_STORAGE,
        uids::ULTRASOUND_MULTI_FRAME_IMAGE_STORAGE,
        uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_SINGLE_BIT_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_GRAYSCALE_BYTE_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::MULTI_FRAME_TRUE_COLOR_SECONDARY_CAPTURE_IMAGE_STORAGE,
        uids::X_RAY_ANGIOGRAPHIC_IMAGE_STORAGE,
        uids::ENHANCED_XA_IMAGE_STORAGE,
        uids::X_RAY_RADIOFLUOROSCOPIC_IMAGE_STORAGE,
        uids::ENHANCED_XRF_IMAGE_STORAGE,
        uids::NUCLEAR_MEDICINE_IMAGE_STORAGE,
        uids::POSITRON_EMISSION_TOMOGRAPHY_IMAGE_STORAGE,
        uids::ENHANCED_PET_IMAGE_STORAGE,
        uids::LEGACY_CONVERTED_ENHANCED_PET_IMAGE_STORAGE,
    ];

    pub fn new(
        ingest: Arc<dyn IngestService>,
        sop_class_uids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            ingest,
            bindings: sop_class_uids
                .into_iter()
                .map(|uid| ServiceBinding::owned(CommandField::CStoreRq, uid.into()))
                .collect(),
        }
    }

    pub fn with_default_storage_sop_classes(ingest: Arc<dyn IngestService>) -> Self {
        Self::new(ingest, Self::DEFAULT_STORAGE_SOP_CLASS_UIDS.iter().copied())
    }
}

#[async_trait]
impl ServiceClassProvider for StorageServiceProvider {
    #[tracing::instrument(skip(self, ctx), fields(command = "C-STORE"))]
    async fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let command = ctx.read_command().await?;
        let request = CStoreRequest::from_command(&command)?;
        tracing::debug!(stage = "validate", "C-STORE request validated");

        let transfer_syntax_uid = ctx
            .association()
            .presentation_contexts()
            .iter()
            .find(|pc| {
                pc.id == request.presentation_context_id
                    && pc.reason == PresentationContextResultReason::Acceptance
            })
            .map(|pc| pc.transfer_syntax.clone());

        let peer_ae = ctx
            .association()
            .peer_ae_title()
            .map(|t| t.as_str().to_string());

        let mut payload: Vec<u8> = Vec::new();
        while let Some(pdv) = ctx.read_data_pdv().await? {
            if payload.write_all(&pdv.data).is_err() {
                return send_failure(
                    ctx,
                    &request,
                    CStoreStatus::OutOfResources,
                    Some("failed to buffer received data set"),
                    Vec::new(),
                )
                .await;
            }
        }
        tracing::debug!(stage = "dataset_received", "C-STORE data set received");

        let upload_id = IngestUploadId::new();
        let mut ingest_request =
            IngestRequest::new(upload_id, ByteStream::once(bytes::Bytes::from(payload)))
                .with_identity_hints(IngestObjectIdentity {
                    sop_class_uid: Some(request.affected_sop_class_uid.clone()),
                    sop_instance_uid: Some(request.affected_sop_instance_uid.clone()),
                    ..Default::default()
                })
                .with_source(IngestSource {
                    source_ae: peer_ae,
                    protocol: Some("dimse".to_string()),
                    ..Default::default()
                })
                .with_payload_representation(IngestPayloadRepresentation::DicomDataSet);

        if let Some(ts) = transfer_syntax_uid {
            ingest_request = ingest_request.with_transfer_syntax_uid(ts);
        }

        tracing::debug!(
            stage = "backend_call",
            backend = "ingest",
            "C-STORE ingest started"
        );

        let (status, error_comment) = match self.ingest.ingest_upload_object(ingest_request).await {
            Ok(result) => outcome_to_status(&result.outcome),
            Err(error) => {
                tracing::warn!(
                    stage = "backend_failure",
                    backend = "ingest",
                    error = %error,
                    "C-STORE ingest failed"
                );
                ingest_error_to_status(&error)
            }
        };

        let response = {
            let mut r = CStoreResponse::for_request(&request, status);
            if let Some(comment) = error_comment {
                r = r.with_error_comment(comment);
            }
            r
        };

        let status_code = response.status.code();
        ctx.send_command_object(
            request.presentation_context_id,
            &response.to_command_object(),
        )
        .await?;
        ctx.record_response_status(status_code);
        tracing::debug!(
            stage = "response",
            status = format!("0x{status_code:04X}"),
            "C-STORE response sent"
        );
        Ok(())
    }
}

impl DescribedServiceClassProvider for StorageServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
}

async fn send_failure(
    ctx: &mut AssociationContext,
    request: &CStoreRequest,
    status: CStoreStatus,
    comment: Option<&str>,
    offending_elements: Vec<Tag>,
) -> Result<(), DimseError> {
    let mut response = CStoreResponse::for_request(request, status);
    if let Some(c) = comment {
        response = response.with_error_comment(c);
    }
    for tag in offending_elements {
        response = response.with_offending_element(tag);
    }
    let status_code = response.status.code();
    ctx.send_command_object(
        request.presentation_context_id,
        &response.to_command_object(),
    )
    .await?;
    ctx.record_response_status(status_code);
    Ok(())
}

fn outcome_to_status(outcome: &IngestObjectOutcome) -> (CStoreStatus, Option<String>) {
    match outcome {
        IngestObjectOutcome::Stored => (CStoreStatus::Success, None),
        IngestObjectOutcome::RejectedCannotUnderstand { reason } => {
            (CStoreStatus::CannotUnderstand, Some(reason.clone()))
        }
        IngestObjectOutcome::RejectedUnsupportedSopClass { reason, .. } => {
            (CStoreStatus::CannotUnderstand, Some(reason.clone()))
        }
        IngestObjectOutcome::RejectedStudyMismatch { .. } => {
            (CStoreStatus::DataSetDoesNotMatchSopClass, None)
        }
        IngestObjectOutcome::RejectedChecksumMismatch { .. } => (
            CStoreStatus::CannotUnderstand,
            Some("checksum mismatch".to_string()),
        ),
        IngestObjectOutcome::RejectedTooLarge { .. } => (
            CStoreStatus::OutOfResources,
            Some("object too large".to_string()),
        ),
        IngestObjectOutcome::ObjectStoreFailed { reason } => {
            (CStoreStatus::OutOfResources, Some(reason.clone()))
        }
        IngestObjectOutcome::RepositoryFailed { reason } => {
            (CStoreStatus::OutOfResources, Some(reason.clone()))
        }
    }
}

fn ingest_error_to_status(error: &IngestError) -> (CStoreStatus, Option<String>) {
    match error {
        IngestError::ObjectKey { .. } | IngestError::ObjectStore { .. } => (
            CStoreStatus::OutOfResources,
            Some("failed to persist received instance".to_string()),
        ),
        IngestError::ContentTooLarge { .. } => (
            CStoreStatus::OutOfResources,
            Some("object too large".to_string()),
        ),
    }
}
