use std::sync::Arc;
use std::time::Instant;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use dicom_dictionary_std::uids;
use dicom_ul::pdu::PresentationContextResultReason;
use raccoon_contract_object_store::{ByteStream, ObjectStoreError, Stream};
use raccoon_service_ingest::{
    IngestError, IngestObjectIdentity, IngestObjectOutcome, IngestPayloadRepresentation,
    IngestRequest, IngestService, IngestSource, IngestUploadId,
};
use tokio::sync::mpsc;
use tracing::Instrument;

use super::message::{CStoreRequest, CStoreResponse, CStoreStatus};
use crate::association::AssociationContext;
use crate::error::DimseError;
use crate::message::CommandField;
use crate::registry::{DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider};

const CSTORE_DATASET_CHUNK_BYTES: usize = 1 << 20;
const CSTORE_DATASET_CHANNEL_DEPTH: usize = 4;

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
        let total_started_at = Instant::now();
        let command = ctx
            .read_command()
            .instrument(tracing::info_span!("dimse.cstore.read_command"))
            .await?;
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

        let upload_id = IngestUploadId::new();
        let (dataset_body, dataset_tx) = dataset_body_stream();
        let mut ingest_request = IngestRequest::new(upload_id, dataset_body)
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

        let ingest = self
            .ingest
            .ingest_upload_object(ingest_request)
            .instrument(tracing::info_span!("dimse.cstore.ingest_service"));
        tokio::pin!(ingest);

        tracing::debug!(
            stage = "backend_call",
            backend = "ingest",
            "C-STORE ingest started"
        );
        let receive = read_dataset(ctx, dataset_tx)
            .instrument(tracing::info_span!("dimse.cstore.receive_dataset"));
        let (dataset, ingest_result) = tokio::join!(receive, ingest);
        let dataset = dataset?;
        tracing::debug!(stage = "dataset_received", "C-STORE data set received");

        let (status, error_comment) = if dataset.buffer_failed {
            (
                CStoreStatus::OutOfResources,
                Some("failed to buffer received data set fragments".to_string()),
            )
        } else {
            match ingest_result {
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
            }
        };

        let response_reason = error_comment.clone();
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
        .instrument(tracing::info_span!("dimse.cstore.send_response"))
        .await?;
        ctx.record_response_status(status_code, response_reason);
        tracing::info!(
            dimse.operation = "C-STORE",
            dimse.cstore.pdv_count = dataset.pdv_count,
            dimse.cstore.payload_bytes = dataset.payload_bytes,
            dimse.cstore.status = format!("0x{status_code:04X}"),
            service.duration_ms = elapsed_ms(total_started_at),
            "DIMSE C-STORE completed"
        );
        tracing::debug!(
            stage = "response",
            status = format!("0x{status_code:04X}"),
            "C-STORE response sent"
        );
        Ok(())
    }
}

struct DatasetReceive {
    pdv_count: u64,
    payload_bytes: u64,
    buffer_failed: bool,
}

fn dataset_body_stream() -> (
    ByteStream,
    mpsc::Sender<raccoon_contract_object_store::Result<Bytes>>,
) {
    let (tx, rx) = mpsc::channel(CSTORE_DATASET_CHANNEL_DEPTH);
    (ByteStream::new(DatasetBodyStream { rx }), tx)
}

struct DatasetBodyStream {
    rx: mpsc::Receiver<raccoon_contract_object_store::Result<Bytes>>,
}

impl Stream for DatasetBodyStream {
    type Item = raccoon_contract_object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(context)
    }
}

async fn read_dataset(
    ctx: &mut AssociationContext,
    tx: mpsc::Sender<raccoon_contract_object_store::Result<Bytes>>,
) -> Result<DatasetReceive, DimseError> {
    let mut tx = Some(tx);
    let mut current_chunk = Vec::new();
    let mut pdv_count = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut buffer_failed = false;

    loop {
        let pdv = match ctx.read_data_pdv().await {
            Ok(Some(pdv)) => pdv,
            Ok(None) => break,
            Err(error) => {
                send_dataset_body_error(
                    &mut tx,
                    format!("failed to read C-STORE data set: {error}"),
                )
                .await;
                return Err(error);
            }
        };
        pdv_count = pdv_count.saturating_add(1);
        let data = pdv.data;
        let chunk_len = data.len();
        payload_bytes = payload_bytes.saturating_add(chunk_len as u64);
        if tx.is_none() || buffer_failed {
            continue;
        }
        if !current_chunk.is_empty()
            && current_chunk.len().saturating_add(chunk_len) > CSTORE_DATASET_CHUNK_BYTES
            && !flush_dataset_chunk(&mut tx, &mut current_chunk).await
        {
            continue;
        }
        if data.len() >= CSTORE_DATASET_CHUNK_BYTES {
            if !send_dataset_chunk(&mut tx, Bytes::from(data)).await {
                continue;
            }
            continue;
        }
        if current_chunk.try_reserve(data.len()).is_err() {
            buffer_failed = true;
            current_chunk.clear();
            send_dataset_body_error(
                &mut tx,
                "failed to buffer received data set fragments".to_string(),
            )
            .await;
            continue;
        }
        current_chunk.extend_from_slice(&data);
    }
    if !current_chunk.is_empty() {
        let _ = flush_dataset_chunk(&mut tx, &mut current_chunk).await;
    }

    Ok(DatasetReceive {
        pdv_count,
        payload_bytes,
        buffer_failed,
    })
}

async fn flush_dataset_chunk(
    tx: &mut Option<mpsc::Sender<raccoon_contract_object_store::Result<Bytes>>>,
    current_chunk: &mut Vec<u8>,
) -> bool {
    send_dataset_chunk(tx, Bytes::from(std::mem::take(current_chunk))).await
}

async fn send_dataset_chunk(
    tx: &mut Option<mpsc::Sender<raccoon_contract_object_store::Result<Bytes>>>,
    bytes: Bytes,
) -> bool {
    let Some(sender) = tx.as_ref() else {
        return false;
    };
    if sender.send(Ok(bytes)).await.is_ok() {
        return true;
    }
    *tx = None;
    false
}

async fn send_dataset_body_error(
    tx: &mut Option<mpsc::Sender<raccoon_contract_object_store::Result<Bytes>>>,
    message: String,
) {
    let Some(sender) = tx.take() else {
        return;
    };
    let _ = sender.send(Err(ObjectStoreError::backend(message))).await;
}

impl DescribedServiceClassProvider for StorageServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        &self.bindings
    }
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

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}
