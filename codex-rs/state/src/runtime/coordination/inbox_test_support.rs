use codex_coordination::BoundedId;
use codex_coordination::ContentEvidence;
use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_coordination::UnavailableReason;

use super::aggregate_test_support::*;
use super::commands_tests::assignment_command;
use crate::StateRuntime;
use crate::model::coordination::AssignmentReservation;
use crate::model::coordination_commands::CommandCiphertext;
use crate::model::coordination_commands::CoordinationCommandIntent;
use crate::model::coordination_commands::RecordCoordinationCommand;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::InboxTransportResolution;
use crate::model::coordination_inbox::PersistRecipientReceipt;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_inbox::ReceiptTargetFence;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_inbox::RecordInboxSelectionOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcome;
use crate::model::coordination_inbox::RecordInboxTransportOutcomeResult;
use crate::runtime::test_support::unique_temp_dir;

pub(super) const RECEIPT_ONE: &str = "019f7c6c-1111-7000-8000-000000000201";
pub(super) const RECEIPT_TWO: &str = "019f7c6c-1111-7000-8000-000000000202";
pub(super) const FOLLOWUP_OPERATION: &str = "019f7c6c-1111-7000-8000-000000000102";
pub(super) const INTERRUPT_OPERATION: &str = "019f7c6c-1111-7000-8000-000000000103";
pub(super) const MESSAGE_OPERATION: &str = "019f7c6c-1111-7000-8000-000000000104";
pub(super) const CLAIM_OPERATION_ONE: &str = "019f7c6c-1111-7000-8000-000000000301";
pub(super) const CLAIM_OPERATION_TWO: &str = "019f7c6c-1111-7000-8000-000000000302";

pub(super) fn claim_operation(value: &str) -> CoordinationOperationId {
    CoordinationOperationId::parse(value).expect("claim operation")
}

pub(super) async fn runtime_with_assignment_command() -> anyhow::Result<std::sync::Arc<StateRuntime>>
{
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(assignment_command())
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    Ok(runtime)
}

pub(super) fn receipt_params(
    operation: &str,
    receipt: &str,
    event: &str,
    expected_revision: u64,
    expected_head_version: u64,
    secondary: Vec<(CoordinationSemanticSlot, &str, &str)>,
) -> PersistRecipientReceipt {
    PersistRecipientReceipt {
        context: context(
            receipt_slot_for(operation),
            event,
            operation,
            true,
            expected_revision,
            secondary,
        ),
        receipt_id: ReceiptId::parse(receipt).expect("receipt"),
        command_operation_id: CoordinationOperationId::parse(operation).expect("operation"),
        target: ReceiptTargetFence {
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version,
        },
    }
}

fn receipt_slot_for(operation: &str) -> CoordinationSemanticSlot {
    match operation {
        INTERRUPT_OPERATION => CoordinationSemanticSlot::InterruptDurablyReceived,
        MESSAGE_OPERATION => CoordinationSemanticSlot::MessageDurablyReceived,
        OPERATION | FOLLOWUP_OPERATION => CoordinationSemanticSlot::AssignmentAccepted,
        _ => panic!("unknown test operation"),
    }
}

pub(super) async fn persist_initial_receipt(
    runtime: &StateRuntime,
) -> anyhow::Result<crate::model::coordination_inbox::InboxReceiptMetadata> {
    let outcome = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            OPERATION,
            RECEIPT_ONE,
            "019f7c6c-1111-7000-8000-000000000702",
            1,
            0,
            Vec::new(),
        ))
        .await?;
    match outcome {
        PersistRecipientReceiptOutcome::Applied(metadata) => Ok(metadata),
        PersistRecipientReceiptOutcome::Duplicate(_) | PersistRecipientReceiptOutcome::Deferred => {
            anyhow::bail!("unexpected receipt outcome")
        }
    }
}

pub(super) async fn persist_initial_assignment_inclusion(
    runtime: &StateRuntime,
) -> anyhow::Result<crate::model::coordination_inbox::InboxReceiptMetadata> {
    persist_assignment_inclusion(
        runtime,
        receipt_params(
            OPERATION,
            RECEIPT_ONE,
            "019f7c6c-1111-7000-8000-000000000702",
            1,
            0,
            Vec::new(),
        ),
        "assignment-attempt-g1",
    )
    .await
}

pub(super) async fn persist_assignment_inclusion(
    runtime: &StateRuntime,
    params: PersistRecipientReceipt,
    inference_attempt_id: &str,
) -> anyhow::Result<crate::model::coordination_inbox::InboxReceiptMetadata> {
    let metadata = match runtime
        .persist_coordination_recipient_receipt(params)
        .await
        .map_err(|error| anyhow::anyhow!("persist assignment receipt: {error}"))?
    {
        PersistRecipientReceiptOutcome::Applied(metadata) => metadata,
        PersistRecipientReceiptOutcome::Duplicate(metadata) => metadata,
        PersistRecipientReceiptOutcome::Deferred => anyhow::bail!("assignment receipt deferred"),
    };
    let now_ms = metadata.expires_at_ms - 10_000;
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms,
            lease_expires_at_ms: now_ms + 1_000,
        })
        .await
        .map_err(|error| anyhow::anyhow!("claim assignment receipt: {error}"))?
    else {
        anyhow::bail!("assignment receipt claim failed")
    };
    let RecordInboxSelectionOutcome::Applied(selection) = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: claim.lease,
            inference_attempt_id: inference_attempt(inference_attempt_id),
            event_context: None,
            selected_at_ms: now_ms + 1,
        })
        .await
        .map_err(|error| anyhow::anyhow!("select assignment receipt: {error}"))?
    else {
        anyhow::bail!("assignment receipt selection failed")
    };
    if !matches!(
        runtime
            .record_coordination_inbox_transport_outcome(RecordInboxTransportOutcome {
                selection: selection.token,
                resolution: InboxTransportResolution::SendSucceeded,
                completed_at_ms: now_ms + 2,
            })
            .await
            .map_err(|error| anyhow::anyhow!("complete assignment receipt: {error}"))?,
        RecordInboxTransportOutcomeResult::Applied(_)
    ) {
        anyhow::bail!("assignment receipt outcome failed")
    }
    Ok(metadata)
}

pub(super) async fn runtime_with_assignment_inclusion()
-> anyhow::Result<std::sync::Arc<StateRuntime>> {
    let runtime = runtime_with_assignment_command().await?;
    persist_initial_assignment_inclusion(&runtime).await?;
    Ok(runtime)
}

pub(super) fn followup_command(expected_revision: u64) -> RecordCoordinationCommand {
    let mut reservation = reserve_params();
    reservation.context = context(
        CoordinationSemanticSlot::AssignmentRequested,
        "019f7c6c-1111-7000-8000-000000000703",
        FOLLOWUP_OPERATION,
        false,
        expected_revision,
        Vec::new(),
    );
    reservation.reservation = AssignmentReservation::Followup {
        expected_owner_thread_id: thread(ROOT),
        expected_owner_turn_id: turn("turn-a"),
        expected_head_version: 1,
    };
    reservation.operation_id =
        CoordinationOperationId::parse(FOLLOWUP_OPERATION).expect("operation");
    reservation.target_principal = target(2).principal;
    RecordCoordinationCommand::new(
        CoordinationCommandIntent::Assignment { reservation },
        CommandCiphertext::new(vec![0xB6; 384]).expect("ciphertext"),
    )
    .expect("followup command")
}

pub(super) fn message_command(expected_revision: u64) -> RecordCoordinationCommand {
    RecordCoordinationCommand::new(
        CoordinationCommandIntent::Message {
            context: context(
                CoordinationSemanticSlot::MessageSubmissionRecorded,
                "019f7c6c-1111-7000-8000-000000000704",
                MESSAGE_OPERATION,
                false,
                expected_revision,
                Vec::new(),
            ),
            operation_id: CoordinationOperationId::parse(MESSAGE_OPERATION).expect("operation"),
            target: target(1),
            content: ContentEvidence::Unavailable {
                reason: UnavailableReason::EncryptedPayload,
            },
            encoded_payload_bytes: EncodedPayloadBytes::new(4).expect("bytes"),
        },
        CommandCiphertext::new(vec![1, 2, 3, 4]).expect("ciphertext"),
    )
    .expect("message command")
}

pub(super) fn interrupt_command(expected_revision: u64) -> RecordCoordinationCommand {
    RecordCoordinationCommand::new(
        CoordinationCommandIntent::Interrupt {
            context: context(
                CoordinationSemanticSlot::InterruptRequested,
                "019f7c6c-1111-7000-8000-000000000705",
                INTERRUPT_OPERATION,
                false,
                expected_revision,
                Vec::new(),
            ),
            operation_id: CoordinationOperationId::parse(INTERRUPT_OPERATION).expect("operation"),
            target: target(1),
        },
        CommandCiphertext::new(Vec::new()).expect("ciphertext"),
    )
    .expect("interrupt command")
}

pub(super) fn inference_attempt(value: &str) -> BoundedId<MAX_ID_BYTES> {
    BoundedId::new(value).expect("inference attempt")
}

pub(super) fn event_id(value: &str) -> CoordinationEventId {
    CoordinationEventId::parse(value).expect("event")
}
