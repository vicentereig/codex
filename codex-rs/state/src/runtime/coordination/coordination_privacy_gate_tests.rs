use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::EncodedPayloadBytes;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::StateEpoch;
use pretty_assertions::assert_eq;

use super::aggregate_test_support::*;
use super::commands::CommandWriteError;
use super::degradation::record_exogenous_terminal_degradation;
use super::degradation_outbox::claim_degradation_publications;
use super::degradation_outbox::resolve_degradation_publication;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::assert_frozen_non_ciphertext_excludes;
use super::failure_injection_support::frozen_state;
use super::inbox_test_support::*;
use super::legacy_checkpoints::advance_legacy_scan_checkpoint;
use super::legacy_links::record_legacy_link;
use super::recovery::RecoveryDisposition;
use super::recovery_batch::recover_coordination_batch;
use super::recovery_test_support::*;
use crate::model::coordination::AcceptAssignment;
use crate::model::coordination_commands::CommandCiphertext;
use crate::model::coordination_commands::CoordinationCommandIntent;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::ClaimInboxReceipt;
use crate::model::coordination_inbox::ClaimInboxReceiptOutcome;
use crate::model::coordination_inbox::RecordInboxSelection;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::TerminalEvidenceKind;
use crate::model::coordination_recovery::TerminalEvidenceOutcome;
use crate::model::coordination_recovery::TerminalProvenance;
use crate::model::coordination_recovery_state::ClaimDegradationPublications;
use crate::model::coordination_recovery_state::ClaimDegradationPublicationsOutcome;
use crate::model::coordination_recovery_state::DegradationPublicationResolution;
use crate::model::coordination_recovery_state::LegacyScanPage;
use crate::model::coordination_recovery_state::ResolveDegradationPublication;
use crate::model::coordination_recovery_state::ResolveDegradationPublicationOutcome;

const PLAINTEXT_LOOKING_SENTINEL: &[u8] = b"PRIVATE_PLAINTEXT_LOOKING_SENTINEL";
const ENCODED_CIPHERTEXT_SENTINEL: &[u8] = b"PRIVATE_ENCODED_CIPHERTEXT_SENTINEL";

#[tokio::test]
async fn coordination_privacy_gate_keeps_payload_bytes_off_durable_and_diagnostic_surfaces()
-> anyhow::Result<()> {
    let runtime = crate::StateRuntime::init(
        crate::runtime::test_support::unique_temp_dir(),
        "test".to_string(),
    )
    .await?;
    let mut command = super::commands_tests::assignment_command();
    command.ciphertext = CommandCiphertext::new(ciphertext(PLAINTEXT_LOOKING_SENTINEL))?;
    let RecordCoordinationCommandOutcome::Applied(command_metadata) = runtime
        .record_coordination_command_intent(command.clone())
        .await?
    else {
        anyhow::bail!("assignment command was not applied")
    };
    runtime
        .accept_coordination_assignment(AcceptAssignment {
            context: context(
                CoordinationSemanticSlot::AssignmentAccepted,
                "019f7c6c-1111-7000-8000-000000000702",
                OPERATION,
                true,
                1,
                Vec::new(),
            ),
            assignment_id: command_metadata.target.assignment_id,
            generation: generation(1),
            receipt_id: codex_coordination::ReceiptId::parse(RECEIPT_ONE)?,
            bound_turn_id: codex_coordination::Evidence::Known {
                value: turn("turn-b"),
            },
            expected_owner_thread_id: thread(ROOT),
            expected_owner_turn_id: turn("turn-a"),
            expected_head_version: 0,
        })
        .await?;
    let receipt_command = payload_command(2, MESSAGE_OPERATION, ENCODED_CIPHERTEXT_SENTINEL)?;
    let RecordCoordinationCommandOutcome::Applied(_) = runtime
        .record_coordination_command_intent(receipt_command.clone())
        .await?
    else {
        anyhow::bail!("message command was not applied")
    };
    let receipt = runtime
        .persist_coordination_recipient_receipt(receipt_params(
            MESSAGE_OPERATION,
            RECEIPT_TWO,
            "019f7c6c-1111-7000-8000-000000000706",
            3,
            1,
            Vec::new(),
        ))
        .await?;
    let metadata = match receipt {
        crate::model::coordination_inbox::PersistRecipientReceiptOutcome::Applied(metadata) => {
            metadata
        }
        other => anyhow::bail!("unexpected receipt result: {other:?}"),
    };
    let ack = runtime
        .coordination_durable_receipt_ack(metadata.receipt_id)
        .await?;
    let ClaimInboxReceiptOutcome::Claimed(claim) = runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id: metadata.receipt_id,
            claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: metadata.expires_at_ms - 2,
            lease_expires_at_ms: metadata.expires_at_ms - 1,
        })
        .await?
    else {
        anyhow::bail!("receipt was not claimed")
    };
    let selection = runtime
        .record_coordination_inclusion_selection(RecordInboxSelection {
            lease: claim.lease.clone(),
            inference_attempt_id: inference_attempt("privacy-attempt"),
            event_context: None,
            selected_at_ms: metadata.expires_at_ms - 1,
        })
        .await?;

    let injected = runtime
        .record_coordination_command_intent_with(
            receipt_command.clone(),
            &CrashInjector::fail_at(
                CrashPoint {
                    boundary: Boundary::Command(super::commands::CommandStep::TransactionBegin),
                    occurrence: 1,
                },
                metadata.expires_at_ms,
            ),
        )
        .await
        .expect_err("injected command failure");
    assert!(matches!(injected, CommandWriteError::Internal(_)));

    let before_purge = frozen_state(&runtime, FrozenStateInputs::new(runtime.codex_home())).await?;
    assert_private(
        &before_purge,
        &[PLAINTEXT_LOOKING_SENTINEL, ENCODED_CIPHERTEXT_SENTINEL],
        [
            format!("{command:?}"),
            format!("{receipt_command:?}"),
            format!("{command_metadata:?}"),
            format!("{metadata:?}"),
            format!("{ack:?}"),
            format!("{claim:?}"),
            format!("{selection:?}"),
            format!("{injected:?}"),
            format!("{before_purge:?}"),
        ],
    );
    assert_live_ciphertext(
        &runtime,
        PLAINTEXT_LOOKING_SENTINEL,
        ENCODED_CIPHERTEXT_SENTINEL,
    )
    .await?;

    let recovered = recover_coordination_batch(&runtime.pool, metadata.expires_at_ms, 100).await?;
    assert!(
        recovered
            .dispositions
            .contains(&RecoveryDisposition::CommandPayloadExpired)
    );
    assert!(
        recovered
            .dispositions
            .contains(&RecoveryDisposition::InboxPayloadExpired)
    );
    let after_purge = frozen_state(&runtime, FrozenStateInputs::new(runtime.codex_home())).await?;
    assert_private(
        &after_purge,
        &[PLAINTEXT_LOOKING_SENTINEL, ENCODED_CIPHERTEXT_SENTINEL],
        [format!("{recovered:?}"), format!("{after_purge:?}")],
    );
    assert_no_live_ciphertext(&runtime).await?;

    exercise_recovery_storage_surfaces(&runtime).await?;
    Ok(())
}

fn payload_command(
    expected_revision: u64,
    operation_id: &str,
    sentinel: &[u8],
) -> anyhow::Result<crate::model::coordination_commands::RecordCoordinationCommand> {
    let mut command = if operation_id == MESSAGE_OPERATION {
        message_command(expected_revision)
    } else {
        followup_command(expected_revision)
    };
    let ciphertext = ciphertext(sentinel);
    if let CoordinationCommandIntent::Message {
        encoded_payload_bytes,
        ..
    } = &mut command.intent
    {
        *encoded_payload_bytes = EncodedPayloadBytes::new(ciphertext.len() as u32)?;
    }
    command.ciphertext = CommandCiphertext::new(ciphertext)?;
    Ok(command)
}

fn ciphertext(sentinel: &[u8]) -> Vec<u8> {
    let mut ciphertext = vec![0xA5; 384];
    ciphertext[..sentinel.len()].copy_from_slice(sentinel);
    ciphertext
}

async fn assert_live_ciphertext(
    runtime: &crate::StateRuntime,
    command_sentinel: &[u8],
    receipt_sentinel: &[u8],
) -> anyhow::Result<()> {
    let command: Vec<u8> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_commands WHERE operation_id=?")
            .bind(OPERATION)
            .fetch_one(&*runtime.pool)
            .await?;
    let receipt: Vec<u8> =
        sqlx::query_scalar("SELECT ciphertext FROM coordination_inbox WHERE receipt_id=?")
            .bind(RECEIPT_TWO)
            .fetch_one(&*runtime.pool)
            .await?;
    assert!(
        command
            .windows(command_sentinel.len())
            .any(|bytes| bytes == command_sentinel)
    );
    assert!(
        receipt
            .windows(receipt_sentinel.len())
            .any(|bytes| bytes == receipt_sentinel)
    );
    Ok(())
}

async fn assert_no_live_ciphertext(runtime: &crate::StateRuntime) -> anyhow::Result<()> {
    let remaining: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM coordination_commands WHERE ciphertext IS NOT NULL \
         UNION ALL SELECT COUNT(*) FROM coordination_inbox WHERE ciphertext IS NOT NULL",
    )
    .fetch_all(&*runtime.pool)
    .await?
    .into_iter()
    .sum();
    assert_eq!(remaining, 0);
    Ok(())
}

async fn exercise_recovery_storage_surfaces(runtime: &crate::StateRuntime) -> anyhow::Result<()> {
    let epoch = StateEpoch::parse(
        &sqlx::query_scalar::<_, String>(
            "SELECT state_epoch FROM coordination_authority WHERE singleton_id=1",
        )
        .fetch_one(&*runtime.pool)
        .await?,
    )?;
    let root = thread_id(super::aggregate_test_support::ROOT);
    let event = compatibility_event(CoordinationSemanticSlot::LegacyInteractionObserved, 11);
    let link = CheckedLegacyLink::new(root, epoch, &event)?;
    let linked = record_legacy_link(&runtime.pool, &link).await?;
    let terminal_event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, 12);
    let observation = ExogenousTerminalObservation {
        root_thread_id: root,
        captured_state_epoch: Some(epoch),
        provenance: TerminalProvenance::Known(LegacySourceIdentity::from_event(&terminal_event)?),
        target_thread_id: thread_id(CHILD),
        target_turn_id: BoundedId::<MAX_ID_BYTES>::new("turn-b")?,
        terminal_kind: TerminalEvidenceKind::Completed,
        terminal_outcome: TerminalEvidenceOutcome::Succeeded,
        included_generations: codex_coordination::Evidence::Known {
            value: vec![codex_coordination::AssignmentGeneration::new(1)?],
        },
        observed_at: 20,
        after_revision: 0,
    };
    let observed =
        record_exogenous_terminal_degradation(&runtime.pool, observation.clone()).await?;
    let page = LegacyScanPage {
        root_thread_id: root,
        expected_state_epoch: epoch,
        source_thread_id: root,
        expected_version: 0,
        expected_prefix_fingerprint: None,
        next_physical_ordinal: 1,
        scanned_prefix_fingerprint: [1; 32],
        last_order: None,
        complete: false,
        links: Vec::new(),
        degradations: Vec::new(),
        now_ms: 21,
    };
    let checkpoint = advance_legacy_scan_checkpoint(&runtime.pool, &page).await?;
    let now_ms: i64 = sqlx::query_scalar(
        "SELECT MAX(updated_at_ms)+1 FROM coordination_degradation_publication_outbox",
    )
    .fetch_one(&*runtime.pool)
    .await?;
    let claim_params = ClaimDegradationPublications {
        root_thread_id: root,
        expected_state_epoch: epoch,
        now_ms,
        lease_expires_at_ms: now_ms + 1_000,
        limit: 10,
    };
    let ClaimDegradationPublicationsOutcome::Claimed(leases) =
        claim_degradation_publications(&runtime.pool, &claim_params).await?
    else {
        anyhow::bail!("degradation publication was deferred")
    };
    let resolve_params = ResolveDegradationPublication {
        lease: leases[0].clone(),
        expected_state_epoch: epoch,
        resolution: DegradationPublicationResolution::Materialized,
        now_ms: now_ms + 1,
    };
    let resolution = resolve_degradation_publication(&runtime.pool, &resolve_params).await?;
    assert!(matches!(
        resolution,
        ResolveDegradationPublicationOutcome::Applied(_)
    ));
    let frozen = frozen_state(&runtime, FrozenStateInputs::new(runtime.codex_home())).await?;
    assert_private(
        &frozen,
        &[PLAINTEXT_LOOKING_SENTINEL, ENCODED_CIPHERTEXT_SENTINEL],
        [
            format!("{link:?}"),
            format!("{linked:?}"),
            format!("{observation:?}"),
            format!("{observed:?}"),
            format!("{page:?}"),
            format!("{checkpoint:?}"),
            format!("{claim_params:?}"),
            format!("{leases:?}"),
            format!("{resolve_params:?}"),
            format!("{resolution:?}"),
            format!("{frozen:?}"),
        ],
    );
    Ok(())
}

fn assert_private<const N: usize>(
    frozen: &super::failure_injection_support::FrozenCoordinationState,
    sentinels: &[&[u8]],
    rendered: [String; N],
) {
    assert_frozen_non_ciphertext_excludes(frozen, sentinels);
    for rendered in rendered {
        for sentinel in sentinels {
            assert!(
                !rendered
                    .as_bytes()
                    .windows(sentinel.len())
                    .any(|bytes| bytes == *sentinel)
            );
        }
    }
}
