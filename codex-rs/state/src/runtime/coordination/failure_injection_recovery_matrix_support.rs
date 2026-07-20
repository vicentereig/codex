use std::sync::Arc;

use codex_coordination::CoordinationOperationId;
use codex_coordination::CoordinationSemanticSlot;

use super::commands_tests::accepted_runtime;
use super::commands_tests::assignment_command;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::inbox_test_support::*;
use super::recovery::RecoveryBatch;
use super::recovery::RecoveryDisposition;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_batch::recover_coordination_batch_with;
use crate::StateRuntime;
use crate::model::coordination_commands::*;
use crate::model::coordination_inbox::*;
use crate::runtime::test_support::unique_temp_dir;

pub(super) const NOW_MS: i64 = 2_000_000_000_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum BatchCase {
    MessagePoison,
    InterruptExpire,
    AssignmentPoisonStrand,
    AssignmentReclaim,
    PendingAssignment,
    StrandedAssignment,
    InboxExpireReceipts,
    InboxReclaimReceipts,
}
pub(super) const BATCH_CASES: [BatchCase; 8] = [
    BatchCase::InboxReclaimReceipts,
    BatchCase::MessagePoison,
    BatchCase::InterruptExpire,
    BatchCase::AssignmentPoisonStrand,
    BatchCase::AssignmentReclaim,
    BatchCase::PendingAssignment,
    BatchCase::StrandedAssignment,
    BatchCase::InboxExpireReceipts,
];

#[derive(Clone)]
pub(super) struct BatchInput {
    pub(super) now_ms: i64,
    pub(super) limit: u32,
    pub(super) command_ciphertexts: Vec<(String, Vec<u8>)>,
    pub(super) inbox_ciphertexts: Vec<(String, Vec<u8>)>,
    pub(super) acks: Vec<CommittedReceiptAck>,
}

impl BatchCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, BatchInput)> {
        let (runtime, target) = match self {
            Self::MessagePoison => {
                let runtime = accepted_runtime().await?.runtime;
                let target = record_realtime(&runtime, message_command(2)).await?;
                (runtime, target)
            }
            Self::InterruptExpire => {
                let runtime = accepted_runtime().await?.runtime;
                let target = record_realtime(&runtime, interrupt_command(2)).await?;
                (runtime, target)
            }
            Self::InboxExpireReceipts | Self::InboxReclaimReceipts => {
                let runtime = accepted_runtime().await?.runtime;
                let target = record_realtime(&runtime, assignment_command()).await?;
                (runtime, target)
            }
            _ => {
                let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
                let target = record(&runtime, assignment_command()).await?;
                (runtime, target)
            }
        };
        let mut acks = Vec::new();
        let limit = match self {
            Self::AssignmentPoisonStrand | Self::InboxExpireReceipts => 2,
            _ => 1,
        };
        let mut recovery_now = target.retry_after_ms;
        match self {
            Self::MessagePoison | Self::AssignmentPoisonStrand => {
                attempted(&runtime, &target, target.retry_after_ms).await?;
                recovery_now = target.retry_after_ms + 50;
            }
            Self::InterruptExpire => {
                let assignment_expires: i64 = sqlx::query_scalar(
                    "SELECT expires_at_ms FROM coordination_commands WHERE operation_id=?",
                )
                .bind(super::aggregate_test_support::OPERATION)
                .fetch_one(&*runtime.pool)
                .await?;
                super::recovery_batch::recover_coordination_batch(
                    &runtime.pool,
                    assignment_expires,
                    1,
                )
                .await?;
                recovery_now = target.expires_at_ms;
            }
            Self::AssignmentReclaim => {
                claim(&runtime, &target, target.retry_after_ms).await?;
                recovery_now = target.retry_after_ms + 50;
            }
            Self::PendingAssignment => {}
            Self::StrandedAssignment => {
                recovery_now = target.expires_at_ms;
                super::recovery_batch::recover_coordination_batch(&runtime.pool, recovery_now, 1)
                    .await?;
            }
            Self::InboxExpireReceipts | Self::InboxReclaimReceipts => {
                record_realtime(&runtime, message_command(2)).await?;
                record_realtime(&runtime, interrupt_command(3)).await?;
                let specs = [
                    (
                        MESSAGE_OPERATION,
                        "019f7c6c-1111-7000-8000-000000000212",
                        "019f7c6c-1111-7000-8000-000000000713",
                    ),
                    (
                        INTERRUPT_OPERATION,
                        "019f7c6c-1111-7000-8000-000000000213",
                        "019f7c6c-1111-7000-8000-000000000714",
                    ),
                ];
                let receipt_count = if matches!(self, Self::InboxExpireReceipts) {
                    2
                } else {
                    1
                };
                for (index, (operation, receipt, event)) in
                    specs.into_iter().take(receipt_count).enumerate()
                {
                    let revision: i64 = sqlx::query_scalar(
                        "SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?",
                    )
                    .bind(super::aggregate_test_support::ROOT)
                    .fetch_one(&*runtime.pool)
                    .await?;
                    let head_version: i64 = sqlx::query_scalar(
                        "SELECT version FROM coordination_assignment_heads WHERE assignment_id=?",
                    )
                    .bind(super::aggregate_test_support::ASSIGNMENT)
                    .fetch_one(&*runtime.pool)
                    .await?;
                    let PersistRecipientReceiptOutcome::Applied(metadata) = runtime
                        .persist_coordination_recipient_receipt(receipt_params(
                            operation,
                            receipt,
                            event,
                            revision as u64,
                            head_version as u64,
                            Vec::new(),
                        ))
                        .await?
                    else {
                        anyhow::bail!("receipt setup")
                    };
                    acks.push(
                        runtime
                            .coordination_durable_receipt_ack(metadata.receipt_id)
                            .await?,
                    );
                    if matches!(self, Self::InboxExpireReceipts) {
                        recovery_now = metadata.expires_at_ms;
                    } else {
                        let claim_now: i64 = sqlx::query_scalar("SELECT MAX(updated_at_ms + 1,retry_after_ms) FROM coordination_inbox WHERE receipt_id=?").bind(metadata.receipt_id.to_string()).fetch_one(&*runtime.pool).await?;
                        let claimed =
                            claim_receipt(&runtime, metadata.receipt_id, index, claim_now).await?;
                        recovery_now = claim_now + 50;
                        let revision: i64 = sqlx::query_scalar("SELECT committed_revision FROM coordination_roots WHERE root_thread_id=?").bind(super::aggregate_test_support::ROOT).fetch_one(&*runtime.pool).await?;
                        runtime
                            .record_coordination_inclusion_selection(RecordInboxSelection {
                                lease: claimed,
                                inference_attempt_id: inference_attempt("recovery-matrix"),
                                event_context: Some(super::aggregate_test_support::context(
                                    CoordinationSemanticSlot::MessageIncludedInModelInput,
                                    "019f7c6c-1111-7000-8000-000000000715",
                                    MESSAGE_OPERATION,
                                    true,
                                    revision as u64,
                                    Vec::new(),
                                )),
                                selected_at_ms: claim_now + 1,
                            })
                            .await?;
                    }
                }
                if matches!(self, Self::InboxExpireReceipts) {
                    for _ in 0..3 {
                        super::recovery_batch::recover_coordination_batch(
                            &runtime.pool,
                            recovery_now,
                            1,
                        )
                        .await?;
                    }
                }
            }
        }
        let command_ciphertexts = command_ciphertexts(&runtime).await?;
        let inbox_ciphertexts = sqlx::query_as("SELECT receipt_id,ciphertext FROM coordination_inbox WHERE ciphertext IS NOT NULL ORDER BY receipt_id").fetch_all(&*runtime.pool).await?;
        Ok((
            runtime,
            BatchInput {
                now_ms: recovery_now,
                limit,
                command_ciphertexts,
                inbox_ciphertexts,
                acks,
            },
        ))
    }

    pub(super) async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &BatchInput,
        injector: &dyn RecoveryFailureInjector,
    ) -> Result<RecoveryBatch, RecoveryWriteError> {
        recover_coordination_batch_with(&runtime.pool, input.now_ms, input.limit, injector).await
    }

    pub(super) fn expected(self) -> Vec<RecoveryDisposition> {
        use RecoveryDisposition::*;
        match self {
            Self::MessagePoison => vec![CommandPoisoned],
            Self::InterruptExpire => vec![CommandPayloadExpired],
            Self::AssignmentPoisonStrand => vec![CommandPoisoned, AssignmentStranded],
            Self::AssignmentReclaim => vec![CommandLeaseReclaimed],
            Self::PendingAssignment => vec![],
            Self::StrandedAssignment => vec![AssignmentStranded],
            Self::InboxExpireReceipts => vec![InboxPayloadExpired; 2],
            Self::InboxReclaimReceipts => vec![InboxLeaseReclaimed],
        }
    }

    pub(super) fn trace(self) -> Vec<CrashPoint> {
        use RecoveryStep::*;
        let middle = match self {
            Self::MessagePoison => vec![
                RecoveryRead,
                RecoveryUpdate,
                AnchorRead,
                RecoveryRead,
                DegradationInsert,
                DegradationOutboxInsert,
            ],
            Self::InterruptExpire => vec![
                RecoveryRead,
                RecoveryRead,
                RecoveryUpdate,
                AnchorRead,
                RecoveryRead,
                DegradationInsert,
                DegradationOutboxInsert,
            ],
            Self::AssignmentPoisonStrand => vec![
                RecoveryRead,
                RecoveryUpdate,
                AnchorRead,
                RecoveryRead,
                DegradationInsert,
                DegradationOutboxInsert,
                RecoveryRead,
                RecoveryRead,
                AnchorRead,
                RecoveryRead,
                DegradationInsert,
                DegradationOutboxInsert,
            ],
            Self::AssignmentReclaim => {
                vec![RecoveryRead, RecoveryRead, RecoveryRead, RecoveryUpdate]
            }
            Self::PendingAssignment => vec![
                RecoveryRead,
                RecoveryRead,
                RecoveryRead,
                RecoveryUpdate,
                AuthorityRead,
                RecoveryRead,
                AuthorityRead,
                RecoveryRead,
            ],
            Self::StrandedAssignment => vec![
                RecoveryRead,
                RecoveryRead,
                RecoveryRead,
                AnchorRead,
                RecoveryRead,
                DegradationInsert,
                DegradationOutboxInsert,
            ],
            Self::InboxExpireReceipts => {
                let mut value = vec![
                    RecoveryRead,
                    RecoveryRead,
                    RecoveryRead,
                    RecoveryUpdate,
                    AuthorityRead,
                    RecoveryRead,
                    RecoveryUpdate,
                    RecoveryUpdate,
                ];
                for _ in 0..2 {
                    value.extend([
                        RecoveryRead,
                        AnchorRead,
                        RecoveryRead,
                        DegradationInsert,
                        DegradationOutboxInsert,
                    ]);
                }
                value
            }
            Self::InboxReclaimReceipts => vec![
                RecoveryRead,
                RecoveryRead,
                RecoveryRead,
                RecoveryUpdate,
                AuthorityRead,
                RecoveryRead,
                AuthorityRead,
                RecoveryRead,
                RecoveryUpdate,
                RecoveryUpdate,
            ],
        };
        recovery_trace(middle, /*root_authority*/ false)
    }
}

async fn record(
    runtime: &StateRuntime,
    command: RecordCoordinationCommand,
) -> anyhow::Result<CoordinationCommandMetadata> {
    match runtime
        .record_coordination_command_intent_with(command, &CrashInjector::recording(NOW_MS))
        .await?
    {
        RecordCoordinationCommandOutcome::Applied(value) => Ok(value),
        _ => anyhow::bail!("duplicate command"),
    }
}
async fn record_realtime(
    runtime: &StateRuntime,
    command: RecordCoordinationCommand,
) -> anyhow::Result<CoordinationCommandMetadata> {
    match runtime.record_coordination_command_intent(command).await? {
        RecordCoordinationCommandOutcome::Applied(value)
        | RecordCoordinationCommandOutcome::Duplicate(value) => Ok(value),
    }
}
async fn claim(
    runtime: &StateRuntime,
    metadata: &CoordinationCommandMetadata,
    now: i64,
) -> anyhow::Result<ClaimedCoordinationCommand> {
    match runtime
        .claim_coordination_command(metadata.operation_id, 0, 0, now, now + 50)
        .await?
    {
        ClaimCoordinationCommandOutcome::Claimed(value) => Ok(value),
        other => anyhow::bail!("claim failed: {other:?}"),
    }
}
async fn attempted(
    runtime: &StateRuntime,
    metadata: &CoordinationCommandMetadata,
    now: i64,
) -> anyhow::Result<()> {
    let claimed = claim(runtime, metadata, now).await?;
    runtime
        .begin_coordination_command_attempt(claimed.lease, now + 1)
        .await?;
    Ok(())
}
async fn claim_receipt(
    runtime: &StateRuntime,
    receipt_id: codex_coordination::ReceiptId,
    index: usize,
    now: i64,
) -> anyhow::Result<InboxLeaseToken> {
    let operation = CoordinationOperationId::parse(
        [
            CLAIM_OPERATION_ONE,
            CLAIM_OPERATION_TWO,
            "019f7c6c-1111-7000-8000-000000000303",
        ][index],
    )?;
    match runtime
        .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
            receipt_id,
            claim_operation_id: operation,
            expected_version: 0,
            expected_lease_epoch: 0,
            now_ms: now,
            lease_expires_at_ms: now + 50,
        })
        .await?
    {
        ClaimInboxReceiptOutcome::Claimed(value) => Ok(value.lease),
        other => anyhow::bail!("receipt claim: {other:?}"),
    }
}
async fn command_ciphertexts(runtime: &StateRuntime) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    Ok(sqlx::query_as("SELECT operation_id,ciphertext FROM coordination_commands WHERE ciphertext IS NOT NULL ORDER BY operation_id").fetch_all(&*runtime.pool).await?)
}

pub(super) fn recovery_trace(
    mut middle: Vec<RecoveryStep>,
    root_authority: bool,
) -> Vec<CrashPoint> {
    let mut steps = vec![
        RecoveryStep::TransactionBegin,
        RecoveryStep::MarkerRead,
        RecoveryStep::MarkerRead,
        RecoveryStep::AuthorityRead,
    ];
    if root_authority {
        steps.push(RecoveryStep::AuthorityRead);
    }
    steps.append(&mut middle);
    steps.extend([RecoveryStep::BeforeCommit, RecoveryStep::AfterCommit]);
    counted(
        &steps
            .into_iter()
            .map(Boundary::Recovery)
            .collect::<Vec<_>>(),
    )
}
pub(super) fn counted(boundaries: &[Boundary]) -> Vec<CrashPoint> {
    boundaries
        .iter()
        .enumerate()
        .map(|(index, boundary)| CrashPoint {
            boundary: *boundary,
            occurrence: boundaries[..index]
                .iter()
                .filter(|value| *value == boundary)
                .count()
                + 1,
        })
        .collect()
}
