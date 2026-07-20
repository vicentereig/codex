use std::sync::Arc;

use codex_coordination::CoordinationFailureCode;

use super::aggregate_journal::AggregateStep;
use super::commands_tests::assignment_command;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashInjector;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::inbox::InboxWriteError;
use super::inbox_test_support::CLAIM_OPERATION_ONE;
use super::inbox_test_support::claim_operation;
use super::inbox_test_support::inference_attempt;
use crate::StateRuntime;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::*;
use crate::runtime::test_support::unique_temp_dir;

pub(super) const NOW_MS: i64 = 1_753_000_000_000;
const LEASE_MS: i64 = 1_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum InboxCase {
    Claim,
    FirstSelection,
    TransportSucceeded,
    TransportFailed,
    TransportUnknown,
    ReclaimLease,
    ExpirePayload,
}

pub(super) const CASES: [InboxCase; 7] = [
    InboxCase::Claim,
    InboxCase::FirstSelection,
    InboxCase::TransportSucceeded,
    InboxCase::TransportFailed,
    InboxCase::TransportUnknown,
    InboxCase::ReclaimLease,
    InboxCase::ExpirePayload,
];

#[derive(Clone)]
pub(super) struct InboxInput {
    operation: InboxOperation,
    pub(super) ack: CommittedReceiptAck,
    pub(super) ciphertext: Vec<u8>,
}

#[derive(Clone)]
enum InboxOperation {
    Claim(ClaimInboxReceipt),
    Selection(RecordInboxSelection),
    Transport(RecordInboxTransportOutcome),
    Reclaim(InboxMaintenanceBatch),
    Expire(InboxMaintenanceBatch),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum InboxOutput {
    Claim(ClaimInboxReceiptOutcome),
    Selection(RecordInboxSelectionOutcome),
    Transport(RecordInboxTransportOutcomeResult),
    Maintenance(InboxMaintenanceOutcome),
}

impl InboxCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, InboxInput)> {
        let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
        let command_clock = CrashInjector::recording(NOW_MS);
        assert!(matches!(
            runtime
                .record_coordination_command_intent_with(assignment_command(), &command_clock)
                .await?,
            RecordCoordinationCommandOutcome::Applied(_)
        ));
        let receipt_clock = CrashInjector::recording(NOW_MS + 1);
        let PersistRecipientReceiptOutcome::Applied(receipt) = runtime
            .persist_coordination_recipient_receipt_with(
                receipt_params_for_matrix(),
                &receipt_clock,
            )
            .await?
        else {
            anyhow::bail!("{self:?}: receipt setup was not applied");
        };
        let ack = runtime
            .coordination_durable_receipt_ack(receipt.receipt_id)
            .await?;
        let ciphertext =
            sqlx::query_scalar("SELECT ciphertext FROM coordination_inbox WHERE receipt_id=?")
                .bind(receipt.receipt_id.to_string())
                .fetch_one(&*runtime.pool)
                .await?;
        let now_ms = NOW_MS + 2;
        let needs_claim = !matches!(self, Self::Claim | Self::ExpirePayload);
        let lease = if needs_claim {
            let lease_expires_at_ms = if matches!(self, Self::ReclaimLease) {
                now_ms + 100
            } else {
                now_ms + LEASE_MS
            };
            let ClaimInboxReceiptOutcome::Claimed(claimed) = runtime
                .claim_coordination_receipt_for_inclusion(ClaimInboxReceipt {
                    receipt_id: receipt.receipt_id,
                    claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
                    expected_version: 0,
                    expected_lease_epoch: 0,
                    now_ms,
                    lease_expires_at_ms,
                })
                .await?
            else {
                anyhow::bail!("{self:?}: claim setup failed");
            };
            Some(claimed.lease)
        } else {
            None
        };
        let needs_selection = matches!(
            self,
            Self::TransportSucceeded | Self::TransportFailed | Self::TransportUnknown
        );
        let selection = if needs_selection {
            let RecordInboxSelectionOutcome::Applied(selection) = runtime
                .record_coordination_inclusion_selection(RecordInboxSelection {
                    lease: lease.clone().expect("selection lease"),
                    inference_attempt_id: inference_attempt("matrix-attempt"),
                    event_context: None,
                    selected_at_ms: now_ms + 1,
                })
                .await?
            else {
                anyhow::bail!("{self:?}: selection setup failed");
            };
            Some(selection)
        } else {
            None
        };
        let operation = match self {
            Self::Claim => InboxOperation::Claim(ClaimInboxReceipt {
                receipt_id: receipt.receipt_id,
                claim_operation_id: claim_operation(CLAIM_OPERATION_ONE),
                expected_version: 0,
                expected_lease_epoch: 0,
                now_ms,
                lease_expires_at_ms: now_ms + LEASE_MS,
            }),
            Self::FirstSelection => InboxOperation::Selection(RecordInboxSelection {
                lease: lease.expect("first-selection lease"),
                inference_attempt_id: inference_attempt("matrix-attempt"),
                event_context: None,
                selected_at_ms: now_ms + 1,
            }),
            Self::TransportSucceeded | Self::TransportFailed | Self::TransportUnknown => {
                let resolution = match self {
                    Self::TransportSucceeded => InboxTransportResolution::SendSucceeded,
                    Self::TransportFailed => InboxTransportResolution::SendFailed {
                        code: CoordinationFailureCode::TargetUnavailable,
                        retry_at_ms: now_ms + 4,
                    },
                    Self::TransportUnknown => InboxTransportResolution::SendUnknown {
                        retry_at_ms: now_ms + 4,
                    },
                    Self::Claim
                    | Self::FirstSelection
                    | Self::ReclaimLease
                    | Self::ExpirePayload => unreachable!("non-transport case"),
                };
                InboxOperation::Transport(RecordInboxTransportOutcome {
                    selection: selection.expect("transport selection").token,
                    resolution,
                    completed_at_ms: now_ms + 2,
                })
            }
            Self::ReclaimLease => InboxOperation::Reclaim(InboxMaintenanceBatch {
                now_ms: now_ms + 100,
                limit: 16,
            }),
            Self::ExpirePayload => InboxOperation::Expire(InboxMaintenanceBatch {
                now_ms: receipt.expires_at_ms,
                limit: 16,
            }),
        };
        Ok((
            runtime,
            InboxInput {
                operation,
                ack,
                ciphertext,
            },
        ))
    }

    pub(super) async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &InboxInput,
        injector: &dyn InboxFailureInjector,
    ) -> Result<InboxOutput, InboxWriteError> {
        match &input.operation {
            InboxOperation::Claim(params) => runtime
                .claim_coordination_receipt_for_inclusion_with(params.clone(), injector)
                .await
                .map(InboxOutput::Claim),
            InboxOperation::Selection(params) => runtime
                .record_coordination_inclusion_selection_with(params.clone(), injector)
                .await
                .map(InboxOutput::Selection),
            InboxOperation::Transport(params) => runtime
                .record_coordination_inbox_transport_outcome_with(params.clone(), injector)
                .await
                .map(InboxOutput::Transport),
            InboxOperation::Reclaim(params) => runtime
                .reclaim_expired_coordination_inbox_leases_with(params.clone(), injector)
                .await
                .map(InboxOutput::Maintenance),
            InboxOperation::Expire(params) => runtime
                .expire_coordination_inbox_payloads_with(params.clone(), injector)
                .await
                .map(InboxOutput::Maintenance),
        }
    }

    pub(super) fn expected_trace(self) -> Vec<CrashPoint> {
        let mut boundaries = vec![
            Boundary::Inbox(InboxStep::TransactionBegin),
            Boundary::Aggregate(AggregateStep::AuthorityRead),
        ];
        match self {
            Self::Claim => boundaries.push(Boundary::Inbox(InboxStep::ClaimUpdate)),
            Self::FirstSelection => boundaries.extend([
                Boundary::Inbox(InboxStep::SelectionInsert),
                Boundary::Inbox(InboxStep::InboxUpdate),
            ]),
            Self::TransportSucceeded | Self::TransportFailed | Self::TransportUnknown => {
                boundaries.extend([
                    Boundary::Inbox(InboxStep::SelectionUpdate),
                    Boundary::Inbox(InboxStep::InboxUpdate),
                ]);
            }
            Self::ReclaimLease | Self::ExpirePayload => boundaries.extend([
                Boundary::Inbox(InboxStep::MaintenanceRead),
                Boundary::Inbox(InboxStep::MaintenanceUpdate),
            ]),
        }
        boundaries.extend([
            Boundary::Aggregate(AggregateStep::BeforeCommit),
            Boundary::Aggregate(AggregateStep::AfterCommit),
        ]);
        counted(&boundaries)
    }

    pub(super) fn stable_output(self, successful: &InboxOutput) -> InboxOutput {
        match (self, successful) {
            (Self::Claim, output) => output.clone(),
            (
                Self::FirstSelection,
                InboxOutput::Selection(RecordInboxSelectionOutcome::Applied(value)),
            ) => InboxOutput::Selection(RecordInboxSelectionOutcome::Duplicate(value.clone())),
            (
                Self::TransportSucceeded | Self::TransportFailed | Self::TransportUnknown,
                InboxOutput::Transport(RecordInboxTransportOutcomeResult::Applied(value)),
            ) => {
                InboxOutput::Transport(RecordInboxTransportOutcomeResult::Duplicate(value.clone()))
            }
            (Self::ReclaimLease | Self::ExpirePayload, InboxOutput::Maintenance(_)) => {
                InboxOutput::Maintenance(InboxMaintenanceOutcome {
                    changed_receipts: Vec::new(),
                })
            }
            _ => panic!("{self:?}: unexpected successful output {successful:?}"),
        }
    }

    pub(super) fn keeps_ciphertext(self) -> bool {
        !matches!(self, Self::ExpirePayload)
    }
}

pub(super) fn assert_success(case: InboxCase, output: &InboxOutput) {
    let successful = match (case, output) {
        (InboxCase::Claim, InboxOutput::Claim(ClaimInboxReceiptOutcome::Claimed(_)))
        | (
            InboxCase::FirstSelection,
            InboxOutput::Selection(RecordInboxSelectionOutcome::Applied(_)),
        )
        | (
            InboxCase::TransportSucceeded
            | InboxCase::TransportFailed
            | InboxCase::TransportUnknown,
            InboxOutput::Transport(RecordInboxTransportOutcomeResult::Applied(_)),
        ) => true,
        (
            InboxCase::ReclaimLease | InboxCase::ExpirePayload,
            InboxOutput::Maintenance(InboxMaintenanceOutcome { changed_receipts }),
        ) => changed_receipts.len() == 1,
        _ => false,
    };
    assert!(successful, "{case:?}: {output:?}");
}

fn counted(boundaries: &[Boundary]) -> Vec<CrashPoint> {
    boundaries
        .iter()
        .enumerate()
        .map(|(index, boundary)| CrashPoint {
            boundary: *boundary,
            occurrence: boundaries[..index]
                .iter()
                .filter(|candidate| *candidate == boundary)
                .count()
                + 1,
        })
        .collect()
}
