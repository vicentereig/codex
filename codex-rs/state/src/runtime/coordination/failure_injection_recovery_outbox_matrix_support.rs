use std::sync::Arc;

use codex_coordination::BoundedId;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::MAX_ID_BYTES;

use super::degradation::record_exogenous_terminal_degradation;
use super::degradation_outbox::claim_degradation_publications_with;
use super::degradation_outbox::resolve_degradation_publication_with;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashPoint;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::CHILD;
use super::recovery_test_support::compatibility_event;
use super::recovery_test_support::runtime_with_root;
use super::recovery_test_support::thread_id;
use crate::StateRuntime;
use crate::model::coordination_recovery::*;
use crate::model::coordination_recovery_state::*;

pub(super) const NOW_MS: i64 = 2_000_000_000_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum OutboxCase {
    Claim,
    Materialized,
    Retry,
    Poisoned,
    RetryExhausted,
}

fn recovery_trace(mut middle: Vec<RecoveryStep>, root_authority: bool) -> Vec<CrashPoint> {
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
    let boundaries = steps
        .into_iter()
        .map(Boundary::Recovery)
        .collect::<Vec<_>>();
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
pub(super) const OUTBOX_CASES: [OutboxCase; 5] = [
    OutboxCase::Claim,
    OutboxCase::Materialized,
    OutboxCase::Retry,
    OutboxCase::Poisoned,
    OutboxCase::RetryExhausted,
];

#[derive(Clone)]
pub(super) enum OutboxInput {
    Claim(ClaimDegradationPublications),
    Resolve(ResolveDegradationPublication),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum OutboxOutput {
    Claim(ClaimDegradationPublicationsOutcome),
    Resolve(ResolveDegradationPublicationOutcome),
}

impl OutboxCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, OutboxInput)> {
        let (runtime, epoch) = runtime_with_root().await?;
        for ordinal in 31..=32 {
            let event = compatibility_event(CoordinationSemanticSlot::TurnCompleted, ordinal);
            record_exogenous_terminal_degradation(
                &runtime.pool,
                ExogenousTerminalObservation {
                    root_thread_id: thread_id(super::aggregate_test_support::ROOT),
                    captured_state_epoch: Some(epoch),
                    provenance: TerminalProvenance::Known(LegacySourceIdentity::from_event(
                        &event,
                    )?),
                    target_thread_id: thread_id(CHILD),
                    target_turn_id: BoundedId::<MAX_ID_BYTES>::new("turn-b")?,
                    terminal_kind: TerminalEvidenceKind::Completed,
                    terminal_outcome: TerminalEvidenceOutcome::Succeeded,
                    included_generations: codex_coordination::Evidence::Known {
                        value: vec![codex_coordination::AssignmentGeneration::new(1)?],
                    },
                    observed_at: NOW_MS - 100,
                    after_revision: 0,
                },
            )
            .await?;
        }
        let claim = ClaimDegradationPublications {
            root_thread_id: thread_id(super::aggregate_test_support::ROOT),
            expected_state_epoch: epoch,
            now_ms: NOW_MS,
            lease_expires_at_ms: NOW_MS + 1_000,
            limit: 10,
        };
        if matches!(self, Self::Claim) {
            return Ok((runtime, OutboxInput::Claim(claim)));
        }
        let ClaimDegradationPublicationsOutcome::Claimed(mut leases) =
            super::degradation_outbox::claim_degradation_publications(&runtime.pool, &claim)
                .await?
        else {
            anyhow::bail!("claim deferred")
        };
        let mut lease = leases.remove(0);
        if matches!(self, Self::RetryExhausted) {
            for index in 0..8 {
                let now_ms = NOW_MS + 1 + index * 10;
                let retry_after_ms = now_ms + 1;
                resolve_degradation_publication_with(
                    &runtime.pool,
                    &ResolveDegradationPublication {
                        lease,
                        expected_state_epoch: epoch,
                        resolution: DegradationPublicationResolution::Retry { retry_after_ms },
                        now_ms,
                    },
                    &super::recovery::NoRecoveryFailure,
                )
                .await?;
                let ClaimDegradationPublicationsOutcome::Claimed(mut claimed) =
                    super::degradation_outbox::claim_degradation_publications(
                        &runtime.pool,
                        &ClaimDegradationPublications {
                            root_thread_id: claim.root_thread_id,
                            expected_state_epoch: epoch,
                            now_ms: retry_after_ms,
                            lease_expires_at_ms: retry_after_ms + 1_000,
                            limit: 1,
                        },
                    )
                    .await?
                else {
                    anyhow::bail!("retry claim deferred")
                };
                lease = claimed.remove(0);
            }
        }
        let resolution = match self {
            Self::Materialized => DegradationPublicationResolution::Materialized,
            Self::Retry | Self::RetryExhausted => DegradationPublicationResolution::Retry {
                retry_after_ms: NOW_MS + 500,
            },
            Self::Poisoned => DegradationPublicationResolution::Poisoned,
            Self::Claim => unreachable!(),
        };
        let now_ms = if matches!(self, Self::RetryExhausted) {
            NOW_MS + 82
        } else {
            NOW_MS + 1
        };
        Ok((
            runtime,
            OutboxInput::Resolve(ResolveDegradationPublication {
                lease,
                expected_state_epoch: epoch,
                resolution,
                now_ms,
            }),
        ))
    }

    pub(super) async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &OutboxInput,
        injector: &dyn RecoveryFailureInjector,
    ) -> Result<OutboxOutput, RecoveryWriteError> {
        match input {
            OutboxInput::Claim(value) => {
                claim_degradation_publications_with(&runtime.pool, value, injector)
                    .await
                    .map(OutboxOutput::Claim)
            }
            OutboxInput::Resolve(value) => {
                resolve_degradation_publication_with(&runtime.pool, value, injector)
                    .await
                    .map(OutboxOutput::Resolve)
            }
        }
    }

    pub(super) fn trace(self) -> Vec<CrashPoint> {
        let middle = if matches!(self, Self::Claim) {
            vec![
                RecoveryStep::PublicationRead,
                RecoveryStep::PublicationUpdate,
                RecoveryStep::PublicationUpdate,
            ]
        } else {
            vec![
                RecoveryStep::PublicationRead,
                RecoveryStep::PublicationUpdate,
            ]
        };
        recovery_trace(middle, /*root_authority*/ true)
    }

    pub(super) fn stable(self, _output: &OutboxOutput) -> OutboxOutput {
        match self {
            Self::Claim => {
                OutboxOutput::Claim(ClaimDegradationPublicationsOutcome::Claimed(Vec::new()))
            }
            Self::Retry => OutboxOutput::Resolve(ResolveDegradationPublicationOutcome::Fenced),
            Self::Materialized => {
                OutboxOutput::Resolve(ResolveDegradationPublicationOutcome::Terminal(
                    DegradationPublicationStatus::Materialized,
                ))
            }
            Self::Poisoned | Self::RetryExhausted => {
                OutboxOutput::Resolve(ResolveDegradationPublicationOutcome::Terminal(
                    DegradationPublicationStatus::Poisoned,
                ))
            }
        }
    }
}
