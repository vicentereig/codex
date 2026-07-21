use std::sync::Arc;

use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashPoint;
use super::projection_outbox::claim_projection_publications;
use super::projection_outbox::claim_projection_publications_with;
use super::projection_outbox::resolve_projection_publication;
use super::projection_outbox::resolve_projection_publication_with;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::runtime_with_root;
use super::recovery_test_support::thread_id;
use crate::StateRuntime;
use crate::model::coordination_recovery_state::*;

pub(super) const NOW_MS: i64 = 2_000_000_000_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum ProjectionOutboxCase {
    Claim,
    Materialized,
    Retry,
    Poisoned,
    RetryExhausted,
}

fn recovery_trace(mut middle: Vec<RecoveryStep>) -> Vec<CrashPoint> {
    let mut steps = vec![
        RecoveryStep::TransactionBegin,
        RecoveryStep::MarkerRead,
        RecoveryStep::MarkerRead,
        RecoveryStep::AuthorityRead,
        RecoveryStep::AuthorityRead,
    ];
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

pub(super) const PROJECTION_OUTBOX_CASES: [ProjectionOutboxCase; 5] = [
    ProjectionOutboxCase::Claim,
    ProjectionOutboxCase::Materialized,
    ProjectionOutboxCase::Retry,
    ProjectionOutboxCase::Poisoned,
    ProjectionOutboxCase::RetryExhausted,
];

#[derive(Clone)]
pub(super) enum ProjectionOutboxInput {
    Claim(ClaimProjectionPublications),
    Resolve(ResolveProjectionPublication),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ProjectionOutboxOutput {
    Claim(ClaimProjectionPublicationsOutcome),
    Resolve(ResolveProjectionPublicationOutcome),
}

impl ProjectionOutboxCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, ProjectionOutboxInput)> {
        // `runtime_with_root` reserves a native assignment, leaving a single
        // pending native revision-1 projection publication with published=0.
        let (runtime, epoch) = runtime_with_root().await?;
        let claim = ClaimProjectionPublications {
            root_thread_id: thread_id(super::aggregate_test_support::ROOT),
            expected_state_epoch: epoch,
            now_ms: NOW_MS,
            lease_expires_at_ms: NOW_MS + 1_000,
            limit: 10,
        };
        if matches!(self, Self::Claim) {
            return Ok((runtime, ProjectionOutboxInput::Claim(claim)));
        }
        let ClaimProjectionPublicationsOutcome::Claimed(mut leases) =
            claim_projection_publications(&runtime.pool, &claim).await?
        else {
            anyhow::bail!("claim deferred")
        };
        let mut lease = leases.remove(0);
        if matches!(self, Self::RetryExhausted) {
            for index in 0..8 {
                let now_ms = NOW_MS + 1 + index * 10;
                let retry_after_ms = now_ms + 1;
                resolve_projection_publication(
                    &runtime.pool,
                    &ResolveProjectionPublication {
                        lease: lease.clone(),
                        expected_state_epoch: epoch,
                        resolution: ProjectionPublicationResolution::Retry { retry_after_ms },
                        now_ms,
                    },
                )
                .await?;
                let ClaimProjectionPublicationsOutcome::Claimed(mut claimed) =
                    claim_projection_publications(
                        &runtime.pool,
                        &ClaimProjectionPublications {
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
            Self::Materialized => ProjectionPublicationResolution::Materialized,
            Self::Retry | Self::RetryExhausted => ProjectionPublicationResolution::Retry {
                retry_after_ms: NOW_MS + 500,
            },
            Self::Poisoned => ProjectionPublicationResolution::Poisoned,
            Self::Claim => unreachable!(),
        };
        let now_ms = if matches!(self, Self::RetryExhausted) {
            NOW_MS + 82
        } else {
            NOW_MS + 1
        };
        Ok((
            runtime,
            ProjectionOutboxInput::Resolve(ResolveProjectionPublication {
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
        input: &ProjectionOutboxInput,
        injector: &dyn RecoveryFailureInjector,
    ) -> Result<ProjectionOutboxOutput, RecoveryWriteError> {
        match input {
            ProjectionOutboxInput::Claim(value) => {
                claim_projection_publications_with(&runtime.pool, value, injector)
                    .await
                    .map(ProjectionOutboxOutput::Claim)
            }
            ProjectionOutboxInput::Resolve(value) => {
                resolve_projection_publication_with(&runtime.pool, value, injector)
                    .await
                    .map(ProjectionOutboxOutput::Resolve)
            }
        }
    }

    pub(super) fn trace(self) -> Vec<CrashPoint> {
        let middle = match self {
            // A single R+1 row is read then leased.
            Self::Claim => vec![
                RecoveryStep::PublicationRead,
                RecoveryStep::PublicationUpdate,
            ],
            // Materialization additionally advances the published watermark.
            Self::Materialized => vec![
                RecoveryStep::PublicationRead,
                RecoveryStep::PublicationUpdate,
                RecoveryStep::PublicationUpdate,
            ],
            Self::Retry | Self::Poisoned | Self::RetryExhausted => {
                vec![
                    RecoveryStep::PublicationRead,
                    RecoveryStep::PublicationUpdate,
                ]
            }
        };
        recovery_trace(middle)
    }

    pub(super) fn stable(self, _output: &ProjectionOutboxOutput) -> ProjectionOutboxOutput {
        match self {
            Self::Claim => ProjectionOutboxOutput::Claim(
                ClaimProjectionPublicationsOutcome::Claimed(Vec::new()),
            ),
            Self::Retry => {
                ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Fenced)
            }
            Self::Materialized => {
                ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Terminal(
                    ProjectionPublicationStatus::Materialized,
                ))
            }
            Self::Poisoned | Self::RetryExhausted => {
                ProjectionOutboxOutput::Resolve(ResolveProjectionPublicationOutcome::Terminal(
                    ProjectionPublicationStatus::Poisoned,
                ))
            }
        }
    }
}
