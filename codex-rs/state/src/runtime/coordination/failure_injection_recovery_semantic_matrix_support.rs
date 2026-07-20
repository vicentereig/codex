use std::sync::Arc;

use codex_coordination::CoordinationEventId;
use codex_coordination::CoordinationSemanticSlot;

use super::aggregate_test_support::ROOT;
use super::aggregate_test_support::reserve_params;
use super::degradation::record_exogenous_terminal_degradation_with;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::CrashPoint;
use super::failure_injection_support::FrozenCoordinationState;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::frozen_state;
use super::failure_injection_tests::observation;
use super::legacy_checkpoints::advance_legacy_scan_checkpoint;
use super::legacy_checkpoints::advance_legacy_scan_checkpoint_with;
use super::legacy_links::correlate_legacy_link_with_native_with;
use super::legacy_links::record_legacy_link;
use super::legacy_links::record_legacy_link_with;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use super::recovery::RecoveryWriteError;
use super::recovery_test_support::compatibility_event;
use super::recovery_test_support::runtime_with_root;
use super::recovery_test_support::thread_id;
use crate::StateRuntime;
use crate::model::coordination_legacy_degradation::CheckedLegacyReductionDegradation;
use crate::model::coordination_recovery::CheckedLegacyLink;
use crate::model::coordination_recovery::DegradationReason;
use crate::model::coordination_recovery::ExogenousTerminalObservation;
use crate::model::coordination_recovery::LegacySourceIdentity;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::model::coordination_recovery::RecordLegacyLinkOutcome;
use crate::model::coordination_recovery_state::AdvanceLegacyScanOutcome;
use crate::model::coordination_recovery_state::LegacyScanPage;

pub(super) const NOW_MS: i64 = 1_753_000_200_000;

#[derive(Clone, Copy, Debug)]
pub(super) enum SemanticCase {
    ExogenousDegradation,
    LegacyLink,
    NativeCorrelation,
    CheckpointInsert,
    CheckpointUpdate,
    SourceChanged,
}

pub(super) const CASES: [SemanticCase; 6] = [
    SemanticCase::ExogenousDegradation,
    SemanticCase::LegacyLink,
    SemanticCase::NativeCorrelation,
    SemanticCase::CheckpointInsert,
    SemanticCase::CheckpointUpdate,
    SemanticCase::SourceChanged,
];

#[derive(Clone)]
pub(super) enum SemanticInput {
    Exogenous(ExogenousTerminalObservation),
    Link(CheckedLegacyLink),
    Correlate {
        link: CheckedLegacyLink,
        native_event_id: CoordinationEventId,
        suppressed_at_ms: i64,
    },
    Checkpoint(LegacyScanPage),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum SemanticOutput {
    Exogenous(RecordExogenousTerminalOutcome),
    Link(RecordLegacyLinkOutcome),
    Checkpoint(AdvanceLegacyScanOutcome),
}

impl SemanticCase {
    pub(super) async fn setup(self) -> anyhow::Result<(Arc<StateRuntime>, SemanticInput)> {
        let (runtime, epoch) = runtime_with_root().await?;
        let root = thread_id(ROOT);
        match self {
            Self::ExogenousDegradation => {
                Ok((runtime, SemanticInput::Exogenous(observation(epoch)?)))
            }
            Self::LegacyLink => {
                let event = compatibility_event(CoordinationSemanticSlot::AssignmentRequested, 7);
                Ok((
                    runtime,
                    SemanticInput::Link(CheckedLegacyLink::new(root, epoch, &event)?),
                ))
            }
            Self::NativeCorrelation => {
                let event = compatibility_event(CoordinationSemanticSlot::AssignmentRequested, 7);
                let link = CheckedLegacyLink::new(root, epoch, &event)?;
                record_legacy_link(&runtime.pool, &link).await?;
                Ok((
                    runtime,
                    SemanticInput::Correlate {
                        link,
                        native_event_id: reserve_params().context.primary.event_id,
                        suppressed_at_ms: NOW_MS,
                    },
                ))
            }
            Self::CheckpointInsert => Ok((
                runtime,
                SemanticInput::Checkpoint(checkpoint_page(epoch, CheckpointPage::Insert)?),
            )),
            Self::CheckpointUpdate | Self::SourceChanged => {
                let initial = checkpoint_page(epoch, CheckpointPage::Initial)?;
                assert!(matches!(
                    advance_legacy_scan_checkpoint(&runtime.pool, &initial).await?,
                    AdvanceLegacyScanOutcome::Advanced(_)
                ));
                let page = if matches!(self, Self::CheckpointUpdate) {
                    checkpoint_page(epoch, CheckpointPage::Update)?
                } else {
                    checkpoint_page(epoch, CheckpointPage::SourceChanged)?
                };
                Ok((runtime, SemanticInput::Checkpoint(page)))
            }
        }
    }

    pub(super) async fn invoke(
        self,
        runtime: &StateRuntime,
        input: &SemanticInput,
        injector: &dyn RecoveryFailureInjector,
    ) -> Result<SemanticOutput, RecoveryWriteError> {
        match (self, input) {
            (Self::ExogenousDegradation, SemanticInput::Exogenous(observation)) => {
                record_exogenous_terminal_degradation_with(
                    &runtime.pool,
                    observation.clone(),
                    injector,
                )
                .await
                .map(SemanticOutput::Exogenous)
            }
            (Self::LegacyLink, SemanticInput::Link(link)) => {
                record_legacy_link_with(&runtime.pool, link, injector)
                    .await
                    .map(SemanticOutput::Link)
            }
            (
                Self::NativeCorrelation,
                SemanticInput::Correlate {
                    link,
                    native_event_id,
                    suppressed_at_ms,
                },
            ) => correlate_legacy_link_with_native_with(
                &runtime.pool,
                link,
                *native_event_id,
                *suppressed_at_ms,
                injector,
            )
            .await
            .map(SemanticOutput::Link),
            (
                Self::CheckpointInsert | Self::CheckpointUpdate | Self::SourceChanged,
                SemanticInput::Checkpoint(page),
            ) => advance_legacy_scan_checkpoint_with(&runtime.pool, page, injector)
                .await
                .map(SemanticOutput::Checkpoint),
            _ => unreachable!("semantic case/input mismatch"),
        }
    }

    pub(super) fn success_trace(self) -> Vec<CrashPoint> {
        use RecoveryStep as S;
        let steps: &[RecoveryStep] = match self {
            Self::ExogenousDegradation => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::AnchorRead,
                S::LegacyRead,
                S::DegradationInsert,
                S::DegradationOutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::LegacyLink => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::AnchorRead,
                S::LegacyRead,
                S::LegacyInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::NativeCorrelation => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::AnchorRead,
                S::LegacyRead,
                S::LegacyRead,
                S::LegacyUpdate,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::CheckpointInsert => checkpoint_success_trace(S::CheckpointInsert),
            Self::CheckpointUpdate => checkpoint_success_trace(S::CheckpointUpdate),
            Self::SourceChanged => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::CheckpointRead,
                S::AnchorRead,
                S::LegacyRead,
                S::DegradationInsert,
                S::DegradationOutboxInsert,
                S::BeforeCommit,
                S::AfterCommit,
            ],
        };
        counted(steps)
    }

    pub(super) fn stable_trace(self) -> Vec<CrashPoint> {
        use RecoveryStep as S;
        let steps: &[RecoveryStep] = match self {
            Self::ExogenousDegradation => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::AnchorRead,
                S::LegacyRead,
                S::PublicationRead,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::LegacyLink | Self::NativeCorrelation => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::AnchorRead,
                S::LegacyRead,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::CheckpointInsert | Self::CheckpointUpdate => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::CheckpointRead,
                S::LegacyRead,
                S::PublicationRead,
                S::BeforeCommit,
                S::AfterCommit,
            ],
            Self::SourceChanged => &[
                S::TransactionBegin,
                S::MarkerRead,
                S::MarkerRead,
                S::AuthorityRead,
                S::AuthorityRead,
                S::CheckpointRead,
                S::AnchorRead,
                S::LegacyRead,
                S::PublicationRead,
                S::BeforeCommit,
                S::AfterCommit,
            ],
        };
        counted(steps)
    }

    pub(super) fn assert_success(self, output: &SemanticOutput) {
        assert!(
            matches!(
                (self, output),
                (
                    Self::ExogenousDegradation,
                    SemanticOutput::Exogenous(RecordExogenousTerminalOutcome::Applied(_))
                ) | (
                    Self::LegacyLink,
                    SemanticOutput::Link(RecordLegacyLinkOutcome::Linked(_))
                ) | (
                    Self::NativeCorrelation,
                    SemanticOutput::Link(RecordLegacyLinkOutcome::Suppressed(_, _))
                ) | (
                    Self::CheckpointInsert | Self::CheckpointUpdate,
                    SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::Advanced(_))
                ) | (
                    Self::SourceChanged,
                    SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::SourceChanged(_))
                )
            ),
            "{self:?}: {output:?}"
        );
    }
}

pub(super) fn stable_output(output: &SemanticOutput) -> SemanticOutput {
    match output {
        SemanticOutput::Exogenous(RecordExogenousTerminalOutcome::Applied(record)) => {
            SemanticOutput::Exogenous(RecordExogenousTerminalOutcome::Duplicate(record.clone()))
        }
        SemanticOutput::Link(RecordLegacyLinkOutcome::Linked(record)) => {
            SemanticOutput::Link(RecordLegacyLinkOutcome::Duplicate(record.clone()))
        }
        SemanticOutput::Link(RecordLegacyLinkOutcome::Suppressed(record, event_id)) => {
            SemanticOutput::Link(RecordLegacyLinkOutcome::Suppressed(
                record.clone(),
                *event_id,
            ))
        }
        SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::Advanced(checkpoint)) => {
            SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::Duplicate(checkpoint.clone()))
        }
        SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::SourceChanged(checkpoint)) => {
            SemanticOutput::Checkpoint(AdvanceLegacyScanOutcome::SourceChanged(
                checkpoint.clone(),
            ))
        }
        other => panic!("unexpected successful output: {other:?}"),
    }
}

pub(super) async fn snapshot(
    runtime: &StateRuntime,
) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

pub(super) fn assert_snapshot_private(snapshot: &FrozenCoordinationState) {
    assert!(!format!("{snapshot:?}").contains("item-recovery-1"));
}

fn counted(steps: &[RecoveryStep]) -> Vec<CrashPoint> {
    steps
        .iter()
        .enumerate()
        .map(|(index, step)| CrashPoint {
            boundary: Boundary::Recovery(*step),
            occurrence: steps[..index]
                .iter()
                .filter(|candidate| *candidate == step)
                .count()
                + 1,
        })
        .collect()
}

fn checkpoint_success_trace(step: RecoveryStep) -> &'static [RecoveryStep] {
    use RecoveryStep as S;
    match step {
        S::CheckpointInsert => &CHECKPOINT_INSERT_TRACE,
        S::CheckpointUpdate => &CHECKPOINT_UPDATE_TRACE,
        _ => unreachable!("checkpoint mutation step"),
    }
}

const CHECKPOINT_INSERT_TRACE: [RecoveryStep; 13] = checkpoint_trace(RecoveryStep::CheckpointInsert);
const CHECKPOINT_UPDATE_TRACE: [RecoveryStep; 13] = checkpoint_trace(RecoveryStep::CheckpointUpdate);

const fn checkpoint_trace(step: RecoveryStep) -> [RecoveryStep; 13] {
    use RecoveryStep as S;
    [
        S::TransactionBegin,
        S::MarkerRead,
        S::MarkerRead,
        S::AuthorityRead,
        S::AuthorityRead,
        S::CheckpointRead,
        S::AnchorRead,
        S::LegacyRead,
        S::DegradationInsert,
        S::DegradationOutboxInsert,
        step,
        S::BeforeCommit,
        S::AfterCommit,
    ]
}

#[derive(Clone, Copy)]
enum CheckpointPage {
    Initial,
    Insert,
    Update,
    SourceChanged,
}

fn checkpoint_page(
    epoch: codex_coordination::StateEpoch,
    kind: CheckpointPage,
) -> anyhow::Result<LegacyScanPage> {
    let root = thread_id(ROOT);
    let degradation = if matches!(kind, CheckpointPage::Initial) {
        Vec::new()
    } else {
        let ordinal = if matches!(kind, CheckpointPage::Insert) {
            11
        } else {
            12
        };
        let event = compatibility_event(
            CoordinationSemanticSlot::LegacyInteractionObserved,
            ordinal,
        );
        vec![CheckedLegacyReductionDegradation::new(
            root,
            epoch,
            LegacySourceIdentity::from_event(&event)?,
            DegradationReason::CorruptSource,
            NOW_MS,
            1,
        )?]
    };
    let existing = matches!(kind, CheckpointPage::Update | CheckpointPage::SourceChanged);
    Ok(LegacyScanPage {
        root_thread_id: root,
        expected_state_epoch: epoch,
        source_thread_id: root,
        expected_version: 0,
        expected_prefix_fingerprint: existing.then_some(if matches!(
            kind,
            CheckpointPage::SourceChanged
        ) {
            [9; 32]
        } else {
            [1; 32]
        }),
        next_physical_ordinal: if existing { 2 } else { 1 },
        scanned_prefix_fingerprint: if existing { [2; 32] } else { [1; 32] },
        last_order: None,
        complete: false,
        links: Vec::new(),
        degradations: degradation,
        now_ms: NOW_MS,
    })
}
