use std::io;
use std::path::Path;
use std::path::PathBuf;

use codex_coordination::StateEpoch;
use pretty_assertions::assert_eq;
use sqlx::SqlitePool;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::aggregate_test_support::reserve_params;
use super::aggregates::ReserveAssignmentOutcome;
use super::authority::AuthorityFailureInjector;
use super::authority::AuthorityWriteStep;
use super::authority::CoordinationAuthorityStatus;
use super::authority::NoFailure as NoAuthorityFailure;
use super::authority::initialize_authority_with;
use super::authority_marker::MARKER_FILE_NAME;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::commands_tests::assignment_command;
use super::degradation::record_exogenous_terminal_degradation;
use super::degradation::record_exogenous_terminal_degradation_with;
use super::failure_injection_support::Boundary;
use super::failure_injection_support::FrozenCoordinationState;
use super::failure_injection_support::FrozenStateInputs;
use super::failure_injection_support::assert_integrity;
use super::failure_injection_support::frozen_state;
use super::failure_injection_tests::delivery_now;
use super::failure_injection_tests::observation;
use super::failure_injection_tests::receipt_params_for_matrix;
use super::failure_injection_tests::runtime_with_command_at;
use super::failure_injection_tests::runtime_with_root_at;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use crate::SqliteConfig;
use crate::StateRuntime;
use crate::migrations::runtime_state_migrator;
use crate::model::coordination_commands::RecordCoordinationCommandOutcome;
use crate::model::coordination_inbox::PersistRecipientReceiptOutcome;
use crate::model::coordination_recovery::RecordExogenousTerminalOutcome;
use crate::runtime::test_support::unique_temp_dir;
use crate::state_db_path;

const NOW_MS: i64 = 4_000_000_000_000;

struct PanicAt {
    boundary: Boundary,
    now_ms: i64,
}

impl PanicAt {
    fn visit(&self, boundary: Boundary) -> anyhow::Result<()> {
        if boundary == self.boundary {
            panic!("injected unwind at {boundary:?}");
        }
        Ok(())
    }
}

impl AggregateFailureInjector for PanicAt {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        self.visit(Boundary::Aggregate(step))
    }

    fn now_ms(&self) -> i64 {
        self.now_ms
    }
}

impl CommandFailureInjector for PanicAt {
    fn after_command_step(&self, step: CommandStep) -> anyhow::Result<()> {
        self.visit(Boundary::Command(step))
    }
}

impl InboxFailureInjector for PanicAt {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()> {
        self.visit(Boundary::Inbox(step))
    }
}

impl RecoveryFailureInjector for PanicAt {
    fn after_recovery_step(&self, step: RecoveryStep) -> anyhow::Result<()> {
        self.visit(Boundary::Recovery(step))
    }
}

struct PanicAuthority;

impl AuthorityFailureInjector for PanicAuthority {
    fn check(&self, step: AuthorityWriteStep) -> io::Result<()> {
        if step == AuthorityWriteStep::AuthorityInsert {
            panic!("injected unwind before authority insert");
        }
        Ok(())
    }
}

#[tokio::test]
async fn aggregate_unwind_rolls_back_and_returns_connection_to_pool() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
    let before = snapshot(&runtime).await?;
    let task_runtime = runtime.clone();
    let unwind = tokio::spawn(async move {
        task_runtime
            .reserve_coordination_assignment_with(
                reserve_params(),
                &PanicAt {
                    boundary: Boundary::Aggregate(AggregateStep::AggregateMutation),
                    now_ms: NOW_MS,
                },
            )
            .await
    })
    .await;
    assert!(unwind.is_err_and(|err| err.is_panic()));
    assert_writer_reusable(&runtime.pool).await?;

    let runtime = reopen_unchanged(runtime, home, &before).await?;
    assert!(matches!(
        runtime
            .reserve_coordination_assignment(reserve_params())
            .await?,
        ReserveAssignmentOutcome::Reserved { .. }
    ));
    assert_integrity(&runtime).await?;
    Ok(())
}

#[tokio::test]
async fn command_unwind_rolls_back_and_returns_connection_to_pool() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(home.clone(), "test".to_string()).await?;
    let before = snapshot(&runtime).await?;
    let task_runtime = runtime.clone();
    let unwind = tokio::spawn(async move {
        task_runtime
            .record_coordination_command_intent_with(
                assignment_command(),
                &PanicAt {
                    boundary: Boundary::Command(CommandStep::CommandInsert),
                    now_ms: NOW_MS,
                },
            )
            .await
    })
    .await;
    assert!(unwind.is_err_and(|err| err.is_panic()));
    assert_writer_reusable(&runtime.pool).await?;

    let runtime = reopen_unchanged(runtime, home, &before).await?;
    assert!(matches!(
        runtime
            .record_coordination_command_intent(assignment_command())
            .await?,
        RecordCoordinationCommandOutcome::Applied(_)
    ));
    assert_integrity(&runtime).await?;
    Ok(())
}

#[tokio::test]
async fn inbox_unwind_rolls_back_and_returns_connection_to_pool() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let runtime = runtime_with_command_at(home.clone()).await?;
    let before = snapshot(&runtime).await?;
    let now_ms = delivery_now(&runtime).await?;
    let task_runtime = runtime.clone();
    let unwind = tokio::spawn(async move {
        task_runtime
            .persist_coordination_recipient_receipt_with(
                receipt_params_for_matrix(),
                &PanicAt {
                    boundary: Boundary::Inbox(InboxStep::ReceiptInsert),
                    now_ms,
                },
            )
            .await
    })
    .await;
    assert!(unwind.is_err_and(|err| err.is_panic()));
    assert_writer_reusable(&runtime.pool).await?;

    let runtime = reopen_unchanged(runtime, home, &before).await?;
    assert!(matches!(
        runtime
            .persist_coordination_recipient_receipt(receipt_params_for_matrix())
            .await?,
        PersistRecipientReceiptOutcome::Applied(_)
    ));
    assert_integrity(&runtime).await?;
    Ok(())
}

#[tokio::test]
async fn recovery_unwind_rolls_back_and_returns_connection_to_pool() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let (runtime, epoch) = runtime_with_root_at(home.clone()).await?;
    let before = snapshot(&runtime).await?;
    let evidence = observation(epoch)?;
    let task_runtime = runtime.clone();
    let task_evidence = evidence.clone();
    let unwind = tokio::spawn(async move {
        record_exogenous_terminal_degradation_with(
            &task_runtime.pool,
            task_evidence,
            &PanicAt {
                boundary: Boundary::Recovery(RecoveryStep::DegradationInsert),
                now_ms: NOW_MS,
            },
        )
        .await
    })
    .await;
    assert!(unwind.is_err_and(|err| err.is_panic()));
    assert_writer_reusable(&runtime.pool).await?;

    let runtime = reopen_unchanged(runtime, home, &before).await?;
    assert!(matches!(
        record_exogenous_terminal_degradation(&runtime.pool, evidence).await?,
        RecordExogenousTerminalOutcome::Applied(_)
    ));
    assert_integrity(&runtime).await?;
    Ok(())
}

#[tokio::test]
async fn authority_unwind_returns_connection_and_reconciles_marker_epoch() -> anyhow::Result<()> {
    let home = unique_temp_dir();
    let pool = migrated_pool(home.as_path()).await?;
    let task_pool = pool.clone();
    let task_home = home.clone();
    let unwind = tokio::spawn(async move {
        initialize_authority_with(&task_pool, task_home.as_path(), None, &PanicAuthority).await
    })
    .await;
    assert!(unwind.is_err_and(|err| err.is_panic()));
    assert_writer_reusable(&pool).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM coordination_authority")
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 0);
    let marker: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(home.join(MARKER_FILE_NAME)).await?)?;
    let marker_epoch = StateEpoch::parse(
        marker["stateEpoch"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("marker should contain stateEpoch"))?,
    )?;

    pool.close().await;
    let pool = migrated_pool(home.as_path()).await?;
    let status =
        initialize_authority_with(&pool, home.as_path(), None, &NoAuthorityFailure).await?;
    assert_eq!(
        status,
        CoordinationAuthorityStatus::Active {
            state_epoch: marker_epoch
        }
    );
    assert_eq!(
        initialize_authority_with(&pool, home.as_path(), None, &NoAuthorityFailure,).await?,
        status
    );
    pool.close().await;
    Ok(())
}

async fn snapshot(runtime: &StateRuntime) -> anyhow::Result<FrozenCoordinationState> {
    frozen_state(runtime, FrozenStateInputs::new(runtime.codex_home())).await
}

async fn assert_writer_reusable(pool: &SqlitePool) -> anyhow::Result<()> {
    pool.begin_with("BEGIN IMMEDIATE").await?.rollback().await?;
    Ok(())
}

async fn reopen_unchanged(
    runtime: std::sync::Arc<StateRuntime>,
    home: PathBuf,
    before: &FrozenCoordinationState,
) -> anyhow::Result<std::sync::Arc<StateRuntime>> {
    assert_eq!(snapshot(&runtime).await?, *before);
    drop(runtime);
    let reopened = StateRuntime::init(home, "test".to_string()).await?;
    assert_eq!(snapshot(&reopened).await?, *before);
    Ok(reopened)
}

async fn migrated_pool(sqlite_home: &Path) -> anyhow::Result<SqlitePool> {
    tokio::fs::create_dir_all(sqlite_home).await?;
    let sqlite = SqliteConfig::new_for_testing(sqlite_home.to_path_buf());
    let pool = sqlite
        .open_read_write_pool(state_db_path(sqlite_home).as_path())
        .await?;
    runtime_state_migrator().run(&pool).await?;
    Ok(pool)
}
