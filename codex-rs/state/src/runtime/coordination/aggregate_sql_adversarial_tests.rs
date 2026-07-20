use pretty_assertions::assert_eq;

use super::aggregate_concurrency_tests::accepted_one;
use super::aggregate_concurrency_tests::terminal;
use super::aggregate_failure_support::durable_snapshot;
use super::aggregate_failure_support::wait_params;
use super::aggregate_race_tests::accepted_one_reserved_two;
use super::aggregate_test_support::*;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;
use codex_coordination::CoordinationSemanticSlot;

async fn rejected_update_preserves_every_row(
    runtime: &StateRuntime,
    sql: &'static str,
) -> anyhow::Result<()> {
    let before = durable_snapshot(runtime).await?;
    assert!(sqlx::query(sql).execute(&*runtime.pool).await.is_err());
    assert_eq!(durable_snapshot(runtime).await?, before);
    Ok(())
}

#[tokio::test]
async fn generation_transition_values_cannot_be_rewritten_cleared_or_smuggled() -> anyhow::Result<()>
{
    let (runtime, _, accept_two) = accepted_one_reserved_two().await?;
    rejected_update_preserves_every_row(
        &runtime,
        "UPDATE coordination_assignment_generations SET accepted_event_id='019f7c6c-1111-7000-8000-000000000799' WHERE generation=1",
    )
    .await?;
    rejected_update_preserves_every_row(
        &runtime,
        "UPDATE coordination_assignment_generations SET accepted_event_id=NULL WHERE generation=1",
    )
    .await?;
    rejected_update_preserves_every_row(
        &runtime,
        "UPDATE coordination_assignment_generations SET lifecycle='terminal',terminal_event_id=accepted_event_id,close_event_id=accepted_event_id,terminal_kind='completed',terminal_reason_json='{\"reason\":\"turnCompleted\",\"turnId\":\"turn-b\"}',close_reason_json='{\"reason\":\"turnCompleted\",\"turnId\":\"turn-b\"}' WHERE generation=1",
    )
    .await?;

    runtime.accept_coordination_assignment(accept_two).await?;
    rejected_update_preserves_every_row(
        &runtime,
        "UPDATE coordination_assignment_generations SET superseded_event_id=NULL WHERE generation=1",
    )
    .await?;
    rejected_update_preserves_every_row(
        &runtime,
        "UPDATE coordination_assignment_generations SET close_reason_json='{\"reason\":\"superseded\",\"byGeneration\":99}' WHERE generation=1",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn terminal_values_cannot_be_rewritten_or_cleared() -> anyhow::Result<()> {
    let (runtime, _) = accepted_one().await?;
    runtime
        .terminal_coordination_assignment(terminal(
            CoordinationSemanticSlot::TurnCompleted,
            "019f7c6c-1111-7000-8000-000000000746",
            "019f7c6c-1111-7000-8000-000000000146",
            false,
        )?)
        .await?;
    for sql in [
        "UPDATE coordination_assignment_generations SET terminal_kind='interrupted' WHERE generation=1",
        "UPDATE coordination_assignment_generations SET terminal_kind=NULL WHERE generation=1",
        "UPDATE coordination_assignment_generations SET terminal_reason_json=NULL WHERE generation=1",
        "UPDATE coordination_assignment_generations SET terminal_event_id=NULL WHERE generation=1",
        "UPDATE coordination_turn_terminals SET terminal_kind='interrupted'",
        "UPDATE coordination_turn_terminal_generations SET close_event_id=NULL",
    ] {
        rejected_update_preserves_every_row(&runtime, sql).await?;
    }
    Ok(())
}

#[tokio::test]
async fn ended_wait_is_first_wins_under_raw_sql() -> anyhow::Result<()> {
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    runtime
        .reserve_coordination_assignment(reserve_params())
        .await?;
    let (start, end) = wait_params(
        CoordinationSemanticSlot::WaitStarted,
        "019f7c6c-1111-7000-8000-000000000760",
        "019f7c6c-1111-7000-8000-000000000160",
        1,
    )?;
    runtime.start_coordination_wait(start).await?;
    runtime.end_coordination_wait(end).await?;
    for sql in [
        "UPDATE coordination_waits SET outcome_json='[\"known\",\"failed\"]'",
        "UPDATE coordination_waits SET outcome_json=NULL,end_event_id=NULL,version=version+1",
        "UPDATE coordination_waits SET end_event_id='019f7c6c-1111-7000-8000-000000000799',version=version+1",
    ] {
        rejected_update_preserves_every_row(&runtime, sql).await?;
    }
    Ok(())
}
