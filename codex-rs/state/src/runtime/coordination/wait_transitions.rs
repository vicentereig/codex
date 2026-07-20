use codex_coordination::CoordinationEventKind;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::Evidence;
use sqlx::Row;
use sqlx::SqliteConnection;

use super::aggregate_journal::*;
use super::aggregates::WaitTransitionOutcome;
use crate::model::coordination::*;

pub(super) async fn start_wait(
    connection: &mut SqliteConnection,
    params: StartCoordinationWait,
    injector: &dyn AggregateFailureInjector,
) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
    validate_identities(&params.context)?;
    if !params.context.secondary.items().is_empty() {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if params.context.primary.operation_id != params.operation_id
        || !matches!(&params.context.responsibility_owner, Evidence::Known { value } if value == &params.context.actor)
    {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    let epoch = authority(connection, injector).await?;
    ensure_root(
        connection,
        &params.context.root_thread_id,
        epoch,
        /*create*/ false,
        injector,
    )
    .await?;
    let kind = CoordinationEventKind::WaitStarted {
        operation_id: params.operation_id,
        targets: params.targets.clone(),
        timeout_ms: params.timeout_ms,
    };
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        CoordinationSemanticSlot::WaitStarted,
        injector,
    )
    .await?
    {
        compare_event(
            &params.context,
            &params.context.primary,
            epoch,
            stored.revision,
            kind,
            &[],
            &stored.event,
        )?;
        return Ok(WaitTransitionOutcome::Duplicate {
            event: stored.event,
        });
    }
    fence_root_revision(connection, &params.context, injector).await?;
    let existing =
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM coordination_waits WHERE operation_id=?")
            .bind(params.operation_id.to_string())
            .fetch_optional(&mut *connection)
            .await
            .map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    if existing.is_some() {
        return Err(CoordinationWriteError::WaitConflict);
    }
    let revisions = allocate(connection, &params.context.root_thread_id, 1, injector).await?;
    let event = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &[],
    )?;
    let actor_turn = known_turn(&params.context.actor)?;
    let now = now_ms(injector);
    sqlx::query("INSERT INTO coordination_waits (operation_id,root_thread_id,actor_thread_id,actor_turn_id,start_event_id,last_revision,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,?)")
        .bind(params.operation_id.to_string()).bind(params.context.root_thread_id.to_string()).bind(params.context.actor.thread_id.to_string()).bind(actor_turn.as_str())
        .bind(event.envelope().event_id.to_string()).bind(revisions[0].get() as i64).bind(now).bind(now).execute(&mut *connection).await.map_err(internal)?;
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(
        connection,
        &params.context,
        std::slice::from_ref(&event),
        injector,
    )
    .await?;
    Ok(WaitTransitionOutcome::Applied { event })
}

pub(super) async fn end_wait(
    connection: &mut SqliteConnection,
    params: EndCoordinationWait,
    injector: &dyn AggregateFailureInjector,
) -> Result<WaitTransitionOutcome, CoordinationWriteError> {
    validate_identities(&params.context)?;
    if !params.context.secondary.items().is_empty() {
        return Err(CoordinationWriteError::IdempotencyConflict);
    }
    if params.context.primary.operation_id != params.operation_id
        || !matches!(&params.context.responsibility_owner, Evidence::Known { value } if value == &params.context.actor)
    {
        return Err(CoordinationWriteError::DivergentIntent);
    }
    let epoch = authority(connection, injector).await?;
    ensure_root(
        connection,
        &params.context.root_thread_id,
        epoch,
        /*create*/ false,
        injector,
    )
    .await?;
    let row = sqlx::query("SELECT root_thread_id,actor_thread_id,actor_turn_id,start_event_id,end_event_id,outcome_json,version FROM coordination_waits WHERE operation_id=?")
        .bind(params.operation_id.to_string()).fetch_optional(&mut *connection).await.map_err(internal)?.ok_or(CoordinationWriteError::WaitConflict)?;
    injector
        .after_step(AggregateStep::AggregateRead)
        .map_err(internal)?;
    if row.get::<String, _>("root_thread_id") != params.context.root_thread_id.to_string() {
        return Err(CoordinationWriteError::RootMismatch);
    }
    if row.get::<String, _>("actor_thread_id") != params.context.actor.thread_id.to_string()
        || row.get::<String, _>("actor_turn_id") != known_turn(&params.context.actor)?.as_str()
    {
        return Err(CoordinationWriteError::OwnerFenced);
    }
    let start = load_event_id(
        connection,
        &row.get::<String, _>("start_event_id"),
        injector,
    )
    .await?;
    let kind = CoordinationEventKind::WaitEnded {
        operation_id: params.operation_id,
        targets: params.targets.clone(),
        outcome: params.outcome.clone(),
        failure: params.failure.clone(),
    };
    if let Some(stored) = load_idempotent(
        connection,
        &params.context,
        &params.context.primary,
        CoordinationSemanticSlot::WaitEnded,
        injector,
    )
    .await?
    {
        compare_event(
            &params.context,
            &params.context.primary,
            epoch,
            stored.revision,
            kind,
            &[&start.event],
            &stored.event,
        )?;
        return Ok(WaitTransitionOutcome::Duplicate {
            event: stored.event,
        });
    }
    fence_root_revision(connection, &params.context, injector).await?;
    if row.get::<i64, _>("version")
        != i64::try_from(params.expected_wait_version).map_err(internal)?
    {
        return Err(CoordinationWriteError::VersionFenced);
    }
    if row.get::<Option<String>, _>("end_event_id").is_some() {
        return Err(CoordinationWriteError::WaitConflict);
    }
    let revisions = allocate(connection, &params.context.root_thread_id, 1, injector).await?;
    let event = make_event(
        &params.context,
        &params.context.primary,
        epoch,
        revisions[0],
        kind,
        &[&start.event],
    )?;
    let outcome_json =
        serde_json::to_string(&(params.outcome, params.failure)).map_err(internal)?;
    let expected_wait_version = i64::try_from(params.expected_wait_version)
        .map_err(|_| CoordinationWriteError::VersionFenced)?;
    let changed = sqlx::query("UPDATE coordination_waits SET end_event_id=?,outcome_json=?,version=version+1,last_revision=?,updated_at_ms=? WHERE operation_id=? AND version=? AND end_event_id IS NULL")
        .bind(event.envelope().event_id.to_string()).bind(outcome_json).bind(revisions[0].get() as i64).bind(now_ms(injector)).bind(params.operation_id.to_string()).bind(expected_wait_version)
        .execute(&mut *connection).await.map_err(internal)?.rows_affected();
    if changed != 1 {
        return Err(CoordinationWriteError::WaitConflict);
    }
    injector
        .after_step(AggregateStep::AggregateMutation)
        .map_err(internal)?;
    journal(
        connection,
        &params.context,
        std::slice::from_ref(&event),
        injector,
    )
    .await?;
    Ok(WaitTransitionOutcome::Applied { event })
}
