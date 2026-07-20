use codex_coordination::AssignmentGeneration;
use codex_coordination::AssignmentId;
use codex_coordination::AssignmentMode;
use codex_coordination::BoundedId;
use codex_coordination::CoordinationEventId;
use codex_coordination::GenerationCloseReason;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::ReceiptId;
use codex_protocol::ThreadId;
use sqlx::Row;

use crate::StateRuntime;
use crate::model::coordination::AssignmentAggregateRecord;
use crate::model::coordination::AssignmentGenerationRecord;
use crate::model::coordination::AssignmentHeadRecord;
use crate::model::coordination::GenerationLifecycle;

impl StateRuntime {
    pub(crate) async fn coordination_assignment_aggregate(
        &self,
        assignment_id: AssignmentId,
    ) -> anyhow::Result<Option<AssignmentAggregateRecord>> {
        let Some(row) = sqlx::query("SELECT assignment_id,root_thread_id,child_thread_id,accepted_generation,next_generation,owner_thread_id,owner_turn_id,version,last_revision FROM coordination_assignment_heads WHERE assignment_id=?")
            .bind(assignment_id.to_string()).fetch_optional(&*self.pool).await? else { return Ok(None); };
        let head = AssignmentHeadRecord {
            assignment_id,
            root_thread_id: thread(row.get("root_thread_id"))?,
            child_thread_id: thread(row.get("child_thread_id"))?,
            accepted_generation: row
                .get::<Option<i64>, _>("accepted_generation")
                .map(generation)
                .transpose()?,
            next_generation: generation(row.get("next_generation"))?,
            owner_thread_id: thread(row.get("owner_thread_id"))?,
            owner_turn_id: BoundedId::new(row.get::<String, _>("owner_turn_id"))?,
            version: row.get::<i64, _>("version").try_into()?,
            last_revision: row.get::<i64, _>("last_revision").try_into()?,
        };
        let rows = sqlx::query("SELECT generation,mode,lifecycle,request_event_id,accepted_event_id,superseded_event_id,terminal_event_id,close_event_id,accepted_receipt_id,close_reason_json,last_revision FROM coordination_assignment_generations WHERE assignment_id=? ORDER BY generation")
            .bind(assignment_id.to_string()).fetch_all(&*self.pool).await?;
        let generations = rows
            .into_iter()
            .map(|row| {
                let optional_event = |field| -> anyhow::Result<Option<CoordinationEventId>> {
                    row.get::<Option<String>, _>(field)
                        .map(|value| CoordinationEventId::parse(&value))
                        .transpose()
                        .map_err(Into::into)
                };
                Ok(AssignmentGenerationRecord {
                    assignment_id,
                    generation: generation(row.get("generation"))?,
                    mode: match row.get::<String, _>("mode").as_str() {
                        "spawn" => AssignmentMode::Spawn,
                        "followup" => AssignmentMode::Followup,
                        value => anyhow::bail!("invalid assignment mode {value}"),
                    },
                    lifecycle: match row.get::<String, _>("lifecycle").as_str() {
                        "reserved" => GenerationLifecycle::Reserved,
                        "accepted" => GenerationLifecycle::Accepted,
                        "abandoned" => GenerationLifecycle::Abandoned,
                        "superseded" => GenerationLifecycle::Superseded,
                        "terminal" => GenerationLifecycle::Terminal,
                        value => anyhow::bail!("invalid generation lifecycle {value}"),
                    },
                    request_event_id: CoordinationEventId::parse(
                        &row.get::<String, _>("request_event_id"),
                    )?,
                    accepted_event_id: optional_event("accepted_event_id")?,
                    superseded_event_id: optional_event("superseded_event_id")?,
                    terminal_event_id: optional_event("terminal_event_id")?,
                    close_event_id: optional_event("close_event_id")?,
                    accepted_receipt_id: row
                        .get::<Option<String>, _>("accepted_receipt_id")
                        .map(|value| ReceiptId::parse(&value))
                        .transpose()?,
                    terminal_reason: row
                        .get::<Option<String>, _>("close_reason_json")
                        .map(|value| serde_json::from_str::<GenerationCloseReason>(&value))
                        .transpose()?,
                    last_revision: row.get::<i64, _>("last_revision").try_into()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Some(AssignmentAggregateRecord { head, generations }))
    }

    pub(crate) async fn coordination_bound_generations(
        &self,
        root_thread_id: ThreadId,
        turn_id: &BoundedId<MAX_ID_BYTES>,
        assignment_id: AssignmentId,
    ) -> anyhow::Result<Vec<AssignmentGeneration>> {
        sqlx::query_scalar::<_,i64>("SELECT generation FROM coordination_turn_bindings WHERE root_thread_id=? AND turn_id=? AND assignment_id=? ORDER BY generation")
            .bind(root_thread_id.to_string()).bind(turn_id.as_str()).bind(assignment_id.to_string()).fetch_all(&*self.pool).await?
            .into_iter().map(generation).collect()
    }
}

fn thread(value: String) -> anyhow::Result<ThreadId> {
    ThreadId::try_from(value.as_str()).map_err(Into::into)
}
fn generation(value: i64) -> anyhow::Result<AssignmentGeneration> {
    AssignmentGeneration::new(value.try_into()?).map_err(Into::into)
}
