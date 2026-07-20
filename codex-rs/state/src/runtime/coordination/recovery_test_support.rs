use codex_coordination::BoundedId;
use codex_coordination::CompatibilityOrdinal;
use codex_coordination::CompatibilitySourceIdentity;
use codex_coordination::CoordinationEvent;
use codex_coordination::CoordinationSemanticSlot;
use codex_coordination::CoordinationSource;
use codex_coordination::Evidence;
use codex_coordination::SourceKey;
use codex_coordination::SourceShape;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use serde_json::Value;
use serde_json::json;
use sqlx::Row;

use super::aggregate_test_support::ASSIGNMENT;
use super::aggregate_test_support::ROOT;
use super::aggregate_test_support::reserve_params;
use super::aggregate_test_support::thread;
use crate::StateRuntime;
use crate::runtime::test_support::unique_temp_dir;

pub(super) const CHILD: &str = "019f7c6c-1111-7000-8000-000000000603";

pub(super) async fn runtime_with_root() -> anyhow::Result<(std::sync::Arc<StateRuntime>, StateEpoch)>
{
    let runtime = StateRuntime::init(unique_temp_dir(), "test".to_string()).await?;
    let mut params = reserve_params();
    params.context.source = CoordinationSource::Native {
        schema_version: codex_coordination::CoordinationSchemaVersion::current(),
        sanitizer_version: codex_coordination::SanitizerVersion::current(),
        suppression_keys: codex_coordination::BoundedList::new(
            vec![SourceKey {
                shape: SourceShape::SubAgentActivity,
                source_item_id: Evidence::Known {
                    value: BoundedId::new("item-recovery-1")?,
                },
                source_ordinal: CompatibilityOrdinal::new(7)?,
            }],
            /*omitted_count*/ 0,
        )?,
    };
    runtime.reserve_coordination_assignment(params).await?;
    let row = sqlx::query("SELECT state_epoch FROM coordination_authority WHERE singleton_id=1")
        .fetch_one(&*runtime.pool)
        .await?;
    let epoch = StateEpoch::parse(&row.get::<String, _>("state_epoch"))?;
    Ok((runtime, epoch))
}

pub(super) fn compatibility_event(
    slot: CoordinationSemanticSlot,
    source_ordinal: u64,
) -> CoordinationEvent {
    let (shape, kind, source_thread_id, source_turn, source_path) = match slot {
        CoordinationSemanticSlot::LegacyInteractionObserved => (
            SourceShape::SubAgentActivity,
            json!({
                "kind":"legacyInteractionObserved",
                "observation":"interactionMarker",
                "target":{"status":"notApplicable"},
                "content":{"status":"unavailable","reason":"sourceDoesNotEncode"},
                "reportedSuccess":{"status":"unavailable","reason":"sourceDoesNotEncode"}
            }),
            ROOT,
            "turn-a",
            "/root",
        ),
        CoordinationSemanticSlot::TurnCompleted => (
            SourceShape::TurnComplete,
            json!({
                "kind":"turnCompleted",
                "target": target(),
                "targetTurnId":"turn-b",
                "outcome":"succeeded",
                "includedGenerations":{"items":[1],"omittedCount":0}
            }),
            CHILD,
            "turn-b",
            "/root/worker",
        ),
        CoordinationSemanticSlot::AssignmentRequested => (
            SourceShape::SubAgentActivity,
            json!({
                "kind":"assignmentRequested",
                "operationId":super::aggregate_test_support::OPERATION,
                "mode":"spawn",
                "target":target(),
                "objective":{"status":"unavailable","reason":"encryptedPayload"},
                "encodedPayloadBytes":384,
                "requestedRuntime":{
                    "model":{"source":"explicit","value":"gpt-5.6-sol"},
                    "reasoningEffort":{"source":"explicit","value":"high"}
                }
            }),
            ROOT,
            "turn-a",
            "/root",
        ),
        _ => panic!("unsupported recovery fixture slot"),
    };
    let source_thread = thread(source_thread_id);
    let identity = CompatibilitySourceIdentity::new(
        shape,
        Some(source_thread),
        Some(BoundedId::new(source_turn).expect("turn")),
        Some(BoundedId::new("item-recovery-1").expect("item")),
        source_ordinal,
        slot,
    )
    .expect("compatibility identity");
    let mut event = json!({
        "eventId": identity.event_id().to_string(),
        "rootThreadId": ROOT,
        "order": {"mode":"compatibility","afterRevision":1,"sourceOrdinal":source_ordinal},
        "occurredAt": 1_753_000_000,
        "actor": principal(source_thread_id, source_turn, source_path),
        "responsibilityOwner":{"status":"known","value":principal(ROOT,"turn-a","/root")},
        "projection":{"threadId":source_thread_id,"turnId":source_turn},
        "causes":{"items":[],"omittedCount":0},
        "source":{
            "source":"compatibility","adapterVersion":1,"sanitizerVersion":1,
            "key":{
                "shape":shape,"sourceItemId":{"status":"known","value":"item-recovery-1"},
                "sourceOrdinal":source_ordinal
            }
        }
    });
    event
        .as_object_mut()
        .expect("event")
        .extend(kind.as_object().expect("kind").clone());
    serde_json::from_value(event).expect("checked compatibility fixture")
}

fn principal(thread_id: &str, turn_id: &str, path: &str) -> Value {
    json!({
        "threadId":thread_id,
        "turnId":{"status":"known","value":turn_id},
        "agentPath":{"status":"known","value":path}
    })
}

fn target() -> Value {
    json!({
        "principal":principal(CHILD,"turn-b","/root/worker"),
        "assignment":{"status":"known","assignmentId":ASSIGNMENT,"generation":1}
    })
}

pub(super) fn thread_id(value: &str) -> ThreadId {
    ThreadId::try_from(value).expect("thread")
}
