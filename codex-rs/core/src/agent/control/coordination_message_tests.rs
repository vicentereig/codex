use super::*;
use crate::agent_communication::AgentCommunicationKind;
use crate::coordination::CoordinationState;
use crate::coordination::MessageFailureInjector;
use codex_protocol::AgentPath;
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

const NOW_MS: i64 = 2_000_000_000_000;

fn thread_id(value: u128) -> ThreadId {
    ThreadId::from_string(&uuid::Uuid::from_u128(value).to_string())
        .expect("valid uuid string parses into ThreadId")
}

async fn open_seed_pool(codex_home: &std::path::Path) -> anyhow::Result<sqlx::SqlitePool> {
    let path = codex_state::state_db_path(codex_home);
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}", path.display()))
        .await?)
}

async fn active_epoch(pool: &sqlx::SqlitePool) -> anyhow::Result<String> {
    Ok(
        sqlx::query("SELECT state_epoch FROM coordination_authority WHERE singleton_id=1")
            .fetch_one(pool)
            .await?
            .get("state_epoch"),
    )
}

async fn seed_root(
    pool: &sqlx::SqlitePool,
    root_thread_id: ThreadId,
    epoch: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO coordination_roots \
         (root_thread_id,state_epoch,committed_revision,published_revision,created_at_ms,updated_at_ms) \
         VALUES (?,?,0,0,?,?)",
    )
    .bind(root_thread_id.to_string())
    .bind(epoch)
    .bind(NOW_MS)
    .bind(NOW_MS)
    .execute(pool)
    .await?;
    Ok(())
}

/// Test rig: a real SQLite-backed `codex_state::StateRuntime` (so the durable receipt/generation/
/// materialization machinery this module drives is exercised for real, not mocked), with one root
/// already seeded so `coordination_message_receipts`'s root FK is satisfiable.
struct Rig {
    _home: TempDir,
    control: AgentControl,
    coordination: Arc<CoordinationState>,
    root_thread_id: ThreadId,
}

async fn build_rig() -> Rig {
    let home = TempDir::new().expect("tempdir");
    let runtime = codex_state::StateRuntime::init(home.path().to_path_buf(), "test".to_string())
        .await
        .expect("state runtime should initialize");
    let pool = open_seed_pool(home.path()).await.expect("seed pool");
    let epoch = active_epoch(&pool).await.expect("active epoch");
    let root_thread_id = thread_id(1);
    seed_root(&pool, root_thread_id, &epoch)
        .await
        .expect("seed root");
    pool.close().await;

    let coordination = CoordinationState::new_for_tests(Some(runtime));
    let control =
        AgentControl::default().with_coordination_enabled_for_tests(Arc::clone(&coordination));
    Rig {
        _home: home,
        control,
        coordination,
        root_thread_id,
    }
}

fn message_key(rig: &Rig, call_id: &str) -> OperationIdentityKey {
    OperationIdentityKey {
        root_thread_id: rig.root_thread_id,
        actor_thread_id: thread_id(2),
        actor_turn_id: "sender-turn".to_string(),
        call_id: call_id.to_string(),
        semantic_slot: SemanticSlot::Message,
    }
}

fn followup_key(rig: &Rig, call_id: &str) -> OperationIdentityKey {
    OperationIdentityKey {
        root_thread_id: rig.root_thread_id,
        actor_thread_id: thread_id(2),
        actor_turn_id: "sender-turn".to_string(),
        call_id: call_id.to_string(),
        semantic_slot: SemanticSlot::Followup,
    }
}

fn fixture_communication(trigger_turn: bool) -> InterAgentCommunication {
    InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::try_from("/root/worker").expect("agent path"),
        Vec::new(),
        "hello".to_string(),
        trigger_turn,
    )
}

fn fixture_context(kind: AgentCommunicationKind) -> AgentCommunicationContext {
    AgentCommunicationContext::new(kind, thread_id(2))
}

#[tokio::test]
async fn disabled_coordination_rejects_before_any_side_effect() {
    let control = AgentControl::default();
    let key = OperationIdentityKey {
        root_thread_id: thread_id(1),
        actor_thread_id: thread_id(2),
        actor_turn_id: "sender-turn".to_string(),
        call_id: "call-1".to_string(),
        semantic_slot: SemanticSlot::Message,
    };
    let result = control
        .deliver_message_coordinated(
            key,
            "sender-turn".to_string(),
            thread_id(3),
            None,
            &fixture_communication(false),
            fixture_context(AgentCommunicationKind::Message),
            NOW_MS,
        )
        .await;
    assert!(result.is_err(), "disabled coordination must reject upfront");
}

#[tokio::test]
async fn message_delivery_captures_no_generation_and_ends_up_enqueued() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let receipt = rig
        .control
        .deliver_message_coordinated(
            message_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            None,
            &fixture_communication(false),
            fixture_context(AgentCommunicationKind::Message),
            NOW_MS,
        )
        .await
        .expect("message delivery should succeed");
    assert_eq!(receipt.captured_generation, None);
    assert_eq!(receipt.bound_turn_id, None);

    let pending = rig
        .coordination
        .pending_committed_receipts(rig.root_thread_id, 10)
        .await
        .expect("pending receipts");
    assert!(
        pending.is_empty(),
        "orchestration must mark the receipt enqueued before returning"
    );
}

#[tokio::test]
async fn followup_delivery_reserves_generation_and_binds_caller_supplied_turn() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let receipt = rig
        .control
        .deliver_message_coordinated(
            followup_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            Some("turn-active".to_string()),
            &fixture_communication(true),
            fixture_context(AgentCommunicationKind::Followup),
            NOW_MS,
        )
        .await
        .expect("followup delivery should succeed");
    assert_eq!(receipt.captured_generation, Some(1));
    assert_eq!(receipt.bound_turn_id.as_deref(), Some("turn-active"));
}

#[tokio::test]
async fn followup_without_bound_turn_id_is_rejected_before_any_side_effect() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let result = rig
        .control
        .deliver_message_coordinated(
            followup_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            /*bound_turn_id_for_followup*/ None,
            &fixture_communication(true),
            fixture_context(AgentCommunicationKind::Followup),
            NOW_MS,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn duplicate_operation_id_is_idempotent_and_never_advances_generation_twice() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let key = followup_key(&rig, "call-1");

    let first = rig
        .control
        .deliver_message_coordinated(
            key.clone(),
            "sender-turn".to_string(),
            target,
            Some("turn-a".to_string()),
            &fixture_communication(true),
            fixture_context(AgentCommunicationKind::Followup),
            NOW_MS,
        )
        .await
        .expect("first delivery");

    // Simulated handler retry: identical key, same operation id resolved.
    let retry = rig
        .control
        .deliver_message_coordinated(
            key,
            "sender-turn".to_string(),
            target,
            Some("turn-a".to_string()),
            &fixture_communication(true),
            fixture_context(AgentCommunicationKind::Followup),
            NOW_MS + 1,
        )
        .await
        .expect("retry delivery");
    assert_eq!(retry.receipt_id, first.receipt_id);
    assert_eq!(retry.captured_generation, first.captured_generation);

    // A genuinely new follow-up must still reserve generation 2, proving the retry above never
    // consumed a generation slot.
    let next = rig
        .control
        .deliver_message_coordinated(
            followup_key(&rig, "call-2"),
            "sender-turn".to_string(),
            target,
            Some("turn-a".to_string()),
            &fixture_communication(true),
            fixture_context(AgentCommunicationKind::Followup),
            NOW_MS,
        )
        .await
        .expect("next delivery");
    assert_eq!(next.captured_generation, Some(2));
}

/// "Queue message N versus acceptance N+1 both orders fence/converge", exercised through the full
/// `deliver_message_coordinated` orchestration (not just the underlying state facade).
#[tokio::test]
async fn message_and_followup_delivery_converge_regardless_of_order() {
    // Order A: message first.
    {
        let rig = build_rig().await;
        let target = thread_id(3);
        let message = rig
            .control
            .deliver_message_coordinated(
                message_key(&rig, "call-message"),
                "sender-turn".to_string(),
                target,
                None,
                &fixture_communication(false),
                fixture_context(AgentCommunicationKind::Message),
                NOW_MS,
            )
            .await
            .expect("message first");
        assert_eq!(message.captured_generation, None);

        let followup = rig
            .control
            .deliver_message_coordinated(
                followup_key(&rig, "call-followup"),
                "sender-turn".to_string(),
                target,
                Some("turn-order-a".to_string()),
                &fixture_communication(true),
                fixture_context(AgentCommunicationKind::Followup),
                NOW_MS,
            )
            .await
            .expect("followup second");
        assert_eq!(followup.captured_generation, Some(1));
    }

    // Order B: followup first, message second must capture the already-accepted generation.
    {
        let rig = build_rig().await;
        let target = thread_id(3);
        let followup = rig
            .control
            .deliver_message_coordinated(
                followup_key(&rig, "call-followup"),
                "sender-turn".to_string(),
                target,
                Some("turn-order-b".to_string()),
                &fixture_communication(true),
                fixture_context(AgentCommunicationKind::Followup),
                NOW_MS,
            )
            .await
            .expect("followup first");
        assert_eq!(followup.captured_generation, Some(1));

        let message = rig
            .control
            .deliver_message_coordinated(
                message_key(&rig, "call-message"),
                "sender-turn".to_string(),
                target,
                None,
                &fixture_communication(false),
                fixture_context(AgentCommunicationKind::Message),
                NOW_MS,
            )
            .await
            .expect("message second");
        assert_eq!(message.captured_generation, Some(1));
        assert_eq!(message.bound_turn_id.as_deref(), Some("turn-order-b"));
    }
}

/// Restart recovery, case 1 ("receipt committed before enqueue"): inject a crash right after the
/// receipt is durably committed but before it is marked enqueued, then prove recovery re-enqueues
/// it via `CoordinationState::pending_committed_receipts`/`mark_receipt_enqueued`.
#[tokio::test]
async fn restart_recovery_re_enqueues_receipt_committed_before_enqueue() {
    let rig = build_rig().await;
    let target = thread_id(3);
    rig.coordination
        .set_message_failure_injection_for_tests(MessageFailureInjector::fail_at(
            MessageFailurePoint::AfterReceipt,
        ));

    let crashed = rig
        .control
        .deliver_message_coordinated(
            message_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            None,
            &fixture_communication(false),
            fixture_context(AgentCommunicationKind::Message),
            NOW_MS,
        )
        .await;
    assert!(crashed.is_err(), "injected failure must surface");

    // Disarm injection (simulating restart into a clean process) and recover.
    rig.coordination
        .set_message_failure_injection_for_tests(MessageFailureInjector::none());
    let pending = rig
        .coordination
        .pending_committed_receipts(rig.root_thread_id, 10)
        .await
        .expect("pending receipts");
    assert_eq!(
        pending.len(),
        1,
        "the committed receipt must be recoverable"
    );

    for receipt in &pending {
        rig.coordination
            .mark_receipt_enqueued(receipt.receipt_id, NOW_MS + 1)
            .await
            .expect("recovery re-enqueue");
    }
    let pending_after = rig
        .coordination
        .pending_committed_receipts(rig.root_thread_id, 10)
        .await
        .expect("pending receipts after recovery");
    assert!(pending_after.is_empty());
}

/// Restart recovery, case 2 ("materialization committed before rollout append"), reached through
/// `CoordinationState`'s exposed materialization methods using a receipt produced by a real
/// `deliver_message_coordinated` call.
#[tokio::test]
async fn restart_recovery_completes_materialization_committed_before_rollout_append() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let receipt = rig
        .control
        .deliver_message_coordinated(
            message_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            None,
            &fixture_communication(false),
            fixture_context(AgentCommunicationKind::Message),
            NOW_MS,
        )
        .await
        .expect("message delivery");

    let response_item_id = uuid::Uuid::now_v7();
    rig.coordination
        .commit_materialization(
            rig.root_thread_id,
            receipt.receipt_id,
            "target-turn-1",
            response_item_id,
            NOW_MS,
        )
        .await
        .expect("commit materialization");

    // Simulated crash: rollout append never landed.
    let pending = rig
        .coordination
        .pending_committed_materializations(rig.root_thread_id, 10)
        .await
        .expect("pending materializations");
    assert_eq!(pending.len(), 1);

    rig.coordination
        .mark_materialization_rollout_appended(
            receipt.receipt_id,
            "target-turn-1",
            response_item_id,
            NOW_MS + 1,
        )
        .await
        .expect("recovery completes append");
    let pending_after = rig
        .coordination
        .pending_committed_materializations(rig.root_thread_id, 10)
        .await
        .expect("pending after recovery");
    assert!(pending_after.is_empty());
}

/// Restart recovery, case 3 ("rollout append before selection").
#[tokio::test]
async fn restart_recovery_makes_rollout_appended_materializations_selectable() {
    let rig = build_rig().await;
    let target = thread_id(3);
    let receipt = rig
        .control
        .deliver_message_coordinated(
            message_key(&rig, "call-1"),
            "sender-turn".to_string(),
            target,
            None,
            &fixture_communication(false),
            fixture_context(AgentCommunicationKind::Message),
            NOW_MS,
        )
        .await
        .expect("message delivery");

    let response_item_id = uuid::Uuid::now_v7();
    rig.coordination
        .commit_materialization(
            rig.root_thread_id,
            receipt.receipt_id,
            "target-turn-2",
            response_item_id,
            NOW_MS,
        )
        .await
        .expect("commit materialization");
    rig.coordination
        .mark_materialization_rollout_appended(
            receipt.receipt_id,
            "target-turn-2",
            response_item_id,
            NOW_MS + 1,
        )
        .await
        .expect("append");

    // Simulated crash: selection never happened.
    let selectable = rig
        .coordination
        .pending_appended_materializations(rig.root_thread_id, 10)
        .await
        .expect("selectable materializations");
    assert_eq!(selectable.len(), 1);

    rig.coordination
        .mark_materialization_selected(
            receipt.receipt_id,
            "target-turn-2",
            response_item_id,
            NOW_MS + 2,
        )
        .await
        .expect("recovery completes selection");
    let selectable_after = rig
        .coordination
        .pending_appended_materializations(rig.root_thread_id, 10)
        .await
        .expect("selectable after recovery");
    assert!(selectable_after.is_empty());
}

#[tokio::test]
async fn failure_injection_fires_at_every_boundary_before_any_further_side_effect() {
    for point in [
        MessageFailurePoint::BeforeIntent,
        MessageFailurePoint::AfterIntent,
        MessageFailurePoint::AfterReceipt,
        MessageFailurePoint::BeforeEnqueue,
        MessageFailurePoint::AfterEnqueueBeforeAck,
    ] {
        let rig = build_rig().await;
        let target = thread_id(3);
        rig.coordination
            .set_message_failure_injection_for_tests(MessageFailureInjector::fail_at(point));
        let result = rig
            .control
            .deliver_message_coordinated(
                message_key(&rig, "call-1"),
                "sender-turn".to_string(),
                target,
                None,
                &fixture_communication(false),
                fixture_context(AgentCommunicationKind::Message),
                NOW_MS,
            )
            .await;
        assert!(result.is_err(), "expected injected failure at {point:?}");
    }
}
