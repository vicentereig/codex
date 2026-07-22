use pretty_assertions::assert_eq;
use uuid::Uuid;

use super::aggregate_test_support::ROOT;
use super::message_api::AcceptFollowupGeneration;
use super::message_api::CaptureQueueMessageReceipt;
use super::message_api::CaptureReceiptOutcome;
use super::message_api::MaterializationStatus;
use super::message_api::MessageReceiptStatus;
use super::message_api::MessageSemanticSlot;
use super::message_api::accept_followup_generation;
use super::message_api::capture_queue_message_receipt;
use super::message_api::commit_materialization;
use super::message_api::mark_materialization_rollout_appended;
use super::message_api::mark_materialization_selected;
use super::message_api::mark_receipt_enqueued;
use super::message_api::pending_appended_materializations;
use super::message_api::pending_committed_materializations;
use super::message_api::pending_committed_receipts;
use super::recovery_test_support::runtime_with_root;
use super::recovery_test_support::thread_id;

const NOW: i64 = 2_000_000_000_000;
const SENDER: &str = "019f7c6c-1111-7000-8000-000000000601";
const TARGET: &str = "019f7c6c-1111-7000-8000-000000000901";
const TARGET_B: &str = "019f7c6c-1111-7000-8000-000000000902";

fn message_params(operation_id: Uuid, target: &str) -> CaptureQueueMessageReceipt {
    CaptureQueueMessageReceipt {
        receipt_id: Uuid::now_v7(),
        operation_id,
        sender_thread_id: thread_id(SENDER),
        sender_turn_id: "sender-turn-1".to_string(),
        target_thread_id: thread_id(target),
        now_ms: NOW,
    }
}

fn followup_params(
    operation_id: Uuid,
    target: &str,
    bound_turn_id: &str,
) -> AcceptFollowupGeneration {
    AcceptFollowupGeneration {
        receipt_id: Uuid::now_v7(),
        operation_id,
        sender_thread_id: thread_id(SENDER),
        sender_turn_id: "sender-turn-1".to_string(),
        target_thread_id: thread_id(target),
        bound_turn_id: bound_turn_id.to_string(),
        now_ms: NOW,
    }
}

#[tokio::test]
async fn queue_message_captures_no_generation_before_any_followup() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let outcome = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture");
    let CaptureReceiptOutcome::Captured(receipt) = outcome else {
        panic!("expected fresh capture");
    };
    assert_eq!(receipt.captured_generation, None);
    assert_eq!(receipt.bound_turn_id, None);
    assert_eq!(receipt.status, MessageReceiptStatus::Committed);
    assert_eq!(receipt.semantic_slot, MessageSemanticSlot::Message);
    assert!(!receipt.trigger_turn);
}

#[tokio::test]
async fn followup_reserves_sequential_generations_and_may_bind_active_turn() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");

    let first = accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(Uuid::now_v7(), TARGET, "turn-active"),
    )
    .await
    .expect("first followup");
    let CaptureReceiptOutcome::Captured(first) = first else {
        panic!("expected fresh capture");
    };
    assert_eq!(first.captured_generation, Some(1));
    assert_eq!(first.bound_turn_id.as_deref(), Some("turn-active"));

    // Second follow-up "may bind same active turn": pass the identical turn id and prove the
    // generation still advances sequentially.
    let second = accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(Uuid::now_v7(), TARGET, "turn-active"),
    )
    .await
    .expect("second followup");
    let CaptureReceiptOutcome::Captured(second) = second else {
        panic!("expected fresh capture");
    };
    assert_eq!(second.captured_generation, Some(2));
    assert_eq!(second.bound_turn_id.as_deref(), Some("turn-active"));
}

#[tokio::test]
async fn followup_generation_is_idempotent_on_operation_id() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let operation_id = Uuid::now_v7();

    let first = accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(operation_id, TARGET, "turn-a"),
    )
    .await
    .expect("first");
    let CaptureReceiptOutcome::Captured(first) = first else {
        panic!("expected fresh capture");
    };

    // Retry with the *same* operation_id but a fresh receipt_id (simulating a handler retry that
    // re-derives a new receipt_id locally before discovering the operation was already handled).
    let retry = accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(operation_id, TARGET, "turn-a"),
    )
    .await
    .expect("retry");
    let CaptureReceiptOutcome::Duplicate(retry) = retry else {
        panic!("expected duplicate, generation must not advance twice");
    };
    assert_eq!(retry.receipt_id, first.receipt_id);
    assert_eq!(retry.captured_generation, Some(1));

    // A genuinely new operation must reserve generation 2, proving the retry above never
    // consumed a generation slot.
    let next = accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(Uuid::now_v7(), TARGET, "turn-a"),
    )
    .await
    .expect("next");
    let CaptureReceiptOutcome::Captured(next) = next else {
        panic!("expected fresh capture");
    };
    assert_eq!(next.captured_generation, Some(2));
}

#[tokio::test]
async fn queue_message_after_followup_acceptance_captures_the_accepted_generation() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");

    accept_followup_generation(
        &runtime,
        thread_id(ROOT),
        epoch,
        followup_params(Uuid::now_v7(), TARGET, "turn-2"),
    )
    .await
    .expect("followup accepted first");

    let outcome = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture after followup");
    let CaptureReceiptOutcome::Captured(receipt) = outcome else {
        panic!("expected fresh capture");
    };
    assert_eq!(receipt.captured_generation, Some(1));
    assert_eq!(receipt.bound_turn_id.as_deref(), Some("turn-2"));
}

/// "Queue message N versus acceptance N+1 both orders fence/converge": run the message-capture
/// and follow-up-acceptance calls in both possible orders (message-first and followup-first) and
/// prove each order converges to the same deterministic, forever-bound receipt shape rather than
/// a torn or ambiguous read.
#[tokio::test]
async fn message_capture_and_followup_acceptance_converge_regardless_of_order() {
    // Order A: message first, then followup.
    {
        let (runtime, epoch) = runtime_with_root().await.expect("runtime");
        let message = capture_queue_message_receipt(
            &runtime,
            thread_id(ROOT),
            epoch,
            message_params(Uuid::now_v7(), TARGET_B),
        )
        .await
        .expect("message first");
        let CaptureReceiptOutcome::Captured(message) = message else {
            panic!("fresh capture");
        };
        // The message committed before any follow-up existed: forever bound to "no generation".
        assert_eq!(message.captured_generation, None);

        let followup = accept_followup_generation(
            &runtime,
            thread_id(ROOT),
            epoch,
            followup_params(Uuid::now_v7(), TARGET_B, "turn-order-a"),
        )
        .await
        .expect("followup second");
        let CaptureReceiptOutcome::Captured(followup) = followup else {
            panic!("fresh capture");
        };
        assert_eq!(followup.captured_generation, Some(1));

        // The already-committed message receipt is immutable: re-reading it must still show the
        // generation it was forever bound to, never the later-accepted one.
        let pending = pending_committed_receipts(&runtime, thread_id(ROOT), 10)
            .await
            .expect("pending receipts");
        let stored_message = pending
            .iter()
            .find(|receipt| receipt.receipt_id == message.receipt_id)
            .expect("message receipt persisted");
        assert_eq!(stored_message.captured_generation, None);
    }

    // Order B: followup first, then message -- the message must capture the now-accepted
    // generation instead of racing past it.
    {
        let (runtime, epoch) = runtime_with_root().await.expect("runtime");
        let followup = accept_followup_generation(
            &runtime,
            thread_id(ROOT),
            epoch,
            followup_params(Uuid::now_v7(), TARGET_B, "turn-order-b"),
        )
        .await
        .expect("followup first");
        let CaptureReceiptOutcome::Captured(followup) = followup else {
            panic!("fresh capture");
        };
        assert_eq!(followup.captured_generation, Some(1));

        let message = capture_queue_message_receipt(
            &runtime,
            thread_id(ROOT),
            epoch,
            message_params(Uuid::now_v7(), TARGET_B),
        )
        .await
        .expect("message second");
        let CaptureReceiptOutcome::Captured(message) = message else {
            panic!("fresh capture");
        };
        assert_eq!(message.captured_generation, Some(1));
        assert_eq!(message.bound_turn_id.as_deref(), Some("turn-order-b"));
    }
}

#[tokio::test]
async fn restart_recovery_re_enqueues_receipts_committed_before_enqueue() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let outcome = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture");
    let CaptureReceiptOutcome::Captured(receipt) = outcome else {
        panic!("fresh capture");
    };

    // Simulated crash: the receipt committed but the queue side effect never landed. Restart
    // recovery must find it as pending.
    let pending = pending_committed_receipts(&runtime, thread_id(ROOT), 10)
        .await
        .expect("pending receipts");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].receipt_id, receipt.receipt_id);
    assert_eq!(pending[0].status, MessageReceiptStatus::Committed);

    // Recovery re-enqueues it.
    mark_receipt_enqueued(&runtime, receipt.receipt_id, NOW + 1)
        .await
        .expect("mark enqueued");
    let pending_after = pending_committed_receipts(&runtime, thread_id(ROOT), 10)
        .await
        .expect("pending receipts after recovery");
    assert!(
        pending_after.is_empty(),
        "recovered receipt must no longer be pending"
    );

    // Idempotent: recovery may call mark_receipt_enqueued again (e.g. a second recovery pass)
    // without erroring.
    mark_receipt_enqueued(&runtime, receipt.receipt_id, NOW + 2)
        .await
        .expect("mark enqueued again is a no-op");
}

#[tokio::test]
async fn restart_recovery_completes_materialization_committed_before_rollout_append() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let receipt = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture");
    let CaptureReceiptOutcome::Captured(receipt) = receipt else {
        panic!("fresh capture");
    };
    let response_item_id = Uuid::now_v7();
    let materialization = commit_materialization(
        &runtime,
        thread_id(ROOT),
        receipt.receipt_id,
        "target-turn-1",
        response_item_id,
        NOW,
    )
    .await
    .expect("commit materialization");
    assert_eq!(materialization.status, MaterializationStatus::Committed);

    // Simulated crash: materialization committed but the rollout append never landed.
    let pending = pending_committed_materializations(&runtime, thread_id(ROOT), 10)
        .await
        .expect("pending materializations");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].receipt_id, receipt.receipt_id);

    // Recovery completes the append.
    mark_materialization_rollout_appended(
        &runtime,
        receipt.receipt_id,
        "target-turn-1",
        response_item_id,
        NOW + 1,
    )
    .await
    .expect("mark rollout appended");
    let pending_after = pending_committed_materializations(&runtime, thread_id(ROOT), 10)
        .await
        .expect("pending after recovery");
    assert!(pending_after.is_empty());
}

#[tokio::test]
async fn restart_recovery_makes_rollout_appended_materializations_selectable() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let receipt = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture");
    let CaptureReceiptOutcome::Captured(receipt) = receipt else {
        panic!("fresh capture");
    };
    let response_item_id = Uuid::now_v7();
    commit_materialization(
        &runtime,
        thread_id(ROOT),
        receipt.receipt_id,
        "target-turn-2",
        response_item_id,
        NOW,
    )
    .await
    .expect("commit");
    mark_materialization_rollout_appended(
        &runtime,
        receipt.receipt_id,
        "target-turn-2",
        response_item_id,
        NOW + 1,
    )
    .await
    .expect("append");

    // Simulated crash: rollout append landed but selection never happened.
    let selectable = pending_appended_materializations(&runtime, thread_id(ROOT), 10)
        .await
        .expect("selectable materializations");
    assert_eq!(selectable.len(), 1);
    assert_eq!(selectable[0].receipt_id, receipt.receipt_id);
    assert_eq!(selectable[0].status, MaterializationStatus::RolloutAppended);

    mark_materialization_selected(
        &runtime,
        receipt.receipt_id,
        "target-turn-2",
        response_item_id,
        NOW + 2,
    )
    .await
    .expect("mark selected");
    let selectable_after = pending_appended_materializations(&runtime, thread_id(ROOT), 10)
        .await
        .expect("selectable after recovery");
    assert!(selectable_after.is_empty());
}

#[tokio::test]
async fn materialization_status_cannot_move_backward() {
    let (runtime, epoch) = runtime_with_root().await.expect("runtime");
    let receipt = capture_queue_message_receipt(
        &runtime,
        thread_id(ROOT),
        epoch,
        message_params(Uuid::now_v7(), TARGET),
    )
    .await
    .expect("capture");
    let CaptureReceiptOutcome::Captured(receipt) = receipt else {
        panic!("fresh capture");
    };
    let response_item_id = Uuid::now_v7();
    commit_materialization(
        &runtime,
        thread_id(ROOT),
        receipt.receipt_id,
        "target-turn-3",
        response_item_id,
        NOW,
    )
    .await
    .expect("commit");
    mark_materialization_rollout_appended(
        &runtime,
        receipt.receipt_id,
        "target-turn-3",
        response_item_id,
        NOW + 1,
    )
    .await
    .expect("append");
    mark_materialization_selected(
        &runtime,
        receipt.receipt_id,
        "target-turn-3",
        response_item_id,
        NOW + 2,
    )
    .await
    .expect("select");

    // Calling "mark rollout appended" again on an already-selected row must be a silent no-op
    // (the underlying UPDATE only matches status='committed'), never a regression.
    mark_materialization_rollout_appended(
        &runtime,
        receipt.receipt_id,
        "target-turn-3",
        response_item_id,
        NOW + 3,
    )
    .await
    .expect("no-op, must not error");
    let selectable = pending_appended_materializations(&runtime, thread_id(ROOT), 10)
        .await
        .expect("selectable");
    assert!(
        selectable.is_empty(),
        "already-selected row must not reappear as merely rollout_appended"
    );
}
