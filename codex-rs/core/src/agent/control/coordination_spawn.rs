//! Enabled-coordination spawn orchestration (Stage 3 contract freeze, Decision 6).
//!
//! This is the "small branch at the existing seam" the Stage 3 preflight calls for, kept out of
//! `spawn.rs` (already 1136 LoC) as its own sibling module. It wraps -- rather than modifies --
//! `AgentControl::begin_agent_spawn_internal`: the disabled/default spawn entry points
//! (`spawn_agent`, `spawn_agent_with_metadata`, `begin_agent_spawn_with_communication`) never
//! call anything in this file, so their behavior is untouched by its existence.
//!
//! `spawn_agent_coordinated` reads `AgentControl`'s crate-private `coordination` field and
//! returns an error immediately, before any controlled side effect, unless that field holds
//! `CoordinationControl::Enabled`. In this stage only test code can produce `Enabled`
//! (`AgentControl::with_coordination_enabled_for_tests` /
//! `CoordinationState::new_for_tests`). There is no production caller.
//!
//! Frozen enabled order (re-verified 2026-07-21): resolve durable operation key (Decision 5) ->
//! preallocate thread+turn ids -> one command transaction reserves assignment g1 + sender intent
//! -> one recipient transaction accepts/binds g1 + durable receipt -> only then create the child
//! with the exact preallocated identity -> enqueue receipt ref/start exact turn -> ack sender.
//! Root usability (state absent/quarantined/poisoned) is checked at every boundary, not once
//! upfront.

use super::spawn::SpawnInitialInput;
use super::*;
use crate::coordination::CoordinationControl;
use crate::coordination::OperationIdentityKey;
use crate::coordination::PreallocatedThreadIdentity;
use crate::coordination::SpawnFailurePoint;
use crate::coordination::SpawnReservationStage;

fn coordination_gate_err(err: impl std::fmt::Display) -> CodexErr {
    CodexErr::Fatal(format!("coordination gate: {err}"))
}

impl AgentControl {
    /// Spawn a child under the enabled coordination path, following the frozen ordering above.
    ///
    /// Reads `self.coordination`: if it is `Disabled` (the unconditional production default),
    /// this returns an error immediately without any controlled side effect. It only proceeds
    /// past that check when `self.coordination` is `Enabled`, which in this stage only test code
    /// can construct (`AgentControl::with_coordination_enabled_for_tests`).
    ///
    /// A duplicate call for the identical `key` (and therefore the identical resolved operation
    /// id) never creates a second child: it resumes from whatever stage the first attempt
    /// reached, and once that stage is `Acknowledged` (the prior attempt ran to completion) it
    /// returns the already-created child's live snapshot directly instead of spawning again.
    pub(crate) async fn spawn_agent_coordinated(
        &self,
        key: OperationIdentityKey,
        config: Config,
        initial_input: Vec<UserInput>,
        session_source: Option<SessionSource>,
        mut options: SpawnAgentOptions,
    ) -> CodexResult<LiveAgent> {
        let coordination = match &self.coordination {
            CoordinationControl::Disabled => {
                return Err(CodexErr::UnsupportedOperation(
                    "coordination is disabled".to_string(),
                ));
            }
            CoordinationControl::Enabled(state) => Arc::clone(state),
        };
        let root_thread_id = key.root_thread_id;

        // Boundary: before the command transaction reserves assignment g1 + sender intent.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_spawn_failure_injection(SpawnFailurePoint::BeforeIntent)
            .map_err(coordination_gate_err)?;

        let operation_id = coordination.operation_identity.resolve(key);
        let identity = coordination
            .spawn_reservations
            .reserve_intent(operation_id, PreallocatedThreadIdentity::generate);

        // Boundary: after intent is committed, before the recipient's accept/bind transaction.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_spawn_failure_injection(SpawnFailurePoint::AfterIntent)
            .map_err(coordination_gate_err)?;

        let stage_before_receipt = coordination.spawn_reservations.stage(operation_id);
        coordination
            .spawn_reservations
            .advance(operation_id, SpawnReservationStage::ReceiptAccepted);

        // Boundary: after the receipt is durably accepted, before creating the child.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_spawn_failure_injection(SpawnFailurePoint::AfterReceipt)
            .map_err(coordination_gate_err)?;
        coordination
            .check_spawn_failure_injection(SpawnFailurePoint::BeforeChildCreation)
            .map_err(coordination_gate_err)?;

        if stage_before_receipt >= Some(SpawnReservationStage::Acknowledged) {
            // A prior attempt for this exact operation already ran to completion (child created
            // and delivered, sender acknowledged). Reuse it; never spawn a second one. An earlier
            // stage (including `ChildCreated`, which can be reached by an attempt that then
            // failed before delivery/ack) re-enters the creation path below instead: if that
            // prior child is still alive, `begin_agent_spawn_internal` surfaces the exact same
            // preallocated-thread-id collision as an error rather than silently duplicating it;
            // if it was already rolled back, this call recreates it under the identical identity.
            return self.reuse_coordinated_child(identity).await;
        }

        options.preallocated_identity = Some(identity);
        let transaction = Box::pin(self.begin_agent_spawn_internal(
            config,
            SpawnInitialInput::UserInput(initial_input),
            session_source,
            options,
        ))
        .await?;
        coordination
            .spawn_reservations
            .advance(operation_id, SpawnReservationStage::ChildCreated);

        // Boundary: after the controlled side effect (child created), before delivering the
        // initial receipt/turn and acknowledging the sender. A failure here leaves `transaction`
        // undelivered; its `Drop` schedules the existing spawn rollback, so no half-visible
        // child survives.
        coordination
            .ensure_root_usable(root_thread_id)
            .map_err(coordination_gate_err)?;
        coordination
            .check_spawn_failure_injection(SpawnFailurePoint::AfterSideEffectBeforeAck)
            .map_err(coordination_gate_err)?;

        let live_agent = transaction.deliver().await?;
        coordination
            .spawn_reservations
            .advance(operation_id, SpawnReservationStage::Acknowledged);
        Ok(live_agent)
    }

    async fn reuse_coordinated_child(
        &self,
        identity: PreallocatedThreadIdentity,
    ) -> CodexResult<LiveAgent> {
        let metadata = self
            .get_agent_metadata(identity.thread_id)
            .unwrap_or_default();
        let status = self.get_status(identity.thread_id).await;
        Ok(LiveAgent {
            thread_id: identity.thread_id,
            metadata,
            status,
        })
    }
}
