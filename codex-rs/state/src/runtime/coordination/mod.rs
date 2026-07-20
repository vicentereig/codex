mod accept_duplicate;
mod accept_transitions;
mod aggregate_event;
mod aggregate_journal;
mod aggregate_queries;
mod aggregates;
mod assignment_recovery;
mod authority;
mod authority_marker;
mod command_event;
mod command_identity;
mod command_leases;
mod command_recovery;
mod command_rows;
mod command_transaction;
mod commands;
mod degradation;
mod degradation_integrity;
mod degradation_outbox;
mod inbox;
mod inbox_claim;
mod inbox_maintenance;
mod inbox_receipt;
mod inbox_receipt_identity;
mod inbox_recovery;
mod inbox_rows;
mod inclusion;
mod inclusion_gate;
mod inclusion_outcome;
mod inclusion_rows;
mod inclusion_selection;
mod legacy_checkpoints;
mod legacy_degradations;
mod legacy_links;
mod maintenance_degradation;
mod recovery;
mod recovery_batch;
mod recovery_guard;
mod reserve_transition;
mod terminal_facts;
mod terminal_transition;
mod wait_transitions;

pub use authority::CoordinationAuthorityStatus;
pub(crate) use authority::initialize_authority;
#[cfg(test)]
pub(crate) use authority_marker::MARKER_FILE_NAME;
pub(crate) use authority_marker::prepare_fresh_after_corruption_marker;

#[cfg(test)]
#[path = "aggregate_concurrency_tests.rs"]
mod aggregate_concurrency_tests;
#[cfg(test)]
#[path = "aggregate_failure_support.rs"]
mod aggregate_failure_support;
#[cfg(test)]
#[path = "aggregate_failure_tests.rs"]
mod aggregate_failure_tests;
#[cfg(test)]
#[path = "aggregate_race_tests.rs"]
mod aggregate_race_tests;
#[cfg(test)]
#[path = "aggregate_schema_tests.rs"]
mod aggregate_schema_tests;
#[cfg(test)]
#[path = "aggregate_sql_adversarial_tests.rs"]
mod aggregate_sql_adversarial_tests;
#[cfg(test)]
#[path = "aggregate_test_support.rs"]
mod aggregate_test_support;
#[cfg(test)]
#[path = "aggregate_transition_tests.rs"]
mod aggregate_transition_tests;
#[cfg(test)]
#[path = "authority_tests.rs"]
mod authority_tests;
#[cfg(test)]
#[path = "capability_off_tests.rs"]
mod capability_off_tests;
#[cfg(test)]
#[path = "command_atomicity_tests.rs"]
mod command_atomicity_tests;
#[cfg(test)]
#[path = "command_fencing_tests.rs"]
mod command_fencing_tests;
#[cfg(test)]
#[path = "command_lease_tests.rs"]
mod command_lease_tests;
#[cfg(test)]
#[path = "command_payload_tests.rs"]
mod command_payload_tests;
#[cfg(test)]
#[path = "commands_tests.rs"]
mod commands_tests;
#[cfg(test)]
#[path = "coordination_privacy_gate_tests.rs"]
mod coordination_privacy_gate_tests;
#[cfg(test)]
#[path = "failure_injection_aggregate_matrix_support.rs"]
mod failure_injection_aggregate_matrix_support;
#[cfg(test)]
#[path = "failure_injection_aggregate_matrix_tests.rs"]
mod failure_injection_aggregate_matrix_tests;
#[cfg(test)]
#[path = "failure_injection_command_kind_tests.rs"]
mod failure_injection_command_kind_tests;
#[cfg(test)]
#[path = "failure_injection_counter_integrity.rs"]
mod failure_injection_counter_integrity;
#[cfg(test)]
#[path = "failure_injection_degradation_integrity.rs"]
mod failure_injection_degradation_integrity;
#[cfg(test)]
#[path = "failure_injection_inbox_matrix_support.rs"]
mod failure_injection_inbox_matrix_support;
#[cfg(test)]
#[path = "failure_injection_inbox_matrix_tests.rs"]
mod failure_injection_inbox_matrix_tests;
#[cfg(test)]
#[path = "failure_injection_integrity.rs"]
mod failure_injection_integrity;
#[cfg(test)]
#[path = "failure_injection_integrity_tests.rs"]
mod failure_injection_integrity_tests;
#[cfg(test)]
#[path = "failure_injection_recovery_batch_matrix_tests.rs"]
mod failure_injection_recovery_batch_matrix_tests;
#[cfg(test)]
#[path = "failure_injection_recovery_matrix_support.rs"]
mod failure_injection_recovery_matrix_support;
#[cfg(test)]
#[path = "failure_injection_recovery_outbox_matrix_support.rs"]
mod failure_injection_recovery_outbox_matrix_support;
#[cfg(test)]
#[path = "failure_injection_recovery_outbox_matrix_tests.rs"]
mod failure_injection_recovery_outbox_matrix_tests;
#[cfg(test)]
#[path = "failure_injection_recovery_semantic_matrix_support.rs"]
mod failure_injection_recovery_semantic_matrix_support;
#[cfg(test)]
#[path = "failure_injection_recovery_semantic_matrix_tests.rs"]
mod failure_injection_recovery_semantic_matrix_tests;
#[cfg(test)]
#[path = "failure_injection_sender_tests.rs"]
mod failure_injection_sender_tests;
#[cfg(test)]
#[path = "failure_injection_snapshot_tests.rs"]
mod failure_injection_snapshot_tests;
#[cfg(test)]
#[path = "failure_injection_support.rs"]
mod failure_injection_support;
#[cfg(test)]
#[path = "failure_injection_tests.rs"]
mod failure_injection_tests;
#[cfg(test)]
#[path = "inbox_failure_tests.rs"]
mod inbox_failure_tests;
#[cfg(test)]
#[path = "inbox_interrupt_tests.rs"]
mod inbox_interrupt_tests;
#[cfg(test)]
#[path = "inbox_privacy_tests.rs"]
mod inbox_privacy_tests;
#[cfg(test)]
#[path = "inbox_receipt_tests.rs"]
mod inbox_receipt_tests;
#[cfg(test)]
#[path = "inbox_sql_adversarial_tests.rs"]
mod inbox_sql_adversarial_tests;
#[cfg(test)]
#[path = "inbox_test_support.rs"]
mod inbox_test_support;
#[cfg(test)]
#[path = "inbox_ttl_tests.rs"]
mod inbox_ttl_tests;
#[cfg(test)]
#[path = "inclusion_retry_tests.rs"]
mod inclusion_retry_tests;
#[cfg(test)]
#[path = "inclusion_terminal_gate_tests.rs"]
mod inclusion_terminal_gate_tests;
#[cfg(test)]
#[path = "recovery_adversarial_tests.rs"]
mod recovery_adversarial_tests;
#[cfg(test)]
#[path = "recovery_checkpoint_tests.rs"]
mod recovery_checkpoint_tests;
#[cfg(test)]
#[path = "recovery_failure_tests.rs"]
mod recovery_failure_tests;
#[cfg(test)]
#[path = "recovery_maintenance_tests.rs"]
mod recovery_maintenance_tests;
#[cfg(test)]
#[path = "recovery_outbox_tests.rs"]
mod recovery_outbox_tests;
#[cfg(test)]
#[path = "recovery_storage_tests.rs"]
mod recovery_storage_tests;
#[cfg(test)]
#[path = "recovery_test_support.rs"]
mod recovery_test_support;
#[cfg(test)]
#[path = "transaction_raii_tests.rs"]
mod transaction_raii_tests;
#[cfg(test)]
#[path = "transaction_seam_tests.rs"]
mod transaction_seam_tests;
