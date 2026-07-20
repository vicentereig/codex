mod accept_duplicate;
mod accept_transitions;
mod aggregate_event;
mod aggregate_journal;
mod aggregate_queries;
mod aggregates;
mod authority;
mod authority_marker;
mod command_event;
mod command_identity;
mod command_leases;
mod command_rows;
mod commands;
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
