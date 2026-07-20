mod accept_duplicate;
mod accept_transitions;
mod aggregate_event;
mod aggregate_journal;
mod aggregate_queries;
mod aggregates;
mod authority;
mod authority_marker;
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
