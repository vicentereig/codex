//! Closed, checked coordination facts shared by durable state and API projections.
//!
//! This crate intentionally contains no runtime producer or public capability.

mod checked;
mod compatibility;
mod event;
mod event_invariants;
mod evidence;

pub use checked::*;
pub use compatibility::*;
pub use event::*;
pub use event_invariants::CoordinationEvent;
pub use evidence::*;

#[cfg(test)]
#[path = "checked_tests.rs"]
mod checked_tests;

#[cfg(test)]
#[path = "compatibility_tests.rs"]
mod compatibility_tests;

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod evidence_tests;

#[cfg(test)]
#[path = "event_fixture_tests.rs"]
mod event_fixture_tests;
