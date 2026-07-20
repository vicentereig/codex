//! Closed, checked coordination facts shared by durable state and API projections.
//!
//! This crate intentionally contains no runtime producer or public capability.

mod checked;

pub use checked::*;

#[cfg(test)]
#[path = "checked_tests.rs"]
mod checked_tests;
