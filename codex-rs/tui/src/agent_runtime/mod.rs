//! Bounded, reducer-owned runtime state for descendant agents.

#[allow(
    dead_code,
    reason = "pricing integrations consume this versioned contract when they can supply a quote"
)]
mod cost_projection;
#[allow(
    dead_code,
    reason = "the focused display contract is landed before its call-site migration"
)]
mod display_identity;
mod lifecycle;
mod model;
mod reducer;
mod snapshot;
mod telemetry;
mod workspace;

#[allow(
    unused_imports,
    reason = "the focused display contract is landed before its call-site migration"
)]
pub(crate) use display_identity::AgentDisplayIdentity;
pub(crate) use lifecycle::AgentRuntimeController;
pub(crate) use model::AgentLifecycle;
pub(crate) use workspace::AgentWorkspace;

#[cfg(test)]
#[path = "reducer_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "display_identity_tests.rs"]
mod display_identity_tests;

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "telemetry_tests.rs"]
mod telemetry_tests;

#[cfg(test)]
#[path = "gallery_tests.rs"]
mod gallery_tests;

#[cfg(test)]
#[path = "cost_projection_tests.rs"]
mod cost_projection_tests;

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod snapshot_tests;

#[cfg(test)]
#[path = "workspace_tests.rs"]
mod workspace_tests;
