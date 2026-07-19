use strum::AsRefStr;
use strum::Display;
use strum::EnumString;

/// Durable lifecycle for a turn-owned delegation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum DurableDelegationStatus {
    Unknown,
    Reserved,
    Bound,
    Running,
    CancelRequested,
    Retryable,
    Completed,
    Failed,
    Cancelled,
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableDelegationRecord {
    pub delegation_id: String,
    pub run_id: String,
    pub parent_thread_id: String,
    pub parent_turn_id: String,
    pub child_thread_id: Option<String>,
    pub agent_path: String,
    pub status: DurableDelegationStatus,
    pub version: i64,
    pub attempt: i64,
    pub lease_epoch: i64,
    pub outcome: Option<String>,
    pub last_error: Option<String>,
    pub terminal_event_id: Option<String>,
    pub delivery_receipt: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegationFinalization {
    Ready,
    Partial,
    Blocked,
}

impl DurableDelegationStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Detached
        )
    }
}

/// Status attached to a directional thread-spawn edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum DirectionalThreadSpawnEdgeStatus {
    Open,
    Closed,
}
