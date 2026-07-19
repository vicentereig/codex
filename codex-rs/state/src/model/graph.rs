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
