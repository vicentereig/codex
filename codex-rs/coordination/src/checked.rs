use std::fmt;
use std::num::NonZeroU32;
use std::num::NonZeroU64;

use codex_protocol::AgentPath;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use uuid::Uuid;
use uuid::Version;

pub const MAX_AGENT_PATH_BYTES: usize = 256;
pub const MAX_CIPHERTEXT_BYTES: u32 = 65_536;
pub const MAX_EVENT_BYTES: usize = 8_192;
pub const MAX_ID_BYTES: usize = 128;
pub const MAX_TEXT_BYTES: usize = 512;

#[derive(Debug, thiserror::Error)]
pub enum CoordinationError {
    #[error("{field} is invalid: {reason}")]
    Invalid {
        field: &'static str,
        reason: &'static str,
    },
    #[error("{field} exceeds its {limit}-byte limit")]
    TooLong { field: &'static str, limit: usize },
    #[error("coordination event invariant failed: {0}")]
    Invariant(&'static str),
    #[error("failed to serialize coordination data: {0}")]
    Serialization(#[from] serde_json::Error),
}

macro_rules! uuid_id {
    ($name:ident, $($version:pat_param)|+) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new_v7() -> Self {
                Self(Uuid::now_v7())
            }

            pub fn parse(value: &str) -> Result<Self, CoordinationError> {
                let uuid = Uuid::parse_str(value).map_err(|_| CoordinationError::Invalid {
                    field: stringify!($name),
                    reason: "must be a UUID",
                })?;
                if uuid.is_nil() {
                    return Err(CoordinationError::Invalid {
                        field: stringify!($name),
                        reason: "must not be nil",
                    });
                }
                if !matches!(uuid.get_version(), $(Some($version))|+) {
                    return Err(CoordinationError::Invalid {
                        field: stringify!($name),
                        reason: "uses a disallowed UUID version",
                    });
                }
                Ok(Self(uuid))
            }

            pub fn as_uuid(self) -> Uuid {
                self.0
            }

        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = CoordinationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(&value).map_err(serde::de::Error::custom)
            }
        }
    };
}

uuid_id!(AssignmentId, Version::SortRand);
uuid_id!(CoordinationEventId, Version::SortRand | Version::Sha1);
uuid_id!(CoordinationOperationId, Version::SortRand);
uuid_id!(HandoffId, Version::SortRand);
uuid_id!(ReceiptId, Version::SortRand);
uuid_id!(ResultId, Version::SortRand);
uuid_id!(StateEpoch, Version::SortRand);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct BoundedId<const N: usize>(String);

impl<const N: usize> BoundedId<N> {
    pub fn new(value: impl Into<String>) -> Result<Self, CoordinationError> {
        let value = value.into();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(CoordinationError::Invalid {
                field: "id",
                reason: "must be non-empty and contain no control characters",
            });
        }
        if value.len() > N {
            return Err(CoordinationError::TooLong {
                field: "id",
                limit: N,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de, const N: usize> Deserialize<'de> for BoundedId<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CoordinationAgentPath(AgentPath);

impl CoordinationAgentPath {
    pub fn new(path: AgentPath) -> Result<Self, CoordinationError> {
        if path.as_str().len() > MAX_AGENT_PATH_BYTES {
            return Err(CoordinationError::TooLong {
                field: "agentPath",
                limit: MAX_AGENT_PATH_BYTES,
            });
        }
        Ok(Self(path))
    }

    pub fn as_agent_path(&self) -> &AgentPath {
        &self.0
    }
}

impl TryFrom<AgentPath> for CoordinationAgentPath {
    type Error = CoordinationError;

    fn try_from(value: AgentPath) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for CoordinationAgentPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(AgentPath::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SanitizedText(String);

impl SanitizedText {
    pub fn from_untrusted(value: &str) -> Self {
        let normalized = value.replace("\r\n", "\n");
        let controls_removed: String = normalized
            .chars()
            .map(|ch| match ch {
                '\t' | '\n' => ch,
                '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{9f}' => ' ',
                _ => ch,
            })
            .collect();
        let redacted = codex_secrets::redact_secrets(controls_removed);
        let collapsed = redacted.split_whitespace().collect::<Vec<_>>().join(" ");
        Self(truncate_utf8(&collapsed, MAX_TEXT_BYTES))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SanitizedText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let sanitized = Self::from_untrusted(&value);
        if sanitized.as_str() != value {
            return Err(serde::de::Error::custom(
                "text is not canonically sanitized",
            ));
        }
        Ok(sanitized)
    }
}

fn truncate_utf8(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_string();
    }
    let mut end = limit.saturating_sub('…'.len_utf8());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AssignmentGeneration(NonZeroU32);

impl AssignmentGeneration {
    pub fn new(value: u32) -> Result<Self, CoordinationError> {
        NonZeroU32::new(value)
            .filter(|value| value.get() <= i32::MAX as u32)
            .map(Self)
            .ok_or(CoordinationError::Invalid {
                field: "generation",
                reason: "must be in 1..=i32::MAX",
            })
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

impl<'de> Deserialize<'de> for AssignmentGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u32::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CoordinationRevision(NonZeroU64);

impl CoordinationRevision {
    pub fn new(value: u64) -> Result<Self, CoordinationError> {
        NonZeroU64::new(value)
            .filter(|value| value.get() <= i64::MAX as u64)
            .map(Self)
            .ok_or(CoordinationError::Invalid {
                field: "revision",
                reason: "must be in 1..=i64::MAX",
            })
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl<'de> Deserialize<'de> for CoordinationRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundedList<T, const N: usize> {
    items: Vec<T>,
    omitted_count: u16,
}

impl<T, const N: usize> BoundedList<T, N> {
    pub fn new(items: Vec<T>, omitted_count: u16) -> Result<Self, CoordinationError> {
        if items.len() > N || (omitted_count > 0 && items.len() != N) {
            return Err(CoordinationError::Invalid {
                field: "boundedList",
                reason: "item cap or omission count is inconsistent",
            });
        }
        Ok(Self {
            items,
            omitted_count,
        })
    }

    pub fn complete(items: Vec<T>) -> Result<Self, CoordinationError> {
        Self::new(items, 0)
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn omitted_count(&self) -> u16 {
        self.omitted_count
    }
}

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for BoundedList<T, N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawList<T> {
            items: Vec<T>,
            omitted_count: u16,
        }
        let raw = RawList::deserialize(deserializer)?;
        Self::new(raw.items, raw.omitted_count).map_err(serde::de::Error::custom)
    }
}
