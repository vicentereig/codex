use serde::Deserialize;
use serde::Serialize;

use crate::BoundedId;
use crate::MAX_ID_BYTES;
use crate::SanitizedText;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UnavailableReason {
    EncryptedPayload,
    MissingLegacyField,
    InvalidLegacyValue,
    OverLimit,
    AmbiguousSource,
    SourceDoesNotEncode,
    CorruptSource,
    StateLossDegraded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Evidence<T> {
    Known { value: T },
    Unavailable { reason: UnavailableReason },
    NotApplicable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContentSource {
    LegacyV1,
    InternalError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ContentEvidence {
    Known {
        value: SanitizedText,
        source: ContentSource,
    },
    Unavailable {
        reason: UnavailableReason,
    },
    NotApplicable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "camelCase")]
pub enum Requested<T> {
    Explicit { value: T },
    Inherited,
    Unavailable { reason: UnavailableReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    Ultra,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestedRuntime {
    model: Requested<BoundedId<MAX_ID_BYTES>>,
    reasoning_effort: Requested<ReasoningEffort>,
}

impl RequestedRuntime {
    pub fn new(
        model: Requested<BoundedId<MAX_ID_BYTES>>,
        reasoning_effort: Requested<ReasoningEffort>,
    ) -> Self {
        Self {
            model,
            reasoning_effort,
        }
    }

    pub fn model(&self) -> &Requested<BoundedId<MAX_ID_BYTES>> {
        &self.model
    }

    pub fn reasoning_effort(&self) -> &Requested<ReasoningEffort> {
        &self.reasoning_effort
    }
}
