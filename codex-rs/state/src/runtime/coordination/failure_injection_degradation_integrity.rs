use codex_coordination::AssignmentGeneration;
use codex_coordination::BoundedId;
use codex_coordination::Evidence;
use codex_coordination::MAX_ID_BYTES;
use codex_coordination::SourceShape;
use codex_coordination::StateEpoch;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;

use crate::StateRuntime;
use crate::model::coordination_recovery::deterministic_degradation_id;
use crate::model::coordination_recovery::source_shape_sql;

macro_rules! wire_enum {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase")]
        enum $name {
            $($variant),+
        }

        impl $name {
            fn as_sql(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }
        }
    };
}

wire_enum!(WireSourceKind {
    ExogenousTerminal => "exogenousTerminal",
    LegacyReduction => "legacyReduction",
    Recovery => "recovery",
});
wire_enum!(WireReason {
    CoordinationTemporarilyUnavailable => "coordinationTemporarilyUnavailable",
    MissingProvenance => "missingProvenance",
    AmbiguousSource => "ambiguousSource",
    OverLimit => "overLimit",
    InvalidLegacyValue => "invalidLegacyValue",
    CorruptSource => "corruptSource",
    PoisonedAttempt => "poisonedAttempt",
    ExpiredPayload => "expiredPayload",
    StateLossDegraded => "stateLossDegraded",
});
wire_enum!(WireSemanticSlot {
    AssignmentRequested => "assignmentRequested",
    AssignmentAccepted => "assignmentAccepted",
    AssignmentGenerationClosed => "assignmentGenerationClosed",
    MessageSubmissionRecorded => "messageSubmissionRecorded",
    MessageDurablyReceived => "messageDurablyReceived",
    MessageIncludedInModelInput => "messageIncludedInModelInput",
    WaitStarted => "waitStarted",
    WaitEnded => "waitEnded",
    InterruptRequested => "interruptRequested",
    InterruptDurablyReceived => "interruptDurablyReceived",
    TurnInterrupted => "turnInterrupted",
    Detached => "detached",
    DependencyDeclared => "dependencyDeclared",
    OwnershipChanged => "ownershipChanged",
    TurnCompleted => "turnCompleted",
    TerminalResultObserved => "terminalResultObserved",
    HandoffDeliveryAttempted => "handoffDeliveryAttempted",
    HandoffDurablyReceived => "handoffDurablyReceived",
    HandoffIncludedInModelInput => "handoffIncludedInModelInput",
    HandoffDeliveryFailed => "handoffDeliveryFailed",
    LegacyInteractionObserved => "legacyInteractionObserved",
});
wire_enum!(WireTerminalKind {
    Completed => "completed",
    Interrupted => "interrupted",
});
wire_enum!(WireTerminalOutcome {
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
    Interrupted => "interrupted",
    Unknown => "unknown",
});
wire_enum!(WireRecoveryRecordKind {
    Assignment => "assignment",
    Command => "command",
    Inbox => "inbox",
});

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceIdentity {
    shape: SourceShape,
    source_thread_id: Option<ThreadId>,
    source_turn_id: Option<BoundedId<MAX_ID_BYTES>>,
    source_item_id: Option<BoundedId<MAX_ID_BYTES>>,
    source_ordinal: u64,
    semantic_slot: WireSemanticSlot,
}

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalIdentity {
    version: u16,
    root_thread_id: ThreadId,
    captured_state_epoch: Option<StateEpoch>,
    source: SourceIdentity,
    reason: WireReason,
    semantic_slot: WireSemanticSlot,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalRecord {
    identity: TerminalIdentity,
    source_kind: WireSourceKind,
    target_thread_id: ThreadId,
    target_turn_id: BoundedId<MAX_ID_BYTES>,
    terminal_kind: WireTerminalKind,
    terminal_outcome: WireTerminalOutcome,
    included_generations: Evidence<Vec<AssignmentGeneration>>,
    observed_at: i64,
    after_revision: u64,
}

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyIdentity {
    version: u16,
    root_thread_id: ThreadId,
    state_epoch: StateEpoch,
    source_kind: WireSourceKind,
    source: SourceIdentity,
    reason: WireReason,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyRecord {
    identity: LegacyIdentity,
    observed_at: i64,
    after_revision: u64,
}

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MaintenanceIdentity {
    version: u16,
    root_thread_id: ThreadId,
    state_epoch: StateEpoch,
    source_kind: WireSourceKind,
    record_kind: WireRecoveryRecordKind,
    record_id: BoundedId<MAX_ID_BYTES>,
    semantic_slot: WireSemanticSlot,
    reason: WireReason,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct MaintenanceRecord {
    identity: MaintenanceIdentity,
    observed_at: i64,
    after_revision: u64,
}

enum DecodedRecord {
    Terminal(TerminalRecord),
    Legacy(LegacyRecord),
    Maintenance(MaintenanceRecord),
}

pub(super) async fn assert_degradation_canonical(runtime: &StateRuntime) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT degradation_id,root_thread_id,state_epoch,source_kind,source_shape,\
         source_thread_id,source_turn_id,source_item_id,source_ordinal,recovery_record_kind,\
         recovery_record_id,semantic_slot,reason,target_thread_id,target_turn_id,terminal_kind,\
         terminal_outcome,included_generations_bytes,identity_bytes,identity_fingerprint,\
         canonical_record_bytes,canonical_record_fingerprint,adapter_version,sanitizer_version,\
         observed_at,after_revision FROM coordination_degradation_records",
    )
    .fetch_all(&*runtime.pool)
    .await?;
    for row in rows {
        let identity: Vec<u8> = row.get("identity_bytes");
        let record: Vec<u8> = row.get("canonical_record_bytes");
        let decoded = decode_record(
            row.get::<String, _>("source_kind").as_str(),
            &identity,
            &record,
        );
        anyhow::ensure!(
            decoded.is_some_and(|decoded| decoded.matches(&row))
                && deterministic_degradation_id(identity.as_slice())?.to_string()
                    == row.get::<String, _>("degradation_id")
                && Sha256::digest(identity.as_slice()).as_slice()
                    == row.get::<Vec<u8>, _>("identity_fingerprint")
                && Sha256::digest(record.as_slice()).as_slice()
                    == row.get::<Vec<u8>, _>("canonical_record_fingerprint"),
            "degradation canonical identity mismatch"
        );
    }
    Ok(())
}

fn decode_record(
    source_kind: &str,
    identity_bytes: &[u8],
    record_bytes: &[u8],
) -> Option<DecodedRecord> {
    match source_kind {
        "exogenousTerminal" => {
            decode_terminal(identity_bytes, record_bytes).map(DecodedRecord::Terminal)
        }
        "legacyReduction" => decode_legacy(identity_bytes, record_bytes).map(DecodedRecord::Legacy),
        "recovery" => {
            decode_maintenance(identity_bytes, record_bytes).map(DecodedRecord::Maintenance)
        }
        _ => None,
    }
}

fn decode_terminal(identity_bytes: &[u8], record_bytes: &[u8]) -> Option<TerminalRecord> {
    let identity = serde_json::from_slice::<TerminalIdentity>(identity_bytes).ok()?;
    let record = serde_json::from_slice::<TerminalRecord>(record_bytes).ok()?;
    (identity == record.identity
        && identity.version == 1
        && identity.reason == WireReason::CoordinationTemporarilyUnavailable
        && identity.semantic_slot == identity.source.semantic_slot
        && matches!(
            identity.source.semantic_slot,
            WireSemanticSlot::TurnCompleted
                | WireSemanticSlot::TurnInterrupted
                | WireSemanticSlot::TerminalResultObserved
        )
        && record.source_kind == WireSourceKind::ExogenousTerminal
        && included_generations_are_valid(&record.included_generations)
        && exact_bytes(&identity, identity_bytes)
        && exact_bytes(&record, record_bytes))
    .then_some(record)
}

fn included_generations_are_valid(
    included_generations: &Evidence<Vec<AssignmentGeneration>>,
) -> bool {
    match included_generations {
        Evidence::Known { value } => {
            value.len() <= 4 && value.windows(2).all(|pair| pair[0] < pair[1])
        }
        Evidence::Unavailable { .. } | Evidence::NotApplicable => true,
    }
}

fn decode_legacy(identity_bytes: &[u8], record_bytes: &[u8]) -> Option<LegacyRecord> {
    let identity = serde_json::from_slice::<LegacyIdentity>(identity_bytes).ok()?;
    let record = serde_json::from_slice::<LegacyRecord>(record_bytes).ok()?;
    (identity == record.identity
        && identity.version == 1
        && identity.source_kind == WireSourceKind::LegacyReduction
        && exact_bytes(&identity, identity_bytes)
        && exact_bytes(&record, record_bytes))
    .then_some(record)
}

fn decode_maintenance(identity_bytes: &[u8], record_bytes: &[u8]) -> Option<MaintenanceRecord> {
    let identity = serde_json::from_slice::<MaintenanceIdentity>(identity_bytes).ok()?;
    let record = serde_json::from_slice::<MaintenanceRecord>(record_bytes).ok()?;
    (identity == record.identity
        && identity.version == 1
        && identity.source_kind == WireSourceKind::Recovery
        && exact_bytes(&identity, identity_bytes)
        && exact_bytes(&record, record_bytes))
    .then_some(record)
}

fn exact_bytes(value: &impl Serialize, bytes: &[u8]) -> bool {
    serde_json::to_vec(value).is_ok_and(|encoded| encoded == bytes)
}

impl DecodedRecord {
    fn matches(&self, row: &sqlx::sqlite::SqliteRow) -> bool {
        match self {
            Self::Terminal(record) => terminal_matches(record, row),
            Self::Legacy(record) => legacy_matches(record, row),
            Self::Maintenance(record) => maintenance_matches(record, row),
        }
    }
}

fn terminal_matches(record: &TerminalRecord, row: &sqlx::sqlite::SqliteRow) -> bool {
    let identity = &record.identity;
    let Ok(included) = serde_json::to_vec(&record.included_generations) else {
        return false;
    };
    row.get::<String, _>("root_thread_id") == identity.root_thread_id.to_string()
        && row.get::<Option<String>, _>("state_epoch")
            == identity
                .captured_state_epoch
                .as_ref()
                .map(ToString::to_string)
        && row.get::<String, _>("source_kind") == record.source_kind.as_sql()
        && source_matches(&identity.source, row)
        && recovery_is_null(row)
        && row.get::<String, _>("semantic_slot") == identity.semantic_slot.as_sql()
        && row.get::<String, _>("reason") == identity.reason.as_sql()
        && row.get::<Option<String>, _>("target_thread_id")
            == Some(record.target_thread_id.to_string())
        && row.get::<Option<String>, _>("target_turn_id").as_deref()
            == Some(record.target_turn_id.as_str())
        && row.get::<Option<String>, _>("terminal_kind").as_deref()
            == Some(record.terminal_kind.as_sql())
        && row.get::<Option<String>, _>("terminal_outcome").as_deref()
            == Some(record.terminal_outcome.as_sql())
        && row.get::<Option<Vec<u8>>, _>("included_generations_bytes") == Some(included)
        && common_record_fields(record.observed_at, record.after_revision, row)
}

fn legacy_matches(record: &LegacyRecord, row: &sqlx::sqlite::SqliteRow) -> bool {
    let identity = &record.identity;
    row.get::<String, _>("root_thread_id") == identity.root_thread_id.to_string()
        && row.get::<Option<String>, _>("state_epoch") == Some(identity.state_epoch.to_string())
        && row.get::<String, _>("source_kind") == identity.source_kind.as_sql()
        && source_matches(&identity.source, row)
        && recovery_is_null(row)
        && row.get::<String, _>("semantic_slot") == identity.source.semantic_slot.as_sql()
        && row.get::<String, _>("reason") == identity.reason.as_sql()
        && terminal_is_null(row)
        && common_record_fields(record.observed_at, record.after_revision, row)
}

fn maintenance_matches(record: &MaintenanceRecord, row: &sqlx::sqlite::SqliteRow) -> bool {
    let identity = &record.identity;
    row.get::<String, _>("root_thread_id") == identity.root_thread_id.to_string()
        && row.get::<Option<String>, _>("state_epoch") == Some(identity.state_epoch.to_string())
        && row.get::<String, _>("source_kind") == identity.source_kind.as_sql()
        && source_is_null(row)
        && row
            .get::<Option<String>, _>("recovery_record_kind")
            .as_deref()
            == Some(identity.record_kind.as_sql())
        && row
            .get::<Option<String>, _>("recovery_record_id")
            .as_deref()
            == Some(identity.record_id.as_str())
        && row.get::<String, _>("semantic_slot") == identity.semantic_slot.as_sql()
        && row.get::<String, _>("reason") == identity.reason.as_sql()
        && terminal_is_null(row)
        && common_record_fields(record.observed_at, record.after_revision, row)
}

fn source_matches(source: &SourceIdentity, row: &sqlx::sqlite::SqliteRow) -> bool {
    row.get::<Option<String>, _>("source_shape").as_deref() == Some(source_shape_sql(source.shape))
        && row.get::<Option<String>, _>("source_thread_id")
            == source.source_thread_id.as_ref().map(ToString::to_string)
        && row.get::<Option<String>, _>("source_turn_id").as_deref()
            == source.source_turn_id.as_ref().map(BoundedId::as_str)
        && row.get::<Option<String>, _>("source_item_id").as_deref()
            == source.source_item_id.as_ref().map(BoundedId::as_str)
        && i64::try_from(source.source_ordinal).ok() == row.get::<Option<i64>, _>("source_ordinal")
}

fn source_is_null(row: &sqlx::sqlite::SqliteRow) -> bool {
    row.get::<Option<String>, _>("source_shape").is_none()
        && row.get::<Option<String>, _>("source_thread_id").is_none()
        && row.get::<Option<String>, _>("source_turn_id").is_none()
        && row.get::<Option<String>, _>("source_item_id").is_none()
        && row.get::<Option<i64>, _>("source_ordinal").is_none()
}

fn recovery_is_null(row: &sqlx::sqlite::SqliteRow) -> bool {
    row.get::<Option<String>, _>("recovery_record_kind")
        .is_none()
        && row.get::<Option<String>, _>("recovery_record_id").is_none()
}

fn terminal_is_null(row: &sqlx::sqlite::SqliteRow) -> bool {
    row.get::<Option<String>, _>("target_thread_id").is_none()
        && row.get::<Option<String>, _>("target_turn_id").is_none()
        && row.get::<Option<String>, _>("terminal_kind").is_none()
        && row.get::<Option<String>, _>("terminal_outcome").is_none()
        && row
            .get::<Option<Vec<u8>>, _>("included_generations_bytes")
            .is_none()
}

fn common_record_fields(
    observed_at: i64,
    after_revision: u64,
    row: &sqlx::sqlite::SqliteRow,
) -> bool {
    row.get::<i64, _>("adapter_version") == 1
        && row.get::<i64, _>("sanitizer_version") == 1
        && row.get::<i64, _>("observed_at") == observed_at
        && i64::try_from(after_revision)
            .is_ok_and(|value| value == row.get::<i64, _>("after_revision"))
}
