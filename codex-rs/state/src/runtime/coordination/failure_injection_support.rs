use std::ffi::OsString;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use sha2::Digest;
use sha2::Sha256;
use sqlx::Row;
use sqlx::TypeInfo;
use sqlx::ValueRef;
use tokio::io::AsyncReadExt;

use super::aggregate_journal::AggregateFailureInjector;
use super::aggregate_journal::AggregateStep;
use super::commands::CommandFailureInjector;
use super::commands::CommandStep;
use super::inbox::InboxFailureInjector;
use super::inbox::InboxStep;
use super::recovery::RecoveryFailureInjector;
use super::recovery::RecoveryStep;
use crate::StateRuntime;

pub(super) use super::failure_injection_integrity::assert_integrity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Boundary {
    Aggregate(AggregateStep),
    Command(CommandStep),
    Inbox(InboxStep),
    Recovery(RecoveryStep),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CrashPoint {
    pub(super) boundary: Boundary,
    pub(super) occurrence: usize,
}

pub(super) struct CrashInjector {
    target: Option<CrashPoint>,
    trace: Mutex<Vec<Boundary>>,
    now_ms: AtomicI64,
}

impl CrashInjector {
    pub(super) fn recording(now_ms: i64) -> Self {
        Self {
            target: None,
            trace: Mutex::new(Vec::new()),
            now_ms: AtomicI64::new(now_ms),
        }
    }

    pub(super) fn fail_at(point: CrashPoint, now_ms: i64) -> Self {
        Self {
            target: Some(point),
            trace: Mutex::new(Vec::new()),
            now_ms: AtomicI64::new(now_ms),
        }
    }

    pub(super) fn advance(&self, millis: i64) {
        self.now_ms.fetch_add(millis, Ordering::SeqCst);
    }

    pub(super) fn trace(&self) -> Vec<CrashPoint> {
        let trace = self.trace.lock().expect("crash trace lock");
        trace
            .iter()
            .enumerate()
            .map(|(index, boundary)| CrashPoint {
                boundary: *boundary,
                occurrence: trace[..index]
                    .iter()
                    .filter(|candidate| *candidate == boundary)
                    .count()
                    + 1,
            })
            .collect()
    }

    fn visit(&self, boundary: Boundary) -> anyhow::Result<()> {
        let mut trace = self.trace.lock().expect("crash trace lock");
        let occurrence = trace
            .iter()
            .filter(|candidate| **candidate == boundary)
            .count()
            + 1;
        trace.push(boundary);
        if self.target
            == Some(CrashPoint {
                boundary,
                occurrence,
            })
        {
            anyhow::bail!("injected crash at {boundary:?} occurrence {occurrence}");
        }
        Ok(())
    }
}

impl AggregateFailureInjector for CrashInjector {
    fn after_step(&self, step: AggregateStep) -> anyhow::Result<()> {
        self.visit(Boundary::Aggregate(step))
    }

    fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

impl CommandFailureInjector for CrashInjector {
    fn after_command_step(&self, step: CommandStep) -> anyhow::Result<()> {
        self.visit(Boundary::Command(step))
    }
}

impl InboxFailureInjector for CrashInjector {
    fn after_inbox_step(&self, step: InboxStep) -> anyhow::Result<()> {
        self.visit(Boundary::Inbox(step))
    }
}

impl RecoveryFailureInjector for CrashInjector {
    fn after_recovery_step(&self, step: RecoveryStep) -> anyhow::Result<()> {
        self.visit(Boundary::Recovery(step))
    }

    fn now_ms(&self) -> i64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct FrozenCoordinationState {
    pub(super) tables: Vec<FrozenTable>,
    pub(super) marker: FrozenMarker,
    pub(super) directory_entries: Vec<FrozenDirectoryEntry>,
    pub(super) controlled_effect_count: usize,
}

pub(super) struct FrozenStateInputs<'a> {
    pub(super) sqlite_home: &'a Path,
    pub(super) controlled_effect_count: usize,
}

impl<'a> FrozenStateInputs<'a> {
    pub(super) fn new(sqlite_home: &'a Path) -> Self {
        Self {
            sqlite_home,
            controlled_effect_count: 0,
        }
    }
}

pub(super) async fn copy_closed_home(source: &Path, target: &Path) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(target).await?;
    let mut entries = tokio::fs::read_dir(source).await?;
    let mut copied = 0;
    while let Some(entry) = entries.next_entry().await? {
        if copied == 8 || !entry.file_type().await?.is_file() {
            anyhow::bail!("unexpected non-file state entry");
        }
        tokio::fs::copy(entry.path(), target.join(entry.file_name())).await?;
        copied += 1;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct FrozenTable {
    pub(super) name: String,
    pub(super) columns: Vec<String>,
    pub(super) rows: Vec<Vec<FrozenCell>>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum FrozenCell {
    Null,
    Integer(i64),
    RealBits(u64),
    Text(RedactedText),
    Blob(RedactedBytes),
    Ciphertext(OpaqueCiphertext),
}

#[derive(Eq, PartialEq)]
pub(super) struct RedactedText(String);

impl fmt::Debug for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted text: {} bytes>", self.0.len())
    }
}

#[derive(Eq, PartialEq)]
pub(super) struct RedactedBytes(Vec<u8>);

impl fmt::Debug for RedactedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted bytes: {} bytes>", self.0.len())
    }
}

#[derive(Eq, PartialEq)]
pub(super) struct OpaqueCiphertext {
    len: usize,
    digest: [u8; 32],
}

impl fmt::Debug for OpaqueCiphertext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted ciphertext: {} bytes>", self.len)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum FrozenMarker {
    Missing,
    Entry {
        kind: FrozenEntryKind,
        contents: FrozenMarkerContents,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum FrozenMarkerContents {
    Bounded(RedactedBytes),
    Oversized { len: u64 },
    NotRegular,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FrozenDirectoryEntry {
    pub(super) name: RedactedOsString,
    pub(super) kind: FrozenEntryKind,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct RedactedOsString(OsString);

impl fmt::Debug for RedactedOsString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "<redacted file name: {} encoded bytes>",
            self.0.len()
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum FrozenEntryKind {
    File,
    Directory,
    Symlink,
    Other,
}

pub(super) async fn frozen_state(
    runtime: &StateRuntime,
    inputs: FrozenStateInputs<'_>,
) -> anyhow::Result<FrozenCoordinationState> {
    let tables = [
        ("coordination_authority", "state_epoch"),
        ("coordination_roots", "root_thread_id"),
        ("coordination_assignment_heads", "assignment_id"),
        (
            "coordination_assignment_generations",
            "assignment_id,generation",
        ),
        (
            "coordination_turn_bindings",
            "root_thread_id,turn_id,assignment_id,generation",
        ),
        ("coordination_waits", "operation_id"),
        (
            "coordination_turn_terminals",
            "root_thread_id,target_thread_id,target_turn_id",
        ),
        (
            "coordination_turn_terminal_generations",
            "root_thread_id,target_thread_id,target_turn_id,assignment_id,generation",
        ),
        ("coordination_dependencies", "operation_id"),
        ("coordination_results", "result_id"),
        ("coordination_handoffs", "handoff_id,attempt"),
        ("coordination_commands", "operation_id"),
        ("coordination_inbox", "receipt_id"),
        (
            "coordination_inbox_inclusions",
            "receipt_id,inference_attempt_id",
        ),
        ("coordination_events", "root_thread_id,revision,event_id"),
        ("coordination_projection_outbox", "event_id"),
        ("coordination_legacy_links", "compatibility_event_id"),
        (
            "coordination_legacy_scan_checkpoints",
            "root_thread_id,source_thread_id,adapter_version",
        ),
        ("coordination_degradation_records", "degradation_id"),
        (
            "coordination_degradation_publication_outbox",
            "degradation_id",
        ),
    ];
    let mut dumps = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let columns = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info({table})")))
            .fetch_all(&*runtime.pool)
            .await?
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {table} ORDER BY {order}"
        )))
        .fetch_all(&*runtime.pool)
        .await?;
        let rows = rows
            .iter()
            .map(|row| freeze_row(table, columns.as_slice(), row))
            .collect::<anyhow::Result<Vec<_>>>()?;
        dumps.push(FrozenTable {
            name: table.to_string(),
            columns,
            rows,
        });
    }
    Ok(FrozenCoordinationState {
        tables: dumps,
        marker: freeze_marker(inputs.sqlite_home).await?,
        directory_entries: freeze_directory(inputs.sqlite_home).await?,
        controlled_effect_count: inputs.controlled_effect_count,
    })
}

fn freeze_row(
    table: &str,
    columns: &[String],
    row: &sqlx::sqlite::SqliteRow,
) -> anyhow::Result<Vec<FrozenCell>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let raw = row.try_get_raw(index)?;
            if raw.is_null() {
                return Ok(FrozenCell::Null);
            }
            let cell = match raw.type_info().name() {
                "INTEGER" => FrozenCell::Integer(row.try_get(index)?),
                "REAL" => FrozenCell::RealBits(row.try_get::<f64, _>(index)?.to_bits()),
                "TEXT" => FrozenCell::Text(RedactedText(row.try_get(index)?)),
                "BLOB" => {
                    let bytes: Vec<u8> = row.try_get(index)?;
                    if column == "ciphertext"
                        && matches!(table, "coordination_commands" | "coordination_inbox")
                    {
                        FrozenCell::Ciphertext(OpaqueCiphertext {
                            len: bytes.len(),
                            digest: Sha256::digest(bytes.as_slice()).into(),
                        })
                    } else {
                        FrozenCell::Blob(RedactedBytes(bytes))
                    }
                }
                kind => anyhow::bail!("unsupported sqlite value kind {kind} in {table}.{column}"),
            };
            Ok(cell)
        })
        .collect()
}

async fn freeze_marker(sqlite_home: &Path) -> anyhow::Result<FrozenMarker> {
    let path = sqlite_home.join(super::authority_marker::MARKER_FILE_NAME);
    let metadata = match tokio::fs::symlink_metadata(path.as_path()).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(FrozenMarker::Missing),
        Err(err) => return Err(err.into()),
    };
    let kind = entry_kind(metadata.file_type());
    let contents = if !metadata.is_file() {
        FrozenMarkerContents::NotRegular
    } else if metadata.len() > super::authority_marker::MAX_MARKER_BYTES {
        FrozenMarkerContents::Oversized {
            len: metadata.len(),
        }
    } else {
        let file = tokio::fs::File::open(path).await?;
        let mut bytes = Vec::new();
        file.take(super::authority_marker::MAX_MARKER_BYTES + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 > super::authority_marker::MAX_MARKER_BYTES {
            FrozenMarkerContents::Oversized {
                len: bytes.len() as u64,
            }
        } else {
            FrozenMarkerContents::Bounded(RedactedBytes(bytes))
        }
    };
    Ok(FrozenMarker::Entry { kind, contents })
}

async fn freeze_directory(sqlite_home: &Path) -> anyhow::Result<Vec<FrozenDirectoryEntry>> {
    let mut reader = tokio::fs::read_dir(sqlite_home).await?;
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let metadata = tokio::fs::symlink_metadata(entry.path()).await?;
        entries.push(FrozenDirectoryEntry {
            name: RedactedOsString(entry.file_name()),
            kind: entry_kind(metadata.file_type()),
        });
    }
    entries.sort();
    Ok(entries)
}

fn entry_kind(file_type: std::fs::FileType) -> FrozenEntryKind {
    if file_type.is_file() {
        FrozenEntryKind::File
    } else if file_type.is_dir() {
        FrozenEntryKind::Directory
    } else if file_type.is_symlink() {
        FrozenEntryKind::Symlink
    } else {
        FrozenEntryKind::Other
    }
}
