//! Durable append-only JSONL writer for the coordination sidecar file.
//!
//! # Durability ordering
//!
//! Every append does, in order: (1) create-or-open the file, (2) write the
//! line, (3) flush the userspace buffer, (4) `fsync` the file's own data
//! (`sync_all`), and — **only** when this call is the one that created the
//! file — (5) `fsync` the parent directory.
//!
//! The file's own `fsync` (step 4) always happens *before* the directory
//! `fsync` (step 5), never after. This ordering is load-bearing, not
//! incidental: a directory `fsync` only makes the directory *entry*
//! (name -> inode mapping) durable. If the directory entry were made durable
//! first and the process crashed before the file's own data landed on disk,
//! a reader after reboot could see a directory listing that names the file
//! while the file itself is empty or truncated — a durable-looking entry
//! pointing at not-yet-durable content. Fsyncing the file's data first and
//! the directory second guarantees that "the directory shows this file"
//! implies "this file's content, as of this fsync, is durable" — the
//! property every reader of this contract depends on. This is the same
//! ordering `codex-state`'s coordination authority marker writer already
//! uses (write+fsync the file, *then* fsync the containing directory; see
//! `state/src/runtime/coordination/authority_marker.rs`), just adapted from
//! a temp-file-then-rename writer to a direct-append writer: there is no
//! rename here, so the "file exists under this name" event is the create
//! call itself, and the directory fsync durably records that create.
//!
//! Subsequent appends to an already-existing file skip the directory fsync
//! entirely: the directory entry does not change on appends, only the
//! file's own content does, so only the file's own `fsync` is needed.
//!
//! # Dedupe / replay safety
//!
//! [`SidecarWriter::open`] scans the existing file (if any) once, building
//! an in-memory index from each record's stable identity (`event_id` or
//! `degradation_id`) to its *structural* key — every field except the
//! diagnostic `materialized_at_ms` timestamp, which legitimately varies
//! between a first successful write and a later retry of the same logical
//! record and must not participate in divergence detection.
//!
//! [`SidecarWriter::append_if_new`] then either: skips the write entirely
//! and returns [`AppendOutcome::AlreadyPresent`] when the identity is
//! already indexed with a matching structural key (a no-op retry after a
//! crash between append and ack); returns [`SidecarWriteError::DivergentIdentity`]
//! and writes nothing when the identity is indexed with a *different*
//! structural key (fail closed — this is a quarantine condition, never
//! silently accepted); or performs the durable append above and returns
//! [`AppendOutcome::Appended`].
//!
//! A crash between a durable append and the caller's outbox ack is
//! recovered by reopening (rescanning): the record's presence in the file
//! is itself sufficient for the retried `append_if_new` call to become a
//! no-op, so the caller can safely retry the whole claim/append/ack
//! sequence without ever producing a duplicate line.

use std::path::Path;
use std::path::PathBuf;

use tokio::io::AsyncWriteExt;

use super::record::SidecarRecord;
use super::record::SidecarRecordError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum WriteStep {
    Create,
    Open,
    Write,
    Flush,
    FileSync,
    DirSync,
}

/// Injects failures between durability steps for crash-matrix tests without
/// weakening the production writer (mirrors
/// `state::runtime::coordination::recovery::RecoveryFailureInjector`).
pub(crate) trait SidecarFailureInjector: Send + Sync {
    fn before_step(&self, step: WriteStep) -> std::io::Result<()>;
}

pub(crate) struct NoSidecarFailure;

impl SidecarFailureInjector for NoSidecarFailure {
    fn before_step(&self, _step: WriteStep) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AppendOutcome {
    Appended,
    AlreadyPresent,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SidecarWriteError {
    #[error(transparent)]
    Validation(#[from] SidecarRecordError),
    #[error(
        "sidecar record identity already exists with different structural content (quarantine)"
    )]
    DivergentIdentity,
    #[error("sidecar file contains an unparseable non-tail line (quarantine)")]
    CorruptSidecar,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub(crate) struct SidecarWriter {
    path: PathBuf,
    /// identity -> structural key (all fields except `materialized_at_ms`).
    seen: std::collections::HashMap<String, Vec<u8>>,
    /// `true` until this writer has itself observed (either by finding at
    /// least one already-valid record at [`Self::open`] time, or by
    /// completing its own directory `fsync`) that the sidecar file's
    /// directory entry is durable. Deliberately re-derived from ground
    /// truth on every `open()` rather than trusted across process restarts:
    /// a valid, parseable, newline-terminated record can only exist in the
    /// file if some earlier append already completed create+fsync+dirsync
    /// (this writer is the file's only writer, so that induction holds).
    /// Until then, every append attempt re-fsyncs the directory — cheap,
    /// and the only way to be certain the create is durable rather than
    /// merely incidental filesystem-metadata writeback that a subsequent
    /// crash could still lose.
    directory_sync_pending: bool,
}

impl SidecarWriter {
    /// Open (without creating) the sidecar file at `path`, scanning any
    /// existing content to rebuild the dedupe index. A missing file is not
    /// an error: the index starts empty and the file is created on first
    /// append. A non-tail line that fails to parse is corruption and fails
    /// closed; a *tail* line that fails to parse is treated as a torn write
    /// from an interrupted append and is truncated away (only the last line
    /// can ever be torn, because every prior append already completed and
    /// fsynced before the next one began).
    pub(crate) async fn open(path: PathBuf) -> Result<Self, SidecarWriteError> {
        let mut seen = std::collections::HashMap::new();
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    path,
                    seen,
                    directory_sync_pending: true,
                });
            }
            Err(err) => return Err(err.into()),
        };
        let ends_with_newline = bytes.last() == Some(&b'\n');
        let lines: Vec<&[u8]> = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .collect();
        let torn_tail = !ends_with_newline;
        for (index, line) in lines.iter().enumerate() {
            let is_tail = torn_tail && index == lines.len() - 1;
            match serde_json::from_slice::<SidecarRecord>(line) {
                Ok(record) => {
                    seen.insert(record.identity().to_string(), record.structural_key());
                }
                Err(_) if is_tail => {
                    // Interrupted append: truncate the torn tail away so the
                    // next append starts from a clean, newline-terminated end.
                    let good_len = bytes.len() - line.len();
                    let file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .await?;
                    file.set_len(good_len as u64).await?;
                    file.sync_all().await?;
                }
                Err(_) => return Err(SidecarWriteError::CorruptSidecar),
            }
        }
        let directory_sync_pending = seen.is_empty();
        Ok(Self {
            path,
            seen,
            directory_sync_pending,
        })
    }

    #[cfg(test)]
    pub(crate) async fn append_if_new(
        &mut self,
        record: &SidecarRecord,
    ) -> Result<AppendOutcome, SidecarWriteError> {
        self.append_if_new_with(record, &NoSidecarFailure).await
    }

    pub(crate) async fn append_if_new_with(
        &mut self,
        record: &SidecarRecord,
        injector: &dyn SidecarFailureInjector,
    ) -> Result<AppendOutcome, SidecarWriteError> {
        record.validate()?;
        let identity = record.identity().to_string();
        let structural_key = record.structural_key();
        if let Some(existing) = self.seen.get(&identity) {
            if *existing == structural_key {
                return Ok(AppendOutcome::AlreadyPresent);
            }
            return Err(SidecarWriteError::DivergentIdentity);
        }

        // Whether the directory's own durability is still outstanding, not
        // merely whether the file happens to exist on disk right now (see
        // the field doc comment above for why those are different checks).
        let creating = self.directory_sync_pending;
        injector.before_step(if creating {
            WriteStep::Create
        } else {
            WriteStep::Open
        })?;
        if creating && let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        let mut line = record.canonical_line()?;
        line.push(b'\n');
        injector.before_step(WriteStep::Write)?;
        file.write_all(&line).await?;
        injector.before_step(WriteStep::Flush)?;
        file.flush().await?;
        injector.before_step(WriteStep::FileSync)?;
        file.sync_all().await?;
        drop(file);

        if creating {
            injector.before_step(WriteStep::DirSync)?;
            let Some(parent) = self.path.parent() else {
                return Err(std::io::Error::other(format!(
                    "sidecar path has no parent: {}",
                    self.path.display()
                ))
                .into());
            };
            sync_directory(parent).await?;
            self.directory_sync_pending = false;
        }

        self.seen.insert(identity, structural_key);
        Ok(AppendOutcome::Appended)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        self.path.as_path()
    }
}

async fn sync_directory(path: &Path) -> std::io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || sync_directory_blocking(path.as_path()))
        .await
        .map_err(std::io::Error::other)?
}

#[cfg(not(windows))]
fn sync_directory_blocking(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory_blocking(path: &Path) -> std::io::Result<()> {
    windows::sync_directory(path)
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::path::Path;

    type Handle = *mut c_void;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const OPEN_EXISTING: u32 = 3;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const INVALID_HANDLE_VALUE: Handle = -1_isize as Handle;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateFileW(
            file_name: *const u16,
            desired_access: u32,
            share_mode: u32,
            security_attributes: *mut c_void,
            creation_disposition: u32,
            flags_and_attributes: u32,
            template_file: Handle,
        ) -> Handle;
        fn FlushFileBuffers(file: Handle) -> i32;
        fn CloseHandle(object: Handle) -> i32;
    }

    pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
        let wide = wide(path);
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null_mut(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let flushed = unsafe { FlushFileBuffers(handle) };
        let flush_error = (flushed == 0).then(io::Error::last_os_error);
        let closed = unsafe { CloseHandle(handle) };
        if let Some(err) = flush_error {
            return Err(err);
        }
        if closed == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }
}
