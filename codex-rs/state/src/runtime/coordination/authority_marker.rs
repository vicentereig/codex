use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use codex_coordination::StateEpoch;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use super::authority::AuthorityFailureInjector;
use super::authority::AuthorityWriteStep;

pub(crate) const MARKER_FILE_NAME: &str = "coordination-authority-v1.json";
const MAX_MARKER_BYTES: u64 = 512;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthorityMarker {
    version: u16,
    state_epoch: StateEpoch,
    disposition: MarkerDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MarkerDisposition {
    Ordinary,
    FreshAfterCorruption,
}

pub(super) enum MarkerRead {
    Missing,
    Valid {
        state_epoch: StateEpoch,
        disposition: MarkerDisposition,
    },
    Rejected(String),
}

pub(super) fn marker_epoch(marker: &MarkerRead) -> Option<StateEpoch> {
    match marker {
        MarkerRead::Valid { state_epoch, .. } => Some(*state_epoch),
        MarkerRead::Missing | MarkerRead::Rejected(_) => None,
    }
}

pub(super) async fn read_marker(path: &Path) -> io::Result<MarkerRead> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_marker_blocking(path.as_path()))
        .await
        .map_err(io::Error::other)?
}

fn read_marker_blocking(path: &Path) -> io::Result<MarkerRead> {
    let mut file = match open_marker_nofollow(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(MarkerRead::Missing),
        Err(err) if err.kind() == io::ErrorKind::InvalidData => {
            return Ok(MarkerRead::Rejected(err.to_string()));
        }
        #[cfg(unix)]
        Err(err) if err.raw_os_error() == Some(libc::ELOOP) => {
            return Ok(MarkerRead::Rejected(
                "coordination authority marker is not a regular file".to_string(),
            ));
        }
        Err(err) => return Err(err),
    };
    if !file.metadata()?.is_file() {
        return Ok(MarkerRead::Rejected(
            "coordination authority marker is not a regular file".to_string(),
        ));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_MARKER_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MARKER_BYTES {
        return Ok(MarkerRead::Rejected(format!(
            "coordination authority marker exceeds {MAX_MARKER_BYTES} bytes"
        )));
    }
    let marker: AuthorityMarker = match serde_json::from_slice(&bytes) {
        Ok(marker) => marker,
        Err(_) => {
            return Ok(MarkerRead::Rejected(
                "coordination authority marker is malformed".to_string(),
            ));
        }
    };
    if marker.version != 1 {
        return Ok(MarkerRead::Rejected(
            "coordination authority marker has an unsupported version or epoch".to_string(),
        ));
    }
    Ok(MarkerRead::Valid {
        state_epoch: marker.state_epoch,
        disposition: marker.disposition,
    })
}

#[cfg(unix)]
fn open_marker_nofollow(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(windows)]
fn open_marker_nofollow(path: &Path) -> io::Result<std::fs::File> {
    windows::open_marker(path)
}

pub(super) async fn persist_marker(
    marker_path: &Path,
    epoch: StateEpoch,
    disposition: MarkerDisposition,
    injector: &dyn AuthorityFailureInjector,
) -> io::Result<()> {
    let parent = marker_path.parent().ok_or_else(|| {
        io::Error::other(format!("marker has no parent: {}", marker_path.display()))
    })?;
    let temp_path = marker_temp_path(marker_path, epoch);
    let result = async {
        injector.check(AuthorityWriteStep::TempWrite)?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temp_path.as_path())
            .await?;
        let mut bytes = serde_json::to_vec(&AuthorityMarker {
            version: 1,
            state_epoch: epoch,
            disposition,
        })?;
        bytes.push(b'\n');
        file.write_all(&bytes).await?;
        file.flush().await?;
        injector.check(AuthorityWriteStep::FileSync)?;
        file.sync_all().await?;
        drop(file);
        injector.check(AuthorityWriteStep::Rename)?;
        replace_marker(temp_path.as_path(), marker_path).await?;
        injector.check(AuthorityWriteStep::DirectorySync)?;
        sync_directory(parent).await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(temp_path.as_path()).await;
    }
    result
}

pub(crate) async fn prepare_fresh_after_corruption_marker(
    sqlite_home: &Path,
) -> io::Result<StateEpoch> {
    let marker_path = sqlite_home.join(MARKER_FILE_NAME);
    let previous_epoch = match read_marker(marker_path.as_path()).await? {
        MarkerRead::Missing => None,
        MarkerRead::Valid { state_epoch, .. } => Some(state_epoch),
        MarkerRead::Rejected(_) => return Ok(StateEpoch::new_v7()),
    };
    let mut epoch = StateEpoch::new_v7();
    while Some(epoch) == previous_epoch {
        epoch = StateEpoch::new_v7();
    }
    persist_marker(
        marker_path.as_path(),
        epoch,
        MarkerDisposition::FreshAfterCorruption,
        &super::authority::NoFailure,
    )
    .await?;
    Ok(epoch)
}

fn marker_temp_path(marker_path: &Path, epoch: StateEpoch) -> PathBuf {
    let mut name = marker_path.as_os_str().to_os_string();
    name.push(format!(".tmp-{epoch}"));
    PathBuf::from(name)
}

async fn sync_directory(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || sync_directory_blocking(path.as_path()))
        .await
        .map_err(io::Error::other)?
}

#[cfg(not(windows))]
async fn replace_marker(source: &Path, destination: &Path) -> io::Result<()> {
    tokio::fs::rename(source, destination).await
}

#[cfg(windows)]
async fn replace_marker(source: &Path, destination: &Path) -> io::Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        windows::replace_file(source.as_path(), destination.as_path())
    })
    .await
    .map_err(io::Error::other)?
}

#[cfg(not(windows))]
fn sync_directory_blocking(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory_blocking(path: &Path) -> io::Result<()> {
    windows::sync_directory(path)
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use std::path::Path;

    type Handle = *mut c_void;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const GENERIC_READ: u32 = 0x8000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const OPEN_EXISTING: u32 = 3;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    const INVALID_HANDLE_VALUE: Handle = -1_isize as Handle;
    const FILE_ATTRIBUTE_TAG_INFO_CLASS: i32 = 9;

    #[repr(C)]
    struct FileAttributeTagInfo {
        file_attributes: u32,
        reparse_tag: u32,
    }

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
        fn MoveFileExW(existing: *const u16, new: *const u16, flags: u32) -> i32;
        fn GetFileInformationByHandleEx(
            file: Handle,
            class: i32,
            information: *mut c_void,
            buffer_size: u32,
        ) -> i32;
    }

    pub(super) fn open_marker(path: &Path) -> io::Result<File> {
        let path = wide(path);
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null_mut(),
                OPEN_EXISTING,
                FILE_FLAG_OPEN_REPARSE_POINT,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle) };
        let mut info = FileAttributeTagInfo {
            file_attributes: 0,
            reparse_tag: 0,
        };
        let inspected = unsafe {
            GetFileInformationByHandleEx(
                handle,
                FILE_ATTRIBUTE_TAG_INFO_CLASS,
                (&mut info as *mut FileAttributeTagInfo).cast(),
                std::mem::size_of::<FileAttributeTagInfo>() as u32,
            )
        };
        if inspected == 0 {
            return Err(io::Error::last_os_error());
        }
        if info.file_attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "coordination authority marker is a reparse point",
            ));
        }
        Ok(file)
    }

    pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
        let path = wide(path);
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
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

    pub(super) fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
        let source = wide(source);
        let destination = wide(destination);
        let replaced = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if replaced == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }
}
