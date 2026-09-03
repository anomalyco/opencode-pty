use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, GENERIC_READ, GENERIC_WRITE};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    GetFileInformationByHandle, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    OPEN_ALWAYS, OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
};

use super::{LOCK_FILE, REGISTRATION_FILE, Registration};
use crate::protocol::PROTOCOL_VERSION;
use crate::transport::Listener;
use crate::transport::windows::{owned_handle, security::PrivateSecurity};

pub fn service_dir() -> Result<PathBuf> {
    let directory = if let Some(path) = std::env::var_os("OPENCODE_PTY_RUNTIME_DIR") {
        PathBuf::from(path)
    } else {
        PathBuf::from(
            std::env::var_os("LOCALAPPDATA")
                .context("LOCALAPPDATA is unavailable; set OPENCODE_PTY_RUNTIME_DIR")?,
        )
        .join("opencode-pty")
    };
    if !directory.is_absolute() {
        bail!("PTY runtime directory must be an absolute path");
    }
    Ok(directory)
}

pub fn registration_path() -> Result<PathBuf> {
    Ok(service_dir()?.join(REGISTRATION_FILE))
}

pub fn read_registration() -> Result<Registration> {
    read_registration_at(&registration_path()?)
}

fn read_registration_at(path: &Path) -> Result<Registration> {
    let security = PrivateSecurity::new()?;
    let mut file = open(
        path,
        GENERIC_READ | READ_CONTROL,
        OPEN_EXISTING,
        FILE_SHARE_READ | FILE_SHARE_DELETE,
        &security,
    )?;
    check_kind(&file, false)?;
    security.check_private(file.as_raw_handle())?;
    let mut data = Vec::new();
    // Registration is tiny; do not allocate an unbounded stale/corrupt file.
    Read::by_ref(&mut file)
        .take(16 * 1024 + 1)
        .read_to_end(&mut data)?;
    if data.len() > 16 * 1024 {
        bail!("PTY registration is too large");
    }
    serde_json::from_slice(&data).context("invalid opencode-pty registration")
}

pub(super) struct Runtime {
    pub registration: Registration,
    _lock: File,
    _directory: File,
}

impl Runtime {
    pub fn bind() -> Result<(Self, Listener)> {
        let security = PrivateSecurity::new()?;
        let directory = service_dir()?;
        if let Some(parent) = directory.parent() {
            fs::create_dir_all(parent)?;
        }
        let wide = wide_path(&directory)?;
        // SAFETY: wide path and private descriptor are valid through creation.
        if unsafe { CreateDirectoryW(wide.as_ptr(), &security.attributes()) } == 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
                return Err(error.into());
            }
        }
        // Deny delete sharing so the private directory and lock cannot be
        // replaced out from under the running daemon.
        let directory_handle = open(
            &directory,
            GENERIC_READ | READ_CONTROL | WRITE_DAC,
            OPEN_EXISTING,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &security,
        )?;
        check_kind(&directory_handle, true)?;
        security.make_private(directory_handle.as_raw_handle())?;
        let directory = fs::canonicalize(&directory)?;
        let lock = open(
            &directory.join(LOCK_FILE),
            GENERIC_READ | GENERIC_WRITE | READ_CONTROL | WRITE_DAC,
            OPEN_ALWAYS,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &security,
        )?;
        check_kind(&lock, false)?;
        security.make_private(lock.as_raw_handle())?;
        lock.try_lock_exclusive()
            .context("another opencode-pty process already owns the service lock")?;

        let instance_id = format!("{:032x}", rand::random::<u128>());
        let registration = Registration {
            socket: PathBuf::from(format!(r"\\.\pipe\opencode-pty-{instance_id}")),
            instance_id,
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            token: format!("{:032x}", rand::random::<u128>()),
        };
        let listener = Listener::bind(&registration.socket)?;
        write_registration(&directory, &registration, &security)?;
        Ok((
            Self {
                registration,
                _lock: lock,
                _directory: directory_handle,
            },
            listener,
        ))
    }
}

fn write_registration(
    directory: &Path,
    registration: &Registration,
    security: &PrivateSecurity,
) -> Result<()> {
    let temporary = directory.join(format!("service.{}.tmp", registration.instance_id));
    let result = (|| -> Result<()> {
        let mut file = open(
            &temporary,
            GENERIC_WRITE | READ_CONTROL,
            CREATE_NEW,
            0,
            security,
        )?;
        file.write_all(&serde_json::to_vec_pretty(registration)?)?;
        file.sync_all()?;
        drop(file);
        let from = wide_path(&temporary)?;
        let to = wide_path(&directory.join(REGISTRATION_FILE))?;
        // SAFETY: both paths are NUL-terminated; replacement stays in the held
        // private directory. Readers allow delete sharing for atomic replacement.
        if unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

pub(super) fn cleanup(registration: &Registration) -> Result<()> {
    let path = registration_path()?;
    if read_registration_at(&path)
        .is_ok_and(|current| current.instance_id == registration.instance_id)
    {
        fs::remove_file(path)?;
    }
    // A named pipe is not a filesystem entry. Its listener remains held until
    // this registration cleanup has completed, then all handles are closed.
    Ok(())
}

fn open(
    path: &Path,
    access: u32,
    creation: u32,
    sharing: u32,
    security: &PrivateSecurity,
) -> Result<File> {
    let path = wide_path(path)?;
    // SAFETY: all pointers are valid; OPEN_REPARSE_POINT lets check_kind reject
    // links rather than inspecting a different target after following them.
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            access,
            sharing,
            &security.attributes(),
            creation,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            null_mut(),
        )
    };
    Ok(File::from(owned_handle(handle)?))
}

fn check_kind(file: &File, directory: bool) -> Result<()> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: file owns a live handle; info is valid writable output.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        bail!("PTY storage must not be a reparse point");
    }
    if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0) != directory {
        bail!("unexpected PTY storage file type");
    }
    Ok(())
}

fn wide_path(path: &Path) -> Result<Vec<u16>> {
    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        bail!("PTY storage path contains NUL");
    }
    wide.push(0);
    Ok(wide)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_is_private_and_atomically_replaceable_with_a_reader_open() {
        let directory =
            std::env::temp_dir().join(format!("pty-registration-{:032x}", rand::random::<u128>()));
        fs::create_dir(&directory).unwrap();
        let security = PrivateSecurity::new().unwrap();
        let mut registration = Registration {
            instance_id: "first".into(),
            pid: 1,
            protocol: PROTOCOL_VERSION,
            socket: PathBuf::from(r"\\.\pipe\opencode-pty-first"),
            token: "secret".into(),
        };
        write_registration(&directory, &registration, &security).unwrap();
        let path = directory.join(REGISTRATION_FILE);
        let mut old_reader = open(
            &path,
            GENERIC_READ | READ_CONTROL,
            OPEN_EXISTING,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            &security,
        )
        .unwrap();
        security.check_private(old_reader.as_raw_handle()).unwrap();
        registration.instance_id = "second".into();
        write_registration(&directory, &registration, &security).unwrap();
        assert_eq!(read_registration_at(&path).unwrap().instance_id, "second");
        let mut old = String::new();
        old_reader.read_to_string(&mut old).unwrap();
        assert_eq!(
            serde_json::from_str::<Registration>(&old)
                .unwrap()
                .instance_id,
            "first"
        );
        drop(old_reader);
        fs::remove_dir_all(directory).unwrap();
    }
}
