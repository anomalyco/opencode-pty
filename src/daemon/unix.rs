use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use sha2::{Digest, Sha256};

use super::{LOCK_FILE, REGISTRATION_FILE, Registration};
use crate::protocol::PROTOCOL_VERSION;
use crate::transport::Listener;

pub fn service_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("OPENCODE_PTY_RUNTIME_DIR") {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(path).join("opencode-pty");
    }
    let uid = nix::unistd::Uid::effective().as_raw();
    std::env::temp_dir().join(format!("opencode-pty-{uid}"))
}

pub fn registration_path() -> PathBuf {
    service_dir().join(REGISTRATION_FILE)
}

pub fn read_registration() -> Result<Registration> {
    let data = fs::read(registration_path()).context("opencode-pty registration is unavailable")?;
    serde_json::from_slice(&data).context("invalid opencode-pty registration")
}

pub(super) struct Runtime {
    pub registration: Registration,
    // Held until the shared server has joined its handlers and removed registration.
    _lock: File,
}

impl Runtime {
    pub fn bind() -> Result<(Self, Listener)> {
        let directory = service_dir();
        fs::create_dir_all(&directory)?;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(LOCK_FILE))?;
        lock.try_lock_exclusive()
            .context("another opencode-pty process already owns the service lock")?;

        let socket = socket_path(&directory)?;
        if socket.exists() {
            fs::remove_file(&socket)?;
        }
        let listener = Listener::bind(&socket)?;
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket,
            token: random_id(),
        };
        write_registration(&directory, &registration)?;
        Ok((
            Self {
                registration,
                _lock: lock,
            },
            listener,
        ))
    }
}

fn socket_path(directory: &Path) -> Result<PathBuf> {
    let directory =
        fs::canonicalize(directory).context("failed to resolve PTY runtime directory")?;
    let digest = Sha256::digest(directory.as_os_str().as_bytes());
    let name = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let root = PathBuf::from("/tmp").join(format!(
        "opencode-pty-{}",
        nix::unistd::Uid::effective().as_raw()
    ));
    ensure_private_directory(&root)?;
    Ok(root.join(format!("{name}.sock")))
}

fn ensure_private_directory(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory)?;
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "PTY socket directory is not a real directory: {}",
            directory.display()
        ));
    }
    let uid = nix::unistd::Uid::effective().as_raw();
    if metadata.uid() != uid {
        return Err(anyhow!(
            "PTY socket directory {} is owned by uid {}, expected {uid}",
            directory.display(),
            metadata.uid()
        ));
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn write_registration(directory: &Path, registration: &Registration) -> Result<()> {
    let temporary = directory.join(format!("service.{}.tmp", registration.instance_id));
    let data = serde_json::to_vec_pretty(registration)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&data)?;
    file.sync_all()?;
    fs::rename(&temporary, registration_path())?;
    Ok(())
}

pub(super) fn cleanup(registration: &Registration) -> Result<()> {
    if read_registration().is_ok_and(|current| current.instance_id == registration.instance_id) {
        fs::remove_file(registration_path())?;
    }
    let _ = fs::remove_file(&registration.socket);
    Ok(())
}

fn random_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_paths_are_short_and_runtime_specific() {
        let base = std::env::temp_dir().join(format!("opencode-pty-socket-test-{}", random_id()));
        let first = base.join("a".repeat(120)).join("database-a");
        let second = base.join("b".repeat(120)).join("database-b");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();

        let first_socket = socket_path(&first).unwrap();
        let second_socket = socket_path(&second).unwrap();

        assert_ne!(first_socket, second_socket);
        assert!(first_socket.as_os_str().as_bytes().len() < 104);
        assert_eq!(first_socket.parent(), second_socket.parent());

        fs::remove_dir_all(base).unwrap();
    }
}
