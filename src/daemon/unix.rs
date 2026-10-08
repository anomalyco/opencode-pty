use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use sha2::{Digest, Sha256};

use super::{LOCK_FILE, REGISTRATION_FILE, Registration, read_registration, registration_path};
use crate::protocol::PROTOCOL_VERSION;
use crate::transport::Listener;

const STALE_RUNTIME_AGE: Duration = Duration::from_secs(10 * 60);

pub(super) struct Runtime {
    pub registration: Registration,
    pub cleanup: Cleanup,
    sweeper: JoinHandle<()>,
    // Held until the shared server has joined its handlers and removed registration.
    lock: File,
}

impl Runtime {
    pub fn bind(directory: &Path) -> Result<(Self, Listener)> {
        let directory = directory.to_path_buf();
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

        let socket_path = socket_path(&directory)?;
        if socket_path.exists() {
            fs::remove_file(&socket_path)?;
        }
        let listener = Listener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        let socket = SocketFile::bound(socket_path)?;
        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket: socket.path.clone(),
            token: random_id(),
        };
        write_registration(&directory, &registration)?;
        let sweeper = {
            let directory = directory.clone();
            thread::spawn(move || sweep_stale_runtimes(&directory))
        };
        Ok((
            Self {
                cleanup: Cleanup {
                    directory,
                    registration: registration.clone(),
                    socket,
                },
                registration,
                sweeper,
                lock,
            },
            listener,
        ))
    }

    /// Removes this daemon's runtime state after the server has joined its handlers.
    pub fn finish(self) -> Result<()> {
        let _ = self.sweeper.join();
        let result = self.cleanup.run();
        drop(self.lock);
        result
    }
}

/// Removes only state that still belongs to this daemon instance.
#[derive(Clone)]
pub(super) struct Cleanup {
    directory: PathBuf,
    registration: Registration,
    socket: SocketFile,
}

impl Cleanup {
    pub fn run(&self) -> Result<()> {
        let result = remove_if_current(&self.directory, &self.registration);
        self.socket.remove_if_current();
        // Remove the lock file before releasing it: a daemon that already opened it
        // fails to lock it, and a later one creates a new lock file.
        let _ = fs::remove_file(self.directory.join(LOCK_FILE));
        let _ = fs::remove_dir(&self.directory);
        result
    }
}

/// The bound socket's inode, so cleanup never unlinks a successor's socket.
#[derive(Clone)]
struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    fn bound(path: PathBuf) -> Result<Self> {
        let metadata = fs::symlink_metadata(&path)?;
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn remove_if_current(&self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Removes sibling runtime directories left by daemons that crashed.
fn sweep_stale_runtimes(directory: &Path) {
    let Some(parent) = directory.parent() else {
        return;
    };
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let candidate = entry.path();
        if same_path(&candidate, directory) {
            continue;
        }
        if let Err(error) = sweep_runtime(&candidate, SystemTime::now()) {
            eprintln!(
                "opencode-pty could not remove stale runtime {}: {error:#}",
                candidate.display()
            );
        }
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    matches!(
        (fs::canonicalize(left), fs::canonicalize(right)),
        (Ok(left), Ok(right)) if left == right
    )
}

fn sweep_runtime(directory: &Path, now: SystemTime) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() {
        return Ok(());
    }
    let age = now.duration_since(metadata.modified()?).unwrap_or_default();
    if age < STALE_RUNTIME_AGE {
        return Ok(());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let known = name == LOCK_FILE
            || name == REGISTRATION_FILE
            || (name.starts_with("service.") && name.ends_with(".tmp"));
        if !known || !entry.file_type()?.is_file() {
            // Not a runtime directory this daemon created.
            return Ok(());
        }
        files.push(entry.path());
    }

    // Only a lock file or a parseable registration proves a daemon created this
    // directory; anything else, including an empty directory, is left alone.
    let _lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join(LOCK_FILE))
    {
        Ok(lock) => {
            if lock.try_lock_exclusive().is_err() {
                return Ok(());
            }
            Some(lock)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let Some(registration) = fs::read(registration_path(directory))
                .ok()
                .and_then(|data| serde_json::from_slice::<Registration>(&data).ok())
            else {
                return Ok(());
            };
            if process_exists(registration.pid) {
                return Ok(());
            }
            None
        }
        Err(error) => return Err(error.into()),
    };

    let socket = socket_file(&socket_root(), &fs::canonicalize(directory)?);
    if fs::symlink_metadata(&socket).is_ok_and(|metadata| metadata.file_type().is_socket()) {
        fs::remove_file(&socket)?;
    }
    // Remove the lock last so a racing daemon cannot claim a half-removed directory.
    files.sort_by_key(|file| file.ends_with(LOCK_FILE));
    for file in files {
        fs::remove_file(file)?;
    }
    fs::remove_dir(directory)?;
    Ok(())
}

fn process_exists(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal zero only checks whether the PID exists.
    let exists = unsafe { libc::kill(pid, 0) } == 0;
    exists || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn socket_path(directory: &Path) -> Result<PathBuf> {
    let directory =
        fs::canonicalize(directory).context("failed to resolve PTY runtime directory")?;
    let root = socket_root();
    ensure_private_directory(&root)?;
    Ok(socket_file(&root, &directory))
}

// Sockets stay in /tmp so their paths fit sun_path; temp cleaners skip sockets.
fn socket_root() -> PathBuf {
    PathBuf::from("/tmp").join(format!(
        "opencode-pty-{}",
        nix::unistd::Uid::effective().as_raw()
    ))
}

fn socket_file(root: &Path, canonical_directory: &Path) -> PathBuf {
    let digest = Sha256::digest(canonical_directory.as_os_str().as_bytes());
    let name = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join(format!("{name}.sock"))
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
    fs::rename(&temporary, registration_path(directory))?;
    Ok(())
}

fn remove_if_current(directory: &Path, registration: &Registration) -> Result<()> {
    if read_registration(directory)
        .is_ok_and(|current| current.instance_id == registration.instance_id)
    {
        fs::remove_file(registration_path(directory))?;
    }
    Ok(())
}

fn random_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;

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

    struct Root(PathBuf);

    impl Root {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("opencode-pty-sweep-test-{}", random_id()));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn runtime(&self, files: &[&str], pid: Option<u32>) -> PathBuf {
            let directory = self.0.join(random_id());
            fs::create_dir(&directory).unwrap();
            for file in files {
                fs::write(directory.join(file), b"").unwrap();
            }
            if let Some(pid) = pid {
                let registration = Registration {
                    instance_id: random_id(),
                    pid,
                    protocol: PROTOCOL_VERSION,
                    socket: PathBuf::from("/nonexistent.sock"),
                    token: random_id(),
                };
                fs::write(
                    directory.join(REGISTRATION_FILE),
                    serde_json::to_vec(&registration).unwrap(),
                )
                .unwrap();
            }
            directory
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn later() -> SystemTime {
        SystemTime::now() + STALE_RUNTIME_AGE + Duration::from_secs(1)
    }

    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("/usr/bin/true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    #[test]
    fn sweep_removes_only_abandoned_old_runtimes() {
        let root = Root::new();
        let abandoned = root.runtime(&[LOCK_FILE, REGISTRATION_FILE], None);
        let young = root.runtime(&[LOCK_FILE], None);
        let foreign = root.runtime(&[LOCK_FILE, "notes.txt"], None);
        let locked = root.runtime(&[LOCK_FILE], None);
        let held = fs::File::open(locked.join(LOCK_FILE)).unwrap();
        held.try_lock_exclusive().unwrap();

        sweep_runtime(&abandoned, later()).unwrap();
        sweep_runtime(&young, SystemTime::now()).unwrap();
        sweep_runtime(&foreign, later()).unwrap();
        sweep_runtime(&locked, later()).unwrap();

        assert!(!abandoned.exists());
        assert!(young.join(LOCK_FILE).exists());
        assert!(foreign.join("notes.txt").exists());
        assert!(locked.join(LOCK_FILE).exists());
        drop(held);
    }

    #[test]
    fn sweep_without_lock_file_checks_registered_pid() {
        let root = Root::new();
        let live = root.runtime(&[], Some(std::process::id()));
        let dead = root.runtime(&[], Some(dead_pid()));

        sweep_runtime(&live, later()).unwrap();
        sweep_runtime(&dead, later()).unwrap();

        assert!(live.join(REGISTRATION_FILE).exists());
        assert!(!dead.exists());
    }

    #[test]
    fn sweep_keeps_directories_without_daemon_evidence() {
        let root = Root::new();
        let empty = root.runtime(&[], None);
        let foreign = root.runtime(&[], None);
        fs::write(foreign.join(REGISTRATION_FILE), br#"{"not":"ours"}"#).unwrap();
        let temporary = root.runtime(&["service.important.tmp"], None);

        for directory in [&empty, &foreign, &temporary] {
            sweep_runtime(directory, later()).unwrap();
            assert!(directory.exists(), "{}", directory.display());
        }
    }

    #[test]
    fn sweep_removes_abandoned_socket() {
        let root = Root::new();
        let abandoned = root.runtime(&[LOCK_FILE], None);
        let socket = socket_path(&abandoned).unwrap();
        drop(UnixListener::bind(&socket).unwrap());

        sweep_runtime(&abandoned, later()).unwrap();

        assert!(!abandoned.exists());
        assert!(!socket.exists());
    }
}
