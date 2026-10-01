use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const REGISTRATION_FILE: &str = "service.json";
pub const LOCK_FILE: &str = "service.lock";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registration {
    pub instance_id: String,
    pub pid: u32,
    pub protocol: u32,
    pub socket: PathBuf,
    pub token: String,
}

/// Shared parent of runtime directories, inside OpenCode's state directory.
/// Registration files must not live in temporary directories, which macOS purges
/// while daemons run.
pub fn default_runtime_root() -> PathBuf {
    resolve_runtime_root(
        std::env::var_os("XDG_STATE_HOME").map(PathBuf::from),
        std::env::home_dir(),
    )
}

fn resolve_runtime_root(state: Option<PathBuf>, home: Option<PathBuf>) -> PathBuf {
    state
        .filter(|path| path.is_absolute())
        .or_else(|| home.map(|home| home.join(".local").join("state")))
        .unwrap_or_else(std::env::temp_dir)
        .join("opencode")
        .join("pty")
}

/// Resolves `<root>/<name>`, where the name is a single plain path component.
pub fn runtime_dir(root: Option<&Path>, name: &str) -> Result<PathBuf> {
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !valid {
        bail!("invalid runtime name {name:?}; use letters, digits, '.', '_', or '-'");
    }
    Ok(root
        .map(Path::to_path_buf)
        .unwrap_or_else(default_runtime_root)
        .join(name))
}

pub fn registration_path(directory: &Path) -> PathBuf {
    directory.join(REGISTRATION_FILE)
}

pub fn read_registration(directory: &Path) -> Result<Registration> {
    let data = std::fs::read(registration_path(directory))
        .context("opencode-pty registration is unavailable")?;
    serde_json::from_slice(&data).context("invalid opencode-pty registration")
}

#[cfg(unix)]
mod unix {
    use std::fs::{self, OpenOptions};
    use std::net::Shutdown;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use anyhow::{Context, Result, anyhow};
    use base64::Engine;
    use fs2::FileExt;
    use sha2::{Digest, Sha256};

    use super::{LOCK_FILE, REGISTRATION_FILE, Registration, read_registration, registration_path};
    use crate::ownership::Ownership;
    use crate::protocol::{
        Envelope, PROTOCOL_VERSION, Request, Response, read_frame, write_frame, write_output_frame,
    };
    use crate::service::{CreateTerminal, StreamEvent, TerminalService};

    const STALE_RUNTIME_AGE: Duration = Duration::from_secs(10 * 60);

    pub fn run(directory: &Path) -> Result<()> {
        let directory = directory.to_path_buf();
        fs::create_dir_all(&directory)?;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let lock_path = directory.join(LOCK_FILE);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lock.try_lock_exclusive()
            .context("another opencode-pty process already owns the service lock")?;

        let socket_path = socket_path(&directory)?;
        if socket_path.exists() {
            fs::remove_file(&socket_path)?;
        }
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let socket = SocketFile::bound(socket_path)?;

        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket: socket.path.clone(),
            token: random_id(),
        };
        let ownership = Arc::new(Mutex::new(Ownership::new(Instant::now())));
        write_registration(&directory, &registration)?;
        let sweeper = {
            let directory = directory.clone();
            thread::spawn(move || sweep_stale_runtimes(&directory))
        };

        let service = Arc::new(TerminalService::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handlers = Vec::<(UnixStream, thread::JoinHandle<()>)>::new();
        while !shutdown.load(Ordering::Acquire) {
            if ownership
                .lock()
                .map_err(|_| anyhow!("ownership lock poisoned"))?
                .tick(Instant::now())
            {
                shutdown.store(true, Ordering::Release);
                break;
            }
            for (_, handler) in handlers.extract_if(.., |(_, handler)| handler.is_finished()) {
                let _ = handler.join();
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    // macOS inherits the listener's nonblocking mode on accepted sockets.
                    stream.set_nonblocking(false)?;
                    let control = stream.try_clone()?;
                    let service = Arc::clone(&service);
                    let shutdown = Arc::clone(&shutdown);
                    let ownership = Arc::clone(&ownership);
                    let registration = registration.clone();
                    let handle = thread::spawn(move || {
                        if let Err(error) = handle_connection(
                            stream,
                            &service,
                            &registration,
                            &shutdown,
                            &ownership,
                        ) {
                            eprintln!("opencode-pty request failed: {error:#}");
                        }
                    });
                    handlers.push((control, handle));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        }

        drop(listener);
        // Unblock partial requests, owner reads, and backpressured subscriptions
        // before joining. PTY workers still use their existing termination path.
        for (stream, _) in &handlers {
            let _ = stream.shutdown(Shutdown::Both);
        }
        let (cleanup_tx, cleanup_rx) = crossbeam_channel::bounded::<()>(1);
        let cleanup_registration = registration.clone();
        let cleanup_registration_directory = directory.clone();
        let cleanup_socket = socket.clone();
        let cleanup_directory = directory.clone();
        let watchdog = thread::spawn(move || {
            if cleanup_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                eprintln!("opencode-pty cleanup timed out; forcing exit");
                let _ = remove_if_current(&cleanup_registration_directory, &cleanup_registration);
                cleanup_socket.remove_if_current();
                remove_runtime_directory(&cleanup_directory);
                std::process::exit(1);
            }
        });
        service.shutdown();
        for (_, handler) in handlers {
            let _ = handler.join();
        }
        let _ = sweeper.join();
        drop(service);
        let result = remove_if_current(&directory, &registration);
        socket.remove_if_current();
        // The lock is still held, so no successor can be using this directory.
        remove_runtime_directory(&directory);
        let _ = cleanup_tx.send(());
        let _ = watchdog.join();
        drop(lock);
        result
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

    fn remove_runtime_directory(directory: &Path) {
        let _ = fs::remove_file(directory.join(LOCK_FILE));
        let _ = fs::remove_dir(directory);
    }

    /// Removes sibling runtime directories left by daemons that crashed.
    fn sweep_stale_runtimes(directory: &Path) {
        let Some(parent) = directory.parent() else {
            return;
        };
        let same = |left: &Path, right: &Path| matches!((fs::canonicalize(left), fs::canonicalize(right)), (Ok(left), Ok(right)) if left == right);
        let Ok(entries) = fs::read_dir(parent) else {
            return;
        };
        for entry in entries.flatten() {
            let candidate = entry.path();
            if same(&candidate, directory) {
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

    fn ensure_private_directory(directory: &std::path::Path) -> Result<()> {
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

    fn handle_connection(
        mut stream: UnixStream,
        service: &TerminalService,
        registration: &Registration,
        shutdown: &AtomicBool,
        ownership: &Mutex<Ownership>,
    ) -> Result<()> {
        let envelope: Envelope = read_frame(&mut stream)?;
        if envelope.token != registration.token {
            return write_frame(
                &mut stream,
                &Response::Error {
                    message: "authentication failed".to_string(),
                },
            );
        }
        if let Request::Own {
            instance_id,
            ticket,
        } = envelope.request
        {
            let claim = if instance_id != registration.instance_id {
                Err(anyhow!("daemon instance_id mismatch"))
            } else if shutdown.load(Ordering::Acquire) {
                Err(anyhow!("daemon is stopping"))
            } else {
                ownership
                    .lock()
                    .map_err(|_| anyhow!("ownership lock poisoned"))?
                    .claim(ticket.as_deref(), Instant::now())
            };
            let generation = match claim {
                Ok(generation) => generation,
                Err(error) => {
                    return write_frame(
                        &mut stream,
                        &Response::Error {
                            message: error.to_string(),
                        },
                    );
                }
            };
            let result =
                owner_connection(&mut stream, registration, shutdown, ownership, generation);
            ownership
                .lock()
                .map_err(|_| anyhow!("ownership lock poisoned"))?
                .disconnect(generation, Instant::now());
            return result;
        }
        if let Request::Subscribe {
            id,
            offset,
            attachment_id,
            role,
            takeover,
        } = envelope.request
        {
            return stream_subscription(
                &mut stream,
                service,
                shutdown,
                SubscriptionRequest {
                    id,
                    offset,
                    attachment_id,
                    role,
                    takeover,
                },
            );
        }
        let stopping = matches!(envelope.request, Request::Shutdown);
        let response = dispatch(envelope.request, service, registration).unwrap_or_else(|error| {
            Response::Error {
                message: format!("{error:#}"),
            }
        });
        let result = write_frame(&mut stream, &response);
        if stopping {
            shutdown.store(true, Ordering::Release);
        }
        result
    }

    fn owner_connection(
        stream: &mut UnixStream,
        registration: &Registration,
        shutdown: &AtomicBool,
        ownership: &Mutex<Ownership>,
        generation: u64,
    ) -> Result<()> {
        write_frame(&mut *stream, &Response::Owned)?;
        while !shutdown.load(Ordering::Acquire) {
            let envelope: Envelope = read_frame(&mut *stream)?;
            if !ownership
                .lock()
                .map_err(|_| anyhow!("ownership lock poisoned"))?
                .is_owner(generation)
            {
                return write_frame(
                    &mut *stream,
                    &Response::Error {
                        message: "owner connection has been superseded".to_string(),
                    },
                );
            }
            let stopping = envelope.token == registration.token
                && matches!(envelope.request, Request::Shutdown);
            let response = if envelope.token != registration.token {
                Response::Error {
                    message: "authentication failed".to_string(),
                }
            } else {
                match envelope.request {
                    Request::PrepareHandoff => {
                        let handoff = ownership
                            .lock()
                            .map_err(|_| anyhow!("ownership lock poisoned"))?
                            .prepare(
                                generation,
                                Instant::now(),
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                            )?;
                        Response::Handoff {
                            ticket: handoff.ticket,
                            expires_at: handoff.expires_at,
                        }
                    }
                    Request::Shutdown => Response::Ok,
                    _ => Response::Error {
                        message: "owner connection only accepts prepare_handoff or shutdown"
                            .to_string(),
                    },
                }
            };
            let result = write_frame(&mut *stream, &response);
            if stopping {
                // A takeover may have occurred while writing the response.
                let owner = ownership
                    .lock()
                    .map_err(|_| anyhow!("ownership lock poisoned"))?;
                if owner.is_owner(generation) {
                    shutdown.store(true, Ordering::Release);
                }
                return result;
            }
            result?;
        }
        Ok(())
    }

    struct SubscriptionRequest {
        id: crate::service::TerminalId,
        offset: u64,
        attachment_id: String,
        role: crate::protocol::AttachmentRole,
        takeover: bool,
    }

    fn stream_subscription(
        stream: &mut UnixStream,
        service: &TerminalService,
        shutdown: &AtomicBool,
        request: SubscriptionRequest,
    ) -> Result<()> {
        use std::io::Read;
        use std::net::Shutdown;

        let attachment = service.attach(
            request.id,
            request.offset,
            request.attachment_id,
            request.role,
            request.takeover,
        )?;
        write_frame(
            &mut *stream,
            &Response::Attached {
                terminal: attachment.terminal.clone(),
                role: attachment.role,
                generation: attachment.generation,
                requested_offset: attachment.replay.requested_offset,
                available_offset: attachment.replay.available_offset,
                end_offset: attachment.replay.end_offset,
                truncated: attachment.replay.truncated,
                replay_base64: base64::engine::general_purpose::STANDARD
                    .encode(&attachment.replay.bytes),
            },
        )?;
        let mut monitor_stream = stream.try_clone()?;
        let (disconnect_tx, disconnect_rx) = crossbeam_channel::bounded::<()>(1);
        let monitor = thread::spawn(move || {
            let mut byte = [0_u8; 1];
            let _ = monitor_stream.read(&mut byte);
            let _ = disconnect_tx.send(());
        });
        let result = (|| loop {
            if shutdown.load(Ordering::Acquire) {
                break Ok(());
            }
            let event = crossbeam_channel::select! {
                recv(disconnect_rx) -> _ => break Ok(()),
                recv(attachment.events) -> event => match event {
                    Ok(event) => event,
                    Err(_) => break Ok(()),
                },
                default(Duration::from_millis(100)) => continue,
            };
            let response = match event {
                StreamEvent::Output { start, end, bytes } => {
                    write_output_frame(&mut *stream, start, end, &bytes)?;
                    continue;
                }
                StreamEvent::Resized {
                    cols,
                    rows,
                    generation,
                    checkpoint,
                } => Response::Resized {
                    cols,
                    rows,
                    generation,
                    checkpoint_base64: base64::engine::general_purpose::STANDARD.encode(checkpoint),
                },
                StreamEvent::Exited {
                    exit_code,
                    final_offset,
                } => Response::Exited {
                    exit_code,
                    final_offset,
                },
                StreamEvent::ControllerChanged {
                    attachment_id,
                    generation,
                } => Response::ControllerChanged {
                    attachment_id,
                    generation,
                },
                StreamEvent::TitleChanged { title } => Response::TitleChanged { title },
                StreamEvent::ForegroundProcessChanged { process } => {
                    Response::ForegroundProcessChanged { process }
                }
            };
            write_frame(&mut *stream, &response)?;
            if matches!(response, Response::Exited { .. }) {
                break Ok(());
            }
        })();
        // A full shutdown can discard a just-written final frame on macOS.
        // Half-close first so the peer drains queued output before closing.
        let _ = stream.shutdown(Shutdown::Write);
        let _ = monitor.join();
        result
    }

    fn dispatch(
        request: Request,
        service: &TerminalService,
        registration: &Registration,
    ) -> Result<Response> {
        Ok(match request {
            Request::Ping => Response::Pong {
                instance_id: registration.instance_id.clone(),
                pid: registration.pid,
                protocol: registration.protocol,
            },
            Request::Create {
                program,
                args,
                cwd,
                title,
                group_id,
                env,
                cols,
                rows,
            } => Response::Created {
                terminal: service.create(CreateTerminal {
                    program,
                    args,
                    cwd,
                    title,
                    group_id,
                    env,
                    cols,
                    rows,
                })?,
            },
            Request::List => Response::Terminals {
                terminals: service.list()?,
            },
            Request::Write {
                id,
                attachment_id,
                data_base64,
            } => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data_base64)
                    .context("invalid input base64")?;
                service.write_for(id, attachment_id, bytes)?;
                Response::Ok
            }
            Request::Resize {
                id,
                attachment_id,
                cols,
                rows,
            } => {
                service.resize_for(id, attachment_id, cols, rows)?;
                Response::Ok
            }
            Request::Control {
                id,
                attachment_id,
                cols,
                rows,
            } => {
                service.control(id, attachment_id, cols, rows)?;
                Response::Ok
            }
            Request::Input {
                id,
                attachment_id,
                cols,
                rows,
                data_base64,
            } => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data_base64)
                    .context("invalid input base64")?;
                service.input(id, attachment_id, cols, rows, bytes)?;
                Response::Ok
            }
            Request::Snapshot { id } => {
                let snapshot = service.snapshot(id)?;
                Response::Snapshot {
                    terminal: snapshot.info,
                    text: snapshot.text,
                    checkpoint_base64: base64::engine::general_purpose::STANDARD
                        .encode(snapshot.checkpoint),
                    cursor_x: snapshot.cursor_x,
                    cursor_y: snapshot.cursor_y,
                }
            }
            Request::ReadRows { id, rows } => {
                let rows = service.read_rows(id, rows)?;
                Response::Rows {
                    terminal: rows.terminal,
                    lines: rows.lines,
                    cursor_x: rows.cursor_x,
                    cursor_y: rows.cursor_y,
                }
            }
            Request::Replay { id, offset } => {
                let replay = service.replay(id, offset)?;
                Response::Replay {
                    requested_offset: replay.requested_offset,
                    available_offset: replay.available_offset,
                    end_offset: replay.end_offset,
                    truncated: replay.truncated,
                    data_base64: base64::engine::general_purpose::STANDARD.encode(replay.bytes),
                }
            }
            Request::Subscribe { .. } => unreachable!("subscriptions are handled before dispatch"),
            Request::Terminate { id } => {
                service.terminate(id)?;
                Response::Ok
            }
            Request::Shutdown => Response::Ok,
            Request::Own { .. } => unreachable!("ownership is handled before dispatch"),
            Request::PrepareHandoff => Response::Error {
                message: "handoff requires the owner connection".to_string(),
            },
        })
    }

    fn write_registration(directory: &std::path::Path, registration: &Registration) -> Result<()> {
        let temporary = directory.join(format!("service.{}.tmp", registration.instance_id));
        let data = serde_json::to_vec_pretty(registration)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        use std::io::Write;
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
        use super::*;

        #[test]
        fn socket_paths_are_short_and_runtime_specific() {
            let base =
                std::env::temp_dir().join(format!("opencode-pty-socket-test-{}", random_id()));
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

        #[test]
        fn runtime_root_avoids_temporary_directories() {
            let home = Some(PathBuf::from("/home/user"));
            assert_eq!(
                super::super::resolve_runtime_root(Some("/state".into()), home.clone()),
                PathBuf::from("/state/opencode/pty")
            );
            assert_eq!(
                super::super::resolve_runtime_root(Some("relative".into()), home.clone()),
                PathBuf::from("/home/user/.local/state/opencode/pty")
            );
            assert_eq!(
                super::super::resolve_runtime_root(None, home),
                PathBuf::from("/home/user/.local/state/opencode/pty")
            );
        }

        #[test]
        fn runtime_names_are_single_components() {
            let root = Path::new("/state");
            assert_eq!(
                super::super::runtime_dir(Some(root), "ee7511b8-7db0.x_1").unwrap(),
                root.join("ee7511b8-7db0.x_1")
            );
            for name in ["", ".", "..", "a/b", "../x", "with space"] {
                assert!(
                    super::super::runtime_dir(Some(root), name).is_err(),
                    "{name:?}"
                );
            }
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
}

#[cfg(unix)]
pub use unix::run;

#[cfg(not(unix))]
pub fn run(_directory: &Path) -> Result<()> {
    anyhow::bail!("persistent opencode-pty transport is not implemented on this platform")
}
