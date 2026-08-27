use std::path::PathBuf;

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

#[cfg(unix)]
mod unix {
    use std::fs::{self, OpenOptions};
    use std::net::Shutdown;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use anyhow::{Context, Result, anyhow};
    use base64::Engine;
    use fs2::FileExt;
    use sha2::{Digest, Sha256};

    use super::{LOCK_FILE, REGISTRATION_FILE, Registration};
    use crate::ownership::Ownership;
    use crate::protocol::{
        Envelope, PROTOCOL_VERSION, Request, Response, read_frame, write_frame, write_output_frame,
    };
    use crate::service::{CreateTerminal, StreamEvent, TerminalService};

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
        let data =
            fs::read(registration_path()).context("opencode-pty registration is unavailable")?;
        serde_json::from_slice(&data).context("invalid opencode-pty registration")
    }

    pub fn run() -> Result<()> {
        let directory = service_dir();
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

        let registration = Registration {
            instance_id: random_id(),
            pid: std::process::id(),
            protocol: PROTOCOL_VERSION,
            socket: socket_path.clone(),
            token: random_id(),
        };
        let ownership = Arc::new(Mutex::new(Ownership::new(Instant::now())));
        write_registration(&directory, &registration)?;

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
        let watchdog = thread::spawn(move || {
            if cleanup_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                eprintln!("opencode-pty cleanup timed out; forcing exit");
                let _ = remove_if_current(&cleanup_registration);
                let _ = fs::remove_file(&cleanup_registration.socket);
                std::process::exit(1);
            }
        });
        service.shutdown();
        for (_, handler) in handlers {
            let _ = handler.join();
        }
        drop(service);
        remove_if_current(&registration)?;
        let _ = fs::remove_file(&socket_path);
        let _ = cleanup_tx.send(());
        let _ = watchdog.join();
        drop(lock);
        Ok(())
    }

    fn socket_path(directory: &std::path::Path) -> Result<PathBuf> {
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
            if let Err(error) = claim {
                return write_frame(
                    &mut stream,
                    &Response::Error {
                        message: error.to_string(),
                    },
                );
            }
            let result = owner_connection(&mut stream, registration, shutdown, ownership);
            ownership
                .lock()
                .map_err(|_| anyhow!("ownership lock poisoned"))?
                .disconnect(Instant::now());
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
    ) -> Result<()> {
        write_frame(&mut *stream, &Response::Owned)?;
        while !shutdown.load(Ordering::Acquire) {
            let envelope: Envelope = read_frame(&mut *stream)?;
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
                shutdown.store(true, Ordering::Release);
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
        fs::rename(&temporary, registration_path())?;
        Ok(())
    }

    fn remove_if_current(registration: &Registration) -> Result<()> {
        if read_registration().is_ok_and(|current| current.instance_id == registration.instance_id)
        {
            fs::remove_file(registration_path())?;
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
    }
}

#[cfg(unix)]
pub use unix::{read_registration, registration_path, run, service_dir};

#[cfg(not(unix))]
pub fn run() -> anyhow::Result<()> {
    anyhow::bail!("persistent opencode-pty transport is not implemented on this platform")
}

#[cfg(not(unix))]
pub fn read_registration() -> anyhow::Result<Registration> {
    anyhow::bail!("persistent opencode-pty transport is not implemented on this platform")
}

#[cfg(not(unix))]
pub fn registration_path() -> PathBuf {
    PathBuf::from("opencode-pty-service.json")
}
