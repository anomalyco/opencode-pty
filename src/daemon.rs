use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const REGISTRATION_FILE: &str = "service.json";
pub const SOCKET_FILE: &str = "service.sock";
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
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    use anyhow::{Context, Result, anyhow};
    use base64::Engine;
    use fs2::FileExt;

    use super::{LOCK_FILE, REGISTRATION_FILE, Registration, SOCKET_FILE};
    use crate::protocol::{Envelope, PROTOCOL_VERSION, Request, Response, read_frame, write_frame};
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

        let socket_path = directory.join(SOCKET_FILE);
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
        write_registration(&directory, &registration)?;

        let service = Arc::new(TerminalService::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let handlers = Mutex::new(Vec::new());
        while !shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let service = Arc::clone(&service);
                    let shutdown = Arc::clone(&shutdown);
                    let registration = registration.clone();
                    let handle = thread::spawn(move || {
                        if let Err(error) =
                            handle_connection(stream, &service, &registration, &shutdown)
                        {
                            eprintln!("opencode-pty request failed: {error:#}");
                        }
                    });
                    handlers
                        .lock()
                        .map_err(|_| anyhow!("handler lock poisoned"))?
                        .push(handle);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        }

        for handler in handlers
            .into_inner()
            .map_err(|_| anyhow!("handler lock poisoned"))?
        {
            let _ = handler.join();
        }
        drop(service);
        remove_if_current(&registration)?;
        let _ = fs::remove_file(&socket_path);
        drop(lock);
        Ok(())
    }

    fn handle_connection(
        mut stream: UnixStream,
        service: &TerminalService,
        registration: &Registration,
        shutdown: &AtomicBool,
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
        let response =
            dispatch(envelope.request, service, registration, shutdown).unwrap_or_else(|error| {
                Response::Error {
                    message: format!("{error:#}"),
                }
            });
        write_frame(&mut stream, &response)
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
        let result = loop {
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
                StreamEvent::Output { start, end, bytes } => Response::Output {
                    start,
                    end,
                    data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                },
                StreamEvent::Resized {
                    cols,
                    rows,
                    generation,
                } => Response::Resized {
                    cols,
                    rows,
                    generation,
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
            };
            write_frame(&mut *stream, &response)?;
            if matches!(response, Response::Exited { .. }) {
                break Ok(());
            }
        };
        let _ = stream.shutdown(Shutdown::Both);
        let _ = monitor.join();
        result
    }

    fn dispatch(
        request: Request,
        service: &TerminalService,
        registration: &Registration,
        shutdown: &AtomicBool,
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
            Request::Shutdown => {
                shutdown.store(true, Ordering::Release);
                Response::Ok
            }
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
