use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use base64::Engine;

use super::control::Control;
use super::{Registration, platform};
use crate::ownership::Ownership;
use crate::protocol::{Envelope, Request, Response, read_frame, write_frame, write_output_frame};
use crate::service::{CreateTerminal, StreamEvent, TerminalService};
use crate::transport::{Cancellation, Connection, Listener};

pub fn run(directory: &Path) -> Result<()> {
    let (runtime, listener) = platform::Runtime::bind(directory)?;
    // Created after bind so bind I/O does not consume the acquire window.
    let control = Arc::new(Control::new(Ownership::new(Instant::now())));
    serve(runtime, listener, control)
}

struct Handler {
    cancellation: Cancellation,
    done: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

/// Marks a handler finished once its connection has been dropped, even on panic.
struct Done(Arc<AtomicBool>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn serve(runtime: platform::Runtime, mut listener: Listener, control: Arc<Control>) -> Result<()> {
    let registration = runtime.registration.clone();
    let registration = &registration;

    let service = Arc::new(TerminalService::default());
    let mut handlers = Vec::<Handler>::new();
    let result = (|| -> Result<()> {
        loop {
            if control.tick(Instant::now())? {
                break;
            }
            for handler in handlers.extract_if(.., |handler| {
                handler.done.load(Ordering::Acquire) || handler.thread.is_finished()
            }) {
                let _ = handler.thread.join();
            }
            match listener.accept() {
                Ok(Some(stream)) => {
                    let cancellation = stream.cancellation()?;
                    let done = Arc::new(AtomicBool::new(false));
                    let spawned = {
                        let done = Arc::clone(&done);
                        let service = Arc::clone(&service);
                        let control = Arc::clone(&control);
                        let registration = registration.clone();
                        thread::Builder::new()
                            .name("opencode-pty-connection".to_string())
                            .spawn(move || {
                                let _done = Done(done);
                                if let Err(error) =
                                    handle_connection(stream, &service, &registration, &control)
                                {
                                    eprintln!("opencode-pty request failed: {error:#}");
                                }
                            })
                    };
                    // A failed spawn drops the connection, so the peer sees EOF.
                    match spawned {
                        Ok(thread) => handlers.push(Handler {
                            cancellation,
                            done,
                            thread,
                        }),
                        Err(error) => {
                            eprintln!("opencode-pty could not start connection handler: {error}");
                        }
                    }
                }
                Ok(None) => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    })();

    control.request_stop();
    listener.stop();
    // Unblock partial requests, owner reads, and backpressured subscriptions
    // before joining. PTY workers still use their existing termination path.
    for handler in &handlers {
        handler.cancellation.cancel();
    }
    let (cleanup_tx, cleanup_rx) = crossbeam_channel::bounded::<()>(1);
    let cleanup = runtime.cleanup.clone();
    let watchdog = thread::Builder::new()
        .name("opencode-pty-watchdog".to_string())
        .spawn(move || {
            if cleanup_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                eprintln!("opencode-pty cleanup timed out; forcing exit");
                let _ = cleanup.run();
                std::process::exit(1);
            }
        })
        .inspect_err(|error| {
            eprintln!("opencode-pty could not start cleanup watchdog: {error}");
        })
        .ok();
    service.shutdown();
    for handler in handlers {
        let _ = handler.thread.join();
    }
    drop(service);
    let cleanup = runtime.finish();
    drop(listener);
    let _ = cleanup_tx.send(());
    if let Some(watchdog) = watchdog {
        let _ = watchdog.join();
    }
    result.and(cleanup)
}

fn handle_connection(
    mut stream: Connection,
    service: &TerminalService,
    registration: &Registration,
    control: &Control,
) -> Result<()> {
    let envelope: Envelope = read_frame(&mut stream)?;
    if envelope.token != registration.token {
        return send_response(
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
        } else if control.stopping() {
            Err(anyhow!("daemon is stopping"))
        } else {
            control
                .update_ownership(|ownership| ownership.claim(ticket.as_deref(), Instant::now()))?
        };
        let generation = match claim {
            Ok(generation) => generation,
            Err(error) => {
                return send_response(
                    &mut stream,
                    &Response::Error {
                        message: error.to_string(),
                    },
                );
            }
        };
        let result = owner_connection(&mut stream, registration, control, generation);
        control.update_ownership(|ownership| ownership.disconnect(generation, Instant::now()))?;
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
            control,
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
    let response =
        dispatch(envelope.request, service, registration).unwrap_or_else(|error| Response::Error {
            message: format!("{error:#}"),
        });
    let result = send_response(&mut stream, &response);
    if stopping {
        control.request_stop();
    }
    result
}

fn owner_connection(
    stream: &mut Connection,
    registration: &Registration,
    control: &Control,
    generation: u64,
) -> Result<()> {
    write_frame(&mut *stream, &Response::Owned)?;
    while !control.stopping() {
        let envelope: Envelope = read_frame(&mut *stream)?;
        if !control.is_owner(generation)? {
            return send_response(
                stream,
                &Response::Error {
                    message: "owner connection has been superseded".to_string(),
                },
            );
        }
        let stopping =
            envelope.token == registration.token && matches!(envelope.request, Request::Shutdown);
        let response = if envelope.token != registration.token {
            Response::Error {
                message: "authentication failed".to_string(),
            }
        } else {
            match envelope.request {
                Request::PrepareHandoff => {
                    let unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                    let handoff = control.update_ownership(|ownership| {
                        ownership.prepare(generation, Instant::now(), unix_ms)
                    })??;
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
        if stopping {
            let result = send_response(stream, &response);
            // A takeover may have occurred while writing the response.
            if control.is_owner(generation)? {
                control.request_stop();
            }
            return result;
        }
        write_frame(&mut *stream, &response)?;
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
    stream: &mut Connection,
    service: &TerminalService,
    control: &Control,
    request: SubscriptionRequest,
) -> Result<()> {
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
    let monitor = stream.monitor_disconnect()?;
    let result = (|| loop {
        if control.stopping() {
            break Ok(());
        }
        let event = crossbeam_channel::select! {
            recv(monitor.disconnected()) -> _ => break Ok(()),
            recv(attachment.events) -> event => match event {
                Ok(event) => event,
                Err(_) => break Ok(()),
            },
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
    monitor.finish();
    result
}

fn send_response(stream: &mut Connection, response: &Response) -> Result<()> {
    write_frame(&mut *stream, response)?;
    stream.finish_response()?;
    Ok(())
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

#[cfg(all(test, unix))]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    use crossbeam_channel::Receiver;

    use super::*;

    const TEST_HANDOFF: Duration = Duration::from_secs(60);

    struct TestDaemon {
        root: PathBuf,
        registration: Registration,
        start: Instant,
        control: Arc<Control>,
        exited: Receiver<(Result<()>, Instant)>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl TestDaemon {
        fn start(acquire: Duration, handoff: Duration) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "opencode-pty-server-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            // The sweeper scans the runtime directory's siblings; keep it alone in `root`.
            let (runtime, listener) = platform::Runtime::bind(&root.join("runtime")).unwrap();
            let registration = runtime.registration.clone();
            let start = Instant::now();
            let control = Arc::new(Control::new(Ownership::with_timeouts(
                start, acquire, handoff,
            )));
            let (sender, exited) = crossbeam_channel::bounded(1);
            let thread = {
                let control = Arc::clone(&control);
                thread::spawn(move || {
                    let result = serve(runtime, listener, control);
                    let _ = sender.send((result, Instant::now()));
                })
            };
            Self {
                root,
                registration,
                start,
                control,
                exited,
                thread: Some(thread),
            }
        }

        fn connect(&self) -> Connection {
            let stream = Connection::connect(&self.registration.socket).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            stream
        }

        fn send(&self, stream: &mut Connection, request: Request) -> Response {
            write_frame(
                &mut *stream,
                &Envelope {
                    token: self.registration.token.clone(),
                    request,
                },
            )
            .unwrap();
            read_frame(stream).unwrap()
        }

        fn own(&self, ticket: Option<String>) -> Connection {
            let mut stream = self.connect();
            let request = Request::Own {
                instance_id: self.registration.instance_id.clone(),
                ticket,
            };
            match self.send(&mut stream, request) {
                Response::Owned => stream,
                response => panic!("own: {response:?}"),
            }
        }

        fn claim(&self) -> Connection {
            self.own(None)
        }

        fn prepare(&self, owner: &mut Connection) -> String {
            match self.send(owner, Request::PrepareHandoff) {
                Response::Handoff { ticket, .. } => ticket,
                response => panic!("prepare: {response:?}"),
            }
        }

        fn exit_within(&self, timeout: Duration) -> Option<(Result<()>, Instant)> {
            self.exited.recv_timeout(timeout).ok()
        }

        fn stopped_within(&self, timeout: Duration) -> Instant {
            let (result, returned_at) = self
                .exit_within(timeout)
                .expect("daemon did not stop in time");
            result.unwrap();
            returned_at
        }
    }

    impl Drop for TestDaemon {
        fn drop(&mut self) {
            self.control.request_stop();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn shutdown_request_stops_immediately() {
        let daemon = TestDaemon::start(Duration::from_secs(60), TEST_HANDOFF);
        let _owner = daemon.claim();
        assert!(matches!(
            daemon.send(&mut daemon.connect(), Request::Shutdown),
            Response::Ok
        ));
        daemon.stopped_within(Duration::from_secs(1));
    }

    #[test]
    fn owner_shutdown_stops_immediately() {
        let daemon = TestDaemon::start(Duration::from_secs(60), TEST_HANDOFF);
        let mut owner = daemon.claim();
        assert!(matches!(
            daemon.send(&mut owner, Request::Shutdown),
            Response::Ok
        ));
        daemon.stopped_within(Duration::from_secs(1));
    }

    #[test]
    fn owner_disconnect_without_handoff_stops_immediately() {
        let daemon = TestDaemon::start(Duration::from_secs(60), TEST_HANDOFF);
        drop(daemon.claim());
        daemon.stopped_within(Duration::from_secs(1));
    }

    #[test]
    fn starting_deadline_fires_on_time() {
        let daemon = TestDaemon::start(Duration::from_millis(300), TEST_HANDOFF);
        let elapsed = daemon.stopped_within(Duration::from_secs(3)) - daemon.start;
        assert!(
            (Duration::from_millis(300)..=Duration::from_millis(1500)).contains(&elapsed),
            "{elapsed:?}"
        );
    }

    #[test]
    fn waiting_deadline_fires_on_time() {
        let daemon = TestDaemon::start(Duration::from_secs(60), Duration::from_millis(500));
        let mut owner = daemon.claim();
        let before = Instant::now();
        daemon.prepare(&mut owner);
        let after = Instant::now();
        drop(owner);
        let returned_at = daemon.stopped_within(Duration::from_secs(3));
        assert!(returned_at >= before + Duration::from_millis(500));
        assert!(returned_at <= after + Duration::from_millis(1500));
    }

    #[test]
    fn handoff_claim_transfers_without_spurious_stop() {
        let daemon = TestDaemon::start(Duration::from_secs(60), TEST_HANDOFF);
        let mut owner = daemon.claim();
        let ticket = daemon.prepare(&mut owner);
        let successor = daemon.own(Some(ticket));
        drop(owner);
        assert!(daemon.exit_within(Duration::from_millis(300)).is_none());
        drop(successor);
        daemon.stopped_within(Duration::from_secs(1));
    }
}
