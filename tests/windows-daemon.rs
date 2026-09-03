#![cfg(windows)]

// Direct-service and daemon tests share the runtime lane's real child fixture,
// rather than maintaining separate console setup.
#[path = "support/terminal_fixture.rs"]
mod terminal_fixture;

use std::io::Write;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
use opencode_pty::daemon::{PipeConnection, Registration};
use opencode_pty::protocol::{
    AttachmentRole, Envelope, Request, Response, SubscriptionEvent, read_frame,
    read_subscription_event, write_frame,
};
use opencode_pty::service::{CreateTerminal, TerminalInfo};
use terminal_fixture::{Command as FixtureCommand, Deadline, Fixture, TempDir};
use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

struct Daemon {
    child: Child,
    registration: Registration,
    directory: PathBuf,
    root: TempDir,
}

impl Daemon {
    fn start() -> Self {
        let root = TempDir::new();
        let directory = root.0.join("runtime");
        let mut child = spawn(&directory);
        let registration = registration(&mut child, &directory);
        Self {
            child,
            registration,
            directory,
            root,
        }
    }

    fn connect(&self) -> PipeConnection {
        let mut stream = PipeConnection::connect(&self.registration.socket).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5)));
        stream.set_write_timeout(Some(Duration::from_secs(5)));
        stream
    }

    fn send(&self, stream: &mut PipeConnection, request: Request) -> Response {
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

    fn request(&self, request: Request) -> Response {
        self.send(&mut self.connect(), request)
    }

    fn own(&self, ticket: Option<String>) -> (PipeConnection, Response) {
        let mut stream = self.connect();
        let response = self.send(
            &mut stream,
            Request::Own {
                instance_id: self.registration.instance_id.clone(),
                ticket,
            },
        );
        (stream, response)
    }

    fn create(&self, fixture: &Fixture) -> TerminalInfo {
        let CreateTerminal {
            program,
            args,
            cwd,
            title,
            group_id,
            env,
            cols,
            rows,
        } = fixture.request();
        match self.request(Request::Create {
            program,
            args,
            cwd,
            title,
            group_id,
            env,
            cols,
            rows,
        }) {
            Response::Created { terminal } => terminal,
            response => panic!("create: {response:?}"),
        }
    }

    fn subscribe(&self, id: u64, role: AttachmentRole) -> PipeConnection {
        let mut stream = self.connect();
        assert!(matches!(
            self.send(
                &mut stream,
                Request::Subscribe {
                    id,
                    offset: 0,
                    attachment_id: "daemon-test".into(),
                    role,
                    takeover: false,
                }
            ),
            Response::Attached { .. }
        ));
        stream
    }

    fn wait(&mut self) {
        let status = wait(&mut self.child);
        assert!(status.success(), "daemon failed: {status}");
        assert!(!self.directory.join("service.json").exists());
        assert!(PipeConnection::connect(&self.registration.socket).is_err());
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn(directory: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("daemon")
        .env("OPENCODE_PTY_RUNTIME_DIR", directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

fn registration(child: &mut Child, directory: &Path) -> Registration {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if let Ok(data) = std::fs::read(directory.join("service.json"))
            && let Ok(registration) = serde_json::from_slice::<Registration>(&data)
            && registration.pid == child.id()
        {
            assert_eq!(registration.protocol, 7);
            assert_eq!(
                registration.socket,
                PathBuf::from(format!(
                    r"\\.\pipe\opencode-pty-{}",
                    registration.instance_id
                ))
            );
            return registration;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "daemon exited before registering"
        );
        assert!(Instant::now() < deadline, "daemon did not register");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "daemon did not exit");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn authenticated_ownership_handoff_and_shutdown_use_real_pipes() {
    let _deadline = Deadline::new();
    let mut daemon = Daemon::start();
    let mut invalid = daemon.connect();
    write_frame(
        &mut invalid,
        &Envelope {
            token: "wrong".into(),
            request: Request::Ping,
        },
    )
    .unwrap();
    assert!(
        matches!(read_frame(&mut invalid).unwrap(), Response::Error { message } if message == "authentication failed")
    );
    drop(invalid);
    assert!(matches!(
        daemon.request(Request::Own {
            instance_id: "wrong".into(),
            ticket: None
        }),
        Response::Error { .. }
    ));
    let (mut owner, response) = daemon.own(None);
    assert!(matches!(response, Response::Owned));
    assert!(matches!(daemon.own(None).1, Response::Error { .. }));
    let Response::Handoff { ticket, expires_at } = daemon.send(&mut owner, Request::PrepareHandoff)
    else {
        panic!("handoff response");
    };
    assert!(
        matches!(daemon.send(&mut owner, Request::PrepareHandoff), Response::Handoff { ticket: repeated, expires_at: deadline } if repeated == ticket && deadline == expires_at)
    );
    assert!(matches!(
        daemon.own(Some(ticket.clone())).1,
        Response::Error { .. }
    ));
    drop(owner);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut successor = loop {
        let (stream, response) = daemon.own(Some(ticket.clone()));
        if matches!(response, Response::Owned) {
            break stream;
        }
        assert!(Instant::now() < deadline, "handoff claim: {response:?}");
        thread::sleep(Duration::from_millis(10));
    };
    assert!(matches!(
        daemon.send(&mut successor, Request::Shutdown),
        Response::Ok
    ));
    drop(successor); // Protocol completion, not Unix half-close.
    daemon.wait();
}

#[test]
fn service_lock_and_stale_registration_are_instance_scoped() {
    let _deadline = Deadline::new();
    let mut daemon = Daemon::start();
    let mut duplicate = spawn(&daemon.directory);
    assert!(!wait(&mut duplicate).success());
    let old = daemon.registration.clone();
    daemon.child.kill().unwrap();
    daemon.child.wait().unwrap();
    assert!(daemon.directory.join("service.json").exists());
    daemon.child = spawn(&daemon.directory);
    daemon.registration = registration(&mut daemon.child, &daemon.directory);
    assert_ne!(old.instance_id, daemon.registration.instance_id);
    assert_ne!(old.token, daemon.registration.token);
    assert_ne!(old.socket, daemon.registration.socket);
    let (owner, response) = daemon.own(None);
    assert!(matches!(response, Response::Owned));
    assert!(matches!(daemon.request(Request::Shutdown), Response::Ok));
    drop(owner);
    daemon.wait();
}

#[test]
fn unclaimed_daemon_cancels_a_partial_request() {
    let _deadline = Deadline::new();
    let mut daemon = Daemon::start();
    let mut partial = daemon.connect();
    partial.write_all(&[0, 0]).unwrap();
    daemon.wait();
}

#[test]
fn named_pipe_daemon_create_input_output_resize_and_shutdown() {
    let _deadline = Deadline::new();
    let mut daemon = Daemon::start();
    let (owner, response) = daemon.own(None);
    assert!(matches!(response, Response::Owned));
    let fixture = Fixture::new();
    // Exercise a path containing spaces/Unicode using the shared child fixture.
    let executable = daemon.root.executable("daemon fixture.exe");
    let mut request = fixture.request();
    request.program = executable.to_str().unwrap().into();
    let CreateTerminal {
        program,
        args,
        cwd,
        title,
        group_id,
        env,
        cols,
        rows,
    } = request;
    let Response::Created { terminal } = daemon.request(Request::Create {
        program,
        args,
        cwd,
        title,
        group_id,
        env,
        cols,
        rows,
    }) else {
        panic!("create response");
    };
    let mut child = fixture.connect();
    let mut subscription = daemon.subscribe(terminal.id, AttachmentRole::Observer);
    child.command(FixtureCommand::Output("\x1b[2J\x1b[Hdaemon-output".into()));
    let mut bytes = Vec::new();
    while !String::from_utf8_lossy(&bytes).contains("daemon-output") {
        if let SubscriptionEvent::Output { bytes: output, .. } =
            read_subscription_event(&mut subscription).unwrap()
        {
            bytes.extend(output);
        }
    }
    let input = b"real-input";
    assert!(matches!(
        daemon.request(Request::Write {
            id: terminal.id,
            attachment_id: None,
            data_base64: base64::engine::general_purpose::STANDARD.encode(input)
        }),
        Response::Ok
    ));
    assert_eq!(
        child.command(FixtureCommand::Read(input.len())),
        serde_json::json!(input)
    );
    assert!(matches!(
        daemon.request(Request::Resize {
            id: terminal.id,
            attachment_id: None,
            cols: 93,
            rows: 31
        }),
        Response::Ok
    ));
    assert_eq!(
        child.command(FixtureCommand::Size),
        serde_json::json!([93, 31])
    );
    assert!(
        matches!(daemon.request(Request::Snapshot { id: terminal.id }), Response::Snapshot { text, .. } if text.contains("daemon-output"))
    );
    // Natural-exit/EOF ordering belongs to the runtime cleanup milestone. This
    // basic daemon test deliberately exercises live-child operations + shutdown.
    drop(subscription);
    assert!(matches!(daemon.request(Request::Shutdown), Response::Ok));
    drop(owner);
    daemon.wait();
}

#[test]
fn owner_loss_cancels_blocked_subscriber_and_partial_request() {
    let _deadline = Deadline::new();
    let mut daemon = Daemon::start();
    let (owner, response) = daemon.own(None);
    assert!(matches!(response, Response::Owned));
    let fixture = Fixture::new();
    let terminal = daemon.create(&fixture);
    let mut child = fixture.connect();
    // SAFETY: open the live child for waiting; PID is only used to obtain a
    // stable process handle for this assertion, not as terminal identity.
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, terminal.pid.unwrap()) };
    assert!(!process.is_null());
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    let _blocked = daemon.subscribe(terminal.id, AttachmentRole::Controller);
    let mut partial = daemon.connect();
    partial.write_all(&[0, 0]).unwrap();
    child.command(FixtureCommand::Output("x".repeat(256 * 1024)));
    thread::sleep(Duration::from_millis(100));
    drop(owner);
    daemon.wait();
    assert_eq!(
        unsafe { WaitForSingleObject(process.as_raw_handle(), 3000) },
        WAIT_OBJECT_0
    );
}
