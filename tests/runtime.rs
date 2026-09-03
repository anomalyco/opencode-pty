//! Native Unix PTY / Windows ConPTY coverage, without daemon or client transport.

#[path = "support/terminal_fixture.rs"]
mod terminal_fixture;

use std::thread;
use std::time::{Duration, Instant};

use opencode_pty::protocol::AttachmentRole;
use opencode_pty::service::{StreamEvent, TerminalId, TerminalService, TerminalSnapshot};
use terminal_fixture::{Command, Deadline, Fixture, TempDir};

fn wait_text(service: &TerminalService, id: TerminalId, expected: &str) -> TerminalSnapshot {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let snapshot = service.snapshot(id).unwrap();
        if snapshot.text.contains(expected) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "missing {expected:?}: {snapshot:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn real_child_input_output_unicode_and_snapshots() {
    let _deadline = Deadline::new();
    let fixture = Fixture::new();
    let service = TerminalService::default();
    let info = service.create(fixture.request()).unwrap();
    assert!(info.pid.is_some_and(|pid| pid != 0));
    let mut child = fixture.connect();
    child.command(Command::Output(
        "\x1b[2J\x1b[Hplain café 界 🙂\r\nREADY".into(),
    ));
    let snapshot = wait_text(&service, info.id, "READY");
    assert!(snapshot.text.contains("plain café 界 🙂"), "{snapshot:?}");
    assert!(!snapshot.checkpoint.is_empty());
    let rows = service.read_rows(info.id, None).unwrap();
    assert!(rows.lines.iter().any(|line| line == "plain café 界 🙂"));
    assert_eq!((rows.cursor_x, rows.cursor_y), (5, 1));
    let input = "typed café 界 🙂\r".as_bytes();
    service.write(info.id, input.to_vec()).unwrap();
    assert_eq!(
        child.command(Command::Read(input.len())),
        serde_json::json!(input)
    );
    let replay = service.replay(info.id, 0).unwrap();
    assert!(!replay.truncated);
    assert_eq!(replay.end_offset, snapshot.info.output_tail);
    assert_eq!(replay.bytes.len() as u64, replay.end_offset);
    assert!(
        service
            .replay(info.id, replay.end_offset)
            .unwrap()
            .bytes
            .is_empty()
    );
    service.terminate(info.id).unwrap();
}

#[test]
fn child_cwd_environment_and_quoted_program_path() {
    let _deadline = Deadline::new();
    let directory = TempDir::new();
    let fixture = Fixture::new();
    let service = TerminalService::default();
    let executable = directory.executable(if cfg!(windows) {
        "child 界.exe"
    } else {
        "child 界"
    });
    let mut request = fixture.request();
    request.program = executable.to_str().unwrap().to_owned();
    request.cwd = directory.0.clone();
    // Additional exact test filters exercise argv quoting, including an empty
    // argument and backslashes immediately preceding a closing quote.
    request
        .args
        .extend(["with spaces", "a\"quote", "trailing slash \\", "", "界"].map(str::to_owned));
    request
        .env
        .insert("PTY_FIXTURE_VALUE".into(), "value café 界 = ok".into());
    let expected_args = request.args.clone();
    let info = service.create(request).unwrap();
    let mut child = fixture.connect();
    let context = child.command(Command::Context);
    assert_eq!(
        std::fs::canonicalize(context["cwd"].as_str().unwrap()).unwrap(),
        std::fs::canonicalize(&directory.0).unwrap()
    );
    assert_eq!(context["args"], serde_json::json!(expected_args));
    assert_eq!(context["value"], "value café 界 = ok");
    assert_eq!(context["term"], "xterm-256color");
    assert_eq!(context["colorterm"], "truecolor");
    service.terminate(info.id).unwrap();
}

#[test]
fn executable_is_resolved_from_child_path() {
    let _deadline = Deadline::new();
    let directory = TempDir::new();
    let fixture = Fixture::new();
    let service = TerminalService::default();
    let name = if cfg!(windows) {
        "pty-path-fixture.exe"
    } else {
        "pty-path-fixture"
    };
    directory.executable(name);
    let mut request = fixture.request();
    request.program = name.into();
    request
        .env
        .insert("PATH".into(), directory.0.to_str().unwrap().into());
    let info = service.create(request).unwrap();
    let mut child = fixture.connect();
    child.command(Command::Output("\r\nPATH_OK".into()));
    wait_text(&service, info.id, "PATH_OK");
    service.terminate(info.id).unwrap();
}

#[test]
fn resize_updates_real_console_and_parser() {
    let _deadline = Deadline::new();
    let fixture = Fixture::new();
    let service = TerminalService::default();
    let info = service.create(fixture.request()).unwrap();
    let mut child = fixture.connect();
    assert_eq!(child.command(Command::Size), serde_json::json!([80, 24]));
    let observer = service
        .attach(
            info.id,
            0,
            "resize-observer".into(),
            AttachmentRole::Observer,
            false,
        )
        .unwrap();
    for (cols, rows) in [(100, 40), (60, 18)] {
        service.resize(info.id, cols, rows).unwrap();
        assert_eq!(
            child.command(Command::Size),
            serde_json::json!([cols, rows])
        );
        let snapshot = service.snapshot(info.id).unwrap();
        assert_eq!((snapshot.info.cols, snapshot.info.rows), (cols, rows));
        assert_eq!(
            service.read_rows(info.id, None).unwrap().lines.len(),
            rows as usize
        );
        loop {
            if let StreamEvent::Resized {
                cols: actual_cols,
                rows: actual_rows,
                checkpoint,
                ..
            } = observer
                .events
                .recv_timeout(Duration::from_secs(15))
                .unwrap()
            {
                assert_eq!((actual_cols, actual_rows), (cols, rows));
                assert!(!checkpoint.is_empty());
                break;
            }
        }
    }
    drop(observer);
    service.terminate(info.id).unwrap();
}

#[test]
fn terminal_query_response_reaches_child() {
    let _deadline = Deadline::new();
    let fixture = Fixture::new();
    let service = TerminalService::default();
    let info = service.create(fixture.request()).unwrap();
    let mut child = fixture.connect();
    child.command(Command::Output("\x1b[2J\x1b[H\x1b[3;7H\x1b[6n".into()));
    let response = b"\x1b[3;7R";
    assert_eq!(
        child.command(Command::Read(response.len())),
        serde_json::json!(response)
    );
    service.write(info.id, b"!".to_vec()).unwrap();
    assert_eq!(child.command(Command::Read(1)), serde_json::json!(b"!"));
    // ConPTY consumes application DSR itself, but portable-pty enables cursor
    // inheritance: ConPTY's own DSR must pass through Ghostty and our writer to
    // initialize the console. On Unix, the application's query passes through.
    let replay = service.replay(info.id, 0).unwrap();
    assert!(
        replay.bytes.windows(4).any(|bytes| bytes == b"\x1b[6n"),
        "no terminal query in {replay:?}"
    );
    service.terminate(info.id).unwrap();
}

#[test]
fn independent_terminals_keep_bounded_replay() {
    let _deadline = Deadline::new();
    let alpha_fixture = Fixture::new();
    let beta_fixture = Fixture::new();
    let service = TerminalService::new(64);
    let alpha = service.create(alpha_fixture.request()).unwrap();
    let beta = service.create(beta_fixture.request()).unwrap();
    assert_ne!(alpha.id, beta.id);
    let mut a = alpha_fixture.connect();
    let mut b = beta_fixture.connect();
    a.command(Command::Output(format!(
        "\x1b[2J\x1b[H{}\r\nALPHA",
        "a".repeat(160)
    )));
    b.command(Command::Output("\x1b[2J\x1b[HBETA".into()));
    let snapshot = wait_text(&service, alpha.id, "ALPHA");
    assert!(!snapshot.text.contains("BETA"));
    assert!(!wait_text(&service, beta.id, "BETA").text.contains("ALPHA"));
    let replay = service.replay(alpha.id, 0).unwrap();
    assert!(replay.truncated);
    assert_eq!(replay.bytes.len(), 64);
    assert_eq!(replay.end_offset - replay.available_offset, 64);
    service.terminate(alpha.id).unwrap();
    service.write(beta.id, b"still-alive".to_vec()).unwrap();
    assert_eq!(
        b.command(Command::Read(11)),
        serde_json::json!(b"still-alive")
    );
    service.terminate(beta.id).unwrap();
    assert!(service.list().unwrap().is_empty());
}
