use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

fn runtime_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "opencode-pty-test-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn stop(runtime: &PathBuf) {
    let _ = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("stop")
        .env("OPENCODE_PTY_RUNTIME_DIR", runtime)
        .status();
    let _ = std::fs::remove_dir_all(runtime);
}

fn output_with_timeout(mut child: std::process::Child) -> std::process::Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if child.try_wait().expect("child status").is_some() {
            return child.wait_with_output().expect("child output");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let output = child.wait_with_output().expect("timed out child output");
    panic!(
        "child timed out\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn created_terminal_id(output: &[u8]) -> String {
    let output = String::from_utf8_lossy(output);
    let marker = "created terminal ";
    let start = output.find(marker).expect("created terminal output") + marker.len();
    output[start..]
        .split_whitespace()
        .next()
        .expect("terminal ID")
        .to_string()
}

#[test]
fn playground_proves_authoritative_query_response() {
    let runtime = runtime_dir("query");
    let mut child = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("play")
        .env("OPENCODE_PTY_RUNTIME_DIR", &runtime)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("playground starts");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"demo\nwait 50\nquit\n")
        .expect("commands written");
    let output = child.wait_with_output().expect("playground exits");
    stop(&runtime);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("QUERY_RESPONSE_OK"), "{stdout}");
}

#[test]
fn terminal_survives_between_cli_processes() {
    let runtime = runtime_dir("persistence");
    let first = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("play")
        .env("OPENCODE_PTY_RUNTIME_DIR", &runtime)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(b"new /bin/sh\nrun printf persistent-marker\\n\nquit\n")?;
            child.wait_with_output()
        })
        .expect("first client exits");
    assert!(first.status.success());
    let terminal_id = created_terminal_id(&first.stdout);

    let second = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("play")
        .env("OPENCODE_PTY_RUNTIME_DIR", &runtime)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(b"list\nscreen\nquit\n")?;
            child.wait_with_output()
        })
        .expect("second client exits");
    stop(&runtime);
    assert!(second.status.success());
    let stdout = String::from_utf8_lossy(&second.stdout);
    assert!(stdout.contains("persistent-marker"), "{stdout}");
    assert!(stdout.contains(&terminal_id), "{stdout}");
}

#[test]
fn observer_stream_replays_and_follows_until_exit() {
    let runtime = runtime_dir("stream");
    let first = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .arg("play")
        .env("OPENCODE_PTY_RUNTIME_DIR", &runtime)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(b"new /bin/sh -c \"printf watched; sleep 2\"\nquit\n")?;
            child.wait_with_output()
        })
        .expect("terminal created");
    assert!(first.status.success());
    let terminal_id = created_terminal_id(&first.stdout);

    let watched = Command::new(env!("CARGO_BIN_EXE_opencode-pty"))
        .args(["watch", &terminal_id])
        .env("OPENCODE_PTY_RUNTIME_DIR", &runtime)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map(output_with_timeout)
        .expect("observer exits with terminal");
    stop(&runtime);
    assert!(
        watched.status.success(),
        "watch failed with {:?}: {}",
        watched.status.code(),
        String::from_utf8_lossy(&watched.stderr)
    );
    assert!(String::from_utf8_lossy(&watched.stdout).contains("watched"));
}
