use std::process::Command;

#[test]
fn demo_preserves_fixture_and_bidirectional_io() {
    let output = Command::new(env!("CARGO_BIN_EXE_pty-handoff-poc"))
        .arg("demo")
        .output()
        .expect("run demo binary");

    assert!(
        output.status.success(),
        "demo failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("demo output is UTF-8");
    let before = value_after(&stdout, "old: child PID before handoff = ");
    let after = value_after(&stdout, "new: child PID after handoff = ");

    assert_eq!(before, after);
    assert!(stdout.contains(&format!(
        "old: pre-handoff response = FIXTURE_RESPONSE pid={before} command=before"
    )));
    assert!(stdout.contains(&format!(
        "new: post-handoff response = FIXTURE_RESPONSE pid={after} command=after"
    )));
    assert!(stdout.contains("demo: SUCCESS"));
}

fn value_after<'a>(output: &'a str, prefix: &str) -> &'a str {
    output
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("missing output prefix: {prefix}"))
}
