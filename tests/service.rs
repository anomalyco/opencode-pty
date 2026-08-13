#![cfg(unix)]

use std::env;
use std::thread;
use std::time::{Duration, Instant};

use opencode_pty::service::{CreateTerminal, TerminalLifecycle, TerminalService};
use opencode_pty::{protocol::AttachmentRole, service::StreamEvent};

fn command(script: &str) -> CreateTerminal {
    CreateTerminal {
        program: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), script.to_string()],
        cwd: env::current_dir().expect("cwd"),
        title: "test".to_string(),
        group_id: "test".to_string(),
        env: std::collections::HashMap::new(),
        cols: 80,
        rows: 24,
    }
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("condition did not become true");
}

#[test]
fn terminals_run_independently_and_one_exit_does_not_stop_service() {
    let service = TerminalService::new(64 * 1024);
    let alpha = service
        .create(command("printf 'alpha\\n'; sleep 0.05"))
        .expect("alpha created");
    let beta = service
        .create(command("printf 'beta\\n'; sleep 0.2"))
        .expect("beta created");

    wait_for(|| {
        service
            .snapshot(alpha.id)
            .is_ok_and(|snapshot| snapshot.text.contains("alpha"))
            && service
                .snapshot(beta.id)
                .is_ok_and(|snapshot| snapshot.text.contains("beta"))
    });
    wait_for(|| {
        service.list().is_ok_and(|terminals| {
            terminals.iter().any(|terminal| {
                terminal.id == alpha.id
                    && matches!(terminal.lifecycle, TerminalLifecycle::Exited { .. })
            })
        })
    });

    let gamma = service
        .create(command("printf 'gamma\\n'; sleep 0.05"))
        .expect("service creates terminal after child exit");
    wait_for(|| {
        service
            .snapshot(gamma.id)
            .is_ok_and(|snapshot| snapshot.text.contains("gamma"))
    });

    service.terminate(alpha.id).expect("alpha removed");
    service.terminate(beta.id).expect("beta removed");
    service.terminate(gamma.id).expect("gamma removed");
}

#[test]
fn hidden_terminal_output_uses_bounded_replay() {
    let service = TerminalService::new(32);
    let terminal = service
        .create(command("printf '0123456789abcdefghijklmnopqrstuvwxyz'"))
        .expect("terminal created");

    wait_for(|| {
        service.list().is_ok_and(|items| {
            items
                .iter()
                .any(|item| item.id == terminal.id && item.output_tail >= 36)
        })
    });
    let replay = service.replay(terminal.id, 0).expect("replay");
    assert!(replay.truncated);
    assert_eq!(replay.bytes.len(), 32);
    assert_eq!(replay.end_offset - replay.available_offset, 32);

    service.terminate(terminal.id).expect("terminal removed");
}

#[test]
fn controller_is_exclusive_and_observer_receives_live_output() {
    let service = TerminalService::new(64 * 1024);
    let terminal = service
        .create(command("sleep 0.05; printf 'streamed\\n'; sleep 0.2"))
        .expect("terminal created");
    let controller = service
        .attach(
            terminal.id,
            0,
            "controller-a".to_string(),
            AttachmentRole::Controller,
            false,
        )
        .expect("controller attached");
    let observer = service
        .attach(
            terminal.id,
            0,
            "observer".to_string(),
            AttachmentRole::Observer,
            false,
        )
        .expect("observer attached");
    assert!(
        service
            .attach(
                terminal.id,
                0,
                "controller-b".to_string(),
                AttachmentRole::Controller,
                false,
            )
            .is_err()
    );
    assert!(
        service
            .write_for(
                terminal.id,
                Some("observer".to_string()),
                b"not allowed".to_vec(),
            )
            .is_err()
    );
    assert!(service.write(terminal.id, b"not allowed".to_vec()).is_err());
    assert!(service.resize(terminal.id, 90, 25).is_err());
    service
        .resize_for(terminal.id, Some("controller-a".to_string()), 90, 25)
        .expect("controller resizes");

    let saw_output = |events: &crossbeam_channel::Receiver<StreamEvent>| {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match events.recv_timeout(Duration::from_millis(50)) {
                Ok(StreamEvent::Output { bytes, .. })
                    if bytes.windows(8).any(|value| value == b"streamed") =>
                {
                    return true;
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
        false
    };
    assert!(saw_output(&controller.events));
    assert!(saw_output(&observer.events));

    let takeover = service
        .attach(
            terminal.id,
            0,
            "controller-b".to_string(),
            AttachmentRole::Controller,
            true,
        )
        .expect("controller takeover");
    assert!(takeover.generation > controller.generation);
    service.terminate(terminal.id).expect("terminal removed");
}
