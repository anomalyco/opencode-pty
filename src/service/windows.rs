use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use portable_pty::{Child, MasterPty};
use windows_sys::Win32::{Foundation::WAIT_TIMEOUT, System::Threading::WaitForSingleObject};

use super::ActorMessage;

/// Keep blocking ClosePseudoConsole off the actor/reader without introducing
/// another worker. The actor hands its unique master here on root exit or on
/// teardown; closing it then produces the real output-pipe EOF.
/// Root-exit detection has at most 10 ms polling latency. After reaping, the
/// worker blocks on the close channel rather than continuing to poll.
pub(super) fn wait_and_close(
    mut child: Box<dyn Child + Send + Sync>,
    events: Sender<ActorMessage>,
    close: Receiver<Box<dyn MasterPty + Send>>,
) {
    let mut reported = false;
    loop {
        if !reported {
            // SAFETY: the child owns this process handle throughout this call;
            // cloned killer handles do not close or replace it. Poll the wait
            // handle rather than GetExitCodeProcess so exit code 259 is not
            // confused with STILL_ACTIVE by portable-pty's try_wait.
            let ready = child
                .as_raw_handle()
                .is_none_or(|handle| unsafe { WaitForSingleObject(handle, 0) != WAIT_TIMEOUT });
            if ready {
                report_exit(&mut *child, &events);
                reported = true;
            }
        }
        let request = if reported {
            close.recv().map_err(|_| RecvTimeoutError::Disconnected)
        } else {
            close.recv_timeout(Duration::from_millis(10))
        };
        match request {
            Ok(master) => {
                if !reported {
                    // Also cover actor failure/early teardown, not just normal
                    // exit or the service's already-issued termination request.
                    let _ = child.kill();
                }
                drop(master);
                break;
            }
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
    if !reported {
        report_exit(&mut *child, &events);
    }
}

fn report_exit(child: &mut dyn Child, events: &Sender<ActorMessage>) {
    let result = child.wait().map(|status| Some(status.exit_code()));
    let _ = events.send(ActorMessage::ChildExited(
        result.map_err(|error| error.to_string()),
    ));
}
