use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, IoSlice, IoSliceMut, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::cmsg_space;
use nix::fcntl::{FcntlArg, FdFlag, OFlag, fcntl};
use nix::pty::openpty;
use nix::sys::signal::{Signal, killpg};
use nix::sys::socket::{ControlMessage, ControlMessageOwned, MsgFlags, recvmsg, sendmsg};
use nix::unistd::Pid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const MAGIC: &[u8; 8] = b"PTYHND01";
const VERSION: u16 = 1;
const METADATA_LEN: usize = 24;
const ACK: &[u8] = b"ADOPTED\n";
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const RESTART_EXIT_CODE: i32 = 75;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Metadata {
    terminal_id: u64,
    child_pid: i32,
}

impl Metadata {
    fn encode(self) -> [u8; METADATA_LEN] {
        let mut bytes = [0_u8; METADATA_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
        bytes[12..20].copy_from_slice(&self.terminal_id.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.child_pid.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8; METADATA_LEN]) -> Result<Self> {
        if &bytes[..8] != MAGIC {
            return Err("invalid handoff magic".into());
        }
        if u16::from_be_bytes(bytes[8..10].try_into()?) != VERSION {
            return Err("unsupported handoff protocol version".into());
        }
        if bytes[10..12] != [0, 0] {
            return Err("nonzero reserved metadata bytes".into());
        }

        let terminal_id = u64::from_be_bytes(bytes[12..20].try_into()?);
        let child_pid = i32::from_be_bytes(bytes[20..24].try_into()?);
        if terminal_id == 0 || child_pid <= 0 {
            return Err("invalid terminal identifier or child PID".into());
        }
        Ok(Self {
            terminal_id,
            child_pid,
        })
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("demo") if args.next().is_none() => run_demo(),
        Some("fixture") if args.next().is_none() => run_fixture(),
        Some("receiver") => {
            if args.next().as_deref() != Some("--socket") {
                return Err("usage: pty-handoff-poc receiver --socket <path>".into());
            }
            let path = args.next().ok_or("missing receiver socket path")?;
            if args.next().is_some() {
                return Err("unexpected receiver argument".into());
            }
            if run_receiver(Path::new(&path))? == WorkerOutcome::Restart {
                std::process::exit(RESTART_EXIT_CODE);
            }
            Ok(())
        }
        _ => Err("usage: pty-handoff-poc <demo|receiver --socket PATH|fixture>".into()),
    }
}

fn run_fixture() -> Result<()> {
    let pid = std::process::id();
    println!("FIXTURE_READY pid={pid}");
    io::stdout().flush()?;

    for line in io::stdin().lock().lines() {
        let command = line?;
        println!("FIXTURE_RESPONSE pid={pid} command={command}");
        io::stdout().flush()?;
        if command == "exit" {
            break;
        }
    }
    Ok(())
}

fn run_demo() -> Result<()> {
    let socket_path = unique_socket_path()?;
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    let (mut fixture, master, metadata) = spawn_fixture()?;

    println!("master: PID = {}", std::process::id());
    println!("master: terminal ID = {:016x}", metadata.terminal_id);
    println!("master: terminal child PID = {}", metadata.child_pid);
    println!("master: type `exit` to replace the worker; Ctrl-D shuts everything down");
    io::stdout().flush()?;

    let result = supervise_workers(&listener, &socket_path, &master, metadata);
    terminate_fixture(metadata.child_pid);
    let _ = fixture.wait();
    remove_socket(&socket_path);
    result
}

fn supervise_workers(
    listener: &UnixListener,
    socket_path: &Path,
    master: &File,
    metadata: Metadata,
) -> Result<()> {
    let executable = env::current_exe()?;
    let mut generation = 1_u64;
    loop {
        let mut worker = Command::new(&executable)
            .arg("receiver")
            .arg("--socket")
            .arg(socket_path)
            .spawn()?;
        println!(
            "master: spawned worker generation {generation}, PID = {}",
            worker.id()
        );
        io::stdout().flush()?;

        let (mut control, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) => {
                let _ = worker.kill();
                let _ = worker.wait();
                return Err(error.into());
            }
        };
        // The supervisor keeps its descriptor idle. Only the active worker
        // reads from or writes to the PTY master.
        if let Err(error) = send_descriptor(&control, metadata, master) {
            let _ = worker.kill();
            let _ = worker.wait();
            return Err(error);
        }
        let mut ack = [0_u8; ACK.len()];
        if let Err(error) = control.read_exact(&mut ack) {
            let _ = worker.kill();
            let _ = worker.wait();
            return Err(error.into());
        }
        if ack != ACK {
            let _ = worker.kill();
            let _ = worker.wait();
            return Err("worker sent an invalid adoption acknowledgment".into());
        }
        drop(control);

        let status = worker.wait()?;
        if status.code() == Some(RESTART_EXIT_CODE) {
            println!("master: worker exited; terminal remains open, starting replacement");
            generation += 1;
            continue;
        }
        if status.success() {
            println!("master: stdin reached EOF; shutting down terminal");
            return Ok(());
        }
        return Err(format!("worker exited unexpectedly with {status}").into());
    }
}

fn spawn_fixture() -> Result<(Child, File, Metadata)> {
    let pty = openpty(None, None)?;
    let slave_in = pty.slave.try_clone()?;
    let slave_out = pty.slave.try_clone()?;
    let slave_err = pty.slave;
    let executable = env::current_exe()?;
    let mut command = Command::new(executable);
    command
        .arg("fixture")
        .stdin(Stdio::from(slave_in))
        .stdout(Stdio::from(slave_out))
        .stderr(Stdio::from(slave_err));

    // SAFETY: pre_exec runs in the single-threaded child immediately before
    // exec. setsid and TIOCSCTTY only use fixed descriptors and async-signal-safe
    // syscalls; fd 0 has already been connected to the PTY slave by Command.
    unsafe {
        command.pre_exec(|| {
            if nix::libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            if nix::libc::ioctl(0, nix::libc::TIOCSCTTY, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    let pid = i32::try_from(child.id())?;
    let terminal_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
        ^ u64::from(child.id());
    let master = File::from(pty.master);
    set_nonblocking(&master)?;
    Ok((
        child,
        master,
        Metadata {
            terminal_id,
            child_pid: pid,
        },
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerOutcome {
    Restart,
    Shutdown,
}

enum StdinEvent {
    Input(Vec<u8>),
    Restart,
    Shutdown,
    Error(String),
}

fn run_receiver(socket_path: &Path) -> Result<WorkerOutcome> {
    let mut control = UnixStream::connect(socket_path)?;
    let (metadata, descriptor) = receive_descriptor(&control)?;
    let mut master = File::from(descriptor);
    set_nonblocking(&master)?;
    control.write_all(ACK)?;
    control.flush()?;
    drop(control);

    println!(
        "worker: PID {} adopted terminal {:016x}, terminal child PID = {}",
        std::process::id(),
        metadata.terminal_id,
        metadata.child_pid
    );
    io::stdout().flush()?;
    proxy_terminal(&mut master)
}

fn proxy_terminal(master: &mut File) -> Result<WorkerOutcome> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || read_stdin_lines(sender));
    let mut stdout = io::stdout().lock();
    let mut output = [0_u8; 4096];

    loop {
        loop {
            match receiver.try_recv() {
                Ok(StdinEvent::Input(bytes)) => write_all_nonblocking(master, &bytes)?,
                Ok(StdinEvent::Restart) => {
                    writeln!(stdout, "worker: `exit` intercepted; closing worker only")?;
                    stdout.flush()?;
                    return Ok(WorkerOutcome::Restart);
                }
                Ok(StdinEvent::Shutdown) => return Ok(WorkerOutcome::Shutdown),
                Ok(StdinEvent::Error(error)) => return Err(error.into()),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(WorkerOutcome::Shutdown),
            }
        }

        match master.read(&mut output) {
            Ok(0) => return Err("PTY reached EOF".into()),
            Ok(count) => {
                stdout.write_all(&output[..count])?;
                stdout.flush()?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn read_stdin_lines(sender: mpsc::Sender<StdinEvent>) {
    let mut stdin = io::stdin().lock();
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match stdin.read(&mut byte) {
            Ok(0) => {
                if !line.is_empty() && sender.send(StdinEvent::Input(line)).is_err() {
                    return;
                }
                let _ = sender.send(StdinEvent::Shutdown);
                return;
            }
            Ok(_) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    let command = String::from_utf8_lossy(&line);
                    if command.trim_end_matches(['\r', '\n']) == "exit" {
                        let _ = sender.send(StdinEvent::Restart);
                        return;
                    }
                    if sender
                        .send(StdinEvent::Input(std::mem::take(&mut line)))
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                let _ = sender.send(StdinEvent::Error(error.to_string()));
                return;
            }
        }
    }
}

fn send_descriptor(
    stream: &UnixStream,
    metadata: Metadata,
    descriptor: &impl AsRawFd,
) -> Result<()> {
    let bytes = metadata.encode();
    let descriptors = [descriptor.as_raw_fd()];
    let ancillary = [ControlMessage::ScmRights(&descriptors)];
    let sent = loop {
        match sendmsg::<()>(
            stream.as_raw_fd(),
            &[IoSlice::new(&bytes)],
            &ancillary,
            MsgFlags::empty(),
            None,
        ) {
            Ok(count) => break count,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(error) => return Err(error.into()),
        }
    };
    if sent == 0 {
        return Err("descriptor transfer wrote no metadata".into());
    }
    if sent < bytes.len() {
        (&*stream).write_all(&bytes[sent..])?;
    }
    Ok(())
}

fn receive_descriptor(stream: &UnixStream) -> Result<(Metadata, OwnedFd)> {
    let mut bytes = [0_u8; METADATA_LEN];
    let mut iov = [IoSliceMut::new(&mut bytes)];
    let mut ancillary = cmsg_space!([RawFd; 4]);
    let message = loop {
        match recvmsg::<()>(
            stream.as_raw_fd(),
            &mut iov,
            Some(&mut ancillary),
            MsgFlags::empty(),
        ) {
            Ok(message) => break message,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(error) => return Err(error.into()),
        }
    };
    if message.flags.contains(MsgFlags::MSG_CTRUNC) {
        return Err("truncated descriptor control message".into());
    }
    let received = message.bytes;
    let mut descriptors = Vec::new();
    for control in message.cmsgs()? {
        if let ControlMessageOwned::ScmRights(raw_descriptors) = control {
            for raw in raw_descriptors {
                // SAFETY: each descriptor returned by SCM_RIGHTS is newly
                // installed in this process and ownership is transferred here.
                descriptors.push(unsafe { OwnedFd::from_raw_fd(raw) });
            }
        } else {
            return Err("unexpected ancillary message".into());
        }
    }
    if descriptors.len() != 1 {
        return Err(format!("expected one descriptor, received {}", descriptors.len()).into());
    }
    if received == 0 {
        return Err("descriptor transfer contained no metadata".into());
    }
    if received < bytes.len() {
        (&*stream).read_exact(&mut bytes[received..])?;
    }
    let metadata = Metadata::decode(&bytes)?;
    let descriptor = descriptors.pop().unwrap();
    fcntl(&descriptor, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
    Ok((metadata, descriptor))
}

fn set_nonblocking(file: &File) -> Result<()> {
    let current = OFlag::from_bits_truncate(fcntl(file, FcntlArg::F_GETFL)?);
    fcntl(file, FcntlArg::F_SETFL(current | OFlag::O_NONBLOCK))?;
    Ok(())
}

fn read_until(file: &mut File, expected: &str) -> Result<String> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut output = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => return Err("PTY reached EOF".into()),
            Ok(count) => {
                output.extend_from_slice(&buffer[..count]);
                if output.len() > 64 * 1024 {
                    return Err("PTY response exceeded 64 KiB".into());
                }
                let text = String::from_utf8_lossy(&output);
                if text.contains(expected) {
                    return Ok(text.into_owned());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(format!("timed out waiting for PTY output: {expected}").into());
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn write_all_nonblocking(file: &mut File, mut bytes: &[u8]) -> Result<()> {
    let deadline = Instant::now() + IO_TIMEOUT;
    while !bytes.is_empty() {
        match file.write(bytes) {
            Ok(0) => return Err("PTY write returned zero bytes".into()),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err("timed out writing PTY input".into());
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn matching_line<'a>(output: &'a str, expected: &str) -> Result<&'a str> {
    output
        .lines()
        .map(str::trim_end)
        .find(|line| line.contains(expected))
        .ok_or_else(|| format!("expected response not found: {expected}").into())
}

fn terminate_fixture(pid: i32) {
    match killpg(Pid::from_raw(pid), Signal::SIGTERM) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
        Err(error) => eprintln!("warning: failed to terminate fixture process group: {error}"),
    }
}

fn unique_socket_path() -> Result<PathBuf> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(env::temp_dir().join(format!("pty-handoff-{}-{nonce}.sock", std::process::id())))
}

fn remove_socket(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => eprintln!("warning: failed to remove {}: {error}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trip_and_validation() {
        let metadata = Metadata {
            terminal_id: 42,
            child_pid: 1234,
        };
        assert_eq!(Metadata::decode(&metadata.encode()).unwrap(), metadata);

        let mut malformed = metadata.encode();
        malformed[8..10].copy_from_slice(&2_u16.to_be_bytes());
        assert!(Metadata::decode(&malformed).is_err());
    }

    #[test]
    fn transfers_a_real_descriptor_with_close_on_exec() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        let (mut data_sender, data_receiver) = UnixStream::pair().unwrap();
        let metadata = Metadata {
            terminal_id: 7,
            child_pid: 99,
        };

        send_descriptor(&sender, metadata, &data_receiver).unwrap();
        let (actual_metadata, descriptor) = receive_descriptor(&receiver).unwrap();
        assert_eq!(actual_metadata, metadata);
        let flags = FdFlag::from_bits_truncate(fcntl(&descriptor, FcntlArg::F_GETFD).unwrap());
        assert!(flags.contains(FdFlag::FD_CLOEXEC));

        data_sender.write_all(b"ok").unwrap();
        let mut adopted = File::from(descriptor);
        let mut bytes = [0_u8; 2];
        adopted.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"ok");
    }
}
