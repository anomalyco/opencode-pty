use std::cell::RefCell;
#[cfg(target_os = "linux")]
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock, mpsc as std_mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use crossbeam_channel::{Receiver, Sender, bounded};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::{Terminal, TerminalOptions};
use portable_pty::{ChildKiller, CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};

use crate::protocol::AttachmentRole;

const DEFAULT_COLS: u16 = 100;
const DEFAULT_ROWS: u16 = 30;
const REPLAY_CAPACITY: usize = 2 * 1024 * 1024;
const ACTOR_QUEUE_CAPACITY: usize = 128;
const WRITER_QUEUE_CAPACITY: usize = 128;
const SUBSCRIBER_QUEUE_CAPACITY: usize = 1024;
const OUTPUT_BATCH_BYTES: usize = 8 * 1024;
const OUTPUT_BATCH_DELAY: Duration = Duration::from_millis(1);
const FOREGROUND_PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(1500);
const MAX_INPUT_BYTES: usize = 1024 * 1024;

pub type TerminalId = u64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum TerminalLifecycle {
    Running,
    Exited { exit_code: Option<u32> },
    Failed { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalInfo {
    pub id: TerminalId,
    pub pid: Option<u32>,
    pub title: String,
    pub foreground_process: Option<String>,
    pub group_id: String,
    pub command: Vec<String>,
    pub cwd: PathBuf,
    pub cols: u16,
    pub rows: u16,
    pub lifecycle: TerminalLifecycle,
    pub output_head: u64,
    pub output_tail: u64,
}

#[derive(Clone, Debug)]
pub struct TerminalSnapshot {
    pub info: TerminalInfo,
    pub text: String,
    pub checkpoint: Vec<u8>,
    pub cursor_x: u16,
    pub cursor_y: u16,
}

#[derive(Clone, Debug)]
pub struct RawReplay {
    pub requested_offset: u64,
    pub available_offset: u64,
    pub end_offset: u64,
    pub truncated: bool,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub enum StreamEvent {
    Output {
        start: u64,
        end: u64,
        bytes: Vec<u8>,
    },
    Resized {
        cols: u16,
        rows: u16,
        generation: u64,
        checkpoint: Vec<u8>,
    },
    Exited {
        exit_code: Option<u32>,
        final_offset: u64,
    },
    ControllerChanged {
        attachment_id: Option<String>,
        generation: u64,
    },
    TitleChanged {
        title: String,
    },
    ForegroundProcessChanged {
        process: Option<String>,
    },
}

pub struct TerminalAttachment {
    pub terminal: TerminalInfo,
    pub role: AttachmentRole,
    pub generation: u64,
    pub replay: RawReplay,
    pub events: Receiver<StreamEvent>,
    actor_tx: Sender<ActorMessage>,
    attachment_id: String,
}

impl Drop for TerminalAttachment {
    fn drop(&mut self) {
        let _ = self.actor_tx.send(ActorMessage::Detach {
            attachment_id: self.attachment_id.clone(),
        });
    }
}

#[derive(Clone, Debug)]
pub struct CreateTerminal {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub title: String,
    pub group_id: String,
    pub env: HashMap<String, String>,
    pub cols: u16,
    pub rows: u16,
}

impl CreateTerminal {
    pub fn shell() -> Result<Self> {
        let program = default_shell();
        let title = PathBuf::from(&program)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("shell")
            .to_string();
        Ok(Self {
            program,
            args: Vec::new(),
            cwd: env::current_dir()?,
            title,
            group_id: format!("local:{}", env::current_dir()?.display()),
            env: HashMap::new(),
            cols: DEFAULT_COLS,
            rows: DEFAULT_ROWS,
        })
    }
}

pub struct TerminalService {
    terminals: Mutex<HashMap<TerminalId, Arc<TerminalHandle>>>,
    replay_capacity: usize,
}

impl Default for TerminalService {
    fn default() -> Self {
        Self::new(REPLAY_CAPACITY)
    }
}

impl TerminalService {
    pub fn new(replay_capacity: usize) -> Self {
        Self {
            terminals: Mutex::new(HashMap::new()),
            replay_capacity,
        }
    }

    pub fn create(&self, request: CreateTerminal) -> Result<TerminalInfo> {
        validate_size(request.cols, request.rows)?;
        if request.program.trim().is_empty() {
            bail!("terminal program cannot be empty");
        }

        let id = loop {
            // JSON clients can represent integers exactly only through 53 bits.
            let id = rand::random::<u64>() & ((1_u64 << 53) - 1);
            if id != 0
                && !self
                    .terminals
                    .lock()
                    .map_err(|_| anyhow!("terminal registry lock poisoned"))?
                    .contains_key(&id)
            {
                break id;
            }
        };
        let handle = TerminalHandle::spawn(id, request, self.replay_capacity)?;
        let info = handle.info();
        self.terminals
            .lock()
            .map_err(|_| anyhow!("terminal registry lock poisoned"))?
            .insert(id, Arc::new(handle));
        Ok(info)
    }

    pub fn list(&self) -> Result<Vec<TerminalInfo>> {
        let mut terminals = self
            .terminals
            .lock()
            .map_err(|_| anyhow!("terminal registry lock poisoned"))?
            .values()
            .map(|terminal| terminal.info())
            .collect::<Vec<_>>();
        terminals.sort_by_key(|terminal| terminal.id);
        Ok(terminals)
    }

    pub fn write(&self, id: TerminalId, bytes: Vec<u8>) -> Result<()> {
        self.write_for(id, None, bytes)
    }

    pub fn write_for(
        &self,
        id: TerminalId,
        attachment_id: Option<String>,
        bytes: Vec<u8>,
    ) -> Result<()> {
        if bytes.len() > MAX_INPUT_BYTES {
            bail!("input exceeds {MAX_INPUT_BYTES} byte limit");
        }
        self.get(id)?.request(|reply| ActorMessage::Write {
            attachment_id,
            bytes,
            reply,
        })
    }

    pub fn resize(&self, id: TerminalId, cols: u16, rows: u16) -> Result<()> {
        self.resize_for(id, None, cols, rows)
    }

    pub fn resize_for(
        &self,
        id: TerminalId,
        attachment_id: Option<String>,
        cols: u16,
        rows: u16,
    ) -> Result<()> {
        validate_size(cols, rows)?;
        self.get(id)?.request(|reply| ActorMessage::Resize {
            attachment_id,
            cols,
            rows,
            reply,
        })
    }

    pub fn control(
        &self,
        id: TerminalId,
        attachment_id: String,
        cols: u16,
        rows: u16,
    ) -> Result<()> {
        validate_size(cols, rows)?;
        self.get(id)?.request(|reply| ActorMessage::Control {
            attachment_id,
            cols,
            rows,
            bytes: None,
            reply,
        })
    }

    pub fn input(
        &self,
        id: TerminalId,
        attachment_id: String,
        cols: u16,
        rows: u16,
        bytes: Vec<u8>,
    ) -> Result<()> {
        validate_size(cols, rows)?;
        if bytes.len() > MAX_INPUT_BYTES {
            bail!("input exceeds {MAX_INPUT_BYTES} byte limit");
        }
        self.get(id)?.request(|reply| ActorMessage::Control {
            attachment_id,
            cols,
            rows,
            bytes: Some(bytes),
            reply,
        })
    }

    pub fn snapshot(&self, id: TerminalId) -> Result<TerminalSnapshot> {
        self.get(id)?
            .request(|reply| ActorMessage::Snapshot { reply })
    }

    pub fn replay(&self, id: TerminalId, offset: u64) -> Result<RawReplay> {
        self.get(id)?
            .request(|reply| ActorMessage::Replay { offset, reply })
    }

    pub fn attach(
        &self,
        id: TerminalId,
        offset: u64,
        attachment_id: String,
        role: AttachmentRole,
        takeover: bool,
    ) -> Result<TerminalAttachment> {
        let terminal = self.get(id)?;
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        terminal
            .actor_tx
            .send(ActorMessage::Attach {
                offset,
                attachment_id: attachment_id.clone(),
                role,
                takeover,
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("terminal actor stopped"))?;
        let attached = reply_rx
            .recv()
            .context("terminal actor dropped attach response")??;
        Ok(TerminalAttachment {
            terminal: terminal.info(),
            role,
            generation: attached.generation,
            replay: attached.replay,
            events: attached.events,
            actor_tx: terminal.actor_tx.clone(),
            attachment_id,
        })
    }

    pub fn terminate(&self, id: TerminalId) -> Result<()> {
        let terminal = self
            .terminals
            .lock()
            .map_err(|_| anyhow!("terminal registry lock poisoned"))?
            .remove(&id)
            .ok_or_else(|| anyhow!("terminal {id} not found"))?;
        terminal.shutdown()
    }

    fn get(&self, id: TerminalId) -> Result<Arc<TerminalHandle>> {
        self.terminals
            .lock()
            .map_err(|_| anyhow!("terminal registry lock poisoned"))?
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("terminal {id} not found"))
    }
}

impl Drop for TerminalService {
    fn drop(&mut self) {
        let terminals = self
            .terminals
            .get_mut()
            .map(|terminals| {
                terminals
                    .drain()
                    .map(|(_, terminal)| terminal)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for terminal in terminals {
            let _ = terminal.shutdown();
        }
    }
}

struct TerminalHandle {
    actor_tx: Sender<ActorMessage>,
    killer: Mutex<Box<dyn ChildKiller + Send + Sync>>,
    info: Arc<RwLock<TerminalInfo>>,
    joins: Mutex<Option<Vec<JoinHandle<()>>>>,
}

impl TerminalHandle {
    fn spawn(id: TerminalId, request: CreateTerminal, replay_capacity: usize) -> Result<Self> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: request.rows,
                cols: request.cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to open PTY")?;

        let mut command = CommandBuilder::new(&request.program);
        command.args(request.args.iter());
        command.cwd(&request.cwd);
        for (key, value) in &request.env {
            command.env(key, value);
        }
        command.env("TERM", "xterm-256color");
        command.env("COLORTERM", "truecolor");

        let mut child = pair
            .slave
            .spawn_command(command)
            .with_context(|| format!("failed to spawn {}", request.program))?;
        let pid = child.process_id();
        let killer = child.clone_killer();
        let mut reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone PTY reader")?;
        let writer = pair
            .master
            .take_writer()
            .context("failed to take PTY writer")?;
        drop(pair.slave);

        let info = Arc::new(RwLock::new(TerminalInfo {
            id,
            pid,
            title: request.title,
            foreground_process: None,
            group_id: request.group_id,
            command: std::iter::once(request.program)
                .chain(request.args)
                .collect(),
            cwd: request.cwd,
            cols: request.cols,
            rows: request.rows,
            lifecycle: TerminalLifecycle::Running,
            output_head: 0,
            output_tail: 0,
        }));

        let (actor_tx, actor_rx) = bounded(ACTOR_QUEUE_CAPACITY);
        let (writer_tx, writer_rx) = bounded(WRITER_QUEUE_CAPACITY);
        let (init_tx, init_rx) = std_mpsc::sync_channel(1);

        let actor_info = Arc::clone(&info);
        let actor_writer_tx = writer_tx.clone();
        let actor_thread = thread::Builder::new()
            .name(format!("opencode-pty-actor-{id}"))
            .spawn(move || {
                let result = run_actor(ActorConfig {
                    master: pair.master,
                    messages: actor_rx,
                    writes: actor_writer_tx,
                    info: actor_info,
                    replay_capacity,
                    cols: request.cols,
                    rows: request.rows,
                    init_tx,
                });
                if let Err(error) = result {
                    eprintln!("terminal {id} actor failed: {error:#}");
                }
            })
            .context("failed to spawn terminal actor")?;

        match init_rx
            .recv()
            .context("terminal actor stopped during initialization")?
        {
            Ok(()) => {}
            Err(error) => {
                let mut killer = killer;
                let _ = killer.kill();
                let _ = actor_thread.join();
                bail!(error);
            }
        }

        let writer_actor_tx = actor_tx.clone();
        let writer_thread = thread::Builder::new()
            .name(format!("opencode-pty-writer-{id}"))
            .spawn(move || run_writer(writer, writer_rx, writer_actor_tx))
            .context("failed to spawn terminal writer")?;

        let reader_actor_tx = actor_tx.clone();
        let reader_thread = thread::Builder::new()
            .name(format!("opencode-pty-reader-{id}"))
            .spawn(move || {
                let mut buffer = [0_u8; 8192];
                loop {
                    match reader.read(&mut buffer) {
                        Ok(0) => {
                            let _ = reader_actor_tx.send(ActorMessage::ReaderEof);
                            break;
                        }
                        Ok(length) => {
                            if reader_actor_tx
                                .send(ActorMessage::Output(buffer[..length].to_vec()))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        #[cfg(unix)]
                        Err(error) if error.raw_os_error() == Some(libc::EIO) => {
                            let _ = reader_actor_tx.send(ActorMessage::ReaderEof);
                            break;
                        }
                        Err(error) => {
                            let _ =
                                reader_actor_tx.send(ActorMessage::ReaderFailed(error.to_string()));
                            break;
                        }
                    }
                }
            })
            .context("failed to spawn terminal reader")?;

        let wait_actor_tx = actor_tx.clone();
        let wait_thread = thread::Builder::new()
            .name(format!("opencode-pty-wait-{id}"))
            .spawn(move || {
                let result = child.wait().map(|status| Some(status.exit_code()));
                let _ = wait_actor_tx.send(ActorMessage::ChildExited(
                    result.map_err(|error| error.to_string()),
                ));
            })
            .context("failed to spawn terminal child waiter")?;

        Ok(Self {
            actor_tx,
            killer: Mutex::new(killer),
            info,
            joins: Mutex::new(Some(vec![
                actor_thread,
                writer_thread,
                reader_thread,
                wait_thread,
            ])),
        })
    }

    fn info(&self) -> TerminalInfo {
        self.info
            .read()
            .map(|info| info.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    fn request<T>(
        &self,
        make: impl FnOnce(std_mpsc::SyncSender<Result<T>>) -> ActorMessage,
    ) -> Result<T> {
        let (reply_tx, reply_rx) = std_mpsc::sync_channel(1);
        self.actor_tx
            .send(make(reply_tx))
            .map_err(|_| anyhow!("terminal actor stopped"))?;
        reply_rx.recv().context("terminal actor dropped response")?
    }

    fn shutdown(&self) -> Result<()> {
        if let Ok(mut killer) = self.killer.lock() {
            let _ = killer.kill();
        }
        let _ = self.actor_tx.send(ActorMessage::Shutdown);
        let joins = self
            .joins
            .lock()
            .map_err(|_| anyhow!("terminal join lock poisoned"))?
            .take()
            .unwrap_or_default();
        for join in joins {
            let _ = join.join();
        }
        Ok(())
    }
}

enum ActorMessage {
    Output(Vec<u8>),
    ReaderEof,
    ReaderFailed(String),
    WriterFailed(String),
    ChildExited(Result<Option<u32>, String>),
    Write {
        attachment_id: Option<String>,
        bytes: Vec<u8>,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    Resize {
        attachment_id: Option<String>,
        cols: u16,
        rows: u16,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    Control {
        attachment_id: String,
        cols: u16,
        rows: u16,
        bytes: Option<Vec<u8>>,
        reply: std_mpsc::SyncSender<Result<()>>,
    },
    Snapshot {
        reply: std_mpsc::SyncSender<Result<TerminalSnapshot>>,
    },
    Replay {
        offset: u64,
        reply: std_mpsc::SyncSender<Result<RawReplay>>,
    },
    Attach {
        offset: u64,
        attachment_id: String,
        role: AttachmentRole,
        takeover: bool,
        reply: std_mpsc::SyncSender<Result<Attached>>,
    },
    Detach {
        attachment_id: String,
    },
    Shutdown,
}

struct Attached {
    generation: u64,
    replay: RawReplay,
    events: Receiver<StreamEvent>,
}

struct Subscriber {
    events: Sender<StreamEvent>,
    role: AttachmentRole,
    last_control: u64,
}

enum WriterMessage {
    Bytes(Vec<u8>),
}

struct ActorConfig {
    master: Box<dyn MasterPty + Send>,
    messages: Receiver<ActorMessage>,
    writes: Sender<WriterMessage>,
    info: Arc<RwLock<TerminalInfo>>,
    replay_capacity: usize,
    cols: u16,
    rows: u16,
    init_tx: std_mpsc::SyncSender<std::result::Result<(), String>>,
}

fn run_actor(config: ActorConfig) -> Result<()> {
    let ActorConfig {
        master,
        messages,
        writes,
        info,
        replay_capacity,
        cols,
        rows,
        init_tx,
    } = config;
    let responses = Rc::new(RefCell::new(VecDeque::<Vec<u8>>::new()));
    let title = Rc::new(RefCell::new(None::<String>));
    let mut terminal = Terminal::new(TerminalOptions {
        cols,
        rows,
        max_scrollback: replay_capacity,
    })?;
    terminal.on_pty_write({
        let responses = Rc::clone(&responses);
        move |_terminal, data| responses.borrow_mut().push_back(data.to_vec())
    })?;
    terminal.on_title_changed({
        let title = Rc::clone(&title);
        move |terminal| {
            if let Ok(value) = terminal.title() {
                *title.borrow_mut() = Some(value.to_owned());
            }
        }
    })?;

    let mut replay = ReplayBuffer::new(replay_capacity);
    let mut subscribers = HashMap::<String, Subscriber>::new();
    let mut controller = None::<String>;
    let mut controller_generation = 0_u64;
    let mut child_exit = None::<std::result::Result<Option<u32>, String>>;
    let mut reader_eof = false;
    let mut exit_published = false;
    let mut pending_message = None;
    let mut next_foreground_process_poll = Instant::now();
    let _ = init_tx.send(Ok(()));

    loop {
        if Instant::now() >= next_foreground_process_poll {
            publish_foreground_process(
                &*master,
                &info,
                &mut subscribers,
                &mut controller,
                &mut controller_generation,
            );
            next_foreground_process_poll = Instant::now() + FOREGROUND_PROCESS_POLL_INTERVAL;
        }
        match receive_actor_message(
            &messages,
            &mut pending_message,
            next_foreground_process_poll.saturating_duration_since(Instant::now()),
        ) {
            Ok(ActorMessage::Output(bytes)) => {
                let (start, end) = replay.append(&bytes);
                terminal.vt_write(&bytes);
                while let Some(response) = responses.borrow_mut().pop_front() {
                    writes
                        .send(WriterMessage::Bytes(response))
                        .map_err(|_| anyhow!("terminal writer stopped"))?;
                }
                update_offsets(&info, &replay);
                broadcast(
                    &mut subscribers,
                    &mut controller,
                    &mut controller_generation,
                    StreamEvent::Output { start, end, bytes },
                );
                if let Some(title) = title.borrow_mut().take() {
                    let changed = if let Ok(mut value) = info.write() {
                        if value.title == title {
                            false
                        } else {
                            value.title = title.clone();
                            true
                        }
                    } else {
                        false
                    };
                    if changed {
                        broadcast(
                            &mut subscribers,
                            &mut controller,
                            &mut controller_generation,
                            StreamEvent::TitleChanged { title },
                        );
                    }
                }
            }
            Ok(ActorMessage::Write {
                attachment_id,
                bytes,
                reply,
            }) => {
                let result =
                    authorize_controller(&controller, attachment_id.as_deref()).and_then(|_| {
                        writes
                            .send(WriterMessage::Bytes(bytes))
                            .map_err(|_| anyhow!("terminal writer stopped"))
                    });
                let _ = reply.send(result);
            }
            Ok(ActorMessage::Resize {
                attachment_id,
                cols,
                rows,
                reply,
            }) => {
                let result = (|| {
                    authorize_controller(&controller, attachment_id.as_deref())?;
                    master.resize(PtySize {
                        rows,
                        cols,
                        pixel_width: 0,
                        pixel_height: 0,
                    })?;
                    terminal.resize(cols, rows, 0, 0)?;
                    if let Ok(mut value) = info.write() {
                        value.cols = cols;
                        value.rows = rows;
                    }
                    let generation = controller_generation;
                    let checkpoint = format_terminal(&terminal, Format::Vt)?.into_bytes();
                    broadcast(
                        &mut subscribers,
                        &mut controller,
                        &mut controller_generation,
                        StreamEvent::Resized {
                            cols,
                            rows,
                            generation,
                            checkpoint,
                        },
                    );
                    Ok(())
                })();
                let _ = reply.send(result);
            }
            Ok(ActorMessage::Control {
                attachment_id,
                cols,
                rows,
                bytes,
                reply,
            }) => {
                let result = (|| {
                    if !subscribers.contains_key(&attachment_id) {
                        bail!("attachment {attachment_id} is not subscribed");
                    }
                    if controller.as_ref() != Some(&attachment_id) {
                        if let Some(current) = controller.as_ref()
                            && let Some(subscriber) = subscribers.get_mut(current)
                        {
                            subscriber.role = AttachmentRole::Observer;
                        }
                        controller_generation = controller_generation.saturating_add(1);
                        controller = Some(attachment_id.clone());
                        if let Some(subscriber) = subscribers.get_mut(&attachment_id) {
                            subscriber.role = AttachmentRole::Controller;
                            subscriber.last_control = controller_generation;
                        }
                        let generation = controller_generation;
                        broadcast(
                            &mut subscribers,
                            &mut controller,
                            &mut controller_generation,
                            StreamEvent::ControllerChanged {
                                attachment_id: Some(attachment_id.clone()),
                                generation,
                            },
                        );
                    }
                    let changed = info
                        .read()
                        .map(|value| value.cols != cols || value.rows != rows)
                        .unwrap_or(true);
                    if changed {
                        master.resize(PtySize {
                            rows,
                            cols,
                            pixel_width: 0,
                            pixel_height: 0,
                        })?;
                        terminal.resize(cols, rows, 0, 0)?;
                        if let Ok(mut value) = info.write() {
                            value.cols = cols;
                            value.rows = rows;
                        }
                        let generation = controller_generation;
                        let checkpoint = format_terminal(&terminal, Format::Vt)?.into_bytes();
                        broadcast(
                            &mut subscribers,
                            &mut controller,
                            &mut controller_generation,
                            StreamEvent::Resized {
                                cols,
                                rows,
                                generation,
                                checkpoint,
                            },
                        );
                    }
                    if let Some(bytes) = bytes {
                        writes
                            .send(WriterMessage::Bytes(bytes))
                            .map_err(|_| anyhow!("terminal writer stopped"))?;
                    }
                    Ok(())
                })();
                let _ = reply.send(result);
            }
            Ok(ActorMessage::Snapshot { reply }) => {
                let _ = reply.send(make_snapshot(&terminal, &info));
            }
            Ok(ActorMessage::Replay { offset, reply }) => {
                let _ = reply.send(replay.read_from(offset));
            }
            Ok(ActorMessage::Attach {
                offset,
                attachment_id,
                role,
                takeover,
                reply,
            }) => {
                let result = (|| {
                    if role == AttachmentRole::Controller {
                        if let Some(current) = controller.as_ref() {
                            if current != &attachment_id && !takeover {
                                bail!("terminal already has controller {current}");
                            }
                            if current != &attachment_id
                                && let Some(subscriber) = subscribers.get_mut(current)
                            {
                                subscriber.role = AttachmentRole::Observer;
                            }
                        }
                        if controller.as_ref() != Some(&attachment_id) {
                            controller_generation = controller_generation.saturating_add(1);
                            controller = Some(attachment_id.clone());
                        }
                    }
                    let replay = replay.read_from(offset)?;
                    let (events_tx, events) = bounded(SUBSCRIBER_QUEUE_CAPACITY);
                    subscribers.insert(
                        attachment_id.clone(),
                        Subscriber {
                            events: events_tx,
                            role,
                            last_control: if role == AttachmentRole::Controller {
                                controller_generation
                            } else {
                                0
                            },
                        },
                    );
                    if role == AttachmentRole::Controller {
                        let controller_id = controller.clone();
                        let generation = controller_generation;
                        broadcast(
                            &mut subscribers,
                            &mut controller,
                            &mut controller_generation,
                            StreamEvent::ControllerChanged {
                                attachment_id: controller_id,
                                generation,
                            },
                        );
                    }
                    Ok(Attached {
                        generation: controller_generation,
                        replay,
                        events,
                    })
                })();
                let _ = reply.send(result);
            }
            Ok(ActorMessage::Detach { attachment_id }) => {
                let removed = subscribers.remove(&attachment_id);
                if removed.is_some() && controller.as_ref() == Some(&attachment_id) {
                    controller_generation = controller_generation.saturating_add(1);
                    controller = promote_latest(&mut subscribers, controller_generation);
                    let generation = controller_generation;
                    let attachment_id = controller.clone();
                    broadcast(
                        &mut subscribers,
                        &mut controller,
                        &mut controller_generation,
                        StreamEvent::ControllerChanged {
                            attachment_id,
                            generation,
                        },
                    );
                }
            }
            Ok(ActorMessage::ChildExited(result)) => {
                child_exit = Some(result);
            }
            Ok(ActorMessage::ReaderFailed(message)) | Ok(ActorMessage::WriterFailed(message)) => {
                if let Ok(mut value) = info.write() {
                    value.lifecycle = TerminalLifecycle::Failed { message };
                }
            }
            Ok(ActorMessage::ReaderEof) => reader_eof = true,
            Ok(ActorMessage::Shutdown) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                break;
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
        }
        if reader_eof
            && !exit_published
            && let Some(result) = child_exit.take()
        {
            let exit_code = result.as_ref().ok().copied().flatten();
            if let Ok(mut value) = info.write() {
                value.lifecycle = match result {
                    Ok(exit_code) => TerminalLifecycle::Exited { exit_code },
                    Err(message) => TerminalLifecycle::Failed { message },
                };
            }
            broadcast(
                &mut subscribers,
                &mut controller,
                &mut controller_generation,
                StreamEvent::Exited {
                    exit_code,
                    final_offset: replay.tail,
                },
            );
            exit_published = true;
        }
    }
    drop(terminal);
    drop(master);
    drop(writes);
    Ok(())
}

fn receive_actor_message(
    messages: &Receiver<ActorMessage>,
    pending: &mut Option<ActorMessage>,
    timeout: Duration,
) -> std::result::Result<ActorMessage, crossbeam_channel::RecvTimeoutError> {
    let message = match pending.take() {
        Some(message) => message,
        None => messages.recv_timeout(timeout)?,
    };
    let ActorMessage::Output(mut bytes) = message else {
        return Ok(message);
    };
    let deadline = Instant::now() + OUTPUT_BATCH_DELAY;
    while bytes.len() < OUTPUT_BATCH_BYTES {
        match messages.recv_deadline(deadline) {
            Ok(ActorMessage::Output(next)) => bytes.extend(next),
            Ok(message) => {
                *pending = Some(message);
                break;
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
            | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(ActorMessage::Output(bytes))
}

fn publish_foreground_process(
    master: &dyn MasterPty,
    info: &Arc<RwLock<TerminalInfo>>,
    subscribers: &mut HashMap<String, Subscriber>,
    controller: &mut Option<String>,
    controller_generation: &mut u64,
) {
    let process = foreground_process(master);
    let changed = if let Ok(mut value) = info.write() {
        if value.foreground_process == process {
            false
        } else {
            value.foreground_process = process.clone();
            true
        }
    } else {
        false
    };
    if changed {
        broadcast(
            subscribers,
            controller,
            controller_generation,
            StreamEvent::ForegroundProcessChanged { process },
        );
    }
}

#[cfg(target_os = "linux")]
fn foreground_process(master: &dyn MasterPty) -> Option<String> {
    let foreground_group = master.process_group_leader()?;
    let tty = master.tty_name();
    let processes = std::fs::read_dir("/proc")
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
        .filter_map(read_process)
        .collect::<Vec<_>>();
    let parents = processes
        .iter()
        .map(|process| (process.pid, process.ppid))
        .collect::<HashMap<_, _>>();
    let mut group = processes
        .iter()
        .filter(|process| process.pgrp == foreground_group)
        .collect::<Vec<_>>();
    if let Some(tty) = tty {
        let attached = group
            .iter()
            .copied()
            .filter(|process| process_attached_to_tty(process.pid, &tty))
            .collect::<Vec<_>>();
        if !attached.is_empty() {
            group = attached;
        }
    }
    group
        .into_iter()
        .max_by_key(|process| (process_depth(process.pid, &parents), process.pid))
        .map(|process| process.name.clone())
}

#[cfg(not(target_os = "linux"))]
fn foreground_process(_master: &dyn MasterPty) -> Option<String> {
    None
}

#[cfg(target_os = "linux")]
struct ProcessInfo {
    pid: i32,
    ppid: i32,
    pgrp: i32,
    name: String,
}

#[cfg(target_os = "linux")]
fn read_process(pid: i32) -> Option<ProcessInfo> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .get(stat.rfind(") ")? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(ProcessInfo {
        pid,
        ppid: fields.get(1)?.parse().ok()?,
        pgrp: fields.get(2)?.parse().ok()?,
        name: std::fs::read_to_string(format!("/proc/{pid}/comm"))
            .ok()?
            .trim()
            .to_string(),
    })
}

#[cfg(target_os = "linux")]
fn process_attached_to_tty(pid: i32, tty: &std::path::Path) -> bool {
    [0, 1, 2]
        .into_iter()
        .any(|fd| std::fs::read_link(format!("/proc/{pid}/fd/{fd}")).is_ok_and(|path| path == tty))
}

#[cfg(target_os = "linux")]
fn process_depth(pid: i32, parents: &HashMap<i32, i32>) -> usize {
    let mut current = pid;
    let mut seen = HashSet::new();
    while current > 0 && seen.insert(current) {
        let Some(parent) = parents.get(&current) else {
            break;
        };
        current = *parent;
    }
    seen.len()
}

fn run_writer(
    mut writer: Box<dyn Write + Send>,
    writer_rx: Receiver<WriterMessage>,
    actor_tx: Sender<ActorMessage>,
) {
    for message in writer_rx {
        let result = match message {
            WriterMessage::Bytes(bytes) => writer.write_all(&bytes).and_then(|_| writer.flush()),
        };
        if let Err(error) = result {
            let _ = actor_tx.send(ActorMessage::WriterFailed(error.to_string()));
            break;
        }
    }
}

fn make_snapshot(
    terminal: &Terminal<'_, '_>,
    info: &Arc<RwLock<TerminalInfo>>,
) -> Result<TerminalSnapshot> {
    let text = format_terminal(terminal, Format::Plain)?;
    let checkpoint = format_terminal(terminal, Format::Vt)?.into_bytes();
    let info = info
        .read()
        .map_err(|_| anyhow!("terminal info lock poisoned"))?
        .clone();
    Ok(TerminalSnapshot {
        info,
        text,
        checkpoint,
        cursor_x: terminal.cursor_x()?,
        cursor_y: terminal.cursor_y()?,
    })
}

fn format_terminal(terminal: &Terminal<'_, '_>, format: Format) -> Result<String> {
    let options = FormatterOptions::new()
        .with_format(format)
        .with_unwrap(false)
        .with_trim(true)
        .with_modes(true)
        .with_scrolling_region(true)
        .with_tabstops(true)
        .with_pwd(true)
        .with_keyboard(true)
        .with_cursor(true)
        .with_style(true)
        .with_hyperlink(true)
        .with_protection(true)
        .with_kitty_keyboard(true)
        .with_charsets(true);
    let mut formatter = Formatter::new(terminal, options)?;
    let bytes = formatter.format_alloc(None)?;
    Ok(String::from_utf8_lossy(bytes.as_ref()).into_owned())
}

fn update_offsets(info: &Arc<RwLock<TerminalInfo>>, replay: &ReplayBuffer) {
    if let Ok(mut info) = info.write() {
        info.output_head = replay.head;
        info.output_tail = replay.tail;
    }
}

struct ReplayBuffer {
    bytes: VecDeque<u8>,
    capacity: usize,
    head: u64,
    tail: u64,
}

impl ReplayBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: VecDeque::with_capacity(capacity.min(64 * 1024)),
            capacity,
            head: 0,
            tail: 0,
        }
    }

    fn append(&mut self, bytes: &[u8]) -> (u64, u64) {
        let start = self.tail;
        self.tail = self.tail.saturating_add(bytes.len() as u64);
        if self.capacity == 0 {
            self.bytes.clear();
            self.head = self.tail;
            return (start, self.tail);
        }
        self.bytes.extend(bytes);
        let excess = self.bytes.len().saturating_sub(self.capacity);
        self.bytes.drain(..excess);
        self.head = self.tail.saturating_sub(self.bytes.len() as u64);
        (start, self.tail)
    }

    fn read_from(&self, requested_offset: u64) -> Result<RawReplay> {
        if requested_offset > self.tail {
            bail!(
                "requested offset {requested_offset} is beyond tail {}",
                self.tail
            );
        }
        let available_offset = requested_offset.max(self.head);
        let skip = (available_offset - self.head) as usize;
        Ok(RawReplay {
            requested_offset,
            available_offset,
            end_offset: self.tail,
            truncated: requested_offset < self.head,
            bytes: self.bytes.iter().skip(skip).copied().collect(),
        })
    }
}

fn broadcast(
    subscribers: &mut HashMap<String, Subscriber>,
    controller: &mut Option<String>,
    controller_generation: &mut u64,
    event: StreamEvent,
) {
    let dropped = subscribers
        .iter()
        .filter_map(|(id, subscriber)| {
            let failed = match subscriber.role {
                AttachmentRole::Controller => subscriber.events.send(event.clone()).is_err(),
                AttachmentRole::Observer => subscriber.events.try_send(event.clone()).is_err(),
            };
            failed.then(|| id.clone())
        })
        .collect::<Vec<_>>();
    for id in dropped {
        subscribers.remove(&id);
        if controller.as_ref() == Some(&id) {
            *controller_generation = controller_generation.saturating_add(1);
            *controller = promote_latest(subscribers, *controller_generation);
            let generation = *controller_generation;
            let attachment_id = controller.clone();
            broadcast(
                subscribers,
                controller,
                controller_generation,
                StreamEvent::ControllerChanged {
                    attachment_id,
                    generation,
                },
            );
        }
    }
}

fn promote_latest(
    subscribers: &mut HashMap<String, Subscriber>,
    generation: u64,
) -> Option<String> {
    let attachment_id = subscribers
        .iter()
        .max_by_key(|(_, subscriber)| subscriber.last_control)
        .map(|(attachment_id, _)| attachment_id.clone())?;
    if let Some(subscriber) = subscribers.get_mut(&attachment_id) {
        subscriber.role = AttachmentRole::Controller;
        subscriber.last_control = generation;
    }
    Some(attachment_id)
}

fn authorize_controller(controller: &Option<String>, attachment_id: Option<&str>) -> Result<()> {
    let Some(attachment_id) = attachment_id else {
        if controller.is_none() {
            return Ok(());
        }
        bail!("terminal input requires the active controller attachment");
    };
    if controller.as_deref() != Some(attachment_id) {
        bail!("attachment {attachment_id} is not the terminal controller");
    }
    Ok(())
}

fn validate_size(cols: u16, rows: u16) -> Result<()> {
    if cols == 0 || rows == 0 {
        bail!("terminal dimensions must be positive");
    }
    Ok(())
}

fn default_shell() -> String {
    env::var("SHELL").unwrap_or_else(|_| {
        if cfg!(windows) {
            "cmd.exe".to_string()
        } else {
            "/bin/sh".to_string()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_reports_truncation_and_offsets() {
        let mut replay = ReplayBuffer::new(5);
        replay.append(b"abc");
        replay.append(b"defg");
        assert_eq!(replay.head, 2);
        assert_eq!(replay.tail, 7);

        let value = replay.read_from(0).unwrap();
        assert!(value.truncated);
        assert_eq!(value.available_offset, 2);
        assert_eq!(value.bytes, b"cdefg");

        let value = replay.read_from(5).unwrap();
        assert!(!value.truncated);
        assert_eq!(value.bytes, b"fg");
    }

    #[test]
    fn controller_backpressures_instead_of_disconnecting() {
        let (events_tx, events) = bounded(1);
        events_tx
            .send(StreamEvent::Output {
                start: 0,
                end: 1,
                bytes: b"a".to_vec(),
            })
            .unwrap();
        let mut subscribers = HashMap::from([(
            "controller".to_string(),
            Subscriber {
                events: events_tx,
                role: AttachmentRole::Controller,
                last_control: 1,
            },
        )]);
        let broadcast = std::thread::spawn(move || {
            let mut controller = Some("controller".to_string());
            let mut generation = 1;
            broadcast(
                &mut subscribers,
                &mut controller,
                &mut generation,
                StreamEvent::Output {
                    start: 1,
                    end: 2,
                    bytes: b"b".to_vec(),
                },
            );
            subscribers.contains_key("controller")
        });

        assert!(matches!(
            events.recv().unwrap(),
            StreamEvent::Output { start: 0, .. }
        ));
        assert!(matches!(
            events.recv().unwrap(),
            StreamEvent::Output { start: 1, .. }
        ));
        assert!(broadcast.join().unwrap());
    }

    #[test]
    fn slow_observer_is_disconnected() {
        let (events_tx, _events) = bounded(1);
        events_tx
            .send(StreamEvent::Output {
                start: 0,
                end: 1,
                bytes: b"a".to_vec(),
            })
            .unwrap();
        let mut subscribers = HashMap::from([(
            "observer".to_string(),
            Subscriber {
                events: events_tx,
                role: AttachmentRole::Observer,
                last_control: 0,
            },
        )]);

        broadcast(
            &mut subscribers,
            &mut None,
            &mut 0,
            StreamEvent::Output {
                start: 1,
                end: 2,
                bytes: b"b".to_vec(),
            },
        );

        assert!(!subscribers.contains_key("observer"));
    }

    #[test]
    fn output_is_batched_without_reordering_other_messages() {
        let (messages_tx, messages) = bounded(4);
        messages_tx
            .send(ActorMessage::Output(b"abc".to_vec()))
            .unwrap();
        messages_tx
            .send(ActorMessage::Output(b"def".to_vec()))
            .unwrap();
        messages_tx.send(ActorMessage::Shutdown).unwrap();
        let mut pending = None;

        assert!(matches!(
            receive_actor_message(&messages, &mut pending, Duration::from_secs(1)).unwrap(),
            ActorMessage::Output(bytes) if bytes == b"abcdef"
        ));
        assert!(matches!(
            receive_actor_message(&messages, &mut pending, Duration::from_secs(1)).unwrap(),
            ActorMessage::Shutdown
        ));
    }
}
