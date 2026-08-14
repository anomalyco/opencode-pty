# Threaded PTY Service Core

Status: threaded core and persistent service API implemented

## Objective

Prove that one Rust service can own multiple portable PTYs while each terminal
uses isolated blocking threads, authoritative libghostty parsing, bounded raw
replay, and explicit lifecycle state.

## Invariants

- One reader consumes each PTY output stream.
- One writer serializes every byte written to each PTY.
- libghostty state is created, accessed, and dropped on one actor thread.
- libghostty callbacks never perform blocking PTY writes or IPC.
- Actor and writer channels are bounded.
- Raw replay is bounded and uses absolute byte offsets.
- Replay truncation is explicit.
- PTY resize and parser resize occur in one ordered actor command.
- Every resize publishes an authoritative post-resize VT checkpoint before subsequent output.
- Child exit and PTY EOF are separate events.
- One terminal exiting does not stop the service or another terminal.
- Service shutdown attempts to terminate and join every terminal runtime.
- A terminal has at most one controller; observers cannot write or resize until interaction promotes them.
- Promotion demotes the previous controller without closing its subscription.
- Promotion, canonical resize, and input are one ordered actor command.
- Subscription replay and live events cross one ordered actor boundary.
- Controller delivery applies backpressure; observers tolerate bounded output bursts and are disconnected when their backlog remains full.
- Adjacent output is coalesced before fanout, flushing at 8 KiB, after 1 ms, or before the next non-output event.

## Data Flow

```text
child PTY output
  -> blocking reader thread
  -> bounded actor queue
  -> append bounded raw replay
  -> libghostty vt_write
  -> collect parser responses
  -> bounded writer queue
  -> blocking writer thread
  -> child PTY input
```

User input also enters through the actor and the same writer queue.

## Snapshot

A snapshot is built on the actor thread and contains owned values only:

- Terminal metadata and lifecycle
- Rows and columns
- Cursor position
- Plain parsed screen text
- VT checkpoint synthesized by libghostty formatter
- Raw replay head and tail offsets

No borrowed libghostty object crosses a thread boundary.

## Failure Boundary

The persistent `opencode-pty` process survives OpenCode server and client
restarts. If `opencode-pty` itself dies, its terminals may die. That is an
accepted boundary and this project does not implement live PTY handoff.

## Persistent Service Boundary

The CLI ensures one detached `opencode-pty` process through a private service
registration and authenticated Unix socket. The process owns the registry and
terminal threads. CLI exit does not affect terminal lifetime.

Protocol v4 uses four-byte big-endian framing with bounded UTF-8 JSON control
frames and tagged raw binary output frames. It supports ping, create, list,
write, resize, atomic interaction promotion, snapshot, replay, replay-to-live
subscriptions, controller takeover, terminate, and destructive service
shutdown.

Registration is written atomically with private permissions and contains an
instance ID, PID, protocol version, socket path, and random credential. A
service lock elects one process and protects stale socket cleanup.

OpenCode chooses a database-keyed runtime directory before spawning the daemon,
so servers using the same database reconnect to one PTY service while different
databases elect independent services. OpenCode starts the daemon only when the
first terminal is created.
