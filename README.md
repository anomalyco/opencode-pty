# PTY Handoff POC

A minimal Rust proof of concept for transferring a live Unix PTY master from an
old server process to a replacement process with `SCM_RIGHTS` during a
coordinated graceful restart. This is an experiment, not production-ready
terminal infrastructure.

## Architecture

```text
                         Unix socket control channel
old server (demo)  --------------------------------------> replacement
      |                    metadata + SCM_RIGHTS                |
      |                                                         |
      +-- PTY master <==== kernel PTY ==== PTY slave -- fixture |
                                ^                                |
                                +------- adopted master ---------+
```

The binary has three modes:

- `demo` is the old server and orchestration entry point.
- `receiver --socket PATH` is the replacement server.
- `fixture` is a deterministic line-oriented child attached to the PTY slave.

The old server creates the PTY and fixture, verifies input and output, and
starts the receiver. The receiver binds a mode-`0600` Unix socket and reports
readiness. The old side then sends protocol version, terminal identifier, child
PID evidence, and exactly one PTY master descriptor in an `SCM_RIGHTS` control
message. The receiver validates the message, explicitly enables close-on-exec,
and acknowledges adoption before the old side closes its descriptor.

## Handoff State Machine

```text
OldOwns
  -> ReceiverReady
  -> DescriptorDuplicated   (both hold a reference; only old has done I/O)
  -> ReceiverAdopted        (receiver sends ADOPTED)
  -> OldReleased            (old closes its descriptor)
  -> ReceiverOwns           (receiver performs post-handoff I/O)
```

The old side performs no PTY I/O after descriptor transfer starts. The receiver
does no PTY I/O before adoption. On any failure before acknowledgment, the old
side retains its descriptor and remains responsible for terminating and reaping
the fixture.

## Run It

Linux or macOS and stable Rust are required.

```sh
cargo run -- demo
```

Representative output:

```text
old: child PID before handoff = 12345
old: fixture startup = FIXTURE_READY pid=12345
old: pre-handoff response = FIXTURE_RESPONSE pid=12345 command=before
old: replacement is ready
old: PTY descriptor duplicated into replacement
old: replacement acknowledged adoption
old: released PTY master after acknowledgment
new: adopted terminal ID = 18...
new: child PID after handoff = 12345
new: post-handoff response = FIXTURE_RESPONSE pid=12345 command=after
demo: SUCCESS - PTY I/O continued across descriptor handoff
```

Run all checks with:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run -- demo
```

## What This Proves

- `SCM_RIGHTS` can install a descriptor for the same open PTY master in another
  process; serializing the numeric descriptor value would not do this.
- Keeping the old reference open through acknowledgment avoids a last-master
  close during a coordinated handoff.
- The same fixture responds through the PTY before and after handoff. Its PID is
  visible evidence, while the successful exchange through the transferred PTY
  is the continuity proof; identity and liveness are not inferred from PID
  alone.
- Old and replacement processes can coordinate so exactly one consumes the PTY
  stream at a time.

## What This Does Not Prove

- This does not preserve application clients, terminal-emulator state, output
  history, server heap state, or terminal sessions across reboot.
- It does not provide authentication, persistence, multiplexing, reconnection,
  crash recovery, or cross-machine migration.
- A detached child surviving is not enough: the PTY master must also remain
  open. If the old server crashes before transferring the descriptor, the
  descriptor disappears with it and no coordinated protocol can recover it.
  Crash tolerance requires a separate long-lived broker or equivalent owner.
- This does not transfer parenthood. The receiver can signal the fixture's
  process group but cannot portably `waitpid` a child created by the old server.
  For deterministic demonstration cleanup, the old `demo` process remains
  alive after releasing the PTY, waits for the receiver to terminate the
  fixture, and then reaps its child. A real replacement architecture must make
  an explicit reaping/orphan policy.

## Portability And Native Boundaries

Linux and macOS both provide Unix-domain `SCM_RIGHTS`, PTYs, `setsid`, and
`TIOCSCTTY`. The implementation uses the portable `nix` wrappers for PTY,
descriptor, socket, and signal operations and does not rely on Linux `pidfd`.
The two narrow unsafe boundaries are the pre-exec `setsid`/`TIOCSCTTY` setup and
constructing an owned descriptor from a descriptor newly returned by
`SCM_RIGHTS`; their safety invariants are documented in the source.

Runtime verification was performed on Linux. The code is written against APIs
available on macOS, but macOS runtime behavior was not tested in this workspace.

See `SPEC.md` for the experiment and `PLAN.md` for the implementation sequence.
