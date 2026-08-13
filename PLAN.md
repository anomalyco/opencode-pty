# Implementation Plan

## Outcome

Produce one command that demonstrates a live PTY child continuing across a
coordinated old-server to new-server handoff:

```sh
cargo run -- demo
```

The output must identify the same child PID before and after handoff and show a
successful request/response through the PTY on both sides.

## Phase 1: Unix Primitives

- Select a small, maintained Unix crate (`nix` or `rustix`) after checking its
  Linux and macOS support for PTYs, ancillary messages, and descriptor flags.
- Add only dependencies required by the POC.
- Implement a narrow descriptor-transfer module around Unix-domain sockets,
  `sendmsg`, `recvmsg`, and `SCM_RIGHTS`.
- Send a versioned metadata message with exactly one descriptor.
- Reject missing descriptors, extra descriptors, truncated control messages,
  unsupported versions, and malformed metadata.
- Set the received descriptor's close-on-exec behavior explicitly.
- Add a focused transfer test using a pipe or socket descriptor before adding
  PTY lifecycle complexity.

## Phase 2: Deterministic PTY Fixture

- Add a `fixture` mode implemented by the same binary.
- Allocate a PTY master/slave pair without depending on the invoking terminal.
- Spawn the fixture attached to the slave for stdin, stdout, and stderr.
- Establish the expected session/controlling-terminal state on Linux and macOS.
- Make the fixture emit its PID and answer line-oriented commands with
  deterministic output.
- Avoid user shells, shell startup files, prompts, and timing-dependent output.
- Prove ordinary pre-handoff input and output through the master.

## Phase 3: Receiver Process

- Add a `receiver --socket <path>` mode.
- Bind a unique, permission-restricted Unix socket.
- Signal readiness without sleeps or polling races.
- Receive and validate terminal metadata and exactly one PTY master descriptor.
- Adopt the descriptor without reading until ownership transfer reaches the
  correct state.
- Send an explicit adoption acknowledgment.
- After handoff, send a command through the PTY and verify the fixture response
  contains the original child PID.
- Cleanly terminate the fixture process group and remove the socket.

## Phase 4: Graceful Handoff Orchestrator

- Add a `demo` mode representing the old server.
- Start the fixture PTY and complete the pre-handoff exchange.
- Start the receiver and wait for readiness.
- Stop old-server PTY reads and writes at one clear ownership boundary.
- Transfer descriptor and metadata while retaining the old descriptor.
- Wait for receiver acknowledgment.
- Close the old descriptor only after acknowledgment.
- Exit the old-server path without sending `SIGHUP` through a last-master close.
- Have the receiver print the final success result and perform cleanup.

The intended state machine is:

```text
OldOwns
  -> ReceiverReady
  -> DescriptorDuplicated
  -> ReceiverAdopted
  -> OldReleased
  -> ReceiverOwns
```

Any failure before `ReceiverAdopted` must leave the old process responsible for
the PTY and child cleanup.

## Phase 5: Failure Coverage

- Receiver fails before connecting.
- Receiver disconnects before descriptor transfer.
- Receiver rejects metadata.
- Receiver receives the descriptor but fails before acknowledgment.
- Old server observes acknowledgment failure and retains ownership.
- Control socket path already exists.
- PTY child exits before handoff.
- PTY reaches EOF during either verification exchange.

Keep failure tests deterministic. Do not add a general retry framework.

## Phase 6: Portability Review

- Keep Linux/macOS differences behind small `cfg` sections.
- Confirm PTY allocation and controlling-terminal behavior on Linux.
- Keep macOS APIs compilable and document whether runtime verification was
  available.
- Do not introduce Linux-only `pidfd` into the portable success path.
- Use process groups for cleanup carefully, with explicit ownership checks.
- Document that receiving a PTY descriptor does not make the receiver the
  fixture's parent and does not transfer portable `waitpid` rights.

## Phase 7: Documentation And Verification

- Expand `README.md` with architecture, usage, sample output, and limitations.
- Explain why sending a numeric file descriptor or retaining only a PID cannot
  work.
- Explain why detached process survival is not equivalent to PTY survival.
- Explain why the mechanism handles graceful replacement but not old-server
  crashes before transfer.
- Record the selected crate and any unsafe/native boundaries.
- Run:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run -- demo
```

## Definition Of Done

- One deterministic command proves pre- and post-handoff PTY I/O.
- The child PID is unchanged across handoff.
- Old and replacement processes never read the PTY concurrently.
- The old descriptor remains open until receiver acknowledgment.
- Failed handoff paths clean up without leaked fixture processes.
- Successful execution leaves no child process or socket file behind.
- Code compiles for Linux and macOS targets supported by the selected crates.
- Documentation clearly limits the result to graceful handoff.
- Repository passes formatting, clippy, and tests without warnings.

## Explicit Non-Goals

- Production terminal service
- OpenCode integration
- Persistent terminal registry
- Multiple terminals
- Multiple attached clients
- Output replay protocol
- WebSocket handoff
- Authentication
- Crash recovery
- Machine-reboot survival
- Windows support
- Live migration between machines
