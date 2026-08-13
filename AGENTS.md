# PTY Handoff POC

## Objective

Build a minimal Rust proof of concept showing that a live Unix PTY master can
move from an old server process to a replacement server process during a
coordinated graceful restart without terminating the child process or losing
interactive terminal I/O.

The POC must run on Linux and macOS.

## Required Demonstration

1. The old server allocates a PTY and starts a deterministic child attached to
   its slave side.
2. The old server proves bidirectional terminal I/O works.
3. It starts a replacement server process and establishes a Unix-domain socket
   control channel.
4. It stops consuming PTY input, then sends the PTY master descriptor to the
   replacement with `sendmsg` and `SCM_RIGHTS`.
5. The replacement receives the descriptor with `recvmsg`, adopts it, and
   acknowledges readiness.
6. Only after acknowledgment, the old server closes its descriptor and exits.
7. The replacement proves that the same child PID is alive and that
   bidirectional PTY I/O still works.
8. The program exits cleanly and does not leave child processes or sockets
   behind.

The success output must clearly show the child PID before and after handoff and
must include child responses produced on both sides of the handoff.

## Constraints

- Use stable Rust unless a concrete platform API requires a small, documented
  unsafe boundary.
- Use Unix descriptor passing, not descriptor-number serialization.
- Keep unsafe code narrow and explain each safety invariant.
- Handle partial reads, partial writes, `EINTR`, EOF, and descriptor cleanup.
- Ensure the receiving descriptor has the intended close-on-exec behavior.
- Never infer identity or liveness from PID alone. PID is evidence in the demo,
  not the PTY identity.
- Ensure exactly one process reads the PTY master during handoff.
- Keep the old descriptor open until the replacement acknowledges adoption.
- If replacement startup or adoption fails, the old process must retain
  ownership and clean up safely.
- Prefer a deterministic fixture child over relying on an interactive user
  shell configuration.
- Do not use tmux, zellij, systemd, launchd, containers, or a persistent broker.
- Do not implement Windows support.
- Do not build an OpenCode integration or UI.
- Do not add persistence, authentication, terminal multiplexing, client
  reconnection, or crash recovery.

## Suggested Shape

One binary with explicit modes is preferred:

```text
pty-handoff-poc demo
pty-handoff-poc receiver --socket <path>
pty-handoff-poc fixture
```

The exact internal module structure is up to the implementation. Keep it small.
Do not extract abstractions that the POC does not need.

The descriptor-transfer message should include a tiny metadata payload, such
as a protocol version and terminal identifier, alongside one PTY master file
descriptor. Reject malformed messages and unexpected descriptor counts.

## Verification

Run all of the following:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run -- demo
```

Add an automated integration test where practical. If macOS is unavailable,
keep the implementation portable and document that runtime verification was
performed only on Linux.

## Documentation

Update `README.md` with:

- A short architecture diagram.
- The handoff state machine.
- How to run the demonstration.
- What the POC proves.
- What it does not prove.
- Linux and macOS differences.
- Why this cannot preserve PTYs after an uncoordinated crash.
- The unresolved child-parent/reaping issue after the old server exits.

Do not claim production readiness.
