# Graceful PTY Descriptor Handoff

Status: proof of concept

## Question

Can a replacement server continue operating an interactive terminal whose PTY
master was created by an older server process, while the terminal child remains
alive throughout a coordinated restart?

## Hypothesis

On Linux and macOS, the old server can send its open PTY master descriptor to a
replacement over a Unix-domain socket using `SCM_RIGHTS`. The kernel installs a
new descriptor for the same open PTY master in the replacement. If the old
server does not close its descriptor before the replacement adopts and
acknowledges it, the slave side never observes the last master reference
closing, so the child can continue to run.

```text
Before:

  old server -- PTY master <==> PTY slave -- fixture child

During handoff:

  old server -- PTY master
                  |
             SCM_RIGHTS
                  |
  new server -- PTY master <==> PTY slave -- fixture child

After acknowledgment:

  new server -- PTY master <==> PTY slave -- fixture child
```

## Scope

The POC transfers one PTY during a graceful, cooperative restart. It proves
continuity of the kernel PTY connection and bidirectional I/O.

It does not attempt to preserve:

- WebSocket or application clients
- JavaScript or server heap state
- A terminal emulator framebuffer
- Output history beyond a tiny POC buffer
- Child-parent ownership or portable exit-status reaping
- Terminals after an old-server crash before transfer
- Terminals after machine reboot
- Windows ConPTY handles

## Success Criteria

- The same fixture-child PID is reported before and after handoff.
- The old server receives a response to input sent before handoff.
- The replacement receives a response to input sent after handoff.
- There is no interval where every PTY master descriptor is closed.
- The old server closes its descriptor only after receiver acknowledgment.
- A failed handoff leaves the old server able to clean up the child.
- The demonstration exits without leaked processes or socket files.

## Handoff Protocol

1. Old server creates the PTY and fixture child.
2. Old server starts receiver with a unique Unix socket path.
3. Receiver binds/listens and reports readiness.
4. Old server pauses PTY reads and client writes.
5. Old server sends protocol metadata plus exactly one descriptor.
6. Receiver validates metadata and descriptor count.
7. Receiver configures and registers the PTY descriptor.
8. Receiver sends an acknowledgment.
9. Old server closes its copy and exits.
10. Receiver resumes I/O and completes the post-handoff check.

Output produced while neither side reads remains in the kernel PTY queue. Both
servers must never read concurrently because duplicated descriptors share that
queue.

## Platform Notes

Linux and macOS both support Unix-domain sockets and `SCM_RIGHTS` descriptor
passing. Process monitoring differs: Linux has `pidfd`; macOS does not. This POC
does not solve transferable process-parent ownership and should not expand into
that problem unless required to make cleanup deterministic.

## Decision After The POC

If this succeeds, compare descriptor handoff with a long-lived terminal broker.
The POC should provide evidence about implementation difficulty, handoff races,
native API requirements, and cross-platform behavior. It should not be treated
as a decision to use handoff in production.
