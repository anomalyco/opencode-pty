# PTY Handoff POC

Rust experiment for transferring a live Unix PTY master from an old server
process to a replacement process with `SCM_RIGHTS` during graceful restart.

See `SPEC.md` for the experiment and `AGENTS.md` for implementation constraints.
