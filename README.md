# opencode-pty

Experimental threaded PTY service core for OpenCode.

It uses `portable-pty` to own terminal child processes and `libghostty-vt` to
maintain authoritative terminal state and answer terminal protocol queries.
Every terminal has isolated blocking I/O and actor threads, so one terminal
cannot block the service or another terminal.

The CLI automatically discovers or starts a persistent background process. The
service uses a private authenticated Unix socket and atomic registration file;
terminals survive individual CLI processes exiting.

## Architecture

```text
opencode-pty process
├── atomic private service registration
├── authenticated framed IPC
├── terminal registry
├── terminal 1
│   ├── reader thread
│   ├── actor/libghostty thread
│   ├── writer thread
│   └── child-wait thread
└── terminal 2
    ├── reader thread
    ├── actor/libghostty thread
    ├── writer thread
    └── child-wait thread
```

The actor owns:

- `portable-pty` master control and resize
- `libghostty-vt::Terminal`
- Bounded raw replay and absolute output offsets
- Parsed screen, cursor, modes, title, and scrollback
- Terminal lifecycle and snapshot requests

The libghostty parser is authoritative for terminal-generated responses. Its
`on_pty_write` responses are sent through the same serialized writer path as
user input without blocking inside the callback.

## Playground

```sh
cargo run -- play
```

Other service commands:

```sh
cargo run -- status
cargo run -- list
cargo run -- stop  # destructive: terminates every terminal
```

`play` and `list` ensure the service is running. `status` and `stop` only connect
to an existing service.

Commands:

```text
new [PROGRAM ARGS...]  create a terminal (default: your shell)
list                  list terminals and lifecycle state
use ID                choose the active terminal
run COMMAND           send a shell command and show parsed screen state
send TEXT             send bytes without Enter
screen [ID]           inspect authoritative libghostty state
replay [ID] [OFFSET]  inspect bounded raw replay safely
resize COLS ROWS      resize PTY and parser together
wait [MILLISECONDS]   wait and show active screen
kill [ID]             terminate and remove a terminal
demo                   prove terminal query responses end to end
help | quit
```

Try:

```text
demo
new
run printf '\033[32mgreen from the shell\033[0m\n'
new /bin/sh
list
use 1
screen
replay 1 0
resize 72 20
kill 2
quit
```

## Reading Terminal Rows

`TerminalService::read_rows(id, rows)` and `TerminalClient::read_rows(id, rows)`
return the last physical rows of the active terminal buffer, including its
retained scrollback. `rows: Option<u16>` defaults to the live terminal height
when omitted/`None`; zero is invalid. Counts larger than the available rows
return all available rows. Soft-wrapped rows stay separate, trailing whitespace
is trimmed, and blank rows (including trailing blank rows) are empty strings.
The alternate screen never exposes primary-screen history.

The protocol 6 additive request is `{"op":"read_rows","id":1,"rows":30}`;
omitted or `null` `rows` uses the current height. Its response is
`{"type":"rows","terminal":{...},"lines":[...],"cursor_x":0,"cursor_y":0}`.
Metadata, lines, and zero-based active-screen cursor coordinates come from one
actor snapshot. Reading does not move the viewport, change selection, or take
control. Existing snapshots, checkpoints, and replay are unchanged.

Row data is bounded to a 1 MiB budget counting JSON-escaped strings, array
punctuation, and owned-string slot overhead. An excessive result is an error,
not silently truncated. The existing 8 MiB transport-frame limit still applies
to the complete response including metadata. Clients using this operation need
a binary with `read_rows` support; older protocol 6 binaries do not support it.

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
printf 'demo\nlist\nquit\n' | cargo run -- play
```

## Releases

Pushing a `vX.Y.Z` tag matching the version in `Cargo.toml` creates an unsigned
GitHub release. The release contains x86-64 and arm64 binaries for Linux GNU,
Linux musl, and macOS, plus `SHA256SUMS`, a machine-readable
`release-manifest.json`, and GitHub build-provenance attestations. Release
builds force Ghostty's Zig code generation to its baseline CPU target so
artifacts do not inherit instruction-set extensions from CI runners. Linux GNU
artifacts support glibc 2.30 and newer.

Tagged releases also publish `@opencode-ai/pty` to npm with optional,
platform-specific binary packages. Installing the npm package selects the native
binary for the current platform and exposes its path as `binaryPath`:

```js
import { binaryPath } from "@opencode-ai/pty"
```

```sh
git tag v0.1.0
push origin v0.1.0
```

Windows artifacts are intentionally excluded until the persistent transport
uses named pipes. Platform signing will be added later.

## Current Limits

- Persistent transport currently uses Unix sockets; Windows named pipes are not
  implemented yet.
- Ordinary API operations use one framed JSON request per connection;
  subscriptions keep the authenticated connection open for ordered live events.
- The OpenCode backend proxy and ordered group APIs are implemented, but the
  OpenTUI client integration is not.
- Windows uses portable-pty's published ConPTY backend and is not hardened yet.
- Child process-tree cleanup is not complete beyond portable-pty's root killer.
- Cold-client checkpoint transport is not implemented.
- A service-process crash loses all terminals by design.
