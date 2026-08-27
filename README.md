# opencode-pty

Experimental threaded PTY service core for OpenCode.

It uses `portable-pty` to own terminal child processes and `libghostty-vt` to
maintain authoritative terminal state and answer terminal protocol queries.
Every terminal has isolated blocking I/O and actor threads, so one terminal
cannot block the service or another terminal.

Every daemon has one owner connection. The playground starts a daemon and holds
that connection until exit; exiting the playground stops the daemon and all its
terminals. Observer commands connect to an existing daemon without taking ownership.
The service uses a private authenticated Unix socket and atomic registration file.

Integrations launch `opencode-pty daemon` (protocol 7).
The server must claim the daemon within 5 seconds by sending the authenticated
framed envelope `{"token":"...","request":{"op":"own","instance_id":"..."}}`.
The response is `{"type":"owned"}`; that connection stays open as the sole
owner. Ordinary requests and subscriptions use their existing separate sockets.
The instance ID and token come from the private registration file.

Losing the owner connection stops the daemon and its terminals unless the owner
first sends `{"token":"...","request":{"op":"prepare_handoff"}}` on that same
socket. The response is `{"type":"handoff","ticket":"...","expires_at":123}`,
where `expires_at` is Unix milliseconds, 120 seconds from preparation. Repeated
preparation during that window returns the same ticket and deadline. After the
old owner disconnects, a successor claims the same instance with the ticket in
its `own` request. A live owner cannot be displaced, and successful adoption
consumes the ticket. Expiry stops an unowned daemon; if the old owner is still
connected, expiry simply cancels the handoff. `shutdown` always stops the daemon,
including during handoff. No ownership or handoff state is persisted.

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

`play` starts and owns a new daemon; it cannot adopt an already running daemon.
`list`, `status`, `watch`, and `stop` only connect to an existing service and never
start one. `quit` stops the playground's daemon and terminals.

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
