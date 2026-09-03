use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const REGISTRATION_FILE: &str = "service.json";
pub const LOCK_FILE: &str = "service.lock";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registration {
    pub instance_id: String,
    pub pid: u32,
    pub protocol: u32,
    /// Opaque local transport endpoint, not necessarily a filesystem entry.
    pub socket: PathBuf,
    pub token: String,
}

#[cfg(unix)]
#[path = "daemon/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "daemon/windows.rs"]
mod platform;
#[cfg(any(unix, windows))]
mod server;

#[cfg(any(unix, windows))]
pub use platform::{read_registration, registration_path, service_dir};
#[cfg(any(unix, windows))]
pub use server::run;

/// Minimal Windows byte-stream client for integrations using protocol framing.
/// This does not start a daemon or implement the interactive TerminalClient CLI.
#[cfg(windows)]
pub use crate::transport::windows::Connection as PipeConnection;

#[cfg(not(any(unix, windows)))]
pub fn run() -> anyhow::Result<()> {
    anyhow::bail!("persistent opencode-pty transport is not implemented on this platform")
}

#[cfg(not(any(unix, windows)))]
pub fn read_registration() -> anyhow::Result<Registration> {
    anyhow::bail!("persistent opencode-pty transport is not implemented on this platform")
}

#[cfg(not(any(unix, windows)))]
pub fn registration_path() -> PathBuf {
    PathBuf::from("opencode-pty-service.json")
}
