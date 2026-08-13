use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::service::{TerminalId, TerminalInfo};

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentRole {
    Controller,
    Observer,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub token: String,
    pub request: Request,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Request {
    Ping,
    Create {
        program: String,
        args: Vec<String>,
        cwd: PathBuf,
        title: String,
        group_id: String,
        env: std::collections::HashMap<String, String>,
        cols: u16,
        rows: u16,
    },
    List,
    Write {
        id: TerminalId,
        attachment_id: Option<String>,
        data_base64: String,
    },
    Resize {
        id: TerminalId,
        attachment_id: Option<String>,
        cols: u16,
        rows: u16,
    },
    Snapshot {
        id: TerminalId,
    },
    Replay {
        id: TerminalId,
        offset: u64,
    },
    Subscribe {
        id: TerminalId,
        offset: u64,
        attachment_id: String,
        role: AttachmentRole,
        takeover: bool,
    },
    Terminate {
        id: TerminalId,
    },
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Response {
    Pong {
        instance_id: String,
        pid: u32,
        protocol: u32,
    },
    Created {
        terminal: TerminalInfo,
    },
    Terminals {
        terminals: Vec<TerminalInfo>,
    },
    Ok,
    Snapshot {
        terminal: TerminalInfo,
        text: String,
        checkpoint_base64: String,
        cursor_x: u16,
        cursor_y: u16,
    },
    Replay {
        requested_offset: u64,
        available_offset: u64,
        end_offset: u64,
        truncated: bool,
        data_base64: String,
    },
    Attached {
        terminal: TerminalInfo,
        role: AttachmentRole,
        generation: u64,
        requested_offset: u64,
        available_offset: u64,
        end_offset: u64,
        truncated: bool,
        replay_base64: String,
    },
    Output {
        start: u64,
        end: u64,
        data_base64: String,
    },
    Resized {
        cols: u16,
        rows: u16,
        generation: u64,
    },
    Exited {
        exit_code: Option<u32>,
        final_offset: u64,
    },
    ControllerChanged {
        attachment_id: Option<String>,
        generation: u64,
    },
    Error {
        message: String,
    },
}

pub fn write_frame(mut writer: impl Write, value: &impl Serialize) -> Result<()> {
    let payload = serde_json::to_vec(value)?;
    if payload.len() > MAX_FRAME_BYTES {
        bail!("frame exceeds {MAX_FRAME_BYTES} byte limit");
    }
    let length = u32::try_from(payload.len()).context("frame length exceeds u32")?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_frame<T: for<'de> Deserialize<'de>>(mut reader: impl Read) -> Result<T> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        bail!("frame claims {length} bytes, limit is {MAX_FRAME_BYTES}");
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload)?;
    serde_json::from_slice(&payload).context("invalid protocol JSON")
}
