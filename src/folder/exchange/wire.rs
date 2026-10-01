//! What travels: pkt-lines ([`crate::pktline`]) carrying first a magic
//! line each way, then one JSON object per control packet, key lists as
//! text packets ending in a flush, and each file's bytes as data packets
//! ending in a flush.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

use super::{Code, Error};
use crate::pktline::{Packet, PktReader, PktWriter};

/// The client's first line: this, a space, then the versions it speaks.
pub(super) const CLIENT_MAGIC: &str = "bigstore-exchange-client";
/// The server's first line: this, a space, then its build.
pub(super) const SERVER_MAGIC: &str = "bigstore-exchange-server";

/// One control message.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Frame {
    /// Server: the version chosen.
    Version {
        version: u32,
    },
    /// Client: work on the store in directory `store`, whose histories are
    /// all under `history`; create the directory if `create`.
    Open {
        store: String,
        history: String,
        create: bool,
    },
    /// Server: the store is open; whether its directory already existed.
    Opened {
        existed: bool,
    },
    /// Client: list the store. Server: `listing`, then the keys, a flush.
    List {},
    Listing {},
    /// Client: then the keys wanted, a flush. Server: per key `file` and
    /// its bytes, then `done`; or an `error` that ends the command.
    Get {},
    /// Client: then per key `file` and its bytes, then `end`. Server:
    /// `stored`, or an `error`.
    Put {},
    File {
        key: String,
        size: u64,
    },
    End {},
    Done {},
    Stored {
        stored: usize,
        present: usize,
    },
    /// Client (version 2): scrub the store, reading every file if `deep`.
    /// Server: `scrubbed`, the damaged keys and a flush, then one
    /// `unreadable` per unreadable file; or an `error`.
    Scrub {
        deep: bool,
    },
    Scrubbed {
        checked: usize,
        damaged: usize,
        unreadable: usize,
    },
    Unreadable {
        key: String,
        reason: String,
    },
    /// Client (version 2): replace `key` with the bytes that follow (a
    /// body of `size` bytes). Server: `healed`, or an `error`.
    Heal {
        key: String,
        size: u64,
    },
    Healed {
        outcome: Outcome,
    },
    /// Client: the session is over.
    Close {},
    /// A refusal: what kind, and of which key. Never a message: what the far
    /// side has to say stays on the far side.
    Error {
        code: Code,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        /// For `version`: the versions the sender speaks.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        versions: Vec<u32>,
    },
}

/// What a heal did on the far side.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    /// The damaged file went to `quarantined`; the verified bytes replaced it.
    Replaced { quarantined: String },
    /// The name was absent: the verified bytes were placed.
    Placed {},
    /// The far copy read good: nothing was written.
    HealedByOther {},
}

impl Frame {
    pub(super) fn refusal(code: Code, key: Option<String>) -> Self {
        Self::Error {
            code,
            key,
            versions: Vec::new(),
        }
    }
}

pub(super) fn broken<E>(_: E) -> anyhow::Error {
    Error::SessionBroken.into()
}

/// Both directions of a session.
pub(super) struct Wire<R, W: Write> {
    pub(super) r: PktReader<R>,
    pub(super) w: PktWriter<W>,
}

impl<R: Read, W: Write> Wire<R, W> {
    pub(super) fn new(r: R, w: W) -> Self {
        Self {
            r: PktReader::new(r),
            w: PktWriter::new(w),
        }
    }

    /// The next packet as one line, its LF stripped; `None` at a clean EOF.
    /// Anything else (a flush, bytes that are not UTF-8) is not a line.
    pub(super) fn line(&mut self) -> Result<Option<String>> {
        match self.r.packet().map_err(broken)? {
            None => Ok(None),
            Some(Packet::Flush) => Err(broken(())),
            Some(Packet::Data(bytes)) => {
                let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
                Ok(Some(String::from_utf8(bytes.to_vec()).map_err(broken)?))
            }
        }
    }

    /// The next frame; `None` at a clean EOF between frames.
    pub(super) fn recv(&mut self) -> Result<Option<Frame>> {
        match self.r.packet().map_err(broken)? {
            None => Ok(None),
            Some(Packet::Flush) => Err(broken(())),
            Some(Packet::Data(bytes)) => Ok(Some(serde_json::from_slice(bytes).map_err(broken)?)),
        }
    }

    /// The next frame, which must come.
    pub(super) fn frame(&mut self) -> Result<Frame> {
        self.recv()?.ok_or_else(|| broken(()))
    }

    /// Queue `frame`; [`Self::flush`] sends what is queued.
    pub(super) fn send(&mut self, frame: &Frame) -> Result<()> {
        let json = serde_json::to_string(frame).expect("frames serialize");
        self.w.text(&json).map_err(broken)
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        self.w.send().map_err(broken)
    }

    /// Queue `keys` as a list.
    pub(super) fn send_keys<'k>(&mut self, keys: impl IntoIterator<Item = &'k str>) -> Result<()> {
        for key in keys {
            self.w.text(key).map_err(broken)?;
        }
        self.w.flush_pkt().map_err(broken)
    }

    /// A list of keys, up to its flush.
    pub(super) fn recv_keys(&mut self) -> Result<Vec<String>> {
        self.r
            .text_list()
            .map_err(broken)?
            .ok_or_else(|| broken(()))?
            .into_iter()
            .map(|key| String::from_utf8(key).map_err(broken))
            .collect()
    }
}
