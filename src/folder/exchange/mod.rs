//! Exchanging store files with a far store over one byte stream: a program
//! run by `ssh` (git's model), or any pipe. The far end runs [`serve`] on
//! its stdin and stdout; this end runs a [`Client`] over that program.
//!
//! The server is dumb: it lists its store, sends the files asked for, and
//! places the files sent. Working out what each side lacks is the caller's
//! (two listings and their difference: names are content, so equal names
//! are equal files). Every file received, on either side, is checked
//! against its name ([`layout::verify`]'s rules) as it arrives, written to
//! `<name>#<random>` beside its final name, and only then placed, without
//! replacing anything: a name already there counts as present. So a cut
//! session leaves at most a `#` temp file, which no listing shows, and
//! never a partial file under a store name.
//!
//! Transfers go in [`Kind`] order, both ways: every object, then every
//! manifest, then every record. A receiver places nothing after the first
//! file it refuses. So a session cut anywhere never leaves a manifest
//! without its objects, nor a record without its content.
//!
//! Version 2 adds [`Client::scrub`], the far side's
//! [`integrity::scrub`](super::integrity::scrub) of its own store, and
//! [`Client::heal`], which sends verified bytes for one key: the far side
//! re-reads its own copy, leaves it if it is good, and otherwise
//! quarantines it and replaces it atomically
//! ([`integrity::replace`](super::integrity::replace)).
//!
//! The protocol has its own versions ([`VERSIONS`]), not the crate's: the
//! client offers every version it speaks and the server picks the highest
//! both do, before anything else. No common version fails on both sides
//! before any store is touched. A version 1 session (a far side built
//! before 0.6) works as it always did, without scrub and heal
//! ([`Client::version`] says which). Errors carry codes and keys, never
//! what the far side wrote elsewhere (its stderr is the caller's to keep or
//! drop); a far scrub's report carries each unreadable file's reason.
//!
//! The server can hold a guard for as long as a store is open
//! ([`ServeOptions::with_open_guard`], say a lock on the store): one that
//! cannot be had refuses the `open` as [`Code::Busy`] ([`Error::Busy`] on
//! the client), before the store is read or written.
//!
//! # The protocol (versions 1 and 2)
//!
//! pkt-lines ([`crate::pktline`]). Control messages are one JSON object per
//! packet; key lists are text packets ending in a flush; a file's bytes
//! are data packets ending in a flush.
//!
//! ```text
//! C: bigstore-exchange-client 2 1              versions offered
//! S: bigstore-exchange-server <build>
//! S: {"version":{"version":2}}                 | {"error":{"code":"version","versions":[..]}}
//! C: {"open":{"store":"D:/…","history":"<h>","create":true}}
//! S: {"opened":{"existed":true}}               | error
//! C: {"list":{}}
//! S: {"listing":{}} <key>… flush               | error
//! C: {"get":{}} <key>… flush
//! S: per key {"file":{"key","size"}} <bytes> flush, then {"done":{}}   | error (ends the get)
//! C: {"put":{}} per key {"file":{"key","size"}} <bytes> flush, then {"end":{}}
//! S: {"stored":{"stored":n,"present":m}}       | error
//! C: {"scrub":{"deep":false}}                                         (version 2)
//! S: {"scrubbed":{"checked","damaged","unreadable"}} <key>… flush,
//!    then per unreadable file {"unreadable":{"key","reason"}}         | error
//! C: {"heal":{"key","size"}} <bytes> flush                            (version 2)
//! S: {"healed":{"outcome":{"replaced":{"quarantined"}}|{"placed":{}}|{"healed_by_other":{}}}}
//!                                              | error
//! C: {"close":{}}, then closes the stream; the server returns
//! ```
//!
//! An error is `{"error":{"code","key"}}`; after one the session goes on,
//! except for `version` and `protocol`. The server refuses a record sent
//! outside the history given to `open`, and any key that is not a store
//! file's (traversal, a temp file, an odd shape).

mod store;
mod wire;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use super::integrity::{self, Replaced, ScrubOptions, ScrubReport, Source, Unreadable};
use super::layout::{self, Kind};
use super::{Error as FolderError, HistoryKey};
use store::{Landed, Live, Scope};
use wire::{broken, Frame, Wire, CLIENT_MAGIC, SERVER_MAGIC};

/// The protocol versions this build speaks, highest first.
pub const VERSIONS: &[u32] = &[2, 1];

/// The first version with scrub and heal.
const SCRUB_AND_HEAL: u32 = 2;

/// Why a side refused a request, as it travels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Code {
    /// No protocol version both sides speak.
    Version,
    /// The store directory cannot be opened, created, or written to (it
    /// does not exist and `create` was false).
    Open,
    /// A record outside the session's history, or a history that is not a
    /// valid [`HistoryKey`].
    Scope,
    /// A key that is not a store file's.
    Key,
    /// A file that is not what its name says, or not the size announced.
    Integrity,
    /// A file asked for that the store does not hold.
    Missing,
    /// Reading or writing the store failed.
    Io,
    /// A request out of order or malformed: the session ends.
    Protocol,
    /// The store is in use: the server's open guard
    /// ([`ServeOptions::with_open_guard`]) refused it. Version 2 only (a
    /// version 1 client is told [`Code::Open`]).
    Busy,
    /// The far copy of a file to heal cannot be read whole, so it was left
    /// as it is.
    Unreadable,
}

impl Code {
    fn as_str(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Open => "open",
            Self::Scope => "scope",
            Self::Key => "key",
            Self::Integrity => "integrity",
            Self::Missing => "missing",
            Self::Io => "io",
            Self::Protocol => "protocol",
            Self::Busy => "busy",
            Self::Unreadable => "unreadable",
        }
    }

    /// The code a local failure to read or place a file travels as.
    fn of(err: &anyhow::Error) -> Self {
        match err.downcast_ref::<FolderError>() {
            Some(FolderError::InvalidStoreKey { .. }) => Self::Key,
            Some(FolderError::Integrity { .. }) => Self::Integrity,
            Some(FolderError::OutOfScope { .. }) => Self::Scope,
            Some(FolderError::Unreadable { .. }) => Self::Unreadable,
            _ => Self::Io,
        }
    }

    /// The code a file asked for and not sent travels as: [`Self::Missing`]
    /// if the store does not hold it.
    fn of_get(err: &anyhow::Error) -> Self {
        let absent = err.chain().any(|e| {
            e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
        });
        match absent {
            true => Self::Missing,
            false => Self::of(err),
        }
    }

    /// Whether the side that sent it ended the session with it.
    fn ends_session(self) -> bool {
        matches!(self, Self::Version | Self::Protocol)
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failure of the session, found with `err.downcast_ref::<exchange::Error>()`.
/// A file this side refuses to place is a [`folder::Error`](FolderError)
/// instead: [`Integrity`](FolderError::Integrity),
/// [`InvalidStoreKey`](FolderError::InvalidStoreKey) or
/// [`OutOfScope`](FolderError::OutOfScope). `Display` holds codes, keys,
/// versions and builds only.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The far program did not answer as an exchange server: it is not
    /// installed there, is another program, or something (a login banner,
    /// a shell profile) wrote to its stdout first.
    NotAServer,
    /// The two sides speak no common protocol version. `far_build` is the
    /// far server's build, when this side is the client.
    Version {
        ours: Vec<u32>,
        theirs: Vec<u32>,
        far_build: Option<String>,
    },
    /// The far side refused a request, for `code`, about `key` if one.
    /// After any code but [`Code::Version`] and [`Code::Protocol`] the
    /// session can go on.
    Refused { code: Code, key: Option<String> },
    /// The far store is in use: its server's open guard refused it
    /// ([`Code::Busy`]). Nothing was read or written there; the session
    /// can go on (open again, or close).
    Busy,
    /// A request the far side's protocol `version` does not have (scrub
    /// and heal need version 2). Nothing was sent.
    Unsupported { version: u32 },
    /// The session broke off: the stream ended outside an orderly close or
    /// failed, the far process exited, or what came was not this protocol.
    /// Files placed before it stay; a temp file being written is removed.
    SessionBroken,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |vs: &[u32]| {
            vs.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Self::NotAServer => {
                f.write_str("the far program did not answer as a bigstore exchange server")
            }
            Self::Version {
                ours,
                theirs,
                far_build,
            } => {
                write!(f, "the far side speaks exchange protocol {}", list(theirs))?;
                if let Some(build) = far_build {
                    write!(f, " (build {build})")?;
                }
                write!(f, ", this side {}", list(ours))
            }
            Self::Refused { code, key } => match key {
                Some(key) => write!(f, "the far side refused {key}: {code}"),
                None => write!(f, "the far side refused: {code}"),
            },
            Self::Busy => f.write_str("the far store is in use"),
            Self::Unsupported { version } => write!(
                f,
                "the far side speaks exchange protocol {version}, which has no scrub or heal"
            ),
            Self::SessionBroken => f.write_str("the exchange session broke off"),
        }
    }
}

impl std::error::Error for Error {}

/// What [`ServeOptions::with_open_guard`] calls: the guard it returns is
/// held until the session ends.
pub type OpenGuard = dyn Fn(&Path) -> Result<Box<dyn Send>> + Send + Sync;

/// How [`serve`] runs.
#[derive(Clone)]
#[non_exhaustive]
pub struct ServeOptions {
    /// This build, sent in the handshake so a mismatch can name both
    /// sides: one line of printable text (anything else is replaced).
    pub build: String,
    open_guard: Option<Arc<OpenGuard>>,
}

impl ServeOptions {
    pub fn new(build: impl Into<String>) -> Self {
        Self {
            build: build.into(),
            open_guard: None,
        }
    }

    /// Call `guard` with the store directory when the client opens it,
    /// before the store is read, created or written. The value it returns
    /// is held until the session ends (`serve` returns), say a lock on the
    /// store; an `Err` refuses the open as [`Code::Busy`], and the session
    /// goes on with no store open.
    pub fn with_open_guard(
        mut self,
        guard: impl Fn(&Path) -> Result<Box<dyn Send>> + Send + Sync + 'static,
    ) -> Self {
        self.open_guard = Some(Arc::new(guard));
        self
    }
}

impl fmt::Debug for ServeOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServeOptions")
            .field("build", &self.build)
            .field("open_guard", &self.open_guard.is_some())
            .finish()
    }
}

/// Serve one session on `reader` and `writer` (stdin and stdout, under
/// `ssh`) until the client closes it; nothing else may write to `writer`.
///
/// A dedicated thread reads `reader`. If it ends or fails outside an
/// orderly close (the client was killed, the connection dropped), the
/// session stops at its next step, whatever it is doing, removes the temp
/// file it was writing, and returns [`Error::SessionBroken`], so the caller
/// can exit non-zero at once rather than linger. Returns `Ok` after the
/// client's close; [`Error::Version`] if the client speaks no version this
/// one does. Refusals of single requests go to the client and the session
/// goes on.
pub fn serve<R, W>(reader: R, writer: W, opts: &ServeOptions) -> Result<()>
where
    R: Read + Send + 'static,
    W: Write,
{
    let (input, closed) = Input::watch(reader);
    let live = move || {
        if closed.load(Ordering::Relaxed) {
            return Err(broken(()));
        }
        Ok(())
    };
    let mut server = Server {
        wire: Wire::new(input, writer),
        store: None,
        live: &live,
        version: 0,
        open_guard: opts.open_guard.clone(),
    };
    server.handshake(opts)?;
    server.run()
}

/// The server's view of the client's stream, fed by the watchdog thread.
struct Input {
    rx: mpsc::Receiver<std::io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    pos: usize,
}

impl Input {
    /// Start the thread reading `reader`; the flag is set once it ends or
    /// fails.
    fn watch<R: Read + Send + 'static>(mut reader: R) -> (Self, Arc<AtomicBool>) {
        let closed = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::sync_channel(16);
        let flag = Arc::clone(&closed);
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 64 << 10];
            loop {
                let read = match reader.read(&mut buf) {
                    Ok(0) => Ok(Vec::new()),
                    Ok(n) => Ok(buf[..n].to_vec()),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => Err(e),
                };
                let last = !matches!(&read, Ok(chunk) if !chunk.is_empty());
                if last {
                    flag.store(true, Ordering::Relaxed);
                }
                if tx.send(read).is_err() || last {
                    return;
                }
            }
        });
        let input = Self {
            rx,
            chunk: Vec::new(),
            pos: 0,
        };
        (input, closed)
    }
}

impl Read for Input {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos == self.chunk.len() {
            match self.rx.recv() {
                Ok(Ok(chunk)) => {
                    self.chunk = chunk;
                    self.pos = 0;
                }
                Ok(Err(e)) => return Err(e),
                // The thread is gone: it saw the end already.
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.chunk.len() - self.pos);
        out[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// The store a session opened.
struct Opened {
    root: PathBuf,
    /// Whether its directory exists (it may not, opened without `create`):
    /// one that does not lists as empty and holds nothing.
    exists: bool,
    scope: Scope,
    /// What the open guard returned, held while the session lasts.
    _guard: Option<Box<dyn Send>>,
}

struct Server<'a, W: Write> {
    wire: Wire<Input, W>,
    store: Option<Opened>,
    live: Live<'a>,
    /// The version agreed.
    version: u32,
    open_guard: Option<Arc<OpenGuard>>,
}

impl<W: Write> Server<'_, W> {
    fn handshake(&mut self, opts: &ServeOptions) -> Result<()> {
        let offered = match self.wire.line()? {
            Some(line) => line
                .strip_prefix(CLIENT_MAGIC)
                .and_then(|rest| rest.strip_prefix(' '))
                .and_then(|versions| {
                    versions
                        .split(' ')
                        .map(|v| v.parse::<u32>().ok())
                        .collect::<Option<Vec<u32>>>()
                }),
            None => None,
        };
        let Some(offered) = offered else {
            return Err(broken(()));
        };
        let build: String = opts
            .build
            .chars()
            .map(|c| if c.is_control() { '?' } else { c })
            .collect();
        self.wire
            .w
            .text(&format!("{SERVER_MAGIC} {build}"))
            .map_err(broken)?;
        let common = offered.iter().copied().filter(|v| VERSIONS.contains(v));
        let Some(version) = common.max() else {
            self.wire.send(&Frame::Error {
                code: Code::Version,
                key: None,
                versions: VERSIONS.to_vec(),
            })?;
            self.wire.flush()?;
            return Err(Error::Version {
                ours: VERSIONS.to_vec(),
                theirs: offered,
                far_build: None,
            }
            .into());
        };
        self.version = version;
        self.wire.send(&Frame::Version { version })?;
        self.wire.flush()
    }

    fn run(&mut self) -> Result<()> {
        loop {
            let frame = self.wire.recv()?.ok_or_else(|| broken(()))?;
            match (frame, &self.store) {
                (Frame::Close {}, _) => return Ok(()),
                (
                    Frame::Open {
                        store,
                        history,
                        create,
                    },
                    None,
                ) => self.open(&store, &history, create)?,
                (Frame::List {}, Some(_)) => self.list()?,
                (Frame::Get {}, Some(_)) => self.get()?,
                (Frame::Put {}, Some(_)) => self.put()?,
                (Frame::Scrub { deep }, Some(_)) if self.version >= SCRUB_AND_HEAL => {
                    self.scrub(deep)?
                }
                (Frame::Heal { key, size }, Some(_)) if self.version >= SCRUB_AND_HEAL => {
                    self.heal(key, size)?
                }
                _ => {
                    self.wire.send(&Frame::refusal(Code::Protocol, None))?;
                    self.wire.flush()?;
                    return Err(broken(()));
                }
            }
            self.wire.flush()?;
        }
    }

    fn open(&mut self, store: &str, history: &str, create: bool) -> Result<()> {
        let Ok(history) = HistoryKey::new(history) else {
            return self.wire.send(&Frame::refusal(Code::Scope, None));
        };
        if store.is_empty() {
            return self.wire.send(&Frame::refusal(Code::Open, None));
        }
        let root = PathBuf::from(store);
        let guard = match &self.open_guard {
            None => None,
            Some(open_guard) => match open_guard(&root) {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let code = match self.version >= SCRUB_AND_HEAL {
                        true => Code::Busy,
                        false => Code::Open,
                    };
                    return self.wire.send(&Frame::refusal(code, None));
                }
            },
        };
        let existed = match std::fs::metadata(crate::types::long_path(&root)?) {
            Ok(meta) if meta.is_dir() => true,
            Ok(_) => return self.wire.send(&Frame::refusal(Code::Open, None)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return self.wire.send(&Frame::refusal(Code::Open, None)),
        };
        if !existed && create {
            let made = crate::types::long_path(&root).and_then(std::fs::create_dir_all);
            if made.is_err() {
                return self.wire.send(&Frame::refusal(Code::Open, None));
            }
        }
        self.store = Some(Opened {
            root,
            exists: existed || create,
            scope: Scope::new(&history),
            _guard: guard,
        });
        self.wire.send(&Frame::Opened { existed })
    }

    fn opened(&self) -> &Opened {
        self.store.as_ref().expect("checked by run")
    }

    fn list(&mut self) -> Result<()> {
        let opened = self.opened();
        let listed = match opened.exists {
            true => store::list(&opened.root, self.live),
            false => Ok(Vec::new()),
        };
        let keys = match listed {
            Ok(keys) => keys,
            Err(e) if is_broken(&e) => return Err(e),
            Err(_) => return self.wire.send(&Frame::refusal(Code::Io, None)),
        };
        self.wire.send(&Frame::Listing {})?;
        self.wire.send_keys(keys.iter().map(String::as_str))
    }

    fn get(&mut self) -> Result<()> {
        let keys = self.wire.recv_keys()?;
        let root = self.opened().root.clone();
        let exists = self.opened().exists;
        for key in keys {
            (self.live)()?;
            let opened = match layout::kind(&key) {
                Kind::Other => Err(FolderError::InvalidStoreKey { key: key.clone() }.into()),
                _ if !exists => Err(std::io::Error::from(std::io::ErrorKind::NotFound).into()),
                _ => store::open(&root, &key),
            };
            let (mut file, size) = match opened {
                Ok(opened) => opened,
                Err(e) => return self.wire.send(&Frame::refusal(Code::of_get(&e), Some(key))),
            };
            self.wire.send(&Frame::File {
                key: key.clone(),
                size,
            })?;
            // A file that falls short is refused by the client, which says so.
            let _ = store::send_body(&mut self.wire.w, &mut file, &key, size, self.live)?;
        }
        self.wire.send(&Frame::Done {})
    }

    fn put(&mut self) -> Result<()> {
        let opened = self.opened();
        let (root, exists) = (opened.root.clone(), opened.exists);
        let mut refusal: Option<(Code, Option<String>)> = (!exists).then_some((Code::Open, None));
        let mut count = (0, 0);
        loop {
            match self.wire.frame()? {
                Frame::File { key, size } => {
                    let into = match &refusal {
                        None => match self.opened().scope.admit(&key) {
                            Ok(()) => Some(root.as_path()),
                            Err(e) => {
                                refusal = Some((Code::of(&e), Some(key.clone())));
                                None
                            }
                        },
                        Some(_) => None,
                    };
                    match store::receive(&mut self.wire.r, into, &key, size, self.live)? {
                        Some(Ok(Landed::Stored)) => count.0 += 1,
                        Some(Ok(Landed::Present)) => count.1 += 1,
                        Some(Err(e)) => refusal = Some((Code::of(&e), Some(key))),
                        None => {}
                    }
                }
                Frame::End {} => break,
                _ => {
                    self.wire.send(&Frame::refusal(Code::Protocol, None))?;
                    self.wire.flush()?;
                    return Err(broken(()));
                }
            }
        }
        match refusal {
            Some((code, key)) => self.wire.send(&Frame::refusal(code, key)),
            None => self.wire.send(&Frame::Stored {
                stored: count.0,
                present: count.1,
            }),
        }
    }

    /// The opened store as an object store, written with fsync.
    fn local_store(&self) -> Result<object_store::local::LocalFileSystem> {
        let root = &self.opened().root;
        Ok(object_store::local::LocalFileSystem::new_with_prefix(root)
            .with_context(|| format!("failed to open {}", root.display()))?
            .with_fsync(true))
    }

    fn scrub(&mut self, deep: bool) -> Result<()> {
        let report = match self.opened().exists {
            false => Ok(ScrubReport::default()),
            true => self.local_store().and_then(|store| {
                let opts = ScrubOptions {
                    deep,
                    ..ScrubOptions::default()
                };
                run(self.live, async move {
                    integrity::scrub_async(&store, &opts).await
                })
            }),
        };
        let report = match report {
            Ok(report) => report,
            Err(e) if is_broken(&e) => return Err(e),
            Err(_) => return self.wire.send(&Frame::refusal(Code::Io, None)),
        };
        self.wire.send(&Frame::Scrubbed {
            checked: report.checked,
            damaged: report.damaged.len(),
            unreadable: report.unreadable.len(),
        })?;
        self.wire
            .send_keys(report.damaged.iter().map(String::as_str))?;
        for Unreadable { key, reason, .. } in report.unreadable {
            let reason = reason.chars().take(MAX_REASON).collect();
            self.wire.send(&Frame::Unreadable { key, reason })?;
        }
        Ok(())
    }

    fn heal(&mut self, key: String, size: u64) -> Result<()> {
        let opened = self.opened();
        let admitted = match (opened.exists, layout::kind(&key)) {
            (false, _) => Err((Code::Open, None)),
            (true, Kind::Other) => Err((Code::Key, Some(key.clone()))),
            (true, _) => opened
                .scope
                .admit(&key)
                .map_err(|e| (Code::of(&e), Some(key.clone()))),
        };
        let root = opened.root.clone();
        let into = admitted.is_ok().then_some(root.as_path());
        let received = store::receive_unplaced(&mut self.wire.r, into, &key, size, self.live)?;
        let tmp = match (admitted, received) {
            (Err((code, key)), _) => return self.wire.send(&Frame::refusal(code, key)),
            (Ok(()), Some(Ok(tmp))) => tmp,
            (Ok(()), Some(Err(e))) => {
                return self.wire.send(&Frame::refusal(Code::of(&e), Some(key)))
            }
            (Ok(()), None) => unreachable!("a store to receive into gets the file"),
        };
        let replaced = self.local_store().and_then(|store| {
            let source = Source::File(tmp.path().to_path_buf());
            let key = key.clone();
            run(self.live, async move {
                integrity::replace_async(&store, &key, source).await
            })
        });
        drop(tmp);
        let outcome = match replaced {
            Ok(Replaced::Replaced { quarantined }) => wire::Outcome::Replaced { quarantined },
            Ok(Replaced::Placed) => wire::Outcome::Placed {},
            Ok(Replaced::HealedByOther) => wire::Outcome::HealedByOther {},
            Err(e) if is_broken(&e) => return Err(e),
            Err(e) => return self.wire.send(&Frame::refusal(Code::of(&e), Some(key))),
        };
        self.wire.send(&Frame::Healed { outcome })
    }
}

/// The longest unreadable reason a far scrub sends, in characters.
const MAX_REASON: usize = 1024;

/// Run `fut` to its end on a runtime of its own, on a thread of its own
/// (so the caller may be on a runtime's thread, even a blocking one),
/// stopping it as [`Error::SessionBroken`] once `live` fails.
fn run<T, F>(live: Live, fut: F) -> Result<T>
where
    T: Send,
    F: std::future::Future<Output = Result<T>> + Send,
{
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to start async runtime")?;
            rt.block_on(async {
                let stopped = async {
                    while !stop.load(Ordering::Relaxed) {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                };
                tokio::select! {
                    done = fut => done,
                    () = stopped => Err(broken(())),
                }
            })
        });
        while !worker.is_finished() {
            if live().is_err() {
                stop.store(true, Ordering::Relaxed);
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        match worker.join() {
            Ok(done) => done,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    })
}

fn is_broken(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<Error>(), Some(Error::SessionBroken))
}

/// What a [`Client::fetch`] or [`Client::send`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Transferred {
    /// Files placed under their names.
    pub stored: usize,
    /// Files whose names the receiving store already held (same name, same
    /// content), so left as they were.
    pub present: usize,
}

type Reader = BufReader<Box<dyn Read + Send>>;
type Writer = Box<dyn Write + Send>;

/// This end of a session with a far [`serve`]. Each call is one request; a
/// refusal ([`Error::Refused`], or a [`folder::Error`](FolderError) for a
/// file this side refused) leaves the session usable, any other failure
/// ends it ([`Error::SessionBroken`] from then on).
pub struct Client {
    wire: Option<Wire<Reader, Writer>>,
    child: Option<Arc<Mutex<Child>>>,
    cancelled: Arc<AtomicBool>,
    far_build: String,
    version: u32,
    scope: Option<Scope>,
}

/// Stops a [`Client`] from another thread: see [`Client::canceller`].
#[derive(Clone)]
pub struct Canceller {
    cancelled: Arc<AtomicBool>,
    child: Option<Arc<Mutex<Child>>>,
}

impl Canceller {
    /// Fail the call under way, and every later one, with
    /// [`folder::Error::Cancelled`](FolderError::Cancelled); for a spawned
    /// client, kill the far program, which ends a call blocked on it. A
    /// [`connect`](Client::connect)ed client has no program to kill: a call
    /// blocked reading stops when its stream does. Idempotent.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(child) = &self.child {
            if let Ok(mut child) = child.lock() {
                let _ = child.kill();
            }
        }
    }
}

impl Client {
    /// Run `command` (say `ssh -T host asset-store backup serve --stdio`)
    /// with its stdin and stdout piped to this client, and agree a protocol
    /// version. Its stderr is left as `command` sets it (inherited by
    /// default). A program that does not answer as a server is
    /// [`Error::NotAServer`]; one speaking another version,
    /// [`Error::Version`]. A command that cannot be started at all (no
    /// `ssh` here) is the plain I/O error, not an [`Error`].
    pub fn spawn(mut command: Command) -> Result<Self> {
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
        let mut child = command
            .spawn()
            .with_context(|| format!("failed to start {:?}", command.get_program()))?;
        let stdin = child.stdin.take().expect("piped");
        let stdout = child.stdout.take().expect("piped");
        Self::start(
            Box::new(stdout),
            Box::new(stdin),
            Some(Arc::new(Mutex::new(child))),
        )
    }

    /// A session over streams already connected to a server: what it
    /// writes, and where to write to it.
    pub fn connect(
        reader: impl Read + Send + 'static,
        writer: impl Write + Send + 'static,
    ) -> Result<Self> {
        Self::start(Box::new(reader), Box::new(writer), None)
    }

    fn start(
        reader: Box<dyn Read + Send>,
        writer: Writer,
        child: Option<Arc<Mutex<Child>>>,
    ) -> Result<Self> {
        let mut client = Self {
            wire: Some(Wire::new(BufReader::new(reader), writer)),
            child,
            cancelled: Arc::default(),
            far_build: String::new(),
            version: 0,
            scope: None,
        };
        (client.far_build, client.version) = client.handshake()?;
        Ok(client)
    }

    /// The far server's build.
    pub fn far_build(&self) -> &str {
        &self.far_build
    }

    /// The protocol version agreed: the highest of [`VERSIONS`] both sides
    /// speak. Below 2, the far side cannot [`scrub`](Self::scrub) or
    /// [`heal`](Self::heal), so its store's integrity is unverified.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// A handle that cancels this client from another thread.
    pub fn canceller(&self) -> Canceller {
        Canceller {
            cancelled: Arc::clone(&self.cancelled),
            child: self.child.clone(),
        }
    }

    fn handshake(&mut self) -> Result<(String, u32)> {
        let wire = self.wire.as_mut().expect("open until closed");
        fn not_a_server<E>(_: E) -> anyhow::Error {
            Error::NotAServer.into()
        }
        let offered: Vec<String> = VERSIONS.iter().map(ToString::to_string).collect();
        wire.w
            .text(&format!("{CLIENT_MAGIC} {}", offered.join(" ")))
            .map_err(not_a_server)?;
        wire.w.send().map_err(not_a_server)?;
        let line = wire.line().map_err(not_a_server)?.unwrap_or_default();
        let build = match line.strip_prefix(SERVER_MAGIC) {
            Some("") => String::new(),
            Some(rest) => match rest.strip_prefix(' ') {
                Some(build) => build.to_string(),
                None => return Err(Error::NotAServer.into()),
            },
            None => return Err(Error::NotAServer.into()),
        };
        match wire.frame()? {
            Frame::Version { version } if VERSIONS.contains(&version) => Ok((build, version)),
            Frame::Error {
                code: Code::Version,
                versions,
                ..
            } => Err(Error::Version {
                ours: VERSIONS.to_vec(),
                theirs: versions,
                far_build: Some(build),
            }
            .into()),
            _ => Err(broken(())),
        }
    }

    /// Open the far store in directory `store` (a path on the far machine),
    /// whose histories are all under `history`, creating the directory if
    /// `create`. Returns whether it already existed. Opened without
    /// `create`, a store that does not exist lists as empty, and a
    /// [`send`](Self::send) to it is refused ([`Code::Open`]). A store the
    /// far side's open guard refuses is [`Error::Busy`].
    pub fn open(&mut self, store: &str, history: &HistoryKey, create: bool) -> Result<bool> {
        let frame = Frame::Open {
            store: store.to_string(),
            history: history.as_str().to_string(),
            create,
        };
        let existed = self.call(|wire| {
            wire.send(&frame)?;
            wire.flush()?;
            match wire.frame()? {
                Frame::Opened { existed } => Ok(Ok(existed)),
                Frame::Error {
                    code: Code::Busy, ..
                } => Ok(Err(Error::Busy.into())),
                Frame::Error { code, key, .. } => Ok(Err(refused(code, key))),
                _ => Err(broken(())),
            }
        })?;
        self.scope = Some(Scope::new(history));
        Ok(existed)
    }

    /// Every store file the far store holds.
    pub fn list(&mut self) -> Result<BTreeSet<String>> {
        self.scope()?;
        self.call(|wire| {
            wire.send(&Frame::List {})?;
            wire.flush()?;
            match wire.frame()? {
                Frame::Listing {} => {}
                Frame::Error { code, key, .. } => return Ok(Err(refused(code, key))),
                _ => return Err(broken(())),
            }
            let keys = wire.recv_keys()?;
            if keys.iter().any(|key| layout::kind(key) == Kind::Other) {
                return Err(broken(()));
            }
            Ok(Ok(keys.into_iter().collect()))
        })
    }

    /// Copy `keys` from the far store into the store in directory `into`,
    /// in [`Kind`] order, each checked against its name and placed as the
    /// module docs say. Refused before anything is asked for: a key that is
    /// not a store file's ([`FolderError::InvalidStoreKey`]), a record
    /// outside the opened history ([`FolderError::OutOfScope`]). A file
    /// that is not what its name says is [`FolderError::Integrity`], and
    /// nothing is placed after it; one the far store lacks is
    /// [`Error::Refused`] with [`Code::Missing`].
    pub fn fetch<I, S>(&mut self, keys: I, into: &Path) -> Result<Transferred>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let keys = self.admit(keys)?;
        if keys.is_empty() {
            return Ok(Transferred::default());
        }
        let cancelled = Arc::clone(&self.cancelled);
        let live = move || cancel_check(&cancelled);
        self.call(|wire| {
            wire.send(&Frame::Get {})?;
            wire.send_keys(keys.iter().map(String::as_str))?;
            wire.flush()?;
            let mut done = Transferred::default();
            let mut refusal: Option<anyhow::Error> = None;
            for key in &keys {
                match wire.frame()? {
                    Frame::File { key: got, size } if got == *key => {
                        let into = refusal.is_none().then_some(into);
                        match store::receive(&mut wire.r, into, key, size, &live)? {
                            Some(Ok(Landed::Stored)) => done.stored += 1,
                            Some(Ok(Landed::Present)) => done.present += 1,
                            Some(Err(e)) => refusal = Some(e),
                            None => {}
                        }
                    }
                    Frame::Error { code, key, .. } => {
                        return Ok(Err(refusal.unwrap_or_else(|| refused(code, key))))
                    }
                    _ => return Err(broken(())),
                }
            }
            match wire.frame()? {
                Frame::Done {} => Ok(refusal.map_or(Ok(done), Err)),
                _ => Err(broken(())),
            }
        })
    }

    /// Copy `keys` from the store in directory `from` to the far store, in
    /// [`Kind`] order; the far side checks each against its name and
    /// places it as the module docs say. Refused before anything is sent: a
    /// key that is not a store file's, a record outside the opened history.
    /// A file the far side refuses is [`Error::Refused`] (say
    /// [`Code::Integrity`]), and nothing after it is placed there; a file
    /// missing here, or that cannot be read whole, stops the sending with
    /// that local error.
    pub fn send<I, S>(&mut self, keys: I, from: &Path) -> Result<Transferred>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let keys = self.admit(keys)?;
        if keys.is_empty() {
            return Ok(Transferred::default());
        }
        let cancelled = Arc::clone(&self.cancelled);
        let live = move || cancel_check(&cancelled);
        self.call(|wire| {
            wire.send(&Frame::Put {})?;
            let mut failure: Option<anyhow::Error> = None;
            for key in &keys {
                live()?;
                let (mut file, size) = match store::open(from, key) {
                    Ok(opened) => opened,
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                };
                wire.send(&Frame::File {
                    key: key.clone(),
                    size,
                })?;
                if let Some(short) = store::send_body(&mut wire.w, &mut file, key, size, &live)? {
                    failure = Some(short);
                    break;
                }
            }
            wire.send(&Frame::End {})?;
            wire.flush()?;
            match wire.frame()? {
                Frame::Stored { stored, present } => {
                    Ok(failure.map_or(Ok(Transferred { stored, present }), Err))
                }
                Frame::Error { code, key, .. } => {
                    Ok(Err(failure.unwrap_or_else(|| refused(code, key))))
                }
                _ => Err(broken(())),
            }
        })
    }

    /// Scrub the far store ([`integrity::scrub`] there, on its own files,
    /// reading every file if `deep`) and return its report: keys relative
    /// to the far store, each unreadable file's reason as the far side gave
    /// it (at most 1024 characters). [`Error::Unsupported`] in a version 1
    /// session; a far store that does not exist reports nothing.
    pub fn scrub(&mut self, deep: bool) -> Result<ScrubReport> {
        self.scope()?;
        self.v2()?;
        self.call(|wire| {
            wire.send(&Frame::Scrub { deep })?;
            wire.flush()?;
            let (checked, damaged, unreadable) = match wire.frame()? {
                Frame::Scrubbed {
                    checked,
                    damaged,
                    unreadable,
                } => (checked, damaged, unreadable),
                Frame::Error { code, key, .. } => return Ok(Err(refused(code, key))),
                _ => return Err(broken(())),
            };
            let keys = wire.recv_keys()?;
            let store_key = |key: &str| layout::kind(key) != Kind::Other;
            if keys.len() != damaged || !keys.iter().all(|k| store_key(k)) {
                return Err(broken(()));
            }
            let mut report = ScrubReport {
                checked,
                damaged: keys,
                unreadable: Vec::with_capacity(unreadable.min(1024)),
            };
            for _ in 0..unreadable {
                match wire.frame()? {
                    Frame::Unreadable { key, reason } if store_key(&key) => {
                        report.unreadable.push(Unreadable::new(key, reason))
                    }
                    _ => return Err(broken(())),
                }
            }
            Ok(Ok(report))
        })
    }

    /// Heal store file `key` in the far store with the bytes of the local
    /// file `file` (a store file of this side, `<store>/<key>`, or a
    /// working copy), which the far side checks against `key` as they
    /// arrive ([`Code::Integrity`] if they are not what it names). The far
    /// side then reads its own copy: good, it is left
    /// ([`Replaced::HealedByOther`]); unreadable, it is left too
    /// ([`Code::Unreadable`]); damaged, it is quarantined and replaced
    /// atomically, as [`integrity::replace`] does. Refused before anything
    /// is sent: a key that is not a store file's, a record outside the
    /// opened history, a version 1 session ([`Error::Unsupported`]). A
    /// local file that cannot be read whole is that local error.
    pub fn heal(&mut self, key: &str, file: &Path) -> Result<Replaced> {
        // One key in, one out: checked, and in scope.
        let key = self.admit([key])?.remove(0);
        self.v2()?;
        let cancelled = Arc::clone(&self.cancelled);
        let live = move || cancel_check(&cancelled);
        let (mut source, size) = {
            let source = std::fs::File::open(crate::types::long_path(file)?)
                .with_context(|| format!("failed to open {}", file.display()))?;
            let size = source.metadata()?.len();
            (source, size)
        };
        self.call(|wire| {
            wire.send(&Frame::Heal {
                key: key.clone(),
                size,
            })?;
            let short = store::send_body(&mut wire.w, &mut source, &key, size, &live)?;
            wire.flush()?;
            let answer = match wire.frame()? {
                Frame::Healed { outcome } => Ok(match outcome {
                    wire::Outcome::Replaced { quarantined } => Replaced::Replaced { quarantined },
                    wire::Outcome::Placed {} => Replaced::Placed,
                    wire::Outcome::HealedByOther {} => Replaced::HealedByOther,
                }),
                Frame::Error { code, key, .. } => Err(refused(code, key)),
                _ => return Err(broken(())),
            };
            Ok(match short {
                Some(short) => Err(short),
                None => answer,
            })
        })
    }

    /// Refuse a request a version 1 session lacks.
    fn v2(&self) -> Result<()> {
        if self.version < SCRUB_AND_HEAL {
            return Err(Error::Unsupported {
                version: self.version,
            }
            .into());
        }
        Ok(())
    }

    /// End the session: the far server returns, and a spawned far program
    /// is waited for. `Ok` only if it exited successfully.
    pub fn close(mut self) -> Result<()> {
        let said = self.call(|wire| {
            wire.send(&Frame::Close {})?;
            wire.flush()?;
            Ok(Ok(()))
        });
        // Closing the pipes is the far side's end of input.
        self.wire = None;
        let Some(child) = self.child.take() else {
            return said;
        };
        loop {
            let status = child
                .lock()
                .map_err(|_| broken(()))?
                .try_wait()
                .context("failed to wait for the far program")?;
            match status {
                Some(status) => {
                    said?;
                    cancel_check(&self.cancelled)?;
                    return match status.success() {
                        true => Ok(()),
                        false => Err(broken(())),
                    };
                }
                None => std::thread::sleep(std::time::Duration::from_millis(10)),
            }
        }
    }

    /// The opened scope; calls before [`open`](Self::open) are a misuse.
    /// A cancelled client says so first.
    fn scope(&self) -> Result<&Scope> {
        cancel_check(&self.cancelled)?;
        self.scope
            .as_ref()
            .context("exchange: open a store before listing or copying")
    }

    /// `keys` as a transfer takes them: checked, in [`Kind`] order, every
    /// record in scope.
    fn admit<I, S>(&self, keys: I) -> Result<Vec<String>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let scope = self.scope()?;
        let keys = store::ordered(keys)?;
        for key in &keys {
            scope.admit(key)?;
        }
        Ok(keys)
    }

    /// Run one request on the wire. The outer `Err` ends the session (it
    /// stays broken); the inner one is a refusal that leaves it usable,
    /// unless the far side ended the session with it.
    fn call<T>(
        &mut self,
        request: impl FnOnce(&mut Wire<Reader, Writer>) -> Result<Result<T>>,
    ) -> Result<T> {
        cancel_check(&self.cancelled)?;
        let Some(wire) = self.wire.as_mut() else {
            return Err(broken(()));
        };
        match request(wire) {
            Ok(answer) => {
                if let Err(e) = &answer {
                    if let Some(Error::Refused { code, .. }) = e.downcast_ref::<Error>() {
                        if code.ends_session() {
                            self.wire = None;
                        }
                    }
                }
                answer
            }
            Err(e) => {
                self.wire = None;
                cancel_check(&self.cancelled)?;
                Err(e)
            }
        }
    }
}

impl Drop for Client {
    /// An unclosed session is abandoned: the far program is killed and
    /// waited for.
    fn drop(&mut self) {
        self.wire = None;
        if let Some(child) = self.child.take() {
            if let Ok(mut child) = child.lock() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn refused(code: Code, key: Option<String>) -> anyhow::Error {
    Error::Refused { code, key }.into()
}

fn cancel_check(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Relaxed) {
        return Err(FolderError::Cancelled.into());
    }
    Ok(())
}
