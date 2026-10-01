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
//! The protocol has its own version ([`VERSION`]), not the crate's, agreed
//! before anything else: a mismatch fails on both sides before any store
//! is touched. Errors carry codes and keys, never what the far side wrote
//! elsewhere (its stderr is the caller's to keep or drop).
//!
//! # The protocol (version 1)
//!
//! pkt-lines ([`crate::pktline`]). Control messages are one JSON object per
//! packet; key lists are text packets ending in a flush; a file's bytes
//! are data packets ending in a flush.
//!
//! ```text
//! C: bigstore-exchange-client 1                versions offered
//! S: bigstore-exchange-server <build>
//! S: {"version":{"version":1}}                 | {"error":{"code":"version","versions":[..]}}
//! C: {"open":{"store":"D:/…","history":"<h>","create":true}}
//! S: {"opened":{"existed":true}}               | error
//! C: {"list":{}}
//! S: {"listing":{}} <key>… flush               | error
//! C: {"get":{}} <key>… flush
//! S: per key {"file":{"key","size"}} <bytes> flush, then {"done":{}}   | error (ends the get)
//! C: {"put":{}} per key {"file":{"key","size"}} <bytes> flush, then {"end":{}}
//! S: {"stored":{"stored":n,"present":m}}       | error
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

use super::layout::{self, Kind};
use super::{Error as FolderError, HistoryKey};
use store::{Landed, Live, Scope};
use wire::{broken, Frame, Wire, CLIENT_MAGIC, SERVER_MAGIC};

/// The protocol version this build speaks.
pub const VERSION: u32 = 1;

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
        }
    }

    /// The code a local failure travels as.
    fn of(err: &anyhow::Error) -> Self {
        match err.downcast_ref::<FolderError>() {
            Some(FolderError::InvalidStoreKey { .. }) => Self::Key,
            Some(FolderError::Integrity { .. }) => Self::Integrity,
            Some(FolderError::OutOfScope { .. }) => Self::Scope,
            _ if err.chain().any(|e| {
                e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
            }) =>
            {
                Self::Missing
            }
            _ => Self::Io,
        }
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
            Self::SessionBroken => f.write_str("the exchange session broke off"),
        }
    }
}

impl std::error::Error for Error {}

/// How [`serve`] runs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServeOptions {
    /// This build, sent in the handshake so a mismatch can name both
    /// sides: one line of printable text (anything else is replaced).
    pub build: String,
}

impl ServeOptions {
    pub fn new(build: impl Into<String>) -> Self {
        Self {
            build: build.into(),
        }
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
}

struct Server<'a, W: Write> {
    wire: Wire<Input, W>,
    store: Option<Opened>,
    live: Live<'a>,
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
        if !offered.contains(&VERSION) {
            self.wire.send(&Frame::Error {
                code: Code::Version,
                key: None,
                versions: vec![VERSION],
            })?;
            self.wire.flush()?;
            return Err(Error::Version {
                ours: vec![VERSION],
                theirs: offered,
                far_build: None,
            }
            .into());
        }
        self.wire.send(&Frame::Version { version: VERSION })?;
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
                Err(e) => return self.wire.send(&Frame::refusal(Code::of(&e), Some(key))),
            };
            self.wire.send(&Frame::File {
                key: key.clone(),
                size,
            })?;
            store::send_body(&mut self.wire.w, &mut file, size, self.live)?;
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
    scope: Option<Scope>,
}

/// Stops a [`Client`] from another thread: see [`Client::canceller`].
#[derive(Clone)]
pub struct Canceller {
    cancelled: Arc<AtomicBool>,
    child: Option<Arc<Mutex<Child>>>,
}

impl Canceller {
    /// Kill the far program (for a spawned client) and fail the call under
    /// way, and every later one, with
    /// [`folder::Error::Cancelled`](FolderError::Cancelled). Idempotent.
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
    /// [`Error::Version`].
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
            scope: None,
        };
        client.far_build = client.handshake()?;
        Ok(client)
    }

    /// The far server's build.
    pub fn far_build(&self) -> &str {
        &self.far_build
    }

    /// A handle that cancels this client from another thread.
    pub fn canceller(&self) -> Canceller {
        Canceller {
            cancelled: Arc::clone(&self.cancelled),
            child: self.child.clone(),
        }
    }

    fn handshake(&mut self) -> Result<String> {
        let wire = self.wire.as_mut().expect("open until closed");
        fn not_a_server<E>(_: E) -> anyhow::Error {
            Error::NotAServer.into()
        }
        wire.w
            .text(&format!("{CLIENT_MAGIC} {VERSION}"))
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
            Frame::Version { version } if version == VERSION => Ok(build),
            Frame::Error {
                code: Code::Version,
                versions,
                ..
            } => Err(Error::Version {
                ours: vec![VERSION],
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
    /// [`send`](Self::send) to it is refused ([`Code::Open`]).
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
    /// missing here stops the sending with the I/O error.
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
                store::send_body(&mut wire.w, &mut file, size, &live)?;
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
    /// stays broken); the inner one is a refusal that leaves it usable.
    fn call<T>(
        &mut self,
        request: impl FnOnce(&mut Wire<Reader, Writer>) -> Result<Result<T>>,
    ) -> Result<T> {
        cancel_check(&self.cancelled)?;
        let Some(wire) = self.wire.as_mut() else {
            return Err(broken(()));
        };
        match request(wire) {
            Ok(answer) => answer,
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
