//! A store directory as the exchange reads and writes it, the same on both
//! sides: listing it, reading a file to send, and placing one received.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::LazyLock;

use super::wire::broken;
use crate::folder::layout::{self, Check, Kind};
use crate::folder::{Error as FolderError, HistoryKey};
use crate::pktline::{PktReader, PktWriter};
use crate::types::long_path;

/// Bytes moved per read or write of a file body.
const CHUNK: usize = 64 << 10;

/// Something to check between pieces of work: `Err` stops the work.
pub(super) type Live<'a> = &'a dyn Fn() -> Result<()>;

/// The histories a session may hold: records under it, in a store's
/// encoded form.
pub(super) struct Scope {
    history: HistoryKey,
    /// `bigstore-history/<history, encoded>/`.
    prefix: String,
}

impl Scope {
    pub(super) fn new(history: &HistoryKey) -> Self {
        let encoded =
            object_store::path::Path::from(format!("bigstore-history/{}", history.as_str()));
        Self {
            history: history.clone(),
            prefix: format!("{encoded}/"),
        }
    }

    /// Refuse a record outside the scope, as [`FolderError::OutOfScope`].
    pub(super) fn admit(&self, key: &str) -> Result<()> {
        if layout::kind(key) == Kind::Record && !key.starts_with(&self.prefix) {
            return Err(FolderError::OutOfScope {
                key: key.to_string(),
                history: self.history.clone(),
            }
            .into());
        }
        Ok(())
    }
}

/// `keys` checked to be store files' ([`FolderError::InvalidStoreKey`]
/// otherwise), without repeats, in the order a transfer places them: by
/// [`Kind`], then by key.
pub(super) fn ordered<I, S>(keys: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut keyed = keys
        .into_iter()
        .map(|key| {
            let key = key.as_ref();
            match layout::kind(key) {
                Kind::Other => Err(FolderError::InvalidStoreKey {
                    key: key.to_string(),
                }
                .into()),
                kind => Ok((kind, key.to_string())),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort();
    keyed.dedup();
    Ok(keyed.into_iter().map(|(_, key)| key).collect())
}

/// Where `key` lives under `root`.
fn path_of(root: &Path, key: &str) -> PathBuf {
    let mut path = root.to_path_buf();
    path.extend(key.split('/'));
    path
}

/// Every store file under `root`: records, then manifests, then objects,
/// each kind listed only once the one before is. Something placed while
/// listing is placed objects first, so any record or manifest listed has
/// its objects listed too. A store with no directory yet is empty.
pub(super) fn list(root: &Path, live: Live) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for (top, kind) in [
        ("bigstore-history", Kind::Record),
        ("files", Kind::Manifest),
        ("files", Kind::Object),
    ] {
        walk(root, top, kind, &mut keys, live)?;
    }
    Ok(keys)
}

fn walk(root: &Path, top: &str, want: Kind, keys: &mut Vec<String>, live: Live) -> Result<()> {
    let base = long_path(root)?;
    for entry in walkdir::WalkDir::new(base.join(top)) {
        live()?;
        let entry = match entry {
            Ok(entry) => entry,
            Err(e)
                if e.depth() == 0
                    && e.io_error()
                        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                return Ok(())
            }
            Err(e) => return Err(e).with_context(|| format!("failed to list {}", root.display())),
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let Ok(rel) = entry.path().strip_prefix(&base) else {
            continue;
        };
        let names: Option<Vec<&str>> = rel.components().map(|c| c.as_os_str().to_str()).collect();
        if let Some(key) = names.map(|names| names.join("/")) {
            if layout::kind(&key) == want {
                keys.push(key);
            }
        }
    }
    Ok(())
}

/// The file `key` under `root`, open, and its size. Not there is an I/O
/// error of kind `NotFound`.
pub(super) fn open(root: &Path, key: &str) -> Result<(File, u64)> {
    let path = path_of(root, key);
    let file = File::open(long_path(&path)?)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let meta = file.metadata()?;
    anyhow::ensure!(meta.is_file(), "{} is not a file", path.display());
    Ok((file, meta.len()))
}

/// Send `size` bytes of `file` as one body. A file that turns out shorter
/// sends what it has: the receiver refuses it, and the session goes on.
pub(super) fn send_body<W: Write>(
    w: &mut PktWriter<W>,
    file: &mut File,
    size: u64,
    live: Live,
) -> Result<()> {
    let mut body = w.content();
    let mut buf = vec![0u8; CHUNK];
    let mut left = size;
    while left > 0 {
        live()?;
        let want = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = match file.read(&mut buf[..want]) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        body.write_all(&buf[..n]).map_err(broken)?;
        left -= n as u64;
    }
    body.finish().map_err(broken)?;
    Ok(())
}

/// What became of a file received.
pub(super) enum Landed {
    /// Placed under its name.
    Stored,
    /// Its name was already there: same name, same content.
    Present,
}

/// Receive one body, said to be `size` bytes of `key`. With `into`, it is
/// checked as it arrives into `<final>#<random>` beside its final name,
/// then placed if it is what `key` names; without, it is read and dropped.
/// The outer `Err` is the session's (the stream broke, or `live` stopped
/// it); the inner one refuses this file, and leaves no temp file behind.
pub(super) fn receive<R: Read>(
    r: &mut PktReader<R>,
    into: Option<&Path>,
    key: &str,
    size: u64,
    live: Live,
) -> Result<Option<Result<Landed>>> {
    let mut refusal = None;
    let mut incoming = match into.map(|root| Incoming::begin(root, key, size)) {
        None => None,
        Some(Ok(incoming)) => Some(incoming),
        Some(Err(e)) => {
            refusal = Some(e);
            None
        }
    };
    let mut body = r.content();
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = body.read(&mut buf).map_err(broken)?;
        if n == 0 {
            break;
        }
        live()?;
        if let Some(Err(e)) = incoming.as_mut().map(|i| i.write(&buf[..n])) {
            refusal = Some(e);
            incoming = None;
        }
    }
    Ok(match (refusal, incoming) {
        (Some(e), _) => Some(Err(e)),
        (None, Some(incoming)) => Some(incoming.place()),
        (None, None) => None,
    })
}

/// A file arriving: checked as it comes, written to a temp file beside its
/// final name, which is deleted unless it is placed.
struct Incoming {
    key: String,
    check: Check,
    tmp: tempfile::NamedTempFile,
    path: PathBuf,
}

impl Incoming {
    fn begin(root: &Path, key: &str, size: u64) -> Result<Self> {
        let check = Check::new(key, size)?;
        let path = path_of(root, key);
        let (dir, name) = match (path.parent(), path.file_name()) {
            (Some(dir), Some(name)) => (dir, name.to_string_lossy()),
            _ => unreachable!("a store key has a directory and a name"),
        };
        let dir = long_path(dir)?;
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let tmp = tempfile::Builder::new()
            .prefix(&format!("{name}#"))
            .rand_bytes(8)
            .tempfile_in(&dir)
            .with_context(|| format!("failed to create a temp file in {}", dir.display()))?;
        Ok(Self {
            key: key.to_string(),
            check,
            tmp,
            path,
        })
    }

    fn write(&mut self, chunk: &[u8]) -> Result<()> {
        self.check.update(chunk);
        self.tmp
            .write_all(chunk)
            .with_context(|| format!("failed to write {}", self.tmp.path().display()))
    }

    /// Place the file under its name if it is what its key names: renamed
    /// there without replacing anything (or hard-linked, where the
    /// filesystem cannot rename so), so it appears whole or not at all. A
    /// name already there is [`Landed::Present`].
    fn place(self) -> Result<Landed> {
        self.check.finish()?;
        let file = self.tmp.as_file();
        file.sync_all()
            .with_context(|| format!("failed to write {}", self.tmp.path().display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o666 & !umask()))?;
        }
        match self.tmp.persist_noclobber(long_path(&self.path)?) {
            Ok(_) => Ok(Landed::Stored),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Landed::Present),
            Err(e) => Err(anyhow::Error::from(e.error))
                .with_context(|| format!("failed to place {}", self.key)),
        }
    }
}

/// The process umask, read once.
#[cfg(unix)]
fn umask() -> u32 {
    static UMASK: LazyLock<u32> = LazyLock::new(crate::folder::current_umask);
    *UMASK
}
