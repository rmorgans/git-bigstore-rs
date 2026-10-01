//! Finding hash-proven damage in a store, and healing it, over any
//! [`ObjectStore`]: a local store directory (`LocalFileSystem`, with fsync
//! on), or an S3 prefix (a `PrefixStore`). Keys are relative to that store,
//! in the form [`layout`] takes them.
//!
//! A store file is in one of four states:
//!
//! - **good**: read whole, it is what its name says ([`layout::verify`]);
//!   or, for an object or manifest, the store reports an ETag equal to the
//!   md5 its name gives (the provider hashed the bytes it stored).
//! - **damaged**: hash-proven only. The read returned exactly the size the
//!   store reported, or that size is over the kind's limit, and the bytes
//!   are not what the name says. An ETag that differs is only a suspicion,
//!   settled by reading the file.
//! - **absent**: the store says there is no such file.
//! - **unreadable**: anything else (an I/O or permission error, a read that
//!   ended short or ran long, a network failure). Nothing here ever changes
//!   an unreadable file.
//!
//! [`scrub`] reports damaged and unreadable files. [`replace`] heals one
//! file with bytes that verify, keeping the damaged bytes in quarantine;
//! [`quarantine`] moves a damaged object or manifest aside, leaving its
//! name absent (sync and `push --repair` refill names). A record is never
//! made absent: only [`replace`] heals it.
//!
//! Quarantined bytes are kept under `quarantine/<key>.<UTC time>` in the
//! same store (time as `20261001T120000.123456789Z`): a [`Kind::Other`]
//! key, which no scrub, listing or transfer touches.
//!
//! Every call comes in a blocking form and an `_async` one, as the rest of
//! [`folder`](super) does; hashing runs on the blocking pool.

use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use std::path::PathBuf;

use super::layout::{self, Check, Kind};
use super::{block_on, each_unordered, Error};
use crate::backend;
use crate::hash::Hasher;
use crate::types::HashFunction;

/// Where quarantined bytes go, relative to the store.
pub const QUARANTINE: &str = "quarantine/";

/// Files up to this size are replaced with one PUT (an S3 ETag then is
/// their md5); larger ones by multipart upload, in parts of this size.
const PART: u64 = 10 << 20;

/// How [`scrub`] runs: `ScrubOptions { deep: true, ..Default::default() }`.
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    /// Read every file, trusting no ETag. Without it an object or manifest
    /// whose ETag is the md5 its name gives counts as good unread.
    pub deep: bool,
    /// Files read at once.
    pub jobs: usize,
}

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            deep: false,
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
        }
    }
}

/// What [`scrub`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScrubReport {
    /// Store files examined (good, damaged or unreadable); a file that
    /// vanished between the listing and its read is not counted.
    pub checked: usize,
    /// Files proven not to be what their names say, sorted.
    pub damaged: Vec<String>,
    /// Files that could not be read whole, sorted by key.
    pub unreadable: Vec<Unreadable>,
}

impl ScrubReport {
    /// Nothing damaged and nothing unreadable.
    pub fn is_clean(&self) -> bool {
        self.damaged.is_empty() && self.unreadable.is_empty()
    }
}

/// A store file that could not be read whole, and why.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub struct Unreadable {
    pub key: String,
    /// The failure, as text: the error chain, or the short read.
    pub reason: String,
}

impl Unreadable {
    pub fn new(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            reason: reason.into(),
        }
    }
}

/// Examine every store file in `store` (one listing, then `opts.jobs`
/// reads at a time): [`Kind::Other`] keys are ignored. Objects and
/// manifests whose listed ETag is their name's md5 are good unread, unless
/// `opts.deep`; every other file (records, any other ETag, none) is read
/// and checked against its name. A listing that fails is the error; a file
/// that fails is [`Unreadable`] in the report. Reads only.
pub fn scrub(store: &dyn ObjectStore, opts: &ScrubOptions) -> Result<ScrubReport> {
    block_on("integrity::scrub", scrub_async(store, opts))?
}

/// [`scrub`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn scrub_async(store: &dyn ObjectStore, opts: &ScrubOptions) -> Result<ScrubReport> {
    let listed: Vec<ObjectMeta> = store
        .list(None)
        .try_filter(|meta| {
            futures::future::ready(layout::kind(meta.location.as_ref()) != Kind::Other)
        })
        .try_collect()
        .await
        .context("failed to list the store")?;
    let deep = opts.deep;
    let states = each_unordered(listed, opts.jobs, |meta| scrub_one(store, meta, deep)).await?;
    let mut report = ScrubReport::default();
    for (key, state) in states {
        match state {
            State::Good => {}
            State::Absent => continue,
            State::Damaged => report.damaged.push(key),
            State::Unreadable(reason) => report.unreadable.push(Unreadable { key, reason }),
        }
        report.checked += 1;
    }
    report.damaged.sort();
    report.unreadable.sort();
    Ok(report)
}

async fn scrub_one(
    store: &dyn ObjectStore,
    meta: ObjectMeta,
    deep: bool,
) -> Result<(String, State)> {
    let key = meta.location.as_ref().to_string();
    if !deep && etag_proves(&key, meta.e_tag.as_deref()) {
        return Ok((key, State::Good));
    }
    let state = examine(store, &meta.location).await;
    Ok((key, state))
}

/// A store file's state, as [the module docs](self) define them.
#[derive(Debug)]
enum State {
    Good,
    Damaged,
    Absent,
    Unreadable(String),
}

/// The md5 an object's or manifest's name gives.
fn named_md5(key: &str) -> Option<String> {
    if !matches!(layout::kind(key), Kind::Object | Kind::Manifest) {
        return None;
    }
    let rest = key.strip_prefix("files/md5/")?;
    let (shard, name) = rest.split_once('/')?;
    let name = name.strip_suffix(".dir").unwrap_or(name);
    Some(format!("{shard}{name}"))
}

/// An ETag as a bare value: quotes and a weak marker gone.
fn etag_value(etag: &str) -> &str {
    etag.strip_prefix("W/").unwrap_or(etag).trim_matches('"')
}

/// Whether `etag` is the md5 `key`'s name gives (objects and manifests).
fn etag_proves(key: &str, etag: Option<&str>) -> bool {
    match (named_md5(key), etag) {
        (Some(md5), Some(etag)) => etag_value(etag).eq_ignore_ascii_case(&md5),
        _ => false,
    }
}

/// Read the file at `location` whole and check it against its name.
async fn examine(store: &dyn ObjectStore, location: &StorePath) -> State {
    let key = location.as_ref().to_string();
    let got = match store.get(location).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => return State::Absent,
        Err(e) => return State::Unreadable(reason(&anyhow::Error::from(e))),
    };
    let size = got.meta.size;
    let check = match Check::new(&key, size) {
        Ok(check) => check,
        Err(e) if is_integrity(&e) => return State::Damaged,
        Err(e) => return State::Unreadable(reason(&e)),
    };
    let check = match feed(check, size, got.into_stream()).await {
        Ok(check) => check,
        Err(reason) => return State::Unreadable(reason),
    };
    match backend::blocking(move || check.finish()).await {
        Ok(()) => State::Good,
        Err(e) if is_integrity(&e) => State::Damaged,
        Err(e) => State::Unreadable(reason(&e)),
    }
}

/// Feed `stream`, a read of `size` bytes, to `check` on the blocking pool
/// as it arrives. `Err` says why the read did not yield exactly `size`
/// bytes.
async fn feed<B>(
    check: Check,
    size: u64,
    mut stream: futures::stream::BoxStream<'static, object_store::Result<B>>,
) -> std::result::Result<Check, String>
where
    B: AsRef<[u8]> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<B>(4);
    let checking = backend::blocking(move || {
        let mut check = check;
        while let Some(chunk) = rx.blocking_recv() {
            check.update(chunk.as_ref());
        }
        Ok(check)
    });
    let feeding = async move {
        let mut read: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| reason(&anyhow::Error::from(e)))?;
            read += chunk.as_ref().len() as u64;
            if read > size || tx.send(chunk).await.is_err() {
                break;
            }
        }
        Ok::<u64, String>(read)
    };
    let (read, check) = tokio::join!(feeding, checking);
    let read = read?;
    let check = check.map_err(|e| reason(&e))?;
    if read != size {
        return Err(format!(
            "read {read} bytes of the {size} the store reported"
        ));
    }
    Ok(check)
}

fn is_integrity(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<Error>(), Some(Error::Integrity { .. }))
}

fn reason(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

/// The bytes [`replace`] writes.
#[derive(Debug, Clone)]
pub enum Source {
    /// In memory.
    Bytes(Vec<u8>),
    /// A local file. It is checked before anything is written and again as
    /// it is uploaded, so a file that changes meanwhile is never placed.
    File(PathBuf),
}

/// What [`replace`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Replaced {
    /// The file was damaged: its bytes are now under `quarantined` and the
    /// key holds the verified bytes.
    Replaced { quarantined: String },
    /// The key was absent: the verified bytes were placed.
    Placed,
    /// Read again just before replacing, the file was good: someone else
    /// healed it. Nothing was written.
    HealedByOther,
}

/// Heal store file `key` with `source`, which must be what `key` names
/// ([`Error::Integrity`] otherwise, before anything is written). The file
/// there is read again first: good, it is left alone
/// ([`Replaced::HealedByOther`]); unreadable, nothing is written
/// ([`Error::Unreadable`]); damaged, its bytes are copied to
/// [quarantine](self) first. Then `source` replaces it in one atomic write
/// (on a `LocalFileSystem`, a temp file renamed over the name). Last, the
/// write is checked: an ETag equal to the bytes' md5, else the file read
/// back and verified, else [`Error::WriteUnverified`].
///
/// Replacing a content-addressed name with bytes that verify is safe
/// whatever is there, so concurrent heals of one key converge on good
/// bytes.
pub fn replace(store: &dyn ObjectStore, key: &str, source: Source) -> Result<Replaced> {
    block_on("integrity::replace", replace_async(store, key, source))?
}

/// [`replace`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn replace_async(store: &dyn ObjectStore, key: &str, source: Source) -> Result<Replaced> {
    let location = store_path(key)?;
    let prepared = prepare(key, source).await?;
    let quarantined = match examine(store, &location).await {
        State::Good => return Ok(Replaced::HealedByOther),
        State::Unreadable(reason) => {
            return Err(Error::Unreadable {
                key: key.to_string(),
                reason,
            }
            .into())
        }
        State::Absent => None,
        State::Damaged => {
            let to = quarantine_key(key);
            store
                .copy(&location, &store_path(&to)?)
                .await
                .with_context(|| format!("failed to quarantine {key}"))?;
            Some(to)
        }
    };
    let written = match prepared {
        Prepared::Bytes { bytes, md5 } => {
            let put = store
                .put(&location, PutPayload::from(bytes))
                .await
                .with_context(|| format!("failed to write {key}"))?;
            put.e_tag
                .is_some_and(|etag| etag_value(&etag).eq_ignore_ascii_case(&md5))
        }
        Prepared::File { path, size } => {
            upload(store, &location, key, path, size).await?;
            false
        }
    };
    if !written && !matches!(examine(store, &location).await, State::Good) {
        return Err(Error::WriteUnverified {
            key: key.to_string(),
        }
        .into());
    }
    Ok(match quarantined {
        Some(quarantined) => Replaced::Replaced { quarantined },
        None => Replaced::Placed,
    })
}

/// A source checked against its key.
enum Prepared {
    /// Its bytes, and their md5.
    Bytes { bytes: Vec<u8>, md5: String },
    /// A file over [`PART`], `size` bytes when checked.
    File { path: PathBuf, size: u64 },
}

async fn prepare(key: &str, source: Source) -> Result<Prepared> {
    if layout::kind(key) == Kind::Other {
        return Err(Error::InvalidStoreKey {
            key: key.to_string(),
        }
        .into());
    }
    let key = key.to_string();
    backend::blocking(move || {
        let bytes = match source {
            Source::Bytes(bytes) => bytes,
            Source::File(path) => {
                let file = std::fs::File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                let size = file.metadata()?.len();
                if size > PART {
                    let mut check = Check::new(&key, size)?;
                    read_chunks(file, &path, |chunk| {
                        check.update(chunk);
                        Ok(())
                    })?;
                    check.finish()?;
                    return Ok(Prepared::File { path, size });
                }
                let mut bytes = Vec::with_capacity(size as usize);
                std::io::Read::read_to_end(&mut std::io::Read::take(file, PART + 1), &mut bytes)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                bytes
            }
        };
        layout::verify(&key, &bytes)?;
        let mut hasher = Hasher::new(HashFunction::Md5);
        hasher.update(&bytes);
        let md5 = hasher.finalize().to_string();
        Ok(Prepared::Bytes { bytes, md5 })
    })
    .await
}

/// Every chunk of `file` to `each`, until it ends. Blocks.
fn read_chunks(
    mut file: std::fs::File,
    path: &std::path::Path,
    mut each: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = match std::io::Read::read(&mut file, &mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(anyhow::Error::from(e))
                    .with_context(|| format!("failed to read {}", path.display()))
            }
        };
        each(&buf[..n])?;
    }
}

/// Upload the file at `path` to `location` by multipart upload, checking it
/// against `key` as it goes: the upload completes only if every byte sent
/// is what `key` names, and is aborted otherwise.
async fn upload(
    store: &dyn ObjectStore,
    location: &StorePath,
    key: &str,
    path: PathBuf,
    size: u64,
) -> Result<()> {
    let multipart = store
        .put_multipart(location)
        .await
        .with_context(|| format!("failed to write {key}"))?;
    let mut writer = WriteMultipart::new_with_chunk_size(multipart, PART as usize);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let checked_key = key.to_string();
    let reading = backend::blocking(move || {
        let mut check = Check::new(&checked_key, size)?;
        let file = std::fs::File::open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        read_chunks(file, &path, |chunk| {
            check.update(chunk);
            tx.blocking_send(chunk.to_vec())
                .map_err(|_| anyhow::anyhow!("the upload of {checked_key} stopped"))
        })?;
        check.finish()
    });
    let sending = async {
        while let Some(chunk) = rx.recv().await {
            writer.wait_for_capacity(4).await?;
            writer.write(&chunk);
        }
        Ok::<(), object_store::Error>(())
    };
    let (read, sent) = tokio::join!(reading, sending);
    let failed = match (read, sent) {
        (Err(e), _) => Some(e),
        (Ok(()), Err(e)) => Some(anyhow::Error::from(e).context(format!("failed to write {key}"))),
        (Ok(()), Ok(())) => None,
    };
    if let Some(e) = failed {
        let _ = writer.abort().await;
        return Err(e);
    }
    writer
        .finish()
        .await
        .with_context(|| format!("failed to write {key}"))?;
    Ok(())
}

/// What [`quarantine`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Quarantined {
    /// The damaged bytes were moved to `quarantined`; the key is absent.
    Moved { quarantined: String },
    /// Read again, the file was good: nothing was moved.
    Good,
    /// There was no such file.
    Absent,
}

/// Move a damaged object or manifest aside to [quarantine](self), leaving
/// its name absent. The file is read again first: a good one is left
/// ([`Quarantined::Good`]), an unreadable one too ([`Error::Unreadable`]).
/// A record is refused ([`Error::RecordKept`]): records are never made
/// absent, only [`replace`]d.
pub fn quarantine(store: &dyn ObjectStore, key: &str) -> Result<Quarantined> {
    block_on("integrity::quarantine", quarantine_async(store, key))?
}

/// [`quarantine`] on the caller's tokio runtime (see [the module
/// docs](super)).
pub async fn quarantine_async(store: &dyn ObjectStore, key: &str) -> Result<Quarantined> {
    match layout::kind(key) {
        Kind::Object | Kind::Manifest => {}
        Kind::Record => {
            return Err(Error::RecordKept {
                key: key.to_string(),
            }
            .into())
        }
        Kind::Other => {
            return Err(Error::InvalidStoreKey {
                key: key.to_string(),
            }
            .into())
        }
    }
    let location = store_path(key)?;
    match examine(store, &location).await {
        State::Good => Ok(Quarantined::Good),
        State::Absent => Ok(Quarantined::Absent),
        State::Unreadable(reason) => Err(Error::Unreadable {
            key: key.to_string(),
            reason,
        }
        .into()),
        State::Damaged => {
            let to = quarantine_key(key);
            store
                .rename(&location, &store_path(&to)?)
                .await
                .with_context(|| format!("failed to quarantine {key}"))?;
            Ok(Quarantined::Moved { quarantined: to })
        }
    }
}

/// `quarantine/<key>.<UTC now, colon-free>`. Times are distinct within a
/// process (a clock that ticks in microseconds, as macOS's does, is moved
/// on by a nanosecond), so two heals never quarantine to one name: two
/// `LocalFileSystem` copies to one name can leave a staging link behind.
fn quarantine_key(key: &str) -> String {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST: AtomicI64 = AtomicI64::new(i64::MIN);
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
    let mut last = LAST.load(Ordering::Relaxed);
    let nanos = loop {
        let next = now.max(last.saturating_add(1));
        match LAST.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break next,
            Err(seen) => last = seen,
        }
    };
    let time = chrono::DateTime::from_timestamp_nanos(nanos).format("%Y%m%dT%H%M%S%.9fZ");
    format!("{QUARANTINE}{key}.{time}")
}

/// `key`, in a store's encoded form, as an object_store path.
fn store_path(key: &str) -> Result<StorePath> {
    StorePath::parse(key).map_err(|_| {
        Error::InvalidStoreKey {
            key: key.to_string(),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_objects_or_manifests_md5_etag_proves_it() {
        let md5 = "d41d8cd98f00b204e9800998ecf8427e";
        let object = format!("files/md5/d4/{}", &md5[2..]);
        let manifest = format!("{object}.dir");
        for key in [&object, &manifest] {
            assert!(etag_proves(key, Some(&format!("\"{md5}\""))));
            assert!(etag_proves(key, Some(&md5.to_uppercase())));
            assert!(etag_proves(key, Some(&format!("W/\"{md5}\""))));
            assert!(!etag_proves(key, Some(&format!("\"{md5}-2\""))));
            assert!(!etag_proves(key, None));
        }
        let record = format!("bigstore-history/k/root/{}.dvc", "d".repeat(32));
        assert!(!etag_proves(&record, Some(md5)));
    }

    #[test]
    fn quarantine_keys_are_other_and_colon_free() {
        let key = format!("bigstore-history/a%7Eb/root/{}.dvc", "d".repeat(32));
        let q = quarantine_key(&key);
        assert!(q.starts_with(&format!("quarantine/{key}.")), "{q}");
        assert!(!q.contains(':'));
        assert_eq!(layout::kind(&q), Kind::Other);
        assert_eq!(store_path(&q).unwrap().as_ref(), q);
    }
}
