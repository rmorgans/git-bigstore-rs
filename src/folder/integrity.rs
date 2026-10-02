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
//! Objects up to [`SINGLE_PUT_MAX`](backend::SINGLE_PUT_MAX) (1 GiB) are
//! written with one PUT, so on S3 their ETag is their md5 and a scrub
//! proves them from the listing alone. A larger object goes up in parts:
//! its ETag never is its md5, so it is read back whole when written and
//! again on every S3 scrub.
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
use crate::backend::{self, etag_value, SINGLE_PUT_MAX};
use crate::hash::Hasher;
use crate::types::HashFunction;

/// Where quarantined bytes go, relative to the store.
pub const QUARANTINE: &str = "quarantine/";

/// The part size of a multipart replace.
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
/// and checked against its name. So on S3 an object written by multipart
/// upload (over 1 GiB, [the module docs](self)) is read whole on every
/// scrub. A listing that fails is the error; a file that fails is
/// [`Unreadable`] in the report. Reads only.
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
            // LocalFileSystem yields empty chunks forever once a file is
            // shorter than it was when opened: that is the end of it.
            if chunk.as_ref().is_empty() {
                break;
            }
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
/// [quarantine](self) first (on a `LocalFileSystem`, a hard link: the
/// store must be on a filesystem that has them). Then `source` replaces it
/// in one atomic write (on a `LocalFileSystem`, a temp file renamed over
/// the name): one PUT up to 1 GiB, held in memory meanwhile within the
/// process's single-PUT budget ([`SINGLE_PUT_MAX`]), else a multipart
/// upload, streamed from a file or sent from `Source::Bytes` a part at a
/// time ([the module docs](self)). Last, the write is checked: an ETag
/// equal to the bytes' md5, else the file read back and verified: not
/// what its name says (or gone), [`Error::WriteUnverified`]; unreadable,
/// [`Error::Unreadable`] (the write may be fine; a later scrub settles
/// it).
///
/// Replacing a content-addressed name with bytes that verify is safe
/// whatever is there, so concurrent heals of one key converge on good
/// bytes.
pub fn replace(store: &dyn ObjectStore, key: &str, source: Source) -> Result<Replaced> {
    block_on("integrity::replace", replace_async(store, key, source))?
}

/// [`replace`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn replace_async(store: &dyn ObjectStore, key: &str, source: Source) -> Result<Replaced> {
    replace_with(store, key, source, SINGLE_PUT_MAX).await
}

/// [`replace_async`], with sources over `single_max` bytes uploaded in
/// parts.
async fn replace_with(
    store: &dyn ObjectStore,
    key: &str,
    source: Source,
    single_max: u64,
) -> Result<Replaced> {
    let location = store_path(key)?;
    let size = match &source {
        Source::Bytes(bytes) => bytes.len() as u64,
        Source::File(path) => tokio::fs::metadata(path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?
            .len(),
    };
    let budget = match size <= single_max {
        true => Some(backend::put_budget(size).await),
        false => None,
    };
    let prepared = prepare(key, source, single_max).await?;
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
            drop(budget);
            put.e_tag
                .is_some_and(|etag| etag_value(&etag).eq_ignore_ascii_case(&md5))
        }
        Prepared::Large { body, size } => {
            upload(store, &location, key, body, size).await?;
            false
        }
    };
    if !written {
        match examine(store, &location).await {
            State::Good => {}
            State::Damaged | State::Absent => {
                return Err(Error::WriteUnverified {
                    key: key.to_string(),
                }
                .into())
            }
            // The write may be fine: a later scrub settles it.
            State::Unreadable(reason) => {
                return Err(Error::Unreadable {
                    key: key.to_string(),
                    reason,
                }
                .into())
            }
        }
    }
    Ok(match quarantined {
        Some(quarantined) => Replaced::Replaced { quarantined },
        None => Replaced::Placed,
    })
}

/// A source checked against its key.
enum Prepared {
    /// Its bytes, and their md5: one PUT.
    Bytes { bytes: Vec<u8>, md5: String },
    /// A source over the single-PUT ceiling, `size` bytes when checked:
    /// multipart.
    Large { body: Large, size: u64 },
}

/// What a multipart replace sends.
enum Large {
    /// A file, checked again as it is read.
    File(PathBuf),
    /// Bytes already checked.
    Bytes(Vec<u8>),
}

async fn prepare(key: &str, source: Source, single_max: u64) -> Result<Prepared> {
    if layout::kind(key) == Kind::Other {
        return Err(Error::InvalidStoreKey {
            key: key.to_string(),
        }
        .into());
    }
    let key = key.to_string();
    backend::blocking(move || {
        let bytes = match source {
            Source::Bytes(bytes) if bytes.len() as u64 > single_max => {
                let size = bytes.len() as u64;
                let mut check = Check::new(&key, size)?;
                check.update(&bytes);
                check.finish()?;
                return Ok(Prepared::Large {
                    body: Large::Bytes(bytes),
                    size,
                });
            }
            Source::Bytes(bytes) => bytes,
            Source::File(path) => {
                let file = std::fs::File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                let size = file.metadata()?.len();
                if size > single_max {
                    let mut check = Check::new(&key, size)?;
                    read_chunks(file, &path, |chunk| {
                        check.update(chunk);
                        Ok(())
                    })?;
                    check.finish()?;
                    return Ok(Prepared::Large {
                        body: Large::File(path),
                        size,
                    });
                }
                let mut bytes = Vec::with_capacity(size as usize);
                let mut limited = std::io::Read::take(file, single_max + 1);
                std::io::Read::read_to_end(&mut limited, &mut bytes)
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

/// Upload `body` to `location` by multipart upload; a file is checked
/// against `key` again as it is read, so the upload completes only if
/// every byte sent is what `key` names, and is aborted otherwise.
async fn upload(
    store: &dyn ObjectStore,
    location: &StorePath,
    key: &str,
    body: Large,
    size: u64,
) -> Result<()> {
    let multipart = store
        .put_multipart(location)
        .await
        .with_context(|| format!("failed to write {key}"))?;
    let mut writer = WriteMultipart::new_with_chunk_size(multipart, PART as usize);
    let sent = match body {
        Large::File(path) => send_file(&mut writer, key, path, size).await,
        Large::Bytes(bytes) => send_bytes(&mut writer, &bytes)
            .await
            .map_err(|e| anyhow::Error::from(e).context(format!("failed to write {key}"))),
    };
    if let Err(e) = sent {
        let _ = writer.abort().await;
        return Err(e);
    }
    writer
        .finish()
        .await
        .with_context(|| format!("failed to write {key}"))?;
    Ok(())
}

/// Send `bytes` through `writer`, a mebibyte at a time.
async fn send_bytes(writer: &mut WriteMultipart, bytes: &[u8]) -> object_store::Result<()> {
    for chunk in bytes.chunks(1 << 20) {
        writer.wait_for_capacity(4).await?;
        writer.write(chunk);
    }
    Ok(())
}

/// Send the file at `path` through `writer`, checking it against `key`
/// (`size` bytes) as it is read on the blocking pool.
async fn send_file(writer: &mut WriteMultipart, key: &str, path: PathBuf, size: u64) -> Result<()> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
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
    // The receiver lives in the sending loop: when a part fails, it is
    // dropped with the loop, so the reader stops instead of blocking on a
    // full channel.
    let sending = async {
        let mut rx = rx;
        while let Some(chunk) = rx.recv().await {
            writer.wait_for_capacity(4).await?;
            writer.write(&chunk);
        }
        Ok::<(), object_store::Error>(())
    };
    let (read, sent) = tokio::join!(reading, sending);
    // A failed send stops the reader, so its error is the cause.
    match (read, sent) {
        (_, Err(e)) => Err(anyhow::Error::from(e).context(format!("failed to write {key}"))),
        (Err(e), Ok(())) => Err(e),
        (Ok(()), Ok(())) => Ok(()),
    }
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
///
/// The move is the store's rename: on a `LocalFileSystem` one rename; on
/// S3 a copy to quarantine, then a delete of the original. So on S3 it
/// needs permission to delete: without it the copy lands, the delete
/// fails, the damaged file stays under its name, and each attempt adds
/// another quarantine copy. Do not call it on a store that cannot delete.
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

    /// `n` bytes of a pattern that does not repeat in step with a part.
    fn patterned(n: u32) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    fn object_key(bytes: &[u8]) -> String {
        let mut hasher = Hasher::new(HashFunction::Md5);
        hasher.update(bytes);
        let md5 = hasher.finalize().to_string();
        format!("files/md5/{}/{}", &md5[..2], &md5[2..])
    }

    /// Sources over the single-PUT ceiling (1 MiB here, 1 GiB for real)
    /// go up in parts, checked as they go and read back after.
    const CEILING: u64 = 1 << 20;

    #[tokio::test]
    async fn a_source_over_the_ceiling_is_a_checked_multipart_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("store");
        let big = patterned(11 << 20);
        let key = object_key(&big);
        let on_disk = root.join(&key);
        std::fs::create_dir_all(on_disk.parent().unwrap()).unwrap();
        std::fs::write(&on_disk, &big[..big.len() - 1]).unwrap();
        let store = backend::store::build_local_store(root.to_str().unwrap()).unwrap();

        // A file that is not the object is refused, the damaged copy kept.
        let wrong = tmp.path().join("wrong");
        let mut other = big.clone();
        other[5 << 20] ^= 1;
        std::fs::write(&wrong, &other).unwrap();
        let err = replace_with(&*store, &key, Source::File(wrong), CEILING)
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<Error>(),
            Some(Error::Integrity { .. })
        ));
        assert_eq!(std::fs::read(&on_disk).unwrap().len(), big.len() - 1);

        let good = tmp.path().join("good");
        std::fs::write(&good, &big).unwrap();
        let healed = replace_with(&*store, &key, Source::File(good), CEILING)
            .await
            .unwrap();
        assert!(matches!(healed, Replaced::Replaced { .. }), "{healed:?}");
        assert!(std::fs::read(&on_disk).unwrap() == big);
        let temps = walkdir::WalkDir::new(&root)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().contains('#'))
            .count();
        assert_eq!(temps, 0);
    }

    #[tokio::test]
    async fn a_failed_part_ends_a_multipart_replace_with_an_error_and_an_abort() {
        let tmp = tempfile::tempdir().unwrap();
        // Six 10 MiB parts: more than the four in flight at once.
        let big = patterned(55 << 20);
        let key = object_key(&big);
        let source = tmp.path().join("good");
        std::fs::write(&source, &big).unwrap();
        let store = backend::testing::WriteFaults {
            fail_part: Some(1),
            ..Default::default()
        };
        let location = store_path(&key).unwrap();
        store
            .inner
            .put(&location, b"damaged".to_vec().into())
            .await
            .unwrap();
        let err = replace_with(&store, &key, Source::File(source), CEILING)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("part failed"), "{err:#}");
        assert_eq!(store.aborted.load(std::sync::atomic::Ordering::SeqCst), 1);
        let kept = store.inner.get(&location).await.unwrap().bytes().await;
        assert_eq!(&kept.unwrap()[..], b"damaged");
    }

    #[tokio::test]
    async fn bytes_over_the_ceiling_go_up_in_parts_and_are_read_back() {
        use std::sync::atomic::Ordering;
        let big = patterned(3 << 20);
        let key = object_key(&big);
        let store = backend::testing::WriteFaults::default();
        let placed = replace_with(&store, &key, Source::Bytes(big.clone()), CEILING)
            .await
            .unwrap();
        assert_eq!(placed, Replaced::Placed);
        assert_eq!(store.multiparts.load(Ordering::SeqCst), 1);
        // One read finding it absent, one reading it back.
        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
        let stored = store.inner.get(&store_path(&key).unwrap()).await.unwrap();
        assert!(stored.bytes().await.unwrap() == big);

        // A provider that damages what it keeps: the read back says so.
        let damaging = backend::testing::WriteFaults {
            corrupt: true,
            ..Default::default()
        };
        let err = replace_with(&damaging, &key, Source::Bytes(big), CEILING)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<Error>(),
                Some(Error::WriteUnverified { .. })
            ),
            "{err:#}"
        );
    }
}
