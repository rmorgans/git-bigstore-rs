//! Git-free, DVC-compatible backup of plain folders.
//!
//! [`push`] backs up one *output* — a directory or a single file — to a
//! remote in DVC 3's layout (`files/md5/xx/rest`, plus a `.dir` manifest for
//! directories), records it as a version in its history on the remote, and
//! writes a DVC 3 `.dvc` pointer next to it naming that version as its
//! *base*. [`pull`] restores an output from a pointer or from history. Real
//! DVC can `dvc pull` what this writes, and this can pull what `dvc push`
//! wrote.
//!
//! History is a graph: each version names the versions it follows, and a
//! push follows the output's base, which must be the latest version (the
//! only *head*). Pushes from one base that race each other both land, as a
//! fork that push reports and pull, status and the next push refuse to
//! guess through; a push with [`Resolve::Merge`] joins it again.
//!
//! Every call here comes in two forms with the same arguments and results.
//! [`push`], [`status`], [`pull`], [`log`], [`keys`] and [`verify`] block:
//! each runs its own tokio runtime, so it must not be called from inside
//! one (that returns an error naming the async form, rather than
//! panicking). [`push_async`], [`status_async`], [`pull_async`],
//! [`log_async`], [`keys_async`] and [`verify_async`] run on the caller's
//! tokio runtime instead, which must have the I/O and time drivers enabled
//! (`Builder::enable_all`, as `#[tokio::main]` and `#[tokio::test]` do):
//! the remote's HTTP client needs both. A `current_thread` runtime works as
//! well as a `multi_thread` one.
//! Their futures are `Send`, so they can be `tokio::spawn`ed with owned
//! arguments moved in. Filesystem and hashing work (walking, snapshotting,
//! classifying, writing and placing files) runs on the runtime's blocking
//! pool, never on the thread polling the future.
//!
//! Dropping a future stops the call like a crash at that point, never
//! leaving a partial file or object: hashing it started stops at the next
//! file, a downloaded file already being placed is placed whole, and
//! uploads and downloads under way are abandoned (an S3 multipart upload
//! may be left for the bucket's lifecycle rule to expire). A push dropped
//! after publishing its history record may not have written its `.dvc`;
//! the next push of the same content adopts the record rather than add
//! another. A [`CancelToken`] stops more gently: transfers under way finish
//! first. [`Remote::open`] makes no request (for `local://` it only creates
//! the directory) and has one form.
//!
//! [`layout`] names a store's files and checks them against their names;
//! [`exchange`] copies them between two stores over one byte stream (an
//! `ssh` session), and only blocks.
//!
//! Nothing here calls git.

mod completeness;
mod error;
pub mod exchange;
mod history;
pub mod layout;
mod snapshot;
mod walk;

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub use crate::backend::store::Credentials;
use crate::backend::{self, Store};
use crate::cache::WorktreeMode;
use crate::config::{BackendConfig, BigstoreConfig};
use crate::dvc::{BigstoreMeta, DvcOutput, DvcPointer, Manifest, ManifestEntry, RecordId};
use crate::types::{check_portable_component, long_path, Hexdigest, Layout, ManifestPath};
pub use completeness::{verify, verify_async, Completeness};
pub use error::{Error, Refusal};
pub use history::{
    keys, keys_async, log, log_async, HistoryKey, HistoryRecord, LogOptions, Selector,
};
pub use walk::{Excludes, DEFAULT_EXCLUDES};

use layout::MAX_MANIFEST_BYTES;
use snapshot::{Snapshot, SnapshotError};
use walk::WalkError;

/// How often push restarts when files change under it.
const PUSH_ATTEMPTS: usize = 3;

// ──────────────────────────────────────────────────
// Remote
// ──────────────────────────────────────────────────

/// Where a remote lives. Nothing is read from per-folder files; the caller
/// owns this configuration.
#[derive(Debug, Clone)]
pub struct RemoteConfig {
    /// `s3://bucket/prefix`, or `local:///path` / `rclone://remote:path`.
    pub url: String,
    /// Required for `s3://`: this mode never defaults to AWS.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub credentials: Credentials,
}

/// An opened remote. Only built by [`Remote::open`], which enforces the
/// endpoint and credential policy.
pub struct Remote {
    store: Store,
    prefix: String,
}

impl Remote {
    /// Refuses, as [`Error::UnsupportedRemote`], any URL but `s3://`,
    /// `local://` (`file://`) and `rclone://`; as [`Error::EndpointRequired`],
    /// `s3://` without an endpoint; and as [`Error::CredentialsMissing`],
    /// `s3://` without credentials. Any other failure to make a client is
    /// [`Error::RemoteUnusable`].
    pub fn open(config: &RemoteConfig) -> Result<Self> {
        let unsupported = || Error::UnsupportedRemote {
            url: config.url.clone(),
        };
        let unusable = || Error::RemoteUnusable {
            url: config.url.clone(),
        };
        let cfg = match config.url.split_once("://") {
            Some(("s3" | "local" | "file" | "rclone", _)) => {
                BigstoreConfig::from_url(&config.url, None).with_context(unusable)?
            }
            _ => return Err(unsupported().into()),
        };
        let (store, prefix) = match &cfg.backend {
            BackendConfig::S3 { bucket, prefix, .. } => {
                let endpoint = config.endpoint.as_deref().ok_or(Error::EndpointRequired)?;
                let store = backend::store::build_strict_s3(
                    bucket,
                    endpoint,
                    config.region.as_deref(),
                    &config.credentials,
                )
                .map_err(|err| match err.downcast_ref::<backend::Error>() {
                    Some(backend::Error::CredentialsMissing) => Error::CredentialsMissing.into(),
                    _ => err.context(unusable()),
                })?;
                (Store::from_object_store(store.into()), prefix.clone())
            }
            BackendConfig::Local { .. } | BackendConfig::Rclone { .. } => {
                (Store::open(&cfg).with_context(unusable)?, String::new())
            }
            _ => return Err(unsupported().into()),
        };
        Ok(Self { store, prefix })
    }

    fn key(&self, rel: &str) -> String {
        match self.prefix.trim_end_matches('/') {
            "" => rel.to_string(),
            p => format!("{p}/{rel}"),
        }
    }

    fn object_key(&self, md5: &Hexdigest) -> String {
        let key = Layout::default()
            .object_key(md5)
            .expect("the default layout addresses md5");
        self.key(&key)
    }

    fn manifest_key(&self, id: &Hexdigest) -> String {
        format!("{}.dir", self.object_key(id))
    }
}

/// `err` as [`Error::Archived`] if a read failed because the object is
/// archived, otherwise unchanged.
fn archived(err: anyhow::Error) -> anyhow::Error {
    match err.downcast_ref::<backend::Error>() {
        Some(backend::Error::Archived { key }) => Error::Archived { key: key.clone() }.into(),
        _ => err,
    }
}

/// Run `fut`, the future of `bigstore::folder::{name}_async`, on a private
/// runtime for the blocking `name`. Refuses (instead of panicking) when the
/// caller is already inside a tokio runtime.
fn block_on<F: Future>(name: &str, fut: F) -> Result<F::Output> {
    anyhow::ensure!(
        tokio::runtime::Handle::try_current().is_err(),
        "bigstore::folder::{name} blocks, so it cannot run inside a tokio runtime; \
         await bigstore::folder::{name}_async instead"
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start async runtime")?;
    Ok(rt.block_on(fut))
}

// Concurrent work made by a closure (`stream::iter(..).map(|x| async ..)`) is
// built by these plain fns, never inline in an async fn: held across an
// `.await` there, rustc cannot prove the closure's future `Send` for every
// lifetime ("implementation of `FnOnce` is not general enough"), and the
// public futures could not be spawned. The opaque return types declare
// `Send` instead.

/// `f` over `items`, at most `jobs` at a time; results in `items`' order.
fn each_in_order<'a, I, F, Fut, T>(
    items: I,
    jobs: usize,
    f: F,
) -> impl Future<Output = Result<Vec<T>>> + Send + 'a
where
    I: IntoIterator,
    I::IntoIter: Send + 'a,
    F: FnMut(I::Item) -> Fut + Send + 'a,
    Fut: Future<Output = Result<T>> + Send + 'a,
    T: Send + 'a,
{
    use futures::stream::{self, StreamExt, TryStreamExt};
    stream::iter(items)
        .map(f)
        .buffered(jobs.max(1))
        .try_collect()
}

/// `f` over `items`, at most `jobs` at a time; results as they finish.
fn each_unordered<'a, I, F, Fut, T>(
    items: I,
    jobs: usize,
    f: F,
) -> impl Future<Output = Result<Vec<T>>> + Send + 'a
where
    I: IntoIterator,
    I::IntoIter: Send + 'a,
    F: FnMut(I::Item) -> Fut + Send + 'a,
    Fut: Future<Output = Result<T>> + Send + 'a,
    T: Send + 'a,
{
    use futures::stream::{self, StreamExt, TryStreamExt};
    stream::iter(items)
        .map(f)
        .buffer_unordered(jobs.max(1))
        .try_collect()
}

// ──────────────────────────────────────────────────
// Cancellation
// ──────────────────────────────────────────────────

/// Stops a push, status, pull or log from another thread. Clones share one
/// flag; the default token is never cancelled (nobody else holds it).
/// Checked between files, objects and history records: a file being
/// hashed, uploaded or downloaded when it is cancelled is finished first. A
/// cancelled call returns [`Error::Cancelled`]. Dropping an async call's
/// future stops it less gently ([the module docs](self) say how).
#[derive(Debug, Clone, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
    /// The token this one is a [`child`](Self::child) of: cancelling that
    /// cancels this one.
    parent: Option<Box<CancelToken>>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel every call holding a clone of this token. Idempotent.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed) || self.parent.as_ref().is_some_and(|p| p.is_cancelled())
    }

    /// A token cancelled with this one, or on its own without cancelling
    /// this one.
    fn child(&self) -> Self {
        Self {
            flag: Arc::default(),
            parent: Some(Box::new(self.clone())),
        }
    }

    fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(Error::Cancelled.into());
        }
        Ok(())
    }
}

/// Run blocking work that checks a cancel token between files on the
/// blocking pool. `work` gets a child of `cancel` that is also cancelled
/// when the future awaiting it is dropped, so it stops at its next check
/// instead of running on unobserved.
async fn unblock<T, F>(cancel: &CancelToken, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&CancelToken) -> Result<T> + Send + 'static,
{
    struct CancelOnDrop(CancelToken);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    let guard = CancelOnDrop(cancel.child());
    let token = guard.0.clone();
    let result = backend::blocking(move || work(&token)).await;
    drop(guard);
    result
}

/// Something whose drop deletes files (snapshot copies and their temp
/// directory), held so that the deleting happens on the blocking pool:
/// awaited in [`Self::delete`], or handed to the pool by `Drop` if the
/// future holding it is dropped first.
struct Scratch<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> Scratch<T> {
    fn get(&self) -> &T {
        self.0.as_ref().expect("present until deleted")
    }

    fn get_mut(&mut self) -> &mut T {
        self.0.as_mut().expect("present until deleted")
    }

    async fn delete(mut self) {
        if let Some(files) = self.0.take() {
            // Dropping never fails; an error here only means the runtime is
            // shutting down, which drops (so deletes) them anyway.
            let _ = backend::blocking(move || {
                drop(files);
                Ok(())
            })
            .await;
        }
    }
}

impl<T: Send + 'static> Drop for Scratch<T> {
    fn drop(&mut self) {
        if let Some(files) = self.0.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(rt) => drop(rt.spawn_blocking(move || drop(files))),
                Err(_) => drop(files),
            }
        }
    }
}

// ──────────────────────────────────────────────────
// Progress
// ──────────────────────────────────────────────────

/// A stretch of work a progress display can show as one bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Phase {
    /// Push and status: snapshotting and hashing the output. Pull: hashing
    /// the local files it may replace.
    Hashing,
    /// Push: uploading contents the remote lacks.
    Uploading,
    /// Pull: downloading and placing files.
    Downloading,
}

/// What [`Progress`] is told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProgressEvent {
    /// A phase starts, with its totals: files, and bytes when known up
    /// front (a directory's download size is not). A push that restarts
    /// because files changed under it starts [`Phase::Hashing`] again.
    Started {
        phase: Phase,
        files: u64,
        bytes: Option<u64>,
    },
    /// Files finished in a phase, and their size. Uploads count distinct
    /// contents; a download counts every file written from one object.
    Advanced {
        phase: Phase,
        files: u64,
        bytes: u64,
    },
}

/// A progress callback for push, status and pull: one event per phase
/// start and per finished file. It runs on worker threads, possibly several
/// at once, so it must be `Send + Sync`, and should return quickly. The
/// default reports nothing and costs nothing.
#[derive(Clone, Default)]
pub struct Progress(Option<Arc<dyn Fn(ProgressEvent) + Send + Sync>>);

impl Progress {
    pub fn new(report: impl Fn(ProgressEvent) + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(report)))
    }

    /// Report `event()`, built only if anyone is listening.
    fn emit(&self, event: impl FnOnce() -> ProgressEvent) {
        if let Some(report) = &self.0 {
            report(event());
        }
    }
}

impl std::fmt::Debug for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = if self.0.is_some() { "set" } else { "none" };
        write!(f, "Progress({state})")
    }
}

// ──────────────────────────────────────────────────
// Push
// ──────────────────────────────────────────────────

/// How to push. Build with [`PushOptions::new`] and set what differs:
/// `PushOptions { jobs: 16, ..PushOptions::new(key) }`.
#[derive(Debug, Clone)]
pub struct PushOptions {
    pub history: HistoryKey,
    /// Concurrent uploads (at least 1).
    pub jobs: usize,
    /// Entries of a directory output to skip; always includes
    /// [`DEFAULT_EXCLUDES`]. Not applied to a single-file output.
    pub exclude: Excludes,
    /// Stops the push before it publishes its history record; after that it
    /// completes.
    pub cancel: CancelToken,
    pub progress: Progress,
    /// What to do when the history has forked.
    pub resolve: Resolve,
    /// Who pushes, recorded in each version: this host's name by default.
    pub writer: String,
    /// The directory the output must stay inside. When set, the output
    /// path given to [`push`] and [`status`] is relative to it, of plain
    /// names only, and nothing from `root` down to the output, nor the
    /// output or its `.dvc`, may be a symlink, junction or other reparse
    /// point ([`Refusal::OutsideRoot`], [`Refusal::SymlinkedComponent`]).
    /// `root` itself may be one. Inside a directory output, a symlink to a
    /// file is still backed up with its target's content, as DVC does.
    /// `None` (the default): the output path is used as given.
    pub root: Option<PathBuf>,
    /// Repair a version whose content the remote lost: never trust a
    /// `.dir` manifest on the remote to mean its objects are there, but ask
    /// for every object and the manifest, and upload whatever is missing.
    /// An output equal to the latest version then publishes nothing
    /// ([`Pushed::AlreadyLatest`]), writes its `.dvc`, and reports the
    /// objects restored as `uploaded`. Otherwise a push as usual. Status
    /// with it counts what such a push would upload. Off by default.
    pub repair: bool,
}

impl PushOptions {
    /// Push to `history` with 8 jobs, the default excludes, a token nobody
    /// else can cancel, no progress reports, forks refused, as this host,
    /// with no root and no repair.
    pub fn new(history: HistoryKey) -> Self {
        Self {
            history,
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
            exclude: Excludes::default(),
            cancel: CancelToken::default(),
            progress: Progress::default(),
            resolve: Resolve::default(),
            writer: gethostname::gethostname().to_string_lossy().into_owned(),
            root: None,
            repair: false,
        }
    }
}

/// What push does when the history has forked (several heads).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resolve {
    /// Refuse, as [`Error::Diverged`].
    #[default]
    Refuse,
    /// Record the output as a merge following every head. Its base must be
    /// one of them: pull a head by id, reconcile the others into it, then
    /// push.
    Merge,
}

/// What [`push`] did. Look at [`outcome`](Self::outcome): a push that
/// forked the history published its version, but is not a plain success.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "a push may have forked the history: check `outcome`"]
pub struct PushReport {
    /// What the push did to history.
    pub outcome: Pushed,
    /// The pointer written, with the version as its base.
    pub pointer: DvcPointer,
    /// The `.dvc` file written (or already identical) next to the output.
    pub pointer_path: PathBuf,
    pub files: usize,
    /// Distinct contents uploaded (files with the same content count once).
    pub uploaded: usize,
    /// Distinct contents the remote already had.
    pub already_present: usize,
    /// Empty directories, which DVC cannot record.
    pub empty_dirs: usize,
    /// Non-fatal observations, e.g. a `.jsonl` without a final newline.
    pub warnings: Vec<String>,
    /// The version the output now is: the one published, or the head it
    /// already was.
    pub version: RecordId,
}

/// What a push did to its output's history.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Pushed {
    /// The output already was the latest version: nothing was published.
    AlreadyLatest,
    /// A version was published, under the remote key `record`, and is the
    /// latest.
    Published { record: String },
    /// A version was published under `record` and the `.dvc` written, but
    /// the versions `with` were published from the same base while this
    /// push ran: the history has forked. Until a push with
    /// [`Resolve::Merge`] joins the heads, every push of this output is
    /// [`Error::Diverged`], and so is a pull of [`Selector::Latest`].
    Forked { record: String, with: Vec<RecordId> },
}

impl Pushed {
    /// The remote key of the history record published, if one was.
    pub fn record(&self) -> Option<&str> {
        match self {
            Self::AlreadyLatest => None,
            Self::Published { record } | Self::Forked { record, .. } => Some(record),
        }
    }
}

/// Back up `output` (a directory or a single file).
///
/// History is checked first, from one listing, before anything is
/// uploaded. An output equal to the latest version publishes no version
/// (and becomes it, whatever its `.dvc` said). Otherwise the output must
/// follow the latest version: its base is that version or, in a `.dvc`
/// without a base (0.2 wrote it), the content recorded is that version's.
/// If not, it is [`Error::StaleBase`]: no `.dvc` while history has
/// versions, or another version landed since. It is [`Error::Diverged`] if
/// the history has forked, unless [`PushOptions::resolve`] merges it.
///
/// Then order is what makes this safe for DVC and for concurrent readers:
/// every object is uploaded first, then the `.dir` manifest (DVC treats a
/// present manifest as "all its objects are present"), then the history
/// record, under a name never written before, then the local `.dvc` with
/// the new base. A failure at any step leaves at most unreferenced objects
/// behind, or a record the next push adopts. Last, history is listed again
/// to report a push that raced this one ([`Pushed::Forked`]).
/// Files are snapshotted while hashed, so an append during the push can
/// never produce an object whose content does not match its key.
///
/// Anything push will not back up is refused as [`Error::Refused`] before
/// anything is published; an output that keeps changing through every retry
/// is [`Error::OutputChanged`].
pub fn push(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    block_on("push", push_async(remote, output, opts))?
}

/// [`push`] on the caller's tokio runtime (see [the module docs](self)).
pub async fn push_async(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    let output = &confine(opts.root.as_deref(), output).await?;
    let (name, pointer_path, local) = locate(output).await?;
    let mut scratch = stage(output, name, opts, |s| s).await?;
    let published = publish(
        remote,
        &mut scratch.get_mut().0,
        &pointer_path,
        local.as_ref(),
        opts,
    )
    .await;
    let staged = &scratch.get().0;
    let report = published.map(|p| PushReport {
        outcome: p.outcome,
        pointer: p.pointer,
        pointer_path,
        files: staged.files,
        uploaded: p.uploaded,
        already_present: p.already_present,
        empty_dirs: staged.empty_dirs,
        warnings: staged.warnings.clone(),
        version: p.version,
    });
    scratch.delete().await;
    report
}

/// What [`publish`] did.
struct Published {
    outcome: Pushed,
    pointer: DvcPointer,
    uploaded: usize,
    already_present: usize,
    version: RecordId,
}

/// Publish a staged output, whose `.dvc` is `local`, in push's order:
/// decide against history, then objects, manifest, history record, `.dvc`;
/// then look for a push that raced this one.
async fn publish(
    remote: &Remote,
    staged: &mut Staged<Snapshot>,
    pointer_path: &Path,
    local: Option<&DvcPointer>,
    opts: &PushOptions,
) -> Result<Published> {
    let jobs = opts.jobs.max(1);
    let next = history::next(
        remote,
        &opts.history,
        &staged.pointer.output,
        local,
        opts.resolve,
        jobs,
    )
    .await?;
    let plan = plan(remote, staged, jobs, opts.repair).await?;
    upload_all(remote, &plan.upload, opts).await?;
    let (uploaded, already_present, upload_manifest) =
        (plan.upload.len(), plan.present.len(), plan.manifest);
    if upload_manifest {
        opts.cancel.check()?;
        let key = remote.manifest_key(staged.id());
        let manifest = staged
            .manifest
            .take()
            .expect("planned only for a staged manifest");
        remote.store.put(&key, manifest).await?;
    }
    // The last point to stop: past it, the version is published.
    opts.cancel.check()?;
    let (version, published) = match next {
        history::Next::Adopt(head) => (head, None),
        history::Next::Publish { parents, after } => {
            let (id, key) = history::append(
                remote,
                &opts.history,
                &staged.pointer,
                parents.clone(),
                after,
                &opts.writer,
            )
            .await?;
            (id, Some((key, parents)))
        }
    };
    let pointer = DvcPointer {
        meta: Some(BigstoreMeta::Base(version.clone())),
        ..staged.pointer.clone()
    };
    write_pointer_file(pointer_path, &pointer).await?;
    let outcome = match published {
        Some((record, parents)) => {
            let with = history::forked_with(remote, &opts.history, &version, &parents).await?;
            if with.is_empty() {
                Pushed::Published { record }
            } else {
                Pushed::Forked { record, with }
            }
        }
        None => Pushed::AlreadyLatest,
    };
    Ok(Published {
        outcome,
        pointer,
        uploaded,
        already_present,
        version,
    })
}

/// What [`status`] found: what [`push`] would do with the same arguments.
#[derive(Debug)]
#[non_exhaustive]
pub struct StatusReport {
    /// The pointer push would write, less its base.
    pub pointer: DvcPointer,
    pub files: usize,
    /// Distinct contents push would upload (files with the same content count
    /// once), and their total size.
    pub to_upload: usize,
    pub to_upload_bytes: u64,
    /// Distinct contents the remote already has, and their total size.
    pub already_present: usize,
    pub already_present_bytes: u64,
    /// Empty directories, which DVC cannot record.
    pub empty_dirs: usize,
    /// Non-fatal observations, as push would report them.
    pub warnings: Vec<String>,
    /// How the output relates to the latest version in its history.
    pub sync: SyncState,
    /// The latest versions as status found them, sorted: none for an empty
    /// history, several for a fork.
    pub heads: Vec<RecordId>,
    /// Whether the output is [`SyncState::InSync`] and the `.dvc` beside it
    /// names that version as its base. An output in sync without it (copied
    /// in, or a crash between a push's record and its `.dvc`) is adopted by
    /// the next push, which writes the `.dvc` and publishes nothing.
    pub based: bool,
}

/// How a local output relates to the latest version in its history (its
/// head), judged by the `.dvc` beside it: its base, the version it was last
/// pushed or pulled as, and the content it had then.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncState {
    /// The history is empty: push would record the first version.
    NoHistory,
    /// The output is the latest version: push would add none. Push may
    /// still rewrite the `.dvc` beside it, if that is missing or records
    /// another base (say the output was copied in, not pulled).
    InSync,
    /// The output changed since the latest version, its base (or, in a
    /// `.dvc` without a base, the content it records): push would add a
    /// version.
    LocalAhead,
    /// The history has a newer version than the output's base, and the
    /// output has not changed since its `.dvc`: a pull with
    /// [`Overwrite::IfUnchanged`] catches up. Push refuses
    /// ([`Error::StaleBase`]).
    RemoteAhead { latest: HistoryRecord },
    /// The output changed, and the latest version is not its base (or it
    /// has none): push refuses ([`Error::StaleBase`]). Set the changes
    /// aside, pull, and redo them.
    Stale {
        base: Option<RecordId>,
        head: HistoryRecord,
    },
    /// The history has forked: several heads. Pull refuses `Latest` and
    /// push refuses ([`Error::Diverged`]) until a push with
    /// [`Resolve::Merge`] joins them.
    Diverged { heads: Vec<RecordId> },
}

/// What [`push`] would do, without doing it: `output` is walked, snapshotted
/// and hashed exactly as push does, and the remote is asked which contents
/// it has and what the heads of the history are (one listing, then the
/// heads fetched). Nothing is written to the remote or beside the output;
/// snapshots go to a private temp directory, one file at a time. Refuses
/// whatever push refuses.
pub fn status(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<StatusReport> {
    block_on("status", status_async(remote, output, opts))?
}

/// [`status`] on the caller's tokio runtime (see [the module docs](self)).
pub async fn status_async(
    remote: &Remote,
    output: &Path,
    opts: &PushOptions,
) -> Result<StatusReport> {
    let output = &confine(opts.root.as_deref(), output).await?;
    let (name, _, local) = locate(output).await?;
    let scratch = stage(output, name, opts, |s| Hashed {
        md5: s.md5().clone(),
        size: s.size(),
    })
    .await?;
    let staged = &scratch.get().0;
    let jobs = opts.jobs.max(1);
    let checked = async {
        let plan = plan(remote, staged, jobs, opts.repair).await?;
        Ok::<_, anyhow::Error>((plan, history::heads_of(remote, &opts.history, jobs).await?))
    }
    .await;
    let report = checked.map(|(plan, heads)| {
        let output = &staged.pointer.output;
        let base = history::base_of(local.as_ref());
        let ids = match &heads {
            history::Heads::None => Vec::new(),
            history::Heads::One(head) => vec![head.id.clone()],
            history::Heads::Many(heads) => heads.iter().map(|h| h.id.clone()).collect(),
        };
        let (sync, based) = match heads {
            history::Heads::None => (SyncState::NoHistory, false),
            history::Heads::One(head) if head.pointer.output == *output => {
                (SyncState::InSync, base == Some(&head.id))
            }
            history::Heads::One(head) if history::follows(&head, local.as_ref()) => {
                (SyncState::LocalAhead, false)
            }
            history::Heads::One(head) => match &local {
                Some(p) if p.output == *output => (SyncState::RemoteAhead { latest: *head }, false),
                _ => (
                    SyncState::Stale {
                        base: base.cloned(),
                        head: *head,
                    },
                    false,
                ),
            },
            history::Heads::Many(heads) => (
                SyncState::Diverged {
                    heads: heads.into_iter().map(|h| h.id).collect(),
                },
                false,
            ),
        };
        let bytes = |cs: &[&Hashed]| cs.iter().map(|c| c.size).sum();
        StatusReport {
            to_upload: plan.upload.len(),
            to_upload_bytes: bytes(&plan.upload),
            already_present: plan.present.len(),
            already_present_bytes: bytes(&plan.present),
            pointer: staged.pointer.clone(),
            files: staged.files,
            empty_dirs: staged.empty_dirs,
            warnings: staged.warnings.clone(),
            sync,
            heads: ids,
            based,
        }
    });
    scratch.delete().await;
    report
}

/// A private temp directory for snapshots: on unix only the owner may enter
/// it (the copies hold whatever the output holds); elsewhere the per-user
/// temp directory already is private.
fn snapshot_tmpdir() -> Result<tempfile::TempDir> {
    #[cfg(unix)]
    let dir = {
        use std::os::unix::fs::PermissionsExt;
        tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
    };
    #[cfg(not(unix))]
    let dir = tempfile::tempdir();
    dir.context("failed to create a temp dir")
}

/// The output's name, its `.dvc` path, and the pointer already there (push
/// refuses to replace anything else).
async fn locate(output: &Path) -> Result<(String, PathBuf, Option<DvcPointer>)> {
    let name = output_name(output)?;
    let pointer_path = pointer_path_for(output)?;
    backend::blocking(move || {
        let existing = check_existing_pointer(&pointer_path, &name)?;
        Ok((name, pointer_path, existing))
    })
    .await
}

/// Where `path` is: with a `root`, `path` is relative to it and confined
/// to it (see [`PushOptions::root`]); without one, `path` as given.
async fn confine(root: Option<&Path>, path: &Path) -> Result<PathBuf> {
    match root {
        None => Ok(path.to_path_buf()),
        Some(root) => {
            let (root, path) = (root.to_path_buf(), path.to_path_buf());
            backend::blocking(move || confine_in(&root, &path)).await
        }
    }
}

/// `root.join(rel)`, once `rel` is checked to be relative, plain names
/// only, and nothing from `root` down to it, nor it or `<it>.dvc`, is a
/// symlink, junction or other reparse point. Something other than a
/// directory on the way is refused; a directory that does not exist yet
/// ends the check (a pull creates it, and what it holds). Blocks.
fn confine_in(root: &Path, rel: &Path) -> Result<PathBuf> {
    use std::path::Component;
    let refused = |path: PathBuf, reason| Error::Refused { path, reason };
    let names = rel
        .components()
        .map(|c| match c {
            Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .filter(|names| !names.is_empty())
        .ok_or_else(|| refused(rel.to_path_buf(), Refusal::OutsideRoot))?;
    let full = root.join(rel);
    let mut cur = root.to_path_buf();
    for (i, name) in names.iter().enumerate() {
        cur.push(name);
        let last = i + 1 == names.len();
        let meta = match std::fs::symlink_metadata(&cur) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && last => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(full),
            Err(e) => return Err(e).with_context(|| format!("failed to stat {}", cur.display())),
        };
        if redirects(&meta) {
            return Err(refused(cur, Refusal::SymlinkedComponent).into());
        }
        if !last && !meta.is_dir() {
            return Err(refused(cur, Refusal::NotADirectory).into());
        }
    }
    let mut dvc = full.clone().into_os_string();
    dvc.push(".dvc");
    let dvc = PathBuf::from(dvc);
    match std::fs::symlink_metadata(&dvc) {
        Ok(meta) if redirects(&meta) => Err(refused(dvc, Refusal::SymlinkedComponent).into()),
        Ok(_) => Ok(full),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(full),
        Err(e) => Err(e).with_context(|| format!("failed to stat {}", dvc.display())),
    }
}

/// Whether this entry is a redirect the root confinement refuses: a symlink,
/// or on Windows any reparse point. That is stricter than needed: besides
/// symlinks, junctions and mount points, it refuses cloud placeholders
/// (OneDrive Files-On-Demand) and compressed or deduplicated files, which
/// lead nowhere else, on the way to the output.
fn redirects(meta: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return true;
        }
    }
    meta.file_type().is_symlink()
}

enum Retry {
    Changed(String),
    Fatal(anyhow::Error),
}

impl From<SnapshotError> for Retry {
    fn from(e: SnapshotError) -> Self {
        match e {
            SnapshotError::Changed(m) => Self::Changed(m),
            SnapshotError::Io(e) => Self::Fatal(e),
        }
    }
}

/// One distinct content of a staged output.
trait Content {
    fn md5(&self) -> &Hexdigest;
}

impl Content for Snapshot {
    fn md5(&self) -> &Hexdigest {
        Snapshot::md5(self)
    }
}

/// A snapshot's digest and size, the copy itself already deleted: all
/// status needs.
struct Hashed {
    md5: Hexdigest,
    size: u64,
}

impl Content for Hashed {
    fn md5(&self) -> &Hexdigest {
        &self.md5
    }
}

/// An output fully snapshotted and hashed: what push publishes.
struct Staged<C> {
    pointer: DvcPointer,
    /// A directory output's `.dir` manifest, serialized: the bytes its id is
    /// the md5 of.
    manifest: Option<Vec<u8>>,
    /// Distinct contents, by md5.
    contents: Vec<C>,
    files: usize,
    warnings: Vec<String>,
    empty_dirs: usize,
}

impl<C> Staged<C> {
    /// The version's id: manifest id or file md5.
    fn id(&self) -> &Hexdigest {
        match &self.pointer.output {
            DvcOutput::Dir { manifest, .. } => manifest,
            DvcOutput::File { md5, .. } => md5,
        }
    }
}

/// Snapshot `output` (named `name`) into a private temp directory, on the
/// blocking pool; see [`stage_in`]. The snapshots and their directory are
/// deleted with the [`Scratch`].
async fn stage<C: Send + 'static>(
    output: &Path,
    name: String,
    opts: &PushOptions,
    keep: fn(Snapshot) -> C,
) -> Result<Scratch<(Staged<C>, tempfile::TempDir)>> {
    let (output, owned) = (output.to_path_buf(), opts.clone());
    unblock(&opts.cancel, move |cancel| {
        let opts = PushOptions {
            cancel: cancel.clone(),
            ..owned
        };
        let tmp = snapshot_tmpdir()?;
        let staged = stage_in(&output, &name, tmp.path(), &opts, keep)?;
        Ok(Scratch(Some((staged, tmp))))
    })
    .await
}

/// Snapshot `output` (named `name`) into `tmp`, restarting while files
/// change under it; `keep` turns each snapshot into what the caller holds.
/// Blocks.
fn stage_in<C>(
    output: &Path,
    name: &str,
    tmp: &Path,
    opts: &PushOptions,
    keep: fn(Snapshot) -> C,
) -> Result<Staged<C>> {
    let meta = std::fs::symlink_metadata(output)
        .with_context(|| format!("failed to stat {}", output.display()))?;
    let mut last_change = String::new();
    for _ in 0..PUSH_ATTEMPTS {
        let attempt = if meta.is_dir() {
            snapshot_dir(output, name, tmp, opts, keep)
        } else if meta.is_file() {
            snapshot_file(output, name, tmp, opts, keep)
        } else {
            return Err(Error::Refused {
                path: output.to_path_buf(),
                reason: Refusal::NotFileOrDirectory,
            }
            .into());
        };
        match attempt {
            Ok(staged) => return Ok(staged),
            Err(Retry::Changed(msg)) => last_change = msg,
            Err(Retry::Fatal(e)) => return Err(e),
        }
    }
    Err(Error::OutputChanged {
        detail: last_change,
    }
    .into())
}

fn hashed(s: &Snapshot) -> ProgressEvent {
    ProgressEvent::Advanced {
        phase: Phase::Hashing,
        files: 1,
        bytes: s.size(),
    }
}

fn warn_unterminated(relpath: &str, s: &Snapshot, warnings: &mut Vec<String>) {
    if relpath.ends_with(".jsonl") && s.unterminated_line() {
        warnings.push(format!(
            "{relpath} does not end in a newline (a line was being written)"
        ));
    }
}

fn snapshot_dir<C>(
    dir: &Path,
    name: &str,
    tmp: &Path,
    opts: &PushOptions,
    keep: fn(Snapshot) -> C,
) -> std::result::Result<Staged<C>, Retry> {
    let walk = match walk::walk(dir, &opts.exclude) {
        Ok(w) => w,
        Err(WalkError::Changed(m)) => return Err(Retry::Changed(m)),
        Err(e) => return Err(Retry::Fatal(e.into())),
    };
    let files = walk.files.len();
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Hashing,
        files: files as u64,
        bytes: Some(
            walk.files
                .iter()
                .map(|f| std::fs::metadata(&f.path).map_or(0, |m| m.len()))
                .sum(),
        ),
    });
    let mut entries = Vec::with_capacity(files);
    let mut contents = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut size = 0;
    for f in walk.files {
        opts.cancel.check().map_err(Retry::Fatal)?;
        let s = snapshot::snapshot(&f.path, tmp)?;
        warn_unterminated(f.relpath.as_str(), &s, &mut warnings);
        opts.progress.emit(|| hashed(&s));
        size += s.size();
        entries.push(ManifestEntry {
            relpath: f.relpath.to_manifest_path(),
            md5: s.md5().clone(),
        });
        if !contents.contains_key(s.md5()) {
            contents.insert(s.md5().clone(), keep(s));
        }
    }
    let manifest = Manifest::from_entries(entries).map_err(Retry::Fatal)?;
    Ok(Staged {
        pointer: DvcPointer {
            output: DvcOutput::Dir {
                manifest: manifest.id(),
                size,
                nfiles: files as u64,
            },
            path: name.to_string(),
            meta: None,
        },
        manifest: Some(manifest.to_bytes()),
        contents: contents.into_values().collect(),
        files,
        warnings,
        empty_dirs: walk.empty_dirs,
    })
}

fn snapshot_file<C>(
    file: &Path,
    name: &str,
    tmp: &Path,
    opts: &PushOptions,
    keep: fn(Snapshot) -> C,
) -> std::result::Result<Staged<C>, Retry> {
    opts.cancel.check().map_err(Retry::Fatal)?;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Hashing,
        files: 1,
        bytes: std::fs::metadata(file).ok().map(|m| m.len()),
    });
    let s = snapshot::snapshot(file, tmp)?;
    opts.progress.emit(|| hashed(&s));
    let mut warnings = Vec::new();
    warn_unterminated(&file.to_string_lossy(), &s, &mut warnings);
    Ok(Staged {
        pointer: DvcPointer {
            output: DvcOutput::File {
                md5: s.md5().clone(),
                size: s.size(),
            },
            path: name.to_string(),
            meta: None,
        },
        manifest: None,
        contents: vec![keep(s)],
        files: 1,
        warnings,
        empty_dirs: 0,
    })
}

/// Which of a staged output's contents the remote lacks.
struct Plan<'a, C> {
    upload: Vec<&'a C>,
    present: Vec<&'a C>,
    /// Whether to upload the manifest after the objects: not for a file
    /// output or a manifest already on the remote.
    manifest: bool,
}

/// What of `staged` the remote lacks. A manifest already on the remote
/// means all its objects are (DVC's own invariant, and ours: a manifest is
/// placed after its objects), so they are not asked for, unless `repair`
/// says the remote may have lost some.
async fn plan<'a, C: Content + Sync>(
    remote: &Remote,
    staged: &'a Staged<C>,
    jobs: usize,
    repair: bool,
) -> Result<Plan<'a, C>> {
    let manifest_there = match staged.manifest {
        Some(_) => remote
            .store
            .head(&remote.manifest_key(staged.id()))
            .await?
            .is_some(),
        None => false,
    };
    if manifest_there && !repair {
        return Ok(Plan {
            upload: Vec::new(),
            present: staged.contents.iter().collect(),
            manifest: false,
        });
    }
    let there: Vec<bool> = each_in_order(&staged.contents, jobs, |c| async move {
        Ok(remote
            .store
            .head(&remote.object_key(c.md5()))
            .await?
            .is_some())
    })
    .await?;
    let (present, upload): (Vec<_>, Vec<_>) = staged
        .contents
        .iter()
        .zip(there)
        .partition(|(_, there)| *there);
    Ok(Plan {
        upload: upload.into_iter().map(|(c, _)| c).collect(),
        present: present.into_iter().map(|(c, _)| c).collect(),
        manifest: staged.manifest.is_some() && !manifest_there,
    })
}

/// Upload `snaps`, `jobs` at a time. Once `cancel` is cancelled no upload
/// starts; those under way finish (dropping one could orphan a multipart
/// upload) and the push stops.
async fn upload_all(remote: &Remote, snaps: &[&Snapshot], opts: &PushOptions) -> Result<()> {
    let cancel = &opts.cancel;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Uploading,
        files: snaps.len() as u64,
        bytes: Some(snaps.iter().map(|s| s.size()).sum()),
    });
    each_unordered(snaps, opts.jobs, |s| async move {
        if cancel.is_cancelled() {
            return Ok(());
        }
        remote
            .store
            .put_file(&remote.object_key(s.md5()), s.path())
            .await
            .with_context(|| format!("upload of {} failed", s.md5()))?;
        opts.progress.emit(|| ProgressEvent::Advanced {
            phase: Phase::Uploading,
            files: 1,
            bytes: s.size(),
        });
        Ok(())
    })
    .await?;
    cancel.check()
}

/// The output's name: one portable path component.
fn output_name(output: &Path) -> Result<String> {
    let refused = |reason| Error::Refused {
        path: output.to_path_buf(),
        reason,
    };
    let name = output
        .file_name()
        .ok_or_else(|| refused(Refusal::NoFileName))?
        .to_str()
        .ok_or_else(|| refused(Refusal::NotUtf8Name))?;
    check_portable_component(name).map_err(|e| {
        refused(Refusal::NonPortableName {
            detail: format!("{e:#}"),
        })
    })?;
    if name.ends_with(".dvc") {
        return Err(refused(Refusal::DvcFile).into());
    }
    Ok(name.to_string())
}

/// `<parent>/<name>.dvc`.
fn pointer_path_for(output: &Path) -> Result<PathBuf> {
    let name = output_name(output)?;
    let parent = output
        .parent()
        .with_context(|| format!("{} has no parent directory", output.display()))?;
    Ok(parent.join(format!("{name}.dvc")))
}

/// The pointer already beside the output, if any. Refuses to replace a
/// `.dvc` that is not a plain bigstore/DVC pointer for this output (e.g. a
/// hand-written one with extra fields or a stage).
fn check_existing_pointer(pointer_path: &Path, name: &str) -> Result<Option<DvcPointer>> {
    let text = match std::fs::read_to_string(pointer_path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", pointer_path.display()))
        }
    };
    let refused = |reason| Error::Refused {
        path: pointer_path.to_path_buf(),
        reason,
    };
    let existing = DvcPointer::parse(&text).with_context(|| refused(Refusal::ForeignPointer))?;
    if existing.path != name {
        return Err(refused(Refusal::PointerForOtherOutput {
            other: existing.path,
        })
        .into());
    }
    Ok(Some(existing))
}

/// Write the pointer atomically, on the blocking pool. A file that already
/// says the same thing is left untouched, whatever its formatting (DVC on
/// Windows writes CRLF), so a no-op push never churns a committed `.dvc`.
async fn write_pointer_file(path: &Path, pointer: &DvcPointer) -> Result<()> {
    let (path, pointer) = (path.to_path_buf(), pointer.clone());
    backend::blocking(move || {
        if DvcPointer::load(&path).is_ok_and(|existing| existing == pointer) {
            return Ok(());
        }
        let dir = path.parent().context("pointer path has no parent")?;
        let mut tmp = tempfile::NamedTempFile::new_in(long_path(dir)?)?;
        tmp.write_all(pointer.to_yaml().as_bytes())?;
        tmp.as_file().sync_all()?;
        persist_with_normal_mode(tmp, &path)
    })
    .await
}

fn persist_with_normal_mode(tmp: tempfile::NamedTempFile, dest: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let umask_masked = 0o666 & !current_umask();
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(umask_masked))?;
    }
    tmp.persist(long_path(dest)?)
        .with_context(|| format!("failed to write {}", dest.display()))?;
    Ok(())
}

/// The process umask, read from a probe file (there is no side-effect-free
/// way to query it).
#[cfg(unix)]
fn current_umask() -> u32 {
    use std::os::unix::fs::PermissionsExt;
    let Ok(dir) = tempfile::tempdir() else {
        return 0o022;
    };
    let probe = dir.path().join("p");
    if std::fs::File::create(&probe).is_err() {
        return 0o022;
    }
    std::fs::metadata(&probe)
        .map(|m| 0o666 & !(m.permissions().mode() & 0o666))
        .unwrap_or(0o022)
}

// ──────────────────────────────────────────────────
// Pull
// ──────────────────────────────────────────────────

/// What to restore.
#[derive(Debug, Clone)]
pub enum PointerSource {
    /// A `.dvc` file; the output is restored next to it, and the `.dvc` is
    /// left as it is.
    File(PathBuf),
    /// A version from the remote's history. Once every file is restored,
    /// `<into>.dvc` is written beside the output with that version as its
    /// base, so pulling an old version makes the next push of it
    /// [`Error::StaleBase`].
    History { key: HistoryKey, at: Selector },
}

/// What pull does with a local file that differs from the version being
/// restored. Every file it replaces or removes is hashed again just before,
/// so one written meanwhile is left as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Overwrite {
    /// Refuse if any local file differs, as [`Error::PullConflict`].
    Refuse,
    /// Catch up: replace a differing file only if it still has the content
    /// the `.dvc` beside the output records (its base: the version it was
    /// last pushed or pulled as), and remove a file that version had, still
    /// unchanged, that the version being restored does not; both are in
    /// the base version on the remote. Any other differing file, and a file
    /// the version removed but that changed locally, is
    /// [`Error::PullConflict`]. A pull from a `.dvc` file
    /// ([`PointerSource::File`]) has no other record of the local content,
    /// so there this is [`Overwrite::Refuse`]; so is a pull with no `.dvc`
    /// beside `into`, or one whose content is not on the remote.
    IfUnchanged,
    /// Replace differing files, whatever they hold.
    Force,
}

/// How to pull. `PullOptions::default()` restores beside the `.dvc`,
/// refuses to replace differing files, runs 8 jobs, cannot be cancelled
/// and has no root.
#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Where to restore. Defaults to the `.dvc` file's `<dir>/<path>`;
    /// required for [`PointerSource::History`].
    pub into: Option<PathBuf>,
    pub overwrite: Overwrite,
    pub jobs: usize,
    /// Stops the pull between files: each file is either left as it was or
    /// fully restored, never partly written.
    pub cancel: CancelToken,
    pub progress: Progress,
    /// The directory the pull must stay inside. When set, `into` and a
    /// [`PointerSource::File`] are relative to it and confined to it as
    /// [`PushOptions::root`] says: nothing from `root` down to the output,
    /// nor the output or its `.dvc`, may be a symlink, junction or other
    /// reparse point. Below the output, as without a root, every directory
    /// must be a real one and every file a regular one. `None` (the
    /// default): paths are used as given.
    pub root: Option<PathBuf>,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            into: None,
            overwrite: Overwrite::Refuse,
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
            cancel: CancelToken::default(),
            progress: Progress::default(),
            root: None,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct PullReport {
    /// The pointer restored: for a pull from history, as written beside the
    /// output, with the version as its base.
    pub pointer: DvcPointer,
    pub written: usize,
    pub unchanged: usize,
    /// Under [`Overwrite::IfUnchanged`], files removed: in the `.dvc`'s
    /// base and unchanged since, but not in the version pulled.
    pub removed: usize,
    /// Local files not in the version pulled, left as they are.
    pub extra_local: usize,
}

/// Restore an output. Every target is classified before anything is written:
/// a symlinked or non-regular path is always refused ([`Error::Refused`]);
/// differing files are refused (as [`Error::PullConflict`]) unless
/// [`PullOptions::overwrite`] allows replacing them; for a pull from
/// history, so is a `.dvc` beside `into` that push would refuse to replace.
/// Files are downloaded to a temp file beside their target, verified, then
/// renamed into place — never linked. Local files not in the version are
/// left alone, except as [`Overwrite::IfUnchanged`] says. A pull from
/// history writes its `.dvc` last, once every file is in place. A history
/// selector that matches nothing is [`Error::NoSuchVersion`]; `Latest` in a
/// forked history is [`Error::Diverged`].
pub fn pull(remote: &Remote, source: &PointerSource, opts: &PullOptions) -> Result<PullReport> {
    block_on("pull", pull_async(remote, source, opts))?
}

/// [`pull`] on the caller's tokio runtime (see [the module docs](self)).
pub async fn pull_async(
    remote: &Remote,
    source: &PointerSource,
    opts: &PullOptions,
) -> Result<PullReport> {
    // The mode restored files get. Only a `.dvc` DVC wrote can mark one
    // (`isexec`); push records none, so history never does.
    let root = opts.root.as_deref();
    let (pointer, mode, default_into, version) = match source {
        PointerSource::File(path) => {
            let path = confine(root, path).await?;
            let (pointer, mode, into) = backend::blocking(move || read_pointer_file(&path)).await?;
            (pointer, mode, Some(into), None)
        }
        PointerSource::History { key, at } => {
            let record = history::select(remote, key, at, opts.jobs.max(1), &opts.cancel).await?;
            (record.pointer, WorktreeMode::Regular, None, Some(record.id))
        }
    };
    let into = match (&opts.into, default_into, root) {
        (Some(into), _, _) => confine(root, into).await?,
        // Beside a `.dvc` already confined: its output name is one plain
        // name (see `pointer_output`), checked like any other.
        (None, Some(into), Some(root)) => {
            let rel = into.strip_prefix(root).context("output outside its root")?;
            confine(Some(root), rel).await?
        }
        (None, Some(into), None) => into,
        (None, None, _) => return Err(Error::DestinationRequired.into()),
    };
    // The `.dvc` a pull from history writes, checked before anything is,
    // and the base its old content records.
    let (beside, base) = match version {
        Some(version) => {
            let (path, pointer_path, existing) = locate(&into).await?;
            let pointer = DvcPointer {
                path,
                meta: Some(BigstoreMeta::Base(version)),
                ..pointer.clone()
            };
            (Some((pointer_path, pointer)), existing)
        }
        None => (None, None),
    };
    opts.cancel.check()?;

    let targets: Vec<(PathBuf, Hexdigest)> = match &pointer.output {
        DvcOutput::Dir { manifest, .. } => {
            // The output root itself, like every directory below it, must
            // not redirect writes (a committed `out -> elsewhere` beside
            // `out.dvc`). A file output's symlink is refused per target.
            let root = tokio::fs::symlink_metadata(&into).await;
            if root.is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(Error::Refused {
                    path: into,
                    reason: Refusal::SymlinkedOutput,
                }
                .into());
            }
            let raw = remote
                .store
                .get(&remote.manifest_key(manifest), MAX_MANIFEST_BYTES)
                .await
                .map_err(archived)?
                .with_context(|| format!("manifest {manifest}.dir is not on the remote"))?;
            let (at, id) = (into.clone(), manifest.clone());
            backend::blocking(move || manifest_targets(&at, &raw, &id)).await?
        }
        DvcOutput::File { md5, .. } => vec![(into.clone(), md5.clone())],
    };
    // Pulling the version the output is based on catches up with nothing:
    // it restores, as `Refuse` does (a file deleted here is written again).
    let base = match (opts.overwrite, base) {
        (Overwrite::IfUnchanged, Some(base)) if base.output != pointer.output => {
            Some(base_targets(remote, &into, &base).await?)
        }
        _ => None,
    };

    let dir = matches!(pointer.output, DvcOutput::Dir { .. });
    let owned = opts.clone();
    let checked = unblock(&opts.cancel, move |cancel| {
        let opts = PullOptions {
            cancel: cancel.clone(),
            ..owned
        };
        check_targets(&into, targets, base, dir, mode, &opts)
    })
    .await?;
    base_on_remote(remote, &checked, opts.jobs.max(1)).await?;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Downloading,
        files: checked.by_object.values().map(|p| p.len() as u64).sum(),
        bytes: match &pointer.output {
            DvcOutput::File { size, .. } => Some(*size),
            DvcOutput::Dir { .. } => None,
        },
    });
    let written = fetch_and_place(remote, checked.by_object, opts, mode).await?;
    opts.cancel.check()?;
    let remove = checked.remove;
    let removed = remove.len();
    backend::blocking(move || remove_unchanged(&remove)).await?;
    let pointer = match beside {
        Some((path, pointer)) => {
            let dir = path.parent().context("pointer path has no parent")?;
            tokio::fs::create_dir_all(long_path(dir)?).await?;
            write_pointer_file(&path, &pointer).await?;
            pointer
        }
        None => pointer,
    };

    Ok(PullReport {
        pointer,
        written,
        unchanged: checked.unchanged,
        removed,
        extra_local: checked.extra_local,
    })
}

/// A `.dvc` file's pointer, the mode its output is restored with, and where
/// that output lives. Pull never rewrites the .dvc, so stage fields and
/// annotations (`dvc import-url`, `dvc add --desc`) do not matter. A missing
/// file is an I/O error, not a refusal. Blocks.
fn read_pointer_file(path: &Path) -> Result<(DvcPointer, WorktreeMode, PathBuf)> {
    std::fs::metadata(path).with_context(|| format!("failed to read {}", path.display()))?;
    let read = DvcPointer::load_lenient(path).with_context(|| Error::Refused {
        path: path.to_path_buf(),
        reason: Refusal::UnrestorablePointer,
    })?;
    let mode = match (&read.pointer.output, read.isexec) {
        (_, false) => WorktreeMode::Regular,
        (DvcOutput::File { .. }, true) => WorktreeMode::Executable,
        (DvcOutput::Dir { .. }, true) => {
            return Err(Error::Refused {
                path: path.to_path_buf(),
                reason: Refusal::ExecutableInDirectory,
            }
            .into())
        }
    };
    let into = pointer_output(path, &read.pointer)?;
    Ok((read.pointer, mode, into))
}

/// Every file of the `.dir` manifest `raw` (whose id is `id`) as its path
/// under `into` and its md5. Refuses names this OS cannot write and names
/// that would collide. Blocks (a manifest may be 64 MiB).
fn manifest_targets(into: &Path, raw: &[u8], id: &Hexdigest) -> Result<Vec<(PathBuf, Hexdigest)>> {
    let manifest = Manifest::parse(raw, id).map_err(|e| {
        match e.downcast::<crate::dvc::ExecutableEntry>() {
            Ok(entry) => Error::Refused {
                path: PathBuf::from(entry.0.as_str()),
                reason: Refusal::ExecutableInDirectory,
            }
            .into(),
            Err(e) => e,
        }
    })?;
    check_case_collisions(&manifest)?;
    manifest
        .entries()
        .iter()
        .map(|e| {
            let path = e.relpath.to_repo_path().with_context(|| Error::Refused {
                path: PathBuf::from(e.relpath.as_str()),
                reason: Refusal::UnwritableName,
            })?;
            Ok((path.to_fs_path(into), e.md5.clone()))
        })
        .collect()
}

/// The files of `base`, the pointer a `.dvc` beside `into` records, as
/// their paths under `into` and md5s: what [`Overwrite::IfUnchanged`] may
/// replace or remove. A directory whose manifest is not on the remote has
/// none (nothing can be proven unchanged).
async fn base_targets(
    remote: &Remote,
    into: &Path,
    base: &DvcPointer,
) -> Result<Vec<(PathBuf, Hexdigest)>> {
    match &base.output {
        DvcOutput::File { md5, .. } => Ok(vec![(into.to_path_buf(), md5.clone())]),
        DvcOutput::Dir { manifest, .. } => {
            let raw = remote
                .store
                .get(&remote.manifest_key(manifest), MAX_MANIFEST_BYTES)
                .await
                .map_err(archived)?;
            match raw {
                None => Ok(Vec::new()),
                Some(raw) => {
                    let (at, id) = (into.to_path_buf(), manifest.clone());
                    backend::blocking(move || manifest_targets(&at, &raw, &id)).await
                }
            }
        }
    }
}

/// Refuse ([`Refusal::BaseNotOnRemote`]) unless every file
/// [`Overwrite::IfUnchanged`] would replace or remove has its content on the
/// remote, at the size it has here: only then is discarding the local copy
/// recoverable. A `.dir` manifest on the remote does not prove its objects
/// are (they can be deleted behind it), so each is asked for.
async fn base_on_remote(remote: &Remote, checked: &Checked, jobs: usize) -> Result<()> {
    let replaced = checked
        .by_object
        .values()
        .flatten()
        .filter_map(|(path, r)| match r {
            Replace::Unchanged(md5) => Some((path, md5)),
            _ => None,
        });
    let removed = checked.remove.iter().map(|(path, md5)| (path, md5));
    let discarded: Vec<(&PathBuf, &Hexdigest)> = replaced.chain(removed).collect();
    let missing = each_in_order(&discarded, jobs, |(path, md5)| async move {
        let size = tokio::fs::metadata(long_path(path)?).await?.len();
        let there = remote.store.head(&remote.object_key(md5)).await?;
        Ok(there.is_none_or(|m| m.size != size).then_some(*path))
    })
    .await?;
    match missing.into_iter().flatten().next() {
        Some(path) => Err(Error::Refused {
            path: path.clone(),
            reason: Refusal::BaseNotOnRemote,
        }
        .into()),
        None => Ok(()),
    }
}

/// What pull found locally, before downloading anything.
struct Checked {
    /// Each object to download, with every path to place it at and what may
    /// be there.
    by_object: BTreeMap<Hexdigest, Vec<(PathBuf, Replace)>>,
    /// Files to remove once every file is placed, with the content each
    /// must still have.
    remove: Vec<(PathBuf, Hexdigest)>,
    unchanged: usize,
    extra_local: usize,
}

/// What placing a file may replace.
#[derive(Clone)]
enum Replace {
    /// Nothing: the path must still be free.
    Nothing,
    /// A file still holding this content (hashed again first).
    Unchanged(Hexdigest),
    /// Whatever is there.
    Anything,
}

/// Classify every target under `into`, hashing the local files a pull may
/// replace, and refuse (or report every conflict) before anything is
/// written; `dir` says `into` is a directory output, whose extra files are
/// counted. `base`, for [`Overwrite::IfUnchanged`], is the content the
/// `.dvc` beside `into` records; `None` allows replacing nothing. Blocks.
fn check_targets(
    into: &Path,
    targets: Vec<(PathBuf, Hexdigest)>,
    base: Option<Vec<(PathBuf, Hexdigest)>>,
    dir: bool,
    mode: WorktreeMode,
    opts: &PullOptions,
) -> Result<Checked> {
    let base = base.unwrap_or_default();
    let in_base: std::collections::HashMap<&Path, &Hexdigest> =
        base.iter().map(|(p, md5)| (p.as_path(), md5)).collect();
    // Per target: `None` if it is already right, else what may be replaced.
    let mut fetch = Vec::with_capacity(targets.len());
    let mut conflicts = Vec::new();
    let mut unchanged = 0;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Hashing,
        files: targets.len() as u64,
        bytes: None,
    });
    for (path, md5) in &targets {
        opts.cancel.check()?;
        let target = classify_target(into, path, md5)?;
        opts.progress.emit(|| ProgressEvent::Advanced {
            phase: Phase::Hashing,
            files: 1,
            bytes: match target {
                Target::Missing => 0,
                Target::Same | Target::Differs(_) => std::fs::metadata(path).map_or(0, |m| m.len()),
            },
        });
        fetch.push(match target {
            Target::Same => {
                unchanged += 1;
                if mode == WorktreeMode::Executable {
                    make_executable(path)?;
                }
                None
            }
            // Deleted here since the base: a local change, not to undo.
            Target::Missing if in_base.contains_key(path.as_path()) => {
                conflicts.push(path.clone());
                None
            }
            Target::Missing => Some(Replace::Nothing),
            Target::Differs(local) => match opts.overwrite {
                Overwrite::Force => Some(Replace::Anything),
                Overwrite::IfUnchanged if in_base.get(path.as_path()) == Some(&&local) => {
                    Some(Replace::Unchanged(local))
                }
                _ => {
                    conflicts.push(path.clone());
                    None
                }
            },
        });
    }
    // What the base had and this version does not: removed if unchanged.
    // A name that differs from a target's only by case or normalization is
    // the same file on macOS and Windows; removing it would remove the
    // target, so it is refused on every OS, before anything is written.
    let rel = |p: &Path| {
        p.strip_prefix(into)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/")
    };
    let wanted: std::collections::HashSet<&Path> =
        targets.iter().map(|(p, _)| p.as_path()).collect();
    let folded: std::collections::HashMap<String, String> = match base.is_empty() {
        true => Default::default(),
        false => targets
            .iter()
            .map(|(p, _)| (fold_name(&rel(p)), rel(p)))
            .collect(),
    };
    let mut remove = Vec::new();
    for (path, md5) in base.iter().filter(|(p, _)| !wanted.contains(p.as_path())) {
        opts.cancel.check()?;
        if let Some(other) = folded.get(&fold_name(&rel(path))) {
            return Err(Error::Refused {
                path: PathBuf::from(rel(path)),
                reason: Refusal::CaseCollision {
                    other: other.clone(),
                },
            }
            .into());
        }
        match classify_target(into, path, md5)? {
            Target::Missing => {}
            Target::Same => remove.push((path.clone(), md5.clone())),
            Target::Differs(_) => conflicts.push(path.clone()),
        }
    }
    if !conflicts.is_empty() {
        return Err(Error::PullConflict { paths: conflicts }.into());
    }
    let extra_local = if dir {
        count_extra(into, &targets, &remove)
    } else {
        0
    };

    // Fetch each object once, then place it at every path that needs it.
    let mut by_object: BTreeMap<Hexdigest, Vec<(PathBuf, Replace)>> = BTreeMap::new();
    for ((path, md5), replace) in targets.into_iter().zip(fetch) {
        if let Some(replace) = replace {
            by_object.entry(md5).or_default().push((path, replace));
        }
    }
    Ok(Checked {
        by_object,
        remove,
        unchanged,
        extra_local,
    })
}

/// Refuse unless `path` is still a regular file holding `md5`, as
/// [`Refusal::ChangedWhilePulling`]. Blocks.
fn still_unchanged(path: &Path, md5: &Hexdigest) -> Result<()> {
    let same = match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_file() => crate::hash::hash_file(path, md5.hash_fn())? == *md5,
        Ok(_) => false,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(e).with_context(|| format!("failed to stat {}", path.display())),
    };
    if !same {
        return Err(Error::Refused {
            path: path.to_path_buf(),
            reason: Refusal::ChangedWhilePulling,
        }
        .into());
    }
    Ok(())
}

/// Remove each file, once it is seen to still hold its content. Blocks.
fn remove_unchanged(files: &[(PathBuf, Hexdigest)]) -> Result<()> {
    for (path, md5) in files {
        still_unchanged(path, md5)?;
        std::fs::remove_file(long_path(path)?)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

/// Where a `.dvc` file's output lives: `pointer.path` beside it. That must
/// be one name this OS can write, as push writes, so a pointer cannot
/// restore outside its own directory (`..`, an absolute path, `a/b`).
fn pointer_output(dvc: &Path, pointer: &DvcPointer) -> Result<PathBuf> {
    let name = pointer.path.as_str();
    let single = ManifestPath::new(name)
        .and_then(|p| p.to_repo_path())
        .is_ok_and(|p| !p.as_str().contains('/'));
    if !single {
        return Err(Error::Refused {
            path: dvc.to_path_buf(),
            reason: Refusal::PointerPathEscapes {
                output: name.to_string(),
            },
        }
        .into());
    }
    let dir = dvc.parent().context("pointer has no parent directory")?;
    Ok(dir.join(name))
}

enum Target {
    Missing,
    Same,
    /// A regular file with other content: its md5.
    Differs(Hexdigest),
}

/// Classify one target. Every ancestor between `root` and the target must be
/// a real directory (or not exist yet), so a symlink cannot redirect a write
/// outside `root`.
fn classify_target(root: &Path, path: &Path, md5: &Hexdigest) -> Result<Target> {
    if let Ok(rel) = path.strip_prefix(root) {
        let mut cur = root.to_path_buf();
        let parts: Vec<_> = rel.components().collect();
        for c in &parts[..parts.len().saturating_sub(1)] {
            cur.push(c);
            match std::fs::symlink_metadata(&cur) {
                Ok(m) if m.is_dir() => {}
                Ok(_) => {
                    return Err(Error::Refused {
                        path: cur,
                        reason: Refusal::NotADirectory,
                    }
                    .into())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
            }
        }
    }
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Target::Missing),
        Err(e) => Err(e.into()),
        Ok(m) if m.is_file() => {
            let local = crate::hash::hash_file(path, md5.hash_fn())?;
            if local == *md5 {
                Ok(Target::Same)
            } else {
                Ok(Target::Differs(local))
            }
        }
        Ok(_) => Err(Error::Refused {
            path: path.to_path_buf(),
            reason: Refusal::NotRegularFile,
        }
        .into()),
    }
}

/// Entries that differ only by case (Unicode, not just ASCII) or by Unicode
/// normalization (`é` as one code point or as `e` + accent) would be one
/// file on macOS (APFS, HFS+) and Windows. Refused on every OS, so a version
/// restores the same everywhere.
fn check_case_collisions(manifest: &Manifest) -> Result<()> {
    let mut seen = std::collections::HashMap::new();
    for e in manifest.entries() {
        if let Some(other) = seen.insert(fold_name(e.relpath.as_str()), e.relpath.as_str()) {
            return Err(Error::Refused {
                path: PathBuf::from(e.relpath.as_str()),
                reason: Refusal::CaseCollision {
                    other: other.to_string(),
                },
            }
            .into());
        }
    }
    Ok(())
}

/// `name` as macOS and Windows compare names: case-folded and NFC.
fn fold_name(name: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    // Lowercasing can decompose (`İ` → `i` + dot), so normalize again.
    let lower = name.nfc().collect::<String>().to_lowercase();
    lower.nfc().collect()
}

/// Files under `root` that are neither targets nor about to be removed.
fn count_extra(
    root: &Path,
    targets: &[(PathBuf, Hexdigest)],
    remove: &[(PathBuf, Hexdigest)],
) -> usize {
    let wanted: std::collections::HashSet<&Path> = targets
        .iter()
        .chain(remove)
        .map(|(p, _)| p.as_path())
        .collect();
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && !wanted.contains(e.path()))
        .count()
}

/// Download each object once and place it at every path that needs it,
/// with `mode`, `opts.jobs` objects at a time. Once cancelled no download
/// starts; those under way finish and are placed whole, and the pull stops.
/// A downloaded object is placed on the blocking pool, and placed whole
/// even if the future is dropped meanwhile.
async fn fetch_and_place(
    remote: &Remote,
    by_object: BTreeMap<Hexdigest, Vec<(PathBuf, Replace)>>,
    opts: &PullOptions,
    mode: WorktreeMode,
) -> Result<usize> {
    let cancel = &opts.cancel;
    let counts: Vec<usize> = each_unordered(by_object, opts.jobs, |(md5, places)| async move {
        if cancel.is_cancelled() {
            return Ok(0);
        }
        let dir = places[0].0.parent().context("target has no parent")?;
        tokio::fs::create_dir_all(dir).await?;
        let tmp = remote
            .store
            .download_verified(&remote.object_key(&md5), &md5, &long_path(dir)?)
            .await
            .map_err(archived)?;
        let progress = opts.progress.clone();
        backend::blocking(move || place_all(tmp, &places, mode, &progress)).await
    })
    .await?;
    cancel.check()?;
    Ok(counts.into_iter().sum())
}

/// Place a verified download at every path in `places`: copies from it
/// first, then the temp file itself. Blocks.
fn place_all(
    tmp: tempfile::NamedTempFile,
    places: &[(PathBuf, Replace)],
    mode: WorktreeMode,
    progress: &Progress,
) -> Result<usize> {
    let bytes = tmp.as_file().metadata()?.len();
    for (path, replace) in &places[1..] {
        let parent = path.parent().context("target has no parent")?;
        std::fs::create_dir_all(parent)?;
        let mut copy = tempfile::NamedTempFile::new_in(long_path(parent)?)?;
        std::io::copy(&mut std::fs::File::open(tmp.path())?, &mut copy)?;
        place(copy, path, replace, mode)?;
    }
    let (first, replace) = &places[0];
    place(tmp, first, replace, mode)?;
    progress.emit(|| ProgressEvent::Advanced {
        phase: Phase::Downloading,
        files: places.len() as u64,
        bytes,
    });
    Ok(places.len())
}

/// Move a verified temp file into place with `mode`'s permissions (the
/// umask applies): never replacing a file that appeared since
/// classification, nor one that changed since, unless the caller forced
/// replacement.
fn place(
    tmp: tempfile::NamedTempFile,
    path: &Path,
    replace: &Replace,
    mode: WorktreeMode,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let bits = match mode {
            WorktreeMode::Regular => 0o666,
            WorktreeMode::Executable => 0o777,
        };
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(bits & !current_umask()))?;
    }
    // Windows has no execute bit.
    #[cfg(not(unix))]
    let _ = mode;
    if let Replace::Unchanged(md5) = replace {
        still_unchanged(path, md5)?;
    }
    if !matches!(replace, Replace::Nothing) {
        tmp.persist(long_path(path)?)
            .with_context(|| format!("failed to write {}", path.display()))?;
    } else {
        tmp.persist_noclobber(long_path(path)?).map_err(|e| {
            let appeared = e.error.kind() == std::io::ErrorKind::AlreadyExists;
            let e = anyhow::Error::from(e.error);
            if appeared {
                e.context(Error::Refused {
                    path: path.to_path_buf(),
                    reason: Refusal::AppearedWhilePulling,
                })
            } else {
                e.context(format!("failed to write {}", path.display()))
            }
        })?;
    }
    Ok(())
}

/// Give a file already in place, which a `.dvc` marks executable, the
/// execute bits the umask allows, as a new executable would get. No-op off
/// unix (no execute bit) or if the owner can already execute it.
fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode();
        if mode & 0o100 == 0 {
            let mode = mode | (0o111 & !current_umask());
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
                .with_context(|| format!("failed to make {} executable", path.display()))?;
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn snapshots_go_to_a_directory_only_the_owner_can_enter() {
        // Snapshots hold whatever the output holds; other users on the host
        // must not read them while a push or status runs.
        let dir = snapshot_tmpdir().unwrap();
        let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "{mode:o}");
    }
}
