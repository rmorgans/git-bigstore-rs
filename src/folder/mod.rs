//! Git-free, DVC-compatible backup of plain folders.
//!
//! [`push`] backs up one *output* — a directory or a single file — to a
//! remote in DVC 3's layout (`files/md5/xx/rest`, plus a `.dir` manifest for
//! directories), writes a DVC 3 `.dvc` pointer next to it, and appends the
//! pointer to a history log on the remote. [`pull`] restores an output from a
//! pointer or from history. Real DVC can `dvc pull` what this writes, and
//! this can pull what `dvc push` wrote.
//!
//! The API is blocking: each call runs its own tokio runtime, so it must not
//! be called from inside one (that returns an error rather than panicking).
//! Nothing here calls git.

mod error;
mod history;
mod snapshot;
mod walk;

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub use crate::backend::store::Credentials;
use crate::backend::{self, Backend};
use crate::config::{BackendConfig, BigstoreConfig};
use crate::dvc::{DvcOutput, DvcPointer, Manifest, ManifestEntry};
use crate::types::{check_portable_component, Hexdigest, Layout, ManifestPath};
pub use error::{Error, Refusal};
pub use history::{keys, log, HistoryKey, HistoryRecord, Selector};
pub use walk::{Excludes, DEFAULT_EXCLUDES};

use snapshot::{Snapshot, SnapshotError};
use walk::WalkError;

/// Largest `.dir` manifest pull will fetch into memory.
const MAX_MANIFEST_BYTES: u64 = 64 << 20;
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
    backend: Backend,
    prefix: String,
}

impl Remote {
    /// Refuses, as [`Error::UnsupportedRemote`], any URL but `s3://`,
    /// `local://` (`file://`) and `rclone://`, and, as
    /// [`Error::EndpointRequired`], `s3://` without an endpoint.
    pub fn open(config: &RemoteConfig) -> Result<Self> {
        let unsupported = || Error::UnsupportedRemote {
            url: config.url.clone(),
        };
        let cfg = match config.url.split_once("://") {
            Some(("s3" | "local" | "file" | "rclone", _)) => {
                BigstoreConfig::from_url(&config.url, None)?
            }
            _ => return Err(unsupported().into()),
        };
        let (backend, prefix) = match &cfg.backend {
            BackendConfig::S3 { bucket, prefix, .. } => {
                let endpoint = config.endpoint.as_deref().ok_or(Error::EndpointRequired)?;
                let store = backend::store::build_strict_s3(
                    bucket,
                    endpoint,
                    config.region.as_deref(),
                    &config.credentials,
                )?;
                (Backend::ObjectStore(store.into()), prefix.clone())
            }
            BackendConfig::Local { .. } | BackendConfig::Rclone { .. } => {
                (backend::from_config(&cfg)?, String::new())
            }
            _ => return Err(unsupported().into()),
        };
        Ok(Self { backend, prefix })
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

/// Run `fut` on a private runtime. Refuses (instead of panicking) when the
/// caller is already inside a tokio runtime.
fn block_on<F: std::future::Future>(fut: F) -> Result<F::Output> {
    anyhow::ensure!(
        tokio::runtime::Handle::try_current().is_err(),
        "bigstore::folder functions are blocking and cannot run inside a tokio runtime; \
         call them from a plain thread or tokio::task::spawn_blocking"
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start async runtime")?;
    Ok(rt.block_on(fut))
}

// ──────────────────────────────────────────────────
// Cancellation
// ──────────────────────────────────────────────────

/// Stops a push, status or pull from another thread. Clones share one flag;
/// the default token is never cancelled (nobody else holds it). Checked
/// between files and between objects: a file being hashed, uploaded or
/// downloaded when it is cancelled is finished first. A cancelled call
/// returns [`Error::Cancelled`].
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel every call holding a clone of this token. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(Error::Cancelled.into());
        }
        Ok(())
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
    /// Stops the push before it writes the `.dvc`; after that it completes.
    pub cancel: CancelToken,
    pub progress: Progress,
}

impl PushOptions {
    /// Push to `history` with 8 jobs, the default excludes, a token nobody
    /// else can cancel and no progress reports.
    pub fn new(history: HistoryKey) -> Self {
        Self {
            history,
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
            exclude: Excludes::default(),
            cancel: CancelToken::default(),
            progress: Progress::default(),
        }
    }
}

#[derive(Debug)]
pub struct PushReport {
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
    /// History record written; `None` if the latest record already was this
    /// version.
    pub history_record: Option<String>,
}

/// Back up `output` (a directory or a single file).
///
/// Order is what makes this safe for DVC and for concurrent readers: every
/// object is uploaded first, then the `.dir` manifest (DVC treats a present
/// manifest as "all its objects are present"), then the local `.dvc`, then
/// the history record. A failure at any step leaves at most unreferenced
/// objects behind. Files are snapshotted while hashed, so an append during
/// the push can never produce an object whose content does not match its key.
///
/// Anything push will not back up is refused as [`Error::Refused`] before
/// anything is published; an output that keeps changing through every retry
/// is [`Error::OutputChanged`].
pub fn push(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    block_on(push_async(remote, output, opts))?
}

async fn push_async(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    let (name, pointer_path, _) = locate(output)?;
    let tmp = tempfile::tempdir().context("failed to create a temp dir")?;
    let staged = stage(output, &name, tmp.path(), opts, |s| s)?;
    let jobs = opts.jobs.max(1);
    let plan = plan(remote, &staged, jobs).await?;

    upload_all(remote, &plan.upload, opts).await?;
    if let Some(manifest) = plan.manifest {
        opts.cancel.check()?;
        let key = remote.manifest_key(staged.id());
        backend::put_bytes(&remote.backend, &key, manifest.to_bytes()).await?;
    }
    // The last point to stop: past it, the .dvc and history must agree.
    opts.cancel.check()?;
    write_pointer_file(&pointer_path, &staged.pointer)?;
    let history_record = history::append(remote, &opts.history, &staged.pointer).await?;

    Ok(PushReport {
        uploaded: plan.upload.len(),
        already_present: plan.present.len(),
        pointer: staged.pointer,
        pointer_path,
        files: staged.files,
        empty_dirs: staged.empty_dirs,
        warnings: staged.warnings,
        history_record,
    })
}

/// What [`status`] found: what [`push`] would do with the same arguments.
#[derive(Debug)]
pub struct StatusReport {
    /// The pointer push would write.
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
}

/// How a local output relates to the latest version in its history, judged
/// by the `.dvc` beside it (the version it was last pushed or pulled as).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncState {
    /// The history is empty: push would record the first version.
    NoHistory,
    /// The output is the latest version: push would add none.
    InSync,
    /// The output changed since the latest version, which its `.dvc`
    /// records: push would add a version.
    LocalAhead,
    /// The history has a newer version than the one the `.dvc` records, and
    /// the output has not changed since: pull to catch up. A push would
    /// record the older content as the newest version.
    RemoteAhead { latest: HistoryRecord },
    /// The output changed and so did the history since its `.dvc` (or there
    /// is no `.dvc` to tell): a pull or a push would set one side aside.
    Diverged { latest: HistoryRecord },
}

/// What [`push`] would do, without doing it: `output` is walked, snapshotted
/// and hashed exactly as push does, and the remote is asked which contents
/// it has and what the latest version is (one history record fetched).
/// Nothing is written to the remote or beside the output; snapshots go to a
/// private temp directory, one file at a time. Refuses whatever push
/// refuses.
pub fn status(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<StatusReport> {
    block_on(status_async(remote, output, opts))?
}

async fn status_async(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<StatusReport> {
    let (name, _, local) = locate(output)?;
    let tmp = tempfile::tempdir().context("failed to create a temp dir")?;
    let staged = stage(output, &name, tmp.path(), opts, |s| Hashed {
        md5: s.md5().clone(),
        size: s.size(),
    })?;
    let plan = plan(remote, &staged, opts.jobs.max(1)).await?;
    let output = &staged.pointer.output;
    let sync = match history::latest(remote, &opts.history).await? {
        None => SyncState::NoHistory,
        Some(latest) if latest.pointer.output == *output => SyncState::InSync,
        Some(latest) => match local {
            Some(p) if p.output == latest.pointer.output => SyncState::LocalAhead,
            Some(p) if p.output == *output => SyncState::RemoteAhead { latest },
            _ => SyncState::Diverged { latest },
        },
    };
    let bytes = |cs: &[&Hashed]| cs.iter().map(|c| c.size).sum();
    Ok(StatusReport {
        to_upload: plan.upload.len(),
        to_upload_bytes: bytes(&plan.upload),
        already_present: plan.present.len(),
        already_present_bytes: bytes(&plan.present),
        pointer: staged.pointer,
        files: staged.files,
        empty_dirs: staged.empty_dirs,
        warnings: staged.warnings,
        sync,
    })
}

/// The output's name, its `.dvc` path, and the pointer already there (push
/// refuses to replace anything else).
fn locate(output: &Path) -> Result<(String, PathBuf, Option<DvcPointer>)> {
    let name = output_name(output)?;
    let pointer_path = pointer_path_for(output)?;
    let existing = check_existing_pointer(&pointer_path, &name)?;
    Ok((name, pointer_path, existing))
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
    /// A directory output's manifest.
    manifest: Option<Manifest>,
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

/// Snapshot `output` (named `name`) into `tmp`, restarting while files
/// change under it; `keep` turns each snapshot into what the caller holds.
fn stage<C>(
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
        },
        manifest: Some(manifest),
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
    /// The manifest to upload after the objects; `None` for a file output
    /// or a manifest already on the remote.
    manifest: Option<&'a Manifest>,
}

async fn plan<'a, C: Content + Sync>(
    remote: &Remote,
    staged: &'a Staged<C>,
    jobs: usize,
) -> Result<Plan<'a, C>> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    // A manifest already on the remote means all its objects are (DVC's own
    // invariant, and ours: it is uploaded last).
    if staged.manifest.is_some()
        && backend::exists(&remote.backend, &remote.manifest_key(staged.id())).await?
    {
        return Ok(Plan {
            upload: Vec::new(),
            present: staged.contents.iter().collect(),
            manifest: None,
        });
    }
    let there: Vec<bool> = stream::iter(&staged.contents)
        .map(|c| async move { backend::exists(&remote.backend, &remote.object_key(c.md5())).await })
        .buffered(jobs)
        .try_collect()
        .await?;
    let (present, upload): (Vec<_>, Vec<_>) = staged
        .contents
        .iter()
        .zip(there)
        .partition(|(_, there)| *there);
    Ok(Plan {
        upload: upload.into_iter().map(|(c, _)| c).collect(),
        present: present.into_iter().map(|(c, _)| c).collect(),
        manifest: staged.manifest.as_ref(),
    })
}

/// Upload `snaps`, `jobs` at a time. Once `cancel` is cancelled no upload
/// starts; those under way finish (dropping one could orphan a multipart
/// upload) and the push stops.
async fn upload_all(remote: &Remote, snaps: &[&Snapshot], opts: &PushOptions) -> Result<()> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let cancel = &opts.cancel;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Uploading,
        files: snaps.len() as u64,
        bytes: Some(snaps.iter().map(|s| s.size()).sum()),
    });
    stream::iter(snaps)
        .map(|s| async move {
            if cancel.is_cancelled() {
                return Ok(());
            }
            backend::upload(&remote.backend, s.path(), &remote.object_key(s.md5()))
                .await
                .with_context(|| format!("upload of {} failed", s.md5()))?;
            opts.progress.emit(|| ProgressEvent::Advanced {
                phase: Phase::Uploading,
                files: 1,
                bytes: s.size(),
            });
            Ok::<_, anyhow::Error>(())
        })
        .buffer_unordered(opts.jobs.max(1))
        .try_collect::<()>()
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

/// Write the pointer atomically. A file that already says the same thing is
/// left untouched, whatever its formatting (DVC on Windows writes CRLF), so a
/// no-op push never churns a committed `.dvc`.
fn write_pointer_file(path: &Path, pointer: &DvcPointer) -> Result<()> {
    let yaml = pointer.to_yaml();
    if DvcPointer::load(path).is_ok_and(|existing| existing == *pointer) {
        return Ok(());
    }
    let dir = path.parent().context("pointer path has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(yaml.as_bytes())?;
    tmp.as_file().sync_all()?;
    persist_with_normal_mode(tmp, path)
}

fn persist_with_normal_mode(tmp: tempfile::NamedTempFile, dest: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let umask_masked = 0o666 & !current_umask();
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(umask_masked))?;
    }
    tmp.persist(dest)
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
    /// A `.dvc` file; the output is restored next to it.
    File(PathBuf),
    /// A version from the remote's history.
    History { key: HistoryKey, at: Selector },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overwrite {
    /// Refuse if any local file differs from the version being restored.
    Refuse,
    /// Replace differing files.
    Force,
}

/// How to pull. `PullOptions::default()` restores beside the `.dvc`,
/// refuses to replace differing files, runs 8 jobs and cannot be cancelled.
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
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            into: None,
            overwrite: Overwrite::Refuse,
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
            cancel: CancelToken::default(),
            progress: Progress::default(),
        }
    }
}

#[derive(Debug)]
pub struct PullReport {
    pub pointer: DvcPointer,
    pub written: usize,
    pub unchanged: usize,
    /// Local files not in the version pulled. Never deleted.
    pub extra_local: usize,
}

/// Restore an output. Every target is classified before anything is written:
/// a symlinked or non-regular path is always refused ([`Error::Refused`]);
/// differing files are refused (as [`Error::PullConflict`]) unless forced.
/// Files are downloaded to a temp file beside their target, verified, then
/// renamed into place — never linked. Local files not in the version are
/// left alone. A history selector that matches nothing is
/// [`Error::NoSuchVersion`].
pub fn pull(remote: &Remote, source: &PointerSource, opts: &PullOptions) -> Result<PullReport> {
    block_on(pull_async(remote, source, opts))?
}

async fn pull_async(
    remote: &Remote,
    source: &PointerSource,
    opts: &PullOptions,
) -> Result<PullReport> {
    let (pointer, default_into) = match source {
        PointerSource::File(path) => {
            let pointer = DvcPointer::load(path)?;
            let into = pointer_output(path, &pointer)?;
            (pointer, Some(into))
        }
        PointerSource::History { key, at } => {
            let record = history::select(remote, key, at, opts.jobs.max(1)).await?;
            (record.pointer, None)
        }
    };
    let into = opts
        .into
        .clone()
        .or(default_into)
        .context("pulling from history needs a destination (`into`)")?;
    opts.cancel.check()?;

    let targets: Vec<(PathBuf, Hexdigest)> = match &pointer.output {
        DvcOutput::Dir { manifest, .. } => {
            // The output root itself, like every directory below it, must
            // not redirect writes (a committed `out -> elsewhere` beside
            // `out.dvc`). A file output's symlink is refused per target.
            if std::fs::symlink_metadata(&into).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(Error::Refused {
                    path: into,
                    reason: Refusal::SymlinkedOutput,
                }
                .into());
            }
            let raw = backend::get_bytes(
                &remote.backend,
                &remote.manifest_key(manifest),
                MAX_MANIFEST_BYTES,
            )
            .await?
            .with_context(|| format!("manifest {manifest}.dir is not on the remote"))?;
            let manifest = Manifest::parse(&raw, manifest)?;
            check_case_collisions(&manifest)?;
            manifest
                .entries()
                .iter()
                .map(|e| {
                    let path = e.relpath.to_repo_path().with_context(|| Error::Refused {
                        path: PathBuf::from(e.relpath.as_str()),
                        reason: Refusal::UnwritableName,
                    })?;
                    Ok((path.to_fs_path(&into), e.md5.clone()))
                })
                .collect::<Result<_>>()?
        }
        DvcOutput::File { md5, .. } => vec![(into.clone(), md5.clone())],
    };

    let mut plan = Vec::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0;
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Hashing,
        files: targets.len() as u64,
        bytes: None,
    });
    for (path, md5) in &targets {
        opts.cancel.check()?;
        let target = classify_target(&into, path, md5)?;
        opts.progress.emit(|| ProgressEvent::Advanced {
            phase: Phase::Hashing,
            files: 1,
            bytes: match target {
                Target::Missing => 0,
                Target::Same | Target::Differs => std::fs::metadata(path).map_or(0, |m| m.len()),
            },
        });
        match target {
            Target::Same => unchanged += 1,
            Target::Missing => plan.push((path, md5, false)),
            Target::Differs => match opts.overwrite {
                Overwrite::Refuse => conflicts.push(path.clone()),
                Overwrite::Force => plan.push((path, md5, true)),
            },
        }
    }
    if !conflicts.is_empty() {
        return Err(Error::PullConflict { paths: conflicts }.into());
    }

    let extra_local = match &pointer.output {
        DvcOutput::Dir { .. } => count_extra(&into, &targets),
        DvcOutput::File { .. } => 0,
    };

    // Fetch each object once, then place it at every path that needs it.
    let mut by_object: BTreeMap<&Hexdigest, Vec<(&PathBuf, bool)>> = BTreeMap::new();
    for (path, md5, replace) in plan {
        by_object.entry(md5).or_default().push((path, replace));
    }
    opts.progress.emit(|| ProgressEvent::Started {
        phase: Phase::Downloading,
        files: by_object.values().map(|places| places.len() as u64).sum(),
        bytes: match &pointer.output {
            DvcOutput::File { size, .. } => Some(*size),
            DvcOutput::Dir { .. } => None,
        },
    });
    let written = fetch_and_place(remote, by_object, opts).await?;

    Ok(PullReport {
        pointer,
        written,
        unchanged,
        extra_local,
    })
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
    Differs,
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
            if crate::hash::hash_file(path, md5.hash_fn())? == *md5 {
                Ok(Target::Same)
            } else {
                Ok(Target::Differs)
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
    use unicode_normalization::UnicodeNormalization;
    let mut seen = std::collections::HashMap::new();
    for e in manifest.entries() {
        // Lowercasing can decompose (`İ` → `i` + dot), so normalize again.
        let lower = e.relpath.as_str().nfc().collect::<String>().to_lowercase();
        let folded: String = lower.nfc().collect();
        if let Some(other) = seen.insert(folded, e.relpath.as_str()) {
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

fn count_extra(root: &Path, targets: &[(PathBuf, Hexdigest)]) -> usize {
    let wanted: std::collections::HashSet<&Path> =
        targets.iter().map(|(p, _)| p.as_path()).collect();
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && !wanted.contains(e.path()))
        .count()
}

/// Download each object once and place it at every path that needs it,
/// `opts.jobs` objects at a time. Once cancelled no download starts; those
/// under way finish and are placed whole, and the pull stops.
async fn fetch_and_place(
    remote: &Remote,
    by_object: BTreeMap<&Hexdigest, Vec<(&PathBuf, bool)>>,
    opts: &PullOptions,
) -> Result<usize> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let cancel = &opts.cancel;
    let counts: Vec<usize> = stream::iter(by_object)
        .map(|(md5, places)| async move {
            if cancel.is_cancelled() {
                return Ok(0);
            }
            let (first, _) = places[0];
            let dir = first.parent().context("target has no parent")?;
            std::fs::create_dir_all(dir)?;
            let tmp =
                backend::download_verified(&remote.backend, &remote.object_key(md5), md5, dir)
                    .await?;
            let bytes = tmp.as_file().metadata()?.len();
            // Extra copies first (from the verified temp), then move the temp.
            for (path, replace) in &places[1..] {
                let parent = path.parent().context("target has no parent")?;
                std::fs::create_dir_all(parent)?;
                let mut copy = tempfile::NamedTempFile::new_in(parent)?;
                std::io::copy(&mut std::fs::File::open(tmp.path())?, &mut copy)?;
                place(copy, path, *replace)?;
            }
            place(tmp, first, places[0].1)?;
            opts.progress.emit(|| ProgressEvent::Advanced {
                phase: Phase::Downloading,
                files: places.len() as u64,
                bytes,
            });
            Ok::<_, anyhow::Error>(places.len())
        })
        .buffer_unordered(opts.jobs.max(1))
        .try_collect()
        .await?;
    cancel.check()?;
    Ok(counts.into_iter().sum())
}

/// Move a verified temp file into place: never replacing a file that
/// appeared since classification unless the caller forced replacement.
fn place(tmp: tempfile::NamedTempFile, path: &Path, replace: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o666 & !current_umask()))?;
    }
    if replace {
        tmp.persist(path)
            .with_context(|| format!("failed to write {}", path.display()))?;
    } else {
        tmp.persist_noclobber(path).map_err(|e| {
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
