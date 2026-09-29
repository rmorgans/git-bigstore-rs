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

mod snapshot;
mod walk;

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

pub use crate::backend::store::Credentials;
use crate::backend::{self, Backend};
use crate::config::{BackendConfig, BigstoreConfig};
use crate::dvc::{DvcOutput, DvcPointer, Manifest, ManifestEntry};
use crate::types::{check_portable_component, Hexdigest, Layout, PortableRelPath};

use snapshot::{Snapshot, SnapshotError};
use walk::WalkError;

/// Largest `.dir` manifest pull will fetch into memory.
const MAX_MANIFEST_BYTES: u64 = 64 << 20;
/// Largest history record pull will fetch.
const MAX_RECORD_BYTES: u64 = 64 << 10;
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
    pub fn open(config: &RemoteConfig) -> Result<Self> {
        let cfg = BigstoreConfig::from_url(&config.url, None)?;
        let (backend, prefix) = match &cfg.backend {
            BackendConfig::S3 { bucket, prefix, .. } if config.url.starts_with("s3://") => {
                let endpoint = config.endpoint.as_deref().context(
                    "S3 remote needs an endpoint (e.g. https://s3.ap-southeast-2.wasabisys.com); \
                     folder mode never defaults to AWS",
                )?;
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
            _ => anyhow::bail!(
                "folder mode supports s3://, local:// and rclone:// remotes, not {}",
                config.url
            ),
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
// History
// ──────────────────────────────────────────────────

/// Identifies an output across hosts and time, e.g.
/// `ST032_Warrawoona/BeatonsCreek_dataset1_September2026/annotations/reviewer=rick/host=xenoglossicist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryKey(PortableRelPath);

impl HistoryKey {
    pub fn new(key: &str) -> Result<Self> {
        Ok(Self(
            PortableRelPath::new(key).context("invalid history key")?,
        ))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// One pushed version of an output.
#[derive(Debug, Clone)]
pub struct HistoryRecord {
    /// Remote key of the record.
    pub key: String,
    /// When it was pushed (UTC), as recorded in the key.
    pub time: chrono::DateTime<chrono::Utc>,
    pub pointer: DvcPointer,
}

/// Record key time format: fixed width, so key order is time order.
const RECORD_TIME: &str = "%Y%m%dT%H%M%S%.9fZ";

impl HistoryRecord {
    /// The id this version is addressed by: manifest id or file md5.
    pub fn id(&self) -> &Hexdigest {
        output_id(&self.pointer.output)
    }
}

fn output_id(output: &DvcOutput) -> &Hexdigest {
    match output {
        DvcOutput::Dir { manifest, .. } => manifest,
        DvcOutput::File { md5, .. } => md5,
    }
}

/// Which version to restore.
#[derive(Debug, Clone)]
pub enum Selector {
    Latest,
    /// An id prefix of at least 8 hex characters; must match one version.
    Id(String),
    /// The newest version pushed at or before this time (RFC 3339).
    AtOrBefore(String),
}

fn history_prefix(remote: &Remote, key: &HistoryKey) -> String {
    remote.key(&format!("bigstore-history/{}/", key.as_str()))
}

async fn read_history(remote: &Remote, key: &HistoryKey) -> Result<Vec<HistoryRecord>> {
    let prefix = history_prefix(remote, key);
    let mut records = Vec::new();
    for object in backend::list(&remote.backend, &prefix).await? {
        let Some(name) = object.strip_prefix(&prefix) else {
            continue;
        };
        // Records sit directly under the prefix; deeper keys are other outputs.
        if name.contains('/') || !name.ends_with(".dvc") {
            continue;
        }
        let Some(time) = name
            .split_once('-')
            .and_then(|(t, _)| chrono::NaiveDateTime::parse_from_str(t, RECORD_TIME).ok())
        else {
            continue; // not a record this version wrote
        };
        let time = time.and_utc();
        let bytes = backend::get_bytes(&remote.backend, &object, MAX_RECORD_BYTES)
            .await?
            .with_context(|| format!("history record {object} vanished"))?;
        let text = String::from_utf8(bytes).with_context(|| format!("{object} is not UTF-8"))?;
        let pointer = DvcPointer::parse(&text).with_context(|| format!("bad record {object}"))?;
        records.push(HistoryRecord {
            key: object,
            time,
            pointer,
        });
    }
    records.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.key.cmp(&b.key)));
    Ok(records)
}

/// Every pushed version of `key`, oldest first.
pub fn log(remote: &Remote, key: &HistoryKey) -> Result<Vec<HistoryRecord>> {
    block_on(read_history(remote, key))?
}

fn select<'a>(records: &'a [HistoryRecord], at: &Selector) -> Result<&'a HistoryRecord> {
    let found = match at {
        Selector::Latest => records.last(),
        Selector::Id(prefix) => {
            anyhow::ensure!(
                prefix.len() >= 8 && prefix.bytes().all(|b| b.is_ascii_hexdigit()),
                "version id prefix must be at least 8 hex characters"
            );
            let prefix = prefix.to_ascii_lowercase();
            let matches: Vec<&HistoryRecord> = records
                .iter()
                .filter(|r| r.id().to_string().starts_with(&prefix))
                .collect();
            match matches.as_slice() {
                [] => None,
                [one] => Some(*one),
                many => {
                    let candidates: String = many
                        .iter()
                        .map(|r| format!("\n  {}  pushed {}", r.id(), r.time.to_rfc3339()))
                        .collect();
                    anyhow::bail!(
                        "version id prefix {prefix} is ambiguous; it matches:{candidates}"
                    )
                }
            }
        }
        Selector::AtOrBefore(when) => {
            let when = chrono::DateTime::parse_from_rfc3339(when)
                .with_context(|| format!("not an RFC 3339 time: {when:?}"))?
                .with_timezone(&chrono::Utc);
            records.iter().rev().find(|r| r.time <= when)
        }
    };
    found.context("no matching version in history")
}

// ──────────────────────────────────────────────────
// Push
// ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PushOptions {
    pub history: HistoryKey,
    /// Concurrent uploads (at least 1).
    pub jobs: usize,
}

#[derive(Debug)]
pub struct PushReport {
    pub pointer: DvcPointer,
    /// The `.dvc` file written (or already identical) next to the output.
    pub pointer_path: PathBuf,
    pub files: usize,
    pub uploaded: usize,
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
pub fn push(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    block_on(push_async(remote, output, opts))?
}

async fn push_async(remote: &Remote, output: &Path, opts: &PushOptions) -> Result<PushReport> {
    let name = output_name(output)?;
    let pointer_path = pointer_path_for(output)?;
    check_existing_pointer(&pointer_path, &name)?;
    let meta = std::fs::symlink_metadata(output)
        .with_context(|| format!("failed to stat {}", output.display()))?;
    let tmp = tempfile::tempdir().context("failed to create a temp dir")?;

    let mut last_change = String::new();
    for _ in 0..PUSH_ATTEMPTS {
        let attempt = if meta.is_dir() {
            snapshot_dir(output, tmp.path())
        } else if meta.is_file() {
            snapshot_file(output, tmp.path())
        } else {
            anyhow::bail!(
                "{} is neither a regular file nor a directory",
                output.display()
            );
        };
        let staged = match attempt {
            Ok(s) => s,
            Err(Retry::Changed(msg)) => {
                last_change = msg;
                continue;
            }
            Err(Retry::Fatal(e)) => return Err(e),
        };
        return publish(remote, staged, name, pointer_path, opts).await;
    }
    anyhow::bail!("{last_change}; the output kept changing, push again later")
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

/// An output fully snapshotted, ready to upload.
struct Staged {
    kind: StagedKind,
    warnings: Vec<String>,
    empty_dirs: usize,
}

enum StagedKind {
    Dir {
        manifest: Manifest,
        snapshots: BTreeMap<Hexdigest, Snapshot>,
        size: u64,
    },
    File(Snapshot),
}

fn warn_unterminated(relpath: &str, s: &Snapshot, warnings: &mut Vec<String>) {
    if relpath.ends_with(".jsonl") && s.unterminated_line() {
        warnings.push(format!(
            "{relpath} does not end in a newline (a line was being written)"
        ));
    }
}

fn snapshot_dir(dir: &Path, tmp: &Path) -> std::result::Result<Staged, Retry> {
    let walk = match walk::walk(dir) {
        Ok(w) => w,
        Err(WalkError::Changed(m)) => return Err(Retry::Changed(m)),
        Err(e) => return Err(Retry::Fatal(e.into())),
    };
    let mut entries = Vec::with_capacity(walk.files.len());
    let mut snapshots = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut size = 0;
    for f in walk.files {
        let s = snapshot::snapshot(&f.path, tmp)?;
        warn_unterminated(f.relpath.as_str(), &s, &mut warnings);
        size += s.size();
        entries.push(ManifestEntry {
            relpath: f.relpath.to_manifest_path(),
            md5: s.md5().clone(),
        });
        snapshots.entry(s.md5().clone()).or_insert(s);
    }
    let manifest = Manifest::from_entries(entries).map_err(Retry::Fatal)?;
    Ok(Staged {
        kind: StagedKind::Dir {
            manifest,
            snapshots,
            size,
        },
        warnings,
        empty_dirs: walk.empty_dirs,
    })
}

fn snapshot_file(file: &Path, tmp: &Path) -> std::result::Result<Staged, Retry> {
    let s = snapshot::snapshot(file, tmp)?;
    let mut warnings = Vec::new();
    warn_unterminated(&file.to_string_lossy(), &s, &mut warnings);
    Ok(Staged {
        kind: StagedKind::File(s),
        warnings,
        empty_dirs: 0,
    })
}

async fn publish(
    remote: &Remote,
    staged: Staged,
    name: String,
    pointer_path: PathBuf,
    opts: &PushOptions,
) -> Result<PushReport> {
    let (pointer, files, uploads, manifest_bytes) = match staged.kind {
        StagedKind::Dir {
            manifest,
            snapshots,
            size,
        } => {
            let id = manifest.id();
            let pointer = DvcPointer {
                output: DvcOutput::Dir {
                    manifest: id.clone(),
                    size,
                    nfiles: manifest.entries().len() as u64,
                },
                path: name,
            };
            // A manifest already on the remote means all its objects are
            // (DVC's own invariant, and ours: it is uploaded last).
            let complete = backend::exists(&remote.backend, &remote.manifest_key(&id)).await?;
            let uploads: Vec<Snapshot> = if complete {
                Vec::new()
            } else {
                snapshots.into_values().collect()
            };
            let bytes = (!complete).then(|| manifest.to_bytes());
            (pointer, manifest.entries().len(), uploads, bytes)
        }
        StagedKind::File(s) => {
            let pointer = DvcPointer {
                output: DvcOutput::File {
                    md5: s.md5().clone(),
                    size: s.size(),
                },
                path: name,
            };
            (pointer, 1, vec![s], None)
        }
    };

    let (uploaded, already_present) = upload_all(remote, &uploads, opts.jobs.max(1)).await?;
    if let (DvcOutput::Dir { manifest, .. }, Some(bytes)) = (&pointer.output, manifest_bytes) {
        backend::put_bytes(&remote.backend, &remote.manifest_key(manifest), bytes).await?;
    }
    write_pointer_file(&pointer_path, &pointer)?;
    let history_record = append_history(remote, &opts.history, &pointer).await?;

    Ok(PushReport {
        pointer,
        pointer_path,
        files,
        uploaded,
        already_present,
        empty_dirs: staged.empty_dirs,
        warnings: staged.warnings,
        history_record,
    })
}

/// Upload each snapshot the remote lacks. Returns (uploaded, already there).
async fn upload_all(remote: &Remote, snaps: &[Snapshot], jobs: usize) -> Result<(usize, usize)> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let results: Vec<bool> = stream::iter(snaps)
        .map(|s| async move {
            let key = remote.object_key(s.md5());
            if backend::exists(&remote.backend, &key).await? {
                return Ok::<_, anyhow::Error>(false);
            }
            backend::upload(&remote.backend, s.path(), &key)
                .await
                .with_context(|| format!("upload of {} failed", s.md5()))?;
            Ok(true)
        })
        .buffer_unordered(jobs)
        .try_collect()
        .await?;
    let uploaded = results.iter().filter(|u| **u).count();
    Ok((uploaded, results.len() - uploaded))
}

/// Append `pointer` to the output's history unless the latest record is the
/// same version. Record keys are `<time>-<id>.dvc` with nanosecond time, so
/// two hosts pushing at once never overwrite each other's record. The time
/// is never earlier than the latest record's, so the newest push is always
/// `Latest` even if this host's clock lags.
async fn append_history(
    remote: &Remote,
    key: &HistoryKey,
    pointer: &DvcPointer,
) -> Result<Option<String>> {
    let records = read_history(remote, key).await?;
    let latest = records.last();
    if latest.is_some_and(|r| r.pointer.output == pointer.output) {
        return Ok(None);
    }
    let mut time = chrono::Utc::now();
    if let Some(r) = latest {
        time = time.max(r.time + chrono::Duration::nanoseconds(1));
    }
    let record = format!(
        "{}{}-{}.dvc",
        history_prefix(remote, key),
        time.format(RECORD_TIME),
        output_id(&pointer.output)
    );
    backend::put_bytes(&remote.backend, &record, pointer.to_yaml().into_bytes()).await?;
    Ok(Some(record))
}

/// The output's name: one portable path component.
fn output_name(output: &Path) -> Result<String> {
    let name = output
        .file_name()
        .and_then(|n| n.to_str())
        .with_context(|| format!("{} has no usable file name", output.display()))?;
    check_portable_component(name)?;
    anyhow::ensure!(
        !name.ends_with(".dvc"),
        "{name}: cannot back up a .dvc file"
    );
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

/// Refuse to replace a `.dvc` that is not a plain bigstore/DVC pointer for
/// this output (e.g. a hand-written one with extra fields or a stage).
fn check_existing_pointer(pointer_path: &Path, name: &str) -> Result<()> {
    let text = match std::fs::read_to_string(pointer_path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", pointer_path.display()))
        }
    };
    let existing = DvcPointer::parse(&text).with_context(|| {
        format!(
            "{} exists and is not a plain DVC 3 pointer; refusing to replace it",
            pointer_path.display()
        )
    })?;
    anyhow::ensure!(
        existing.path == name,
        "{} points at {:?}, not {name:?}; refusing to replace it",
        pointer_path.display(),
        existing.path
    );
    Ok(())
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

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Where to restore. Defaults to the `.dvc` file's `<dir>/<path>`;
    /// required for [`PointerSource::History`].
    pub into: Option<PathBuf>,
    pub overwrite: Overwrite,
    pub jobs: usize,
}

#[derive(Debug)]
pub struct PullReport {
    pub pointer: DvcPointer,
    pub written: usize,
    pub unchanged: usize,
    /// Local files not in the version pulled. Never deleted.
    pub extra_local: usize,
}

/// Local files that differ from the version being pulled, under
/// [`Overwrite::Refuse`]. Nothing was written.
#[derive(Debug)]
pub struct PullConflict {
    pub paths: Vec<PathBuf>,
}

impl std::fmt::Display for PullConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} local file(s) differ from the version being pulled (nothing written; \
             use force to replace):",
            self.paths.len()
        )?;
        for p in &self.paths {
            write!(f, "\n  {}", p.display())?;
        }
        Ok(())
    }
}

impl std::error::Error for PullConflict {}

/// Restore an output. Every target is classified before anything is written:
/// a symlinked or non-regular path is always an error; differing files are
/// refused (as [`PullConflict`]) unless forced. Files are downloaded to a
/// temp file beside their target, verified, then renamed into place — never
/// linked. Local files not in the version are left alone.
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
            let into = path
                .parent()
                .context("pointer has no parent directory")?
                .join(&pointer.path);
            (pointer, Some(into))
        }
        PointerSource::History { key, at } => {
            let records = read_history(remote, key).await?;
            (select(&records, at)?.pointer.clone(), None)
        }
    };
    let into = opts
        .into
        .clone()
        .or(default_into)
        .context("pulling from history needs a destination (`into`)")?;

    let targets: Vec<(PathBuf, Hexdigest)> = match &pointer.output {
        DvcOutput::Dir { manifest, .. } => {
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
                    let path = e
                        .relpath
                        .to_repo_path()
                        .with_context(|| format!("cannot restore {:?}", e.relpath.as_str()))?;
                    Ok((path.to_fs_path(&into), e.md5.clone()))
                })
                .collect::<Result<_>>()?
        }
        DvcOutput::File { md5, .. } => vec![(into.clone(), md5.clone())],
    };

    let mut plan = Vec::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0;
    for (path, md5) in &targets {
        match classify_target(&into, path, md5)? {
            Target::Same => unchanged += 1,
            Target::Missing => plan.push((path, md5, false)),
            Target::Differs => match opts.overwrite {
                Overwrite::Refuse => conflicts.push(path.clone()),
                Overwrite::Force => plan.push((path, md5, true)),
            },
        }
    }
    if !conflicts.is_empty() {
        return Err(PullConflict { paths: conflicts }.into());
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
    let written = fetch_and_place(remote, by_object, opts.jobs.max(1)).await?;

    Ok(PullReport {
        pointer,
        written,
        unchanged,
        extra_local,
    })
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
                Ok(_) => anyhow::bail!(
                    "{} is in the way (not a directory); refusing to write through it",
                    cur.display()
                ),
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
        Ok(_) => anyhow::bail!(
            "{} is not a regular file; refusing to replace it",
            path.display()
        ),
    }
}

/// Entries that differ only by ASCII case would collide on macOS and Windows.
fn check_case_collisions(manifest: &Manifest) -> Result<()> {
    let mut seen = std::collections::HashMap::new();
    for e in manifest.entries() {
        let folded = e.relpath.as_str().to_ascii_lowercase();
        if let Some(other) = seen.insert(folded, e.relpath.as_str()) {
            anyhow::bail!(
                "{other:?} and {:?} differ only by case and would collide on this filesystem",
                e.relpath.as_str()
            );
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

async fn fetch_and_place(
    remote: &Remote,
    by_object: BTreeMap<&Hexdigest, Vec<(&PathBuf, bool)>>,
    jobs: usize,
) -> Result<usize> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let counts: Vec<usize> = stream::iter(by_object)
        .map(|(md5, places)| async move {
            let (first, _) = places[0];
            let dir = first.parent().context("target has no parent")?;
            std::fs::create_dir_all(dir)?;
            let tmp =
                backend::download_verified(&remote.backend, &remote.object_key(md5), md5, dir)
                    .await?;
            // Extra copies first (from the verified temp), then move the temp.
            for (path, replace) in &places[1..] {
                let parent = path.parent().context("target has no parent")?;
                std::fs::create_dir_all(parent)?;
                let mut copy = tempfile::NamedTempFile::new_in(parent)?;
                std::io::copy(&mut std::fs::File::open(tmp.path())?, &mut copy)?;
                place(copy, path, *replace)?;
            }
            place(tmp, first, places[0].1)?;
            Ok::<_, anyhow::Error>(places.len())
        })
        .buffer_unordered(jobs)
        .try_collect()
        .await?;
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
            anyhow::Error::from(e.error).context(format!(
                "{} appeared while pulling; left untouched",
                path.display()
            ))
        })?;
    }
    Ok(())
}
