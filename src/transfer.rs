use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};

use crate::backend::Store;
use crate::cache;
use crate::config::BigstoreConfig;
use crate::filter::{self, WorktreeFile};
use crate::git::{self, IndexBlob, IndexEntry};
use crate::hash;
use crate::types::{HashFunction, Hexdigest, Pointer, RepoPath};

pub const DEFAULT_CONCURRENCY: usize = 8;

/// One content object and every tracked path that references it. Transfers
/// run per object, so identical files move once. Only built by [`objects`],
/// which guarantees at least one path.
pub struct Object {
    hexdigest: Hexdigest,
    paths: Vec<RepoPath>,
}

/// The objects referenced by pointer entries. Entries whose index blob is raw
/// content have nothing to transfer and are skipped.
pub fn objects<'a>(entries: impl IntoIterator<Item = &'a IndexEntry>) -> Vec<Object> {
    let mut by_digest: BTreeMap<&Hexdigest, Vec<RepoPath>> = BTreeMap::new();
    for entry in entries {
        if let IndexBlob::Pointer(p) = &entry.blob {
            by_digest
                .entry(p.hexdigest())
                .or_default()
                .push(entry.path.clone());
        }
    }
    by_digest
        .into_iter()
        .map(|(hexdigest, paths)| Object {
            hexdigest: hexdigest.clone(),
            paths,
        })
        .collect()
}

/// The remote store and the local cache a transfer moves objects between.
pub struct Remote<'a> {
    pub store: &'a Store,
    pub cfg: &'a BigstoreConfig,
    /// Common git dir holding the object cache.
    pub git_dir: &'a Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Push,
    Pull,
}

/// Outcome of a push or pull, counted in files (paths), not objects.
pub struct Report {
    pub direction: Direction,
    pub transferred: usize,
    pub up_to_date: usize,
    pub failed: Vec<Failure>,
}

pub struct Failure {
    pub paths: Vec<RepoPath>,
    pub error: String,
}

impl Report {
    pub fn print(&self) {
        if self.transferred > 0 {
            match self.direction {
                Direction::Push => eprintln!("{} file(s) uploaded", self.transferred),
                Direction::Pull => {
                    eprintln!("{} file(s) downloaded and verified", self.transferred)
                }
            }
        }
        if self.up_to_date > 0 {
            eprintln!("{} file(s) already up to date", self.up_to_date);
        }
        for f in &self.failed {
            let paths: Vec<&str> = f.paths.iter().map(RepoPath::as_str).collect();
            eprintln!("FAILED: {} — {}", paths.join(", "), f.error);
        }
    }
}

enum Outcome {
    Transferred,
    UpToDate,
}

/// Run `op` over every object with at most `jobs` in flight.
async fn run<'a, F, Fut>(objects: &'a [Object], jobs: usize, direction: Direction, op: F) -> Report
where
    F: Fn(&'a Object) -> Fut,
    Fut: Future<Output = Result<Outcome>> + 'a,
{
    let progress = Progress::new(objects.len());
    let results: Vec<(&Object, Result<Outcome>)> = stream::iter(objects)
        .map(|obj| {
            let fut = op(obj);
            async move { (obj, fut.await) }
        })
        .buffer_unordered(jobs)
        .inspect(|(obj, _)| progress.advance(&obj.paths[0]))
        .collect()
        .await;
    progress.finish();

    let mut report = Report {
        direction,
        transferred: 0,
        up_to_date: 0,
        failed: Vec::new(),
    };
    for (obj, result) in results {
        match result {
            Ok(Outcome::Transferred) => report.transferred += obj.paths.len(),
            Ok(Outcome::UpToDate) => report.up_to_date += obj.paths.len(),
            Err(e) => report.failed.push(Failure {
                paths: obj.paths.clone(),
                error: format!("{e:#}"),
            }),
        }
    }
    report
}

// ──────────────────────────────────────────────────
// Push
// ──────────────────────────────────────────────────
//
// 1. Already on the remote → up to date (dedup).
// 2. Otherwise it must be in the local cache → upload it.
// 3. Neither → failure. The pointer is committed but its content exists
//    nowhere this clone can reach; exiting 0 here would hide data loss.

pub async fn push(remote: &Remote<'_>, objects: &[Object], jobs: usize) -> Report {
    run(objects, jobs, Direction::Push, |obj| {
        upload_one(remote, &obj.hexdigest)
    })
    .await
}

async fn upload_one(remote: &Remote<'_>, hexdigest: &Hexdigest) -> Result<Outcome> {
    let key = remote.cfg.remote_object_key(hexdigest)?;
    if remote.store.head(&key).await?.is_some() {
        return Ok(Outcome::UpToDate);
    }
    let cache_path = cache::object_path(remote.git_dir, hexdigest);
    anyhow::ensure!(
        cache_path.is_file(),
        "not in the local cache and not on the remote \
         (push from the clone that committed it)"
    );
    remote.store.put_file(&key, &cache_path).await?;
    Ok(Outcome::Transferred)
}

// ──────────────────────────────────────────────────
// Pull
// ──────────────────────────────────────────────────
//
// Fetch fills the cache; `checkout` then updates the working tree.
// 1. Already cached → up to date.
// 2. md5 objects: import from the local DVC cache if present (verified).
// 3. Download to a temp file, hashing as it streams; verify; persist
//    atomically. The cache never holds unverified content.

pub async fn pull(
    remote: &Remote<'_>,
    dvc_cache_root: &Path,
    objects: &[Object],
    jobs: usize,
) -> Report {
    run(objects, jobs, Direction::Pull, |obj| {
        fetch_one(remote, dvc_cache_root, &obj.hexdigest)
    })
    .await
}

async fn fetch_one(
    remote: &Remote<'_>,
    dvc_cache_root: &Path,
    hexdigest: &Hexdigest,
) -> Result<Outcome> {
    // Resolving the key first rejects layouts that cannot address this hash
    // function before anything is written to the cache.
    let key = remote.cfg.remote_object_key(hexdigest)?;
    if cache::object_path(remote.git_dir, hexdigest).is_file() {
        return Ok(Outcome::UpToDate);
    }

    if hexdigest.hash_fn() == HashFunction::Md5 {
        let (root, git_dir, digest) = (
            dvc_cache_root.to_path_buf(),
            remote.git_dir.to_path_buf(),
            hexdigest.clone(),
        );
        let imported = tokio::task::spawn_blocking(move || {
            cache::import_from_dvc_cache(&root, &git_dir, &digest)
        })
        .await?
        .context("DVC cache import failed")?;
        match imported {
            cache::DvcImportResult::Imported => return Ok(Outcome::Transferred),
            cache::DvcImportResult::AlreadyCached => return Ok(Outcome::UpToDate),
            cache::DvcImportResult::NotInDvcCache => {}
        }
    }

    anyhow::ensure!(
        remote.store.head(&key).await?.is_some(),
        "not found on remote"
    );
    download_to_cache(remote, &key, hexdigest)
        .await
        .context("download failed")?;
    Ok(Outcome::Transferred)
}

async fn download_to_cache(remote: &Remote<'_>, key: &str, expected: &Hexdigest) -> Result<()> {
    cache::ensure_cache_dir(remote.git_dir)?;
    let tmp = remote
        .store
        .download_verified(key, expected, &cache::cache_dir(remote.git_dir))
        .await?;

    let dest = cache::object_path(remote.git_dir, expected);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match tmp.persist_noclobber(&dest) {
        Ok(_) => Ok(()),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.error.into()),
    }
}

/// Result of [`checkout`].
pub struct Checkout {
    pub checked_out: usize,
    /// Files an interrupted earlier pull left behind, now repaired.
    pub recovered: usize,
    /// Paths left as they were because git could not check them out.
    pub failed: Vec<Failure>,
}

/// Replace every unsmudged pointer file whose object is cached with its
/// content.
///
/// Git writes the files (`checkout-index -u` through the smudge filter), so
/// they get the index's file mode and the index's stat data is refreshed —
/// the tree is clean afterwards. Only a working-tree file that is exactly the
/// index's pointer is touched: local edits, other content, missing files and
/// skip-worktree (sparse) entries are left alone.
///
/// Git skips entries whose stat data still matches the index, which a
/// freshly checked-out pointer does, so each pointer file is removed first.
/// Before removing anything the paths are written to a journal in this
/// worktree's git directory:
///
/// - If git fails part-way (it then writes no index at all), each path is
///   reconciled on its own: a file git did write is verified against its
///   object and checked out again so the index is refreshed; a path git never
///   reached is retried alone and, if that fails, gets its original pointer
///   bytes back. The index blob is never changed.
/// - If pull is killed, the journal survives and the next pull repairs the
///   paths it names — missing files and files with stale index data — while
///   files deleted by the user (not in the journal) stay deleted.
///
/// Nothing that appears at a path while pull runs is ever overwritten.
pub fn checkout(
    repo_root: &Path,
    git_dir: &Path,
    journal: &Path,
    entries: &[IndexEntry],
) -> Result<Checkout> {
    // Journaled paths this run does not handle (outside the pull patterns,
    // unreadable) stay in the journal for a later pull.
    let mut carried = read_journal(journal)?;
    let mut result = Checkout {
        checked_out: 0,
        recovered: 0,
        failed: Vec::new(),
    };

    let mut candidates = Vec::new();
    for entry in entries.iter().filter(|e| !e.skip_worktree) {
        let IndexBlob::Pointer(pointer) = &entry.blob else {
            continue;
        };
        let fs_path = entry.path.to_fs_path(repo_root);
        let cached = cache::object_path(git_dir, pointer.hexdigest()).is_file();
        let interrupted = carried.remove(&entry.path);
        // Hashing content is only needed to recognise a file an interrupted
        // pull wrote; every other checked-out file is left alone unread.
        let state = match classify(&fs_path, pointer, interrupted) {
            Ok(state) => state,
            Err(e) => {
                if interrupted {
                    carried.insert(entry.path.clone());
                }
                result.failed.push(Failure {
                    paths: vec![entry.path.clone()],
                    error: format!("{e:#}"),
                });
                continue;
            }
        };
        let found = match state {
            Found::Pointer { raw, permissions } if cached => Found::Pointer { raw, permissions },
            Found::Missing | Found::Object if interrupted => {
                result.recovered += 1;
                state
            }
            _ => continue,
        };
        candidates.push(Candidate {
            path: &entry.path,
            pointer,
            fs_path,
            found,
        });
    }

    write_journal(
        journal,
        carried.iter().chain(candidates.iter().map(|c| c.path)),
    )?;

    let mut in_flight = Vec::new();
    for c in candidates {
        match c.clear() {
            Ok(()) => in_flight.push(c),
            Err(e) => result.failed.push(c.failure(format!("{e:#}"))),
        }
    }
    let paths: Vec<&RepoPath> = in_flight.iter().map(|c| c.path).collect();
    let mut unresolved = Vec::new();
    if git::checkout_index(repo_root, &paths).is_ok() {
        result.checked_out += in_flight.len();
    } else {
        for c in &in_flight {
            match c.checkout_alone(repo_root) {
                Ok(()) => result.checked_out += 1,
                Err(e) => {
                    // Still missing: a later pull must know this pull removed it.
                    if std::fs::symlink_metadata(&c.fs_path).is_err() {
                        unresolved.push(c.path);
                    }
                    result.failed.push(c.failure(format!("{e:#}")));
                }
            }
        }
    }

    write_journal(journal, carried.iter().chain(unresolved))?;
    Ok(result)
}

/// What a pointer entry's working-tree path holds, as far as checkout cares.
enum Found {
    /// Exactly the index's pointer, with its bytes and permissions (so a
    /// failed checkout can put it back unchanged).
    Pointer {
        raw: Vec<u8>,
        permissions: std::fs::Permissions,
    },
    /// Nothing.
    Missing,
    /// The object's exact content.
    Object,
    /// Anything else: never touched.
    Other,
}

/// `hash_content`: whether to check if other content is the object itself
/// (reads the whole file).
fn classify(fs_path: &Path, pointer: &Pointer, hash_content: bool) -> Result<Found> {
    Ok(match filter::worktree_file(fs_path)? {
        WorktreeFile::Missing => Found::Missing,
        WorktreeFile::Pointer(p) if p == *pointer => Found::Pointer {
            raw: std::fs::read(fs_path)?,
            permissions: std::fs::metadata(fs_path)?.permissions(),
        },
        WorktreeFile::Content
            if hash_content
                && hash::hash_file(fs_path, pointer.hash_fn())? == *pointer.hexdigest() =>
        {
            Found::Object
        }
        WorktreeFile::Pointer(_) | WorktreeFile::Content => Found::Other,
    })
}

struct Candidate<'a> {
    path: &'a RepoPath,
    pointer: &'a Pointer,
    fs_path: PathBuf,
    found: Found,
}

impl Candidate<'_> {
    /// Remove whatever git would otherwise consider up to date.
    fn clear(&self) -> Result<()> {
        match self.found {
            Found::Missing => Ok(()),
            _ => std::fs::remove_file(&self.fs_path)
                .with_context(|| format!("failed to replace {}", self.path)),
        }
    }

    /// Reconcile one path after a failed batch checkout.
    fn checkout_alone(&self, repo_root: &Path) -> Result<()> {
        match classify(&self.fs_path, self.pointer, true)? {
            // Git wrote it but recorded no stat data: write it again.
            Found::Object => {
                std::fs::remove_file(&self.fs_path)?;
                self.retry(repo_root)
            }
            Found::Missing => self.retry(repo_root),
            Found::Pointer { .. } | Found::Other => {
                anyhow::bail!("the file changed while pull was running; left untouched")
            }
        }
    }

    fn retry(&self, repo_root: &Path) -> Result<()> {
        let Err(e) = git::checkout_index(repo_root, &[self.path]) else {
            return Ok(());
        };
        let restored = match &self.found {
            Found::Pointer { raw, permissions } => restore(&self.fs_path, raw, permissions),
            // Nothing was there before this pull: leave it missing.
            _ => Ok(()),
        };
        match restored {
            Ok(()) => Err(e),
            Err(restore) => Err(e.context(format!(
                "could not restore the pointer file ({restore:#}); run `git checkout -- {}`",
                self.path
            ))),
        }
    }

    fn failure(&self, error: String) -> Failure {
        Failure {
            paths: vec![self.path.clone()],
            error,
        }
    }
}

/// Put a pointer file back after a failed checkout. `create_new` guarantees
/// that anything that appeared at the path meanwhile is reported, never
/// replaced.
fn restore(path: &Path, raw: &[u8], permissions: &std::fs::Permissions) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("{} exists", path.display()))?;
    file.write_all(raw)?;
    file.set_permissions(permissions.clone())?;
    Ok(())
}

/// Paths a previous pull removed and may not have finished checking out.
fn read_journal(journal: &Path) -> Result<std::collections::BTreeSet<RepoPath>> {
    let bytes = match std::fs::read(journal) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Default::default()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", journal.display())),
    };
    bytes
        .split(|&b| b == 0)
        .filter(|p| !p.is_empty())
        .map(RepoPath::from_git_bytes)
        .collect()
}

/// Durably replace the journal; an empty set removes it.
fn write_journal<'a>(journal: &Path, paths: impl Iterator<Item = &'a RepoPath>) -> Result<()> {
    use std::io::Write;
    let mut bytes = Vec::new();
    for p in paths {
        bytes.extend_from_slice(p.as_str().as_bytes());
        bytes.push(0);
    }
    if bytes.is_empty() {
        return match std::fs::remove_file(journal) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        };
    }
    let dir = journal.parent().context("journal path has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(&bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(journal)?;
    Ok(())
}

/// A bar on stderr counting finished objects, with the `progress` feature;
/// nothing without it.
#[cfg(feature = "progress")]
struct Progress(indicatif::ProgressBar);

#[cfg(feature = "progress")]
impl Progress {
    fn new(total: usize) -> Self {
        let pb = indicatif::ProgressBar::new(total as u64);
        pb.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {msg}",
            )
            .expect("progress template is valid")
            .progress_chars("#>-"),
        );
        Self(pb)
    }

    fn advance(&self, path: &RepoPath) {
        self.0.set_message(path.to_string());
        self.0.inc(1);
    }

    fn finish(self) {
        self.0.finish_and_clear();
    }
}

#[cfg(not(feature = "progress"))]
struct Progress;

#[cfg(not(feature = "progress"))]
impl Progress {
    fn new(_total: usize) -> Self {
        Self
    }

    fn advance(&self, _path: &RepoPath) {}

    fn finish(self) {}
}
