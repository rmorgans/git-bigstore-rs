use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use object_store::ObjectStoreExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

use crate::backend::{self, Backend};
use crate::cache;
use crate::config::BigstoreConfig;
use crate::filter::{self, WorktreeFile};
use crate::git::{self, IndexBlob, IndexEntry};
use crate::hash::{self, Hasher};
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
    pub store: &'a Backend,
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
    let pb = progress_bar(objects.len() as u64);
    let results: Vec<(&Object, Result<Outcome>)> = stream::iter(objects)
        .map(|obj| {
            let fut = op(obj);
            async move { (obj, fut.await) }
        })
        .buffer_unordered(jobs)
        .inspect(|(obj, _)| {
            pb.set_message(obj.paths[0].to_string());
            pb.inc(1);
        })
        .collect()
        .await;
    pb.finish_and_clear();

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
    if backend::exists(remote.store, &key).await? {
        return Ok(Outcome::UpToDate);
    }
    let cache_path = cache::object_path(remote.git_dir, hexdigest);
    anyhow::ensure!(
        cache_path.is_file(),
        "not in the local cache and not on the remote \
         (push from the clone that committed it)"
    );
    backend::upload(remote.store, &cache_path, &key).await?;
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
        backend::exists(remote.store, &key).await?,
        "not found on remote"
    );
    download_to_cache(remote, &key, hexdigest)
        .await
        .context("download failed")?;
    Ok(Outcome::Transferred)
}

async fn download_to_cache(remote: &Remote<'_>, key: &str, expected: &Hexdigest) -> Result<()> {
    cache::ensure_cache_dir(remote.git_dir)?;
    let tmp = tempfile::NamedTempFile::new_in(cache::cache_dir(remote.git_dir))?;

    let actual = match remote.store {
        Backend::ObjectStore(store) => {
            let mut file = tokio::fs::File::from_std(tmp.reopen()?);
            let mut stream = store
                .get(&object_store::path::Path::from(key))
                .await?
                .into_stream();
            let mut hasher = Hasher::new(expected.hash_fn());
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                hasher.update(&chunk);
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            hasher.finalize()
        }
        Backend::Rclone(_) => {
            backend::download(remote.store, key, tmp.path()).await?;
            let (path, hash_fn) = (tmp.path().to_path_buf(), expected.hash_fn());
            tokio::task::spawn_blocking(move || hash::hash_file(&path, hash_fn)).await??
        }
    };

    anyhow::ensure!(
        actual == *expected,
        "integrity check failed: expected {expected}, got {actual}"
    );

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
    /// Paths left as pointers because git could not check them out.
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
/// Paths are classified before anything changes. Their pointer files are
/// then removed (git skips entries whose stat data still matches the index,
/// which a freshly checked-out pointer does) and git checks them all out in
/// one batch. If the batch fails, git has written no index, so every path is
/// reconciled on its own: content git did write is kept (after checking it is
/// the object) and its index entry refreshed; paths git never reached are
/// retried alone, and on failure get their pointer back. Nothing that
/// appears at a path meanwhile is ever overwritten, a failed pull leaves the
/// tree clean, and re-running pull converges.
pub fn checkout(repo_root: &Path, git_dir: &Path, entries: &[IndexEntry]) -> Result<Checkout> {
    let mut result = Checkout {
        checked_out: 0,
        failed: Vec::new(),
    };
    let mut candidates = Vec::new();
    for entry in entries.iter().filter(|e| !e.skip_worktree) {
        let IndexBlob::Pointer(pointer) = &entry.blob else {
            continue;
        };
        if !cache::object_path(git_dir, pointer.hexdigest()).is_file() {
            continue;
        }
        let fs_path = entry.path.to_fs_path(repo_root);
        if matches!(filter::worktree_file(&fs_path)?, WorktreeFile::Pointer(p) if p == *pointer) {
            candidates.push(Candidate {
                path: &entry.path,
                pointer,
                permissions: std::fs::metadata(&fs_path)?.permissions(),
                fs_path,
            });
        }
    }

    let mut removed = Vec::new();
    for c in candidates {
        match std::fs::remove_file(&c.fs_path) {
            Ok(()) => removed.push(c),
            Err(e) => result
                .failed
                .push(c.failure(format!("failed to replace: {e}"))),
        }
    }
    let paths: Vec<&RepoPath> = removed.iter().map(|c| c.path).collect();
    if git::checkout_index(repo_root, &paths).is_ok() {
        result.checked_out += removed.len();
        return Ok(result);
    }

    let mut written = Vec::new();
    for c in removed {
        match filter::worktree_file(&c.fs_path)? {
            WorktreeFile::Missing => match git::checkout_index(repo_root, &[c.path]) {
                Ok(()) => result.checked_out += 1,
                Err(e) => {
                    let error =
                        match restore_pointer(&c.fs_path, &c.pointer.encode(), &c.permissions) {
                            Ok(()) => format!("{e:#}"),
                            Err(restore) => format!(
                                "{e:#}; could not restore the pointer file ({restore:#}), \
                             run `git checkout -- {}`",
                                c.path
                            ),
                        };
                    result.failed.push(c.failure(error));
                }
            },
            WorktreeFile::Content
                if hash::hash_file(&c.fs_path, c.pointer.hash_fn())
                    .ok()
                    .as_ref()
                    == Some(c.pointer.hexdigest()) =>
            {
                written.push(c);
            }
            _ => result.failed.push(
                c.failure("the file changed while pull was running; left untouched".to_string()),
            ),
        }
    }
    let paths: Vec<&RepoPath> = written.iter().map(|c| c.path).collect();
    match git::refresh_entries(repo_root, &paths) {
        Ok(()) => result.checked_out += written.len(),
        Err(e) => result
            .failed
            .extend(written.iter().map(|c| c.failure(format!("{e:#}")))),
    }
    Ok(result)
}

struct Candidate<'a> {
    path: &'a RepoPath,
    pointer: &'a Pointer,
    fs_path: PathBuf,
    permissions: std::fs::Permissions,
}

impl Candidate<'_> {
    fn failure(&self, error: String) -> Failure {
        Failure {
            paths: vec![self.path.clone()],
            error,
        }
    }
}

/// Put a pointer file back after a failed checkout. Git runs the smudge
/// filter before creating the file, so after a failure the path is normally
/// empty; `create_new` guarantees that anything that did appear there in the
/// meantime is reported, never replaced.
fn restore_pointer(path: &Path, pointer: &[u8], permissions: &std::fs::Permissions) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("{} exists", path.display()))?;
    file.write_all(pointer)?;
    file.set_permissions(permissions.clone())?;
    Ok(())
}

fn progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {msg}")
            .expect("progress template is valid")
            .progress_chars("#>-"),
    );
    pb
}
