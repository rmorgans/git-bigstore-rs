use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

use crate::backend::{self, Backend};
use crate::cache;
use crate::config::BigstoreConfig;
use crate::filter;
use crate::git;
use crate::types::{HashFunction, Hexdigest, Pointer};

/// The result of a push/pull operation.
pub struct TransferSummary {
    pub uploaded: u64,
    pub downloaded: u64,
    pub skipped: u64,
    pub verified: u64,
    pub failed: Vec<TransferError>,
}

pub struct TransferError {
    pub path: String,
    pub error: String,
}

impl TransferSummary {
    fn new() -> Self {
        Self {
            uploaded: 0,
            downloaded: 0,
            skipped: 0,
            verified: 0,
            failed: Vec::new(),
        }
    }

    pub fn print(&self) {
        if self.uploaded > 0 {
            eprintln!("{} file(s) uploaded", self.uploaded);
        }
        if self.downloaded > 0 {
            eprintln!(
                "{} file(s) downloaded ({} verified)",
                self.downloaded, self.verified
            );
        }
        if self.skipped > 0 {
            eprintln!("{} file(s) already up to date", self.skipped);
        }
        for err in &self.failed {
            eprintln!("FAILED: {} — {}", err.path, err.error);
        }
    }
}

// ──────────────────────────────────────────────────
// Download lifecycle
// ──────────────────────────────────────────────────
//
// 1. Parse pointer from git (validates hash_fn + hexdigest)
// 2. Check local cache — skip if present
// 3. Check remote — warn if missing
// 4. Download to temp file, hashing content as it streams
// 5. Verify hash matches pointer's hexdigest
// 6. Persist verified temp file to cache (atomic)
// 7. Copy from cache to working tree
//
// If verification fails, the temp file is discarded.
// The cache never contains unverified content.

/// Download a single object: remote → verified cache → working tree.
async fn download_one(
    store: &Backend,
    cfg: &BigstoreConfig,
    git_dir: &Path,
    repo_root: &Path,
    path: &str,
    pointer: &Pointer,
    pb: &ProgressBar,
) -> Result<DownloadOutcome> {
    // Step 1: Verify the layout supports this hash function before doing any work.
    // Fail early rather than pulling into cache and then failing on push.
    cfg.remote_object_key(&pointer.hexdigest, pointer.hash_fn)?;

    let cache_path = cache::object_path(git_dir, &pointer.hexdigest, pointer.hash_fn);

    // Step 2: Already in cache?
    if cache_path.exists() {
        pb.set_message(format!("{path} (cached)"));
        pb.inc(1);
        let full_path = repo_root.join(path);
        cache::copy_to_working_tree(&cache_path, &full_path)?;
        return Ok(DownloadOutcome::Skipped);
    }

    // Step 2b: DVC cache fallback (md5 only)
    if pointer.hash_fn == HashFunction::Md5 {
        let dvc_path = cache::dvc_cache_path(repo_root, &pointer.hexdigest);
        if dvc_path.exists() {
            pb.set_message(format!("{path} (dvc cache, verifying)"));
            // Verify hash before trusting DVC cache
            let actual = hash_file(&dvc_path, pointer.hash_fn)?;
            anyhow::ensure!(
                &actual == &pointer.hexdigest,
                "DVC cache integrity check failed for {path}: expected {}, got {actual}",
                pointer.hexdigest
            );

            // Verified — atomic persist to bigstore cache
            if let Some(parent) = cache_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut tmp = tempfile::NamedTempFile::new_in(
                cache_path.parent().expect("cache path has parent"),
            )?;
            std::io::copy(&mut std::fs::File::open(&dvc_path)?, &mut tmp)?;
            tmp.flush()?;
            match tmp.persist_noclobber(&cache_path) {
                Ok(_) => {}
                Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.error.into()),
            }

            let full_path = repo_root.join(path);
            cache::copy_to_working_tree(&cache_path, &full_path)?;
            pb.set_message(format!("{path} (from dvc cache, verified)"));
            pb.inc(1);
            return Ok(DownloadOutcome::Downloaded);
        }
    }

    // Step 3: Exists on remote?
    let remote_key = cfg.remote_object_key(&pointer.hexdigest, pointer.hash_fn)?;
    if !backend::exists(store, &remote_key).await? {
        return Ok(DownloadOutcome::NotFound);
    }

    // Step 4: Download to temp file, hashing as we stream
    pb.set_message(format!("{path} (downloading)"));
    let verified_path =
        download_and_verify(store, &remote_key, git_dir, pointer.hash_fn, &pointer.hexdigest)
            .await
            .with_context(|| format!("downloading {path}"))?;

    // Step 6 happened inside download_and_verify (atomic persist)
    // Step 7: Copy to working tree
    let full_path = repo_root.join(path);
    cache::copy_to_working_tree(&verified_path, &full_path)?;

    pb.set_message(format!("{path} (verified)"));
    pb.inc(1);

    Ok(DownloadOutcome::Downloaded)
}

/// Download from remote, hash while streaming, verify, persist to cache.
/// Returns the final cache path on success.
async fn download_and_verify(
    store: &Backend,
    remote_key: &str,
    git_dir: &Path,
    hash_fn: HashFunction,
    expected: &Hexdigest,
) -> Result<std::path::PathBuf> {
    cache::ensure_cache_dir(git_dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(cache::cache_dir(git_dir))?;
    let mut hasher = Hasher::new(hash_fn);

    // Stream download → hash + write to temp
    match store {
        Backend::ObjectStore(obj_store) => {
            use futures::StreamExt;

            let obj_path = object_store::path::Path::from(remote_key);
            let result = obj_store.get(&obj_path).await?;
            let mut stream = result.into_stream();

            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                hasher.update(&bytes);
                tmp.write_all(&bytes)?;
            }
        }
        Backend::Rclone(_) => {
            // For rclone, download to temp first, then hash
            let tmp_dl =
                tempfile::NamedTempFile::new_in(cache::cache_dir(git_dir))?;
            backend::download(store, remote_key, tmp_dl.path()).await?;

            let mut file = std::fs::File::open(tmp_dl.path())?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = std::io::Read::read(&mut file, &mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                tmp.write_all(&buf[..n])?;
            }
        }
    }

    tmp.flush()?;

    // Step 5: Verify
    let actual_hex = hasher.finalize_hex();
    let actual = Hexdigest::new(&actual_hex, hash_fn)
        .context("internal error: sha256 produced invalid hex")?;

    anyhow::ensure!(
        &actual == expected,
        "integrity check failed: expected {expected}, got {actual}"
    );

    // Step 6: Atomic persist to cache
    let dest = cache::object_path(git_dir, expected, hash_fn);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match tmp.persist_noclobber(&dest) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }

    Ok(dest)
}

enum DownloadOutcome {
    Downloaded,
    Skipped,
    NotFound,
}

// ──────────────────────────────────────────────────
// Upload lifecycle
// ──────────────────────────────────────────────────
//
// 1. Parse pointer from git
// 2. Check local cache — skip if not cached (nothing to upload)
// 3. Check remote — skip if already present (dedup)
// 4. Upload from cache to remote
//
// No verification needed: the cache was verified on write (clean filter)
// or on download (download_and_verify).

async fn upload_one(
    store: &Backend,
    cfg: &BigstoreConfig,
    git_dir: &Path,
    path: &str,
    pointer: &Pointer,
    pb: &ProgressBar,
) -> Result<UploadOutcome> {
    let cache_path = cache::object_path(git_dir, &pointer.hexdigest, pointer.hash_fn);

    // Step 2: In local cache?
    if !cache_path.exists() {
        return Ok(UploadOutcome::NotCached);
    }

    // Step 3: Already on remote?
    let remote_key = cfg.remote_object_key(&pointer.hexdigest, pointer.hash_fn)?;
    if backend::exists(store, &remote_key).await? {
        pb.set_message(format!("{path} (exists)"));
        pb.inc(1);
        return Ok(UploadOutcome::Skipped);
    }

    // Step 4: Upload
    pb.set_message(format!("{path} (uploading)"));
    backend::upload(store, &cache_path, &remote_key).await?;
    pb.set_message(format!("{path} (done)"));
    pb.inc(1);

    Ok(UploadOutcome::Uploaded)
}

enum UploadOutcome {
    Uploaded,
    Skipped,
    NotCached,
}

// ──────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────

pub async fn push(tracked: &[(String, String)]) -> Result<TransferSummary> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;
    let cfg = config_load(&repo_root)?;
    let store = backend::from_config(&cfg)?;

    let mp = MultiProgress::new();
    let pb = mp.add(progress_bar(tracked.len() as u64));
    let mut summary = TransferSummary::new();

    for (path, _filter) in tracked {
        let pointer = match filter::read_pointer_from_git(path)? {
            Some(p) => p,
            None => continue,
        };

        match upload_one(&store, &cfg, &git_dir, path, &pointer, &pb).await {
            Ok(UploadOutcome::Uploaded) => summary.uploaded += 1,
            Ok(UploadOutcome::Skipped) => summary.skipped += 1,
            Ok(UploadOutcome::NotCached) => {
                tracing::debug!(%path, "not in local cache, skipping");
                summary.skipped += 1;
            }
            Err(e) => {
                summary.failed.push(TransferError {
                    path: path.clone(),
                    error: format!("{e:#}"),
                });
            }
        }
    }

    pb.finish_and_clear();
    Ok(summary)
}

pub async fn pull(tracked: &[(String, String)]) -> Result<TransferSummary> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;
    let cfg = config_load(&repo_root)?;
    let store = backend::from_config(&cfg)?;

    let mp = MultiProgress::new();
    let pb = mp.add(progress_bar(tracked.len() as u64));
    let mut summary = TransferSummary::new();

    for (path, _filter) in tracked {
        let pointer = match filter::read_pointer_from_git(path)? {
            Some(p) => p,
            None => continue,
        };

        match download_one(&store, &cfg, &git_dir, &repo_root, path, &pointer, &pb).await {
            Ok(DownloadOutcome::Downloaded) => {
                summary.downloaded += 1;
                summary.verified += 1;
            }
            Ok(DownloadOutcome::Skipped) => summary.skipped += 1,
            Ok(DownloadOutcome::NotFound) => {
                summary.failed.push(TransferError {
                    path: path.clone(),
                    error: "not found on remote".to_string(),
                });
            }
            Err(e) => {
                summary.failed.push(TransferError {
                    path: path.clone(),
                    error: format!("{e:#}"),
                });
            }
        }
    }

    pb.finish_and_clear();
    Ok(summary)
}

fn config_load(repo_root: &Path) -> Result<BigstoreConfig> {
    BigstoreConfig::find_and_load(repo_root)
}

/// Create a hasher for the given hash function.
enum Hasher {
    Sha256(Sha256),
    Md5(md5::Md5),
}

impl Hasher {
    fn new(hash_fn: HashFunction) -> Self {
        match hash_fn {
            HashFunction::Sha256 => Self::Sha256(Sha256::new()),
            HashFunction::Md5 => Self::Md5(<md5::Md5 as Digest>::new()),
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha256(h) => h.update(data),
            Self::Md5(h) => Digest::update(h, data),
        }
    }

    fn finalize_hex(self) -> String {
        match self {
            Self::Sha256(h) => hex::encode(h.finalize()),
            Self::Md5(h) => hex::encode(Digest::finalize(h)),
        }
    }
}

/// Hash a file on disk, returning a validated hexdigest.
pub fn hash_file(path: &Path, hash_fn: HashFunction) -> Result<Hexdigest> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Hasher::new(hash_fn);
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hex_str = hasher.finalize_hex();
    Hexdigest::new(&hex_str, hash_fn)
        .context("internal error: hasher produced invalid hex")
}

fn progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{bar:30.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}
