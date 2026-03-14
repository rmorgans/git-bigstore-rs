use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::transfer;
use crate::types::{HashFunction, Hexdigest};

/// Root cache directory inside .git
pub fn cache_dir(git_dir: &Path) -> PathBuf {
    git_dir.join("bigstore").join("objects")
}

/// Full path to a cached object.
/// Layout: .git/bigstore/objects/{hash_fn}/<first2>/<rest>
///
/// Safe: Hexdigest is validated — no path traversal possible.
pub fn object_path(git_dir: &Path, hexdigest: &Hexdigest, hash_fn: HashFunction) -> PathBuf {
    cache_dir(git_dir)
        .join(hash_fn.as_str())
        .join(hexdigest.prefix())
        .join(hexdigest.rest())
}

/// Create the cache directory structure.
pub fn ensure_cache_dir(git_dir: &Path) -> Result<()> {
    let dir = cache_dir(git_dir);
    std::fs::create_dir_all(dir.join("sha256"))?;
    std::fs::create_dir_all(dir.join("md5"))?;
    Ok(())
}

/// Find the DVC project root by walking up from `start` to find the nearest
/// ancestor containing a `.dvc/` directory. Returns None if no DVC project found.
pub fn find_dvc_project_root(start: &Path) -> Option<PathBuf> {
    let mut dir = if start.is_file() {
        start.parent()?.to_path_buf()
    } else {
        start.to_path_buf()
    };
    loop {
        if dir.join(".dvc").is_dir() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Resolve the effective DVC cache root by asking DVC itself.
///
/// Runs `dvc cache dir` from `dvc_project_root` (the directory containing `.dvc/`).
/// Only shells out when the DVC project has a config file (`.dvc/config`),
/// since `dvc cache dir` returns the global cache even for bare `.dvc/` directories.
/// Falls back to `{dvc_project_root}/.dvc/cache` when dvc is not installed or
/// when no DVC config exists.
pub fn resolve_dvc_cache_root(dvc_project_root: &Path) -> Result<PathBuf> {
    use std::io::ErrorKind;

    // Only ask DVC if a config file exists — bare .dvc/ directories
    // (e.g. created by mkdir -p .dvc/cache) should use the default path.
    let has_config = dvc_project_root.join(".dvc/config").exists()
        || dvc_project_root.join(".dvc/config.local").exists();

    if !has_config {
        return Ok(dvc_project_root.join(".dvc/cache"));
    }

    match std::process::Command::new("dvc")
        .args(["cache", "dir"])
        .current_dir(dvc_project_root)
        .stderr(std::process::Stdio::piped())
        .output()
    {
        Ok(output) if output.status.success() => {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if path.is_empty() {
                anyhow::bail!(
                    "`dvc cache dir` returned empty output in {}",
                    dvc_project_root.display()
                );
            }
            Ok(PathBuf::from(path))
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "`dvc cache dir` failed in {}:\n{stderr}",
                dvc_project_root.display()
            );
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // dvc not installed — fall back to default location
            Ok(dvc_project_root.join(".dvc/cache"))
        }
        Err(e) => {
            anyhow::bail!("failed to run `dvc cache dir`: {e}");
        }
    }
}

/// Path to a DVC cache object under a resolved cache root.
/// Layout: {dvc_cache_root}/files/md5/<first2>/<rest>
pub fn dvc_cache_path(dvc_cache_root: &Path, hexdigest: &Hexdigest) -> PathBuf {
    dvc_cache_root
        .join("files/md5")
        .join(hexdigest.prefix())
        .join(hexdigest.rest())
}

/// Atomically copy a file into place.
/// Writes to a temp file in the destination directory, then renames.
pub fn copy_atomically(src: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = dest.parent().expect("dest has a parent directory");
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    std::io::copy(&mut std::fs::File::open(src)?, &mut tmp)?;
    std::io::Write::flush(&mut tmp)?;
    tmp.persist(dest)?;
    Ok(())
}

/// Atomically copy a file into place, failing if the destination already exists.
pub fn copy_atomically_noclobber(src: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = dest.parent().expect("dest has a parent directory");
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    std::io::copy(&mut std::fs::File::open(src)?, &mut tmp)?;
    std::io::Write::flush(&mut tmp)?;
    match tmp.persist_noclobber(dest) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.error.into()),
    }
}

/// Copy a cached object to the working tree atomically.
pub fn copy_to_working_tree(cache_path: &Path, dest: &Path) -> Result<()> {
    copy_atomically(cache_path, dest)
}

/// Result of attempting to import an MD5 object from the local DVC cache.
pub enum DvcImportResult {
    /// Object was verified and imported into bigstore cache.
    Imported,
    /// Object was already in bigstore cache (DVC cache not consulted).
    AlreadyCached,
    /// Object not found in DVC cache (and not in bigstore cache).
    NotInDvcCache,
}

/// Import an MD5 object from the local DVC cache into the bigstore cache.
///
/// MD5-specific by design: DVC cache paths are always MD5-sharded,
/// so this function only accepts MD5 hexdigests.
///
/// `dvc_cache_root` is the resolved DVC cache directory (from `resolve_dvc_cache_root`).
///
/// On success, the object is hash-verified and atomically persisted.
/// Returns `Err` for integrity failures or I/O errors.
pub fn import_md5_from_dvc_cache(
    dvc_cache_root: &Path,
    git_dir: &Path,
    hexdigest: &Hexdigest,
) -> Result<DvcImportResult> {
    let bs_cache = object_path(git_dir, hexdigest, HashFunction::Md5);
    if bs_cache.exists() {
        return Ok(DvcImportResult::AlreadyCached);
    }

    let dvc_path = dvc_cache_path(dvc_cache_root, hexdigest);
    if !dvc_path.exists() {
        return Ok(DvcImportResult::NotInDvcCache);
    }

    // Verify hash before trusting DVC cache
    let actual = transfer::hash_file(&dvc_path, HashFunction::Md5)
        .context("failed to hash DVC cache object")?;
    anyhow::ensure!(
        actual == *hexdigest,
        "DVC cache integrity check failed: expected {hexdigest}, got {actual}"
    );

    // Atomic persist to bigstore cache
    if let Some(parent) = bs_cache.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match copy_atomically_noclobber(&dvc_path, &bs_cache) {
        Ok(()) => Ok(DvcImportResult::Imported),
        Err(_) if bs_cache.exists() => Ok(DvcImportResult::AlreadyCached),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HashFunction;

    #[test]
    fn object_path_structure() {
        let git_dir = PathBuf::from("/repo/.git");
        let hex = "ab".repeat(32);
        let digest = Hexdigest::new(&hex, HashFunction::Sha256).unwrap();
        let path = object_path(&git_dir, &digest, HashFunction::Sha256);
        assert_eq!(
            path,
            PathBuf::from(format!(
                "/repo/.git/bigstore/objects/sha256/{}/{}",
                digest.prefix(),
                digest.rest()
            ))
        );
    }

    #[test]
    fn object_path_md5() {
        let git_dir = PathBuf::from("/repo/.git");
        let hex = "ab".repeat(16);
        let digest = Hexdigest::new(&hex, HashFunction::Md5).unwrap();
        let path = object_path(&git_dir, &digest, HashFunction::Md5);
        assert_eq!(
            path,
            PathBuf::from(format!(
                "/repo/.git/bigstore/objects/md5/{}/{}",
                digest.prefix(),
                digest.rest()
            ))
        );
    }
}
