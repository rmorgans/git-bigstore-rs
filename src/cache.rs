use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::hash;
use crate::types::Hexdigest;

/// Root cache directory inside the (common) git directory.
pub fn cache_dir(git_dir: &Path) -> PathBuf {
    git_dir.join("bigstore").join("objects")
}

/// Scratch space for filter-process spools, on the cache's filesystem.
pub(crate) fn spool_dir(git_dir: &Path) -> PathBuf {
    git_dir.join("bigstore").join("tmp")
}

/// Full path to a cached object.
/// Layout: .git/bigstore/objects/{hash_fn}/<first2>/<rest>
///
/// Safe: Hexdigest is validated — no path traversal possible.
pub fn object_path(git_dir: &Path, hexdigest: &Hexdigest) -> PathBuf {
    cache_dir(git_dir)
        .join(hexdigest.hash_fn().as_str())
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

/// Path to an object under a resolved DVC cache root.
/// Layout: {dvc_cache_root}/files/{hash_fn}/<first2>/<rest>. DVC itself only
/// writes md5 objects, so other hash functions simply never exist there.
pub fn dvc_cache_path(dvc_cache_root: &Path, hexdigest: &Hexdigest) -> PathBuf {
    dvc_cache_root
        .join("files")
        .join(hexdigest.hash_fn().as_str())
        .join(hexdigest.prefix())
        .join(hexdigest.rest())
}

/// Atomically write a copy of `src` to a working-tree path. The file gets the
/// permissions of any newly created file (0666 minus umask), not the
/// owner-only mode of a temp file.
pub fn copy_to_worktree(src: &Path, dest: &Path) -> Result<()> {
    let parent = dest
        .parent()
        .with_context(|| format!("{} has no parent directory", dest.display()))?;
    std::fs::create_dir_all(parent)?;
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o666));
    }
    let mut tmp = builder.tempfile_in(parent)?;
    std::io::copy(&mut std::fs::File::open(src)?, &mut tmp)?;
    tmp.persist(dest)?;
    Ok(())
}

/// Atomically copy a file into place, failing if the destination already exists.
fn copy_atomically_noclobber(src: &Path, dest: &Path) -> std::io::Result<()> {
    let parent = dest.parent().expect("cache object paths have a parent");
    std::fs::create_dir_all(parent)?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    std::io::copy(&mut std::fs::File::open(src)?, &mut tmp)?;
    tmp.persist_noclobber(dest).map(drop).map_err(|e| e.error)
}

/// Result of attempting to import an object from the local DVC cache.
pub enum DvcImportResult {
    /// Object was verified and imported into bigstore cache.
    Imported,
    /// Object was already in bigstore cache (DVC cache not consulted).
    AlreadyCached,
    /// Object not found in DVC cache (and not in bigstore cache).
    NotInDvcCache,
}

/// Import an object from the local DVC cache into the bigstore cache.
///
/// `dvc_cache_root` is the resolved DVC cache directory (from `resolve_dvc_cache_root`).
///
/// On success, the object is hash-verified and atomically persisted.
/// Returns `Err` for integrity failures or I/O errors.
pub fn import_from_dvc_cache(
    dvc_cache_root: &Path,
    git_dir: &Path,
    hexdigest: &Hexdigest,
) -> Result<DvcImportResult> {
    let bs_cache = object_path(git_dir, hexdigest);
    if bs_cache.is_file() {
        return Ok(DvcImportResult::AlreadyCached);
    }

    let dvc_path = dvc_cache_path(dvc_cache_root, hexdigest);
    if !dvc_path.is_file() {
        return Ok(DvcImportResult::NotInDvcCache);
    }

    // Verify hash before trusting DVC cache
    let actual = hash::hash_file(&dvc_path, hexdigest.hash_fn())
        .context("failed to hash DVC cache object")?;
    anyhow::ensure!(
        actual == *hexdigest,
        "DVC cache integrity check failed: expected {hexdigest}, got {actual}"
    );

    match copy_atomically_noclobber(&dvc_path, &bs_cache) {
        Ok(()) => Ok(DvcImportResult::Imported),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(DvcImportResult::AlreadyCached)
        }
        Err(e) => Err(e.into()),
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
        let path = object_path(&git_dir, &digest);
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
        let path = object_path(&git_dir, &digest);
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
