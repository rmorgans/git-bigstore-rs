use anyhow::Result;
use std::path::{Path, PathBuf};

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

/// Path to a DVC cache object.
/// Layout: .dvc/cache/files/md5/<first2>/<rest>
pub fn dvc_cache_path(repo_root: &Path, hexdigest: &Hexdigest) -> PathBuf {
    repo_root
        .join(".dvc/cache/files/md5")
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
