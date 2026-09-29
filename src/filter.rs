use anyhow::{Context, Result};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::cache;
use crate::git;
use crate::hash::Hasher;
use crate::types::{HashFunction, Pointer, MAX_POINTER_BYTES};

/// The first bytes of a stream, classified by [`Pointer::parse`] — the one
/// rule shared by both filters and the working-tree check.
enum Head {
    /// The whole stream is a pointer; `raw` is every byte of it.
    Pointer { pointer: Pointer, raw: Vec<u8> },
    /// Content; `head` is its first bytes, the rest is still in the reader.
    Content { head: Vec<u8> },
}

fn read_head(reader: &mut impl Read) -> io::Result<Head> {
    let mut head = Vec::with_capacity(MAX_POINTER_BYTES + 1);
    // One byte past the limit: a pointer is always shorter, so anything that
    // fills the buffer is content.
    reader
        .take(MAX_POINTER_BYTES as u64 + 1)
        .read_to_end(&mut head)?;
    Ok(match Pointer::parse(&head) {
        Some(pointer) => Head::Pointer { pointer, raw: head },
        None => Head::Content { head },
    })
}

/// Clean filter: file content -> pointer (stdin -> stdout).
///
/// Input that is already a pointer passes through unchanged (idempotent).
/// Everything else — including text that merely starts like a pointer — is
/// hashed into the cache and replaced by its pointer.
pub fn clean() -> Result<()> {
    let mut reader = io::stdin().lock();
    let mut writer = io::stdout().lock();

    let head = match read_head(&mut reader)? {
        Head::Pointer { raw, .. } => return Ok(writer.write_all(&raw)?),
        Head::Content { head } => head,
    };

    let git_dir = git::common_dir()?;
    cache::ensure_cache_dir(&git_dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(cache::cache_dir(&git_dir))?;
    let mut hasher = Hasher::new(HashFunction::Sha256);

    hasher.update(&head);
    tmp.write_all(&head)?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        tmp.write_all(&buf[..n])?;
    }
    let hexdigest = hasher.finalize();

    let dest = cache::object_path(&git_dir, &hexdigest);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Atomic persist; AlreadyExists means a concurrent clean of the same content.
    match tmp.persist_noclobber(&dest) {
        Ok(_) => {}
        Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }

    writer.write_all(&Pointer::new(hexdigest).encode())?;
    Ok(())
}

/// Smudge filter: pointer -> file content (stdin -> stdout).
///
/// A pointer whose object is cached becomes that object's content; one that
/// is not cached passes through (`git bigstore pull` fetches it later).
/// Anything that is not a pointer passes through byte for byte.
pub fn smudge() -> Result<()> {
    let mut reader = io::stdin().lock();
    let mut writer = io::stdout().lock();

    match read_head(&mut reader)? {
        Head::Content { head } => {
            writer.write_all(&head)?;
            io::copy(&mut reader, &mut writer)?;
        }
        Head::Pointer { pointer, raw } => {
            let cache_path = cache::object_path(&git::common_dir()?, pointer.hexdigest());
            match std::fs::File::open(&cache_path) {
                Ok(mut object) => {
                    io::copy(&mut object, &mut writer)?;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => writer.write_all(&raw)?,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed to open {}", cache_path.display()))
                }
            }
        }
    }
    Ok(())
}

/// What the working tree holds at a tracked path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorktreeFile {
    /// Nothing there: deleted, or outside a sparse checkout.
    Missing,
    /// A pointer that has not been smudged into content.
    Pointer(Pointer),
    /// Anything else: checked-out content, a local edit, a directory or a
    /// symlink. Never overwritten by `pull`.
    Content,
}

pub fn worktree_file(path: &Path) -> Result<WorktreeFile> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(WorktreeFile::Missing),
        Err(e) => return Err(e).with_context(|| format!("failed to stat {}", path.display())),
    };
    if !meta.is_file() {
        return Ok(WorktreeFile::Content);
    }
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    Ok(match read_head(&mut file)? {
        Head::Pointer { pointer, .. } => WorktreeFile::Pointer(pointer),
        Head::Content { .. } => WorktreeFile::Content,
    })
}
