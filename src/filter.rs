use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::path::Path;

use crate::cache;
use crate::catfile::CatFileBatch;
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
/// hashed into the cache and replaced by its pointer: sha256, unless the
/// index holds an md5 pointer for `path` that this content still matches.
/// Git passes `path` through `%f`; without it the pointer is always sha256.
pub fn clean(path: Option<&OsStr>) -> Result<()> {
    let mut reader = io::stdin().lock();
    let mut writer = io::stdout().lock();

    let head = match read_head(&mut reader)? {
        Head::Pointer { raw, .. } => return Ok(writer.write_all(&raw)?),
        Head::Content { head } => head,
    };

    let indexed = match path.and_then(OsStr::to_str) {
        Some(path) => index_pointer(&mut CatFileBatch::start(Path::new("."))?, path)?,
        None => None,
    };
    // Content matching an md5 pointer keeps it: git re-cleans checked-out
    // files (racy timestamps, touched files) and a sha256 pointer would show
    // them as modified and re-stage them.
    let mut kept = indexed
        .filter(|p| p.hash_fn() != HashFunction::Sha256)
        .map(|p| (Hasher::new(p.hash_fn()), p));

    let git_dir = git::common_dir()?;
    cache::ensure_cache_dir(&git_dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(cache::cache_dir(&git_dir))?;
    let mut hasher = Hasher::new(HashFunction::Sha256);

    let mut update = |data: &[u8]| -> io::Result<()> {
        hasher.update(data);
        if let Some((h, _)) = kept.as_mut() {
            h.update(data);
        }
        tmp.write_all(data)
    };
    update(&head)?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        update(&buf[..n])?;
    }
    let pointer = match kept.map(|(h, p)| (h.finalize(), p)) {
        Some((digest, p)) if digest == *p.hexdigest() => p,
        _ => Pointer::new(hasher.finalize()),
    };

    let dest = cache::object_path(&git_dir, pointer.hexdigest());
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Atomic persist; AlreadyExists means a concurrent clean of the same content.
    match tmp.persist_noclobber(&dest) {
        Ok(_) => {}
        Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }

    writer.write_all(&pointer.encode())?;
    Ok(())
}

/// The pointer git's index holds at root-relative `path` (stage 0), if any.
fn index_pointer(index: &mut CatFileBatch, path: &str) -> Result<Option<Pointer>> {
    // cat-file --batch reads one name per line.
    if path.contains('\n') {
        return Ok(None);
    }
    index.read_pointer(&format!(":0:{path}"))
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
