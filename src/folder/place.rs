//! Single storage: a file's bytes placed as a new file beside a name, by
//! reflink, else hard link, else copy, and hashed back before anyone
//! publishes it.
//!
//! The new file is always created here (`O_CREAT | O_EXCL`): nothing is
//! ever cloned into an existing or truncated file, where OpenZFS has had
//! read bugs (a clone over a recently truncated file reading back zeros,
//! freed blocks reading back cloned content).

use anyhow::{Context, Result};
use std::fs::File;
use std::io;
use std::path::Path;

use crate::types::{long_path, HashFunction, Hexdigest};

/// How push stores objects and pull writes files, for
/// [`PushOptions::link`](super::PushOptions::link) and
/// [`PullOptions::link`](super::PullOptions::link).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Link {
    /// Push hashes a private snapshot copy of each file and uploads that;
    /// pull downloads each object to a temp file and renames it into place.
    #[default]
    Copy,
    /// Single storage, for write-once files and a `local://` remote: push
    /// hashes each file where it is and places it into the store, and pull
    /// places each object from the store into the output, with no second
    /// copy. Files of at least `min_bytes` are placed as a reflink, else a
    /// hard link, else a copy; smaller ones are copied. A hard link shares
    /// the file: writing the working file afterwards changes the store's
    /// object, which a scrub then finds damaged.
    Place { min_bytes: u64 },
}

impl Link {
    /// Whether a file of `size` bytes is placed by reflink or hard link.
    pub(super) fn links(self, size: u64) -> bool {
        matches!(self, Self::Place { min_bytes } if size >= min_bytes)
    }
}

/// How a file was placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Method {
    /// A reflink: a new file sharing the source's blocks.
    Cloned,
    /// A hard link: the source file itself, under a second name.
    Linked,
    Copied,
}

/// Placements counted: reflinks and hard links, as the reports give them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Tally {
    /// Files placed by reflink or hard link.
    pub linked: usize,
    /// Of those, by reflink.
    pub cloned: usize,
}

impl Tally {
    pub(super) fn add(&mut self, method: Method) {
        match method {
            Method::Cloned => {
                self.linked += 1;
                self.cloned += 1;
            }
            Method::Linked => self.linked += 1,
            Method::Copied => {}
        }
    }

    pub(super) fn sum(tallies: impl IntoIterator<Item = Self>) -> Self {
        tallies.into_iter().fold(Self::default(), |a, b| Self {
            linked: a.linked + b.linked,
            cloned: a.cloned + b.cloned,
        })
    }
}

/// A new file placed by [`beside`], deleted when dropped unless persisted,
/// and what its bytes hash to as read back.
pub(super) struct Placed {
    pub tmp: tempfile::NamedTempFile,
    pub method: Method,
    pub md5: Hexdigest,
    pub size: u64,
}

/// Create a new file named `<prefix><random>` in `dir` holding `src`'s
/// bytes: with `link`, a reflink of it, else a hard link to it, else a
/// copy; without, a copy. Then read it back whole. The caller compares
/// what it read with what it expects before persisting it. `src` is
/// resolved first, so a symlink is never what gets linked. Blocks.
pub(super) fn beside(src: &Path, dir: &Path, prefix: &str, link: bool) -> Result<Placed> {
    beside_with(src, dir, prefix, link, &OS)
}

/// How a file is linked: the OS's own calls, or in tests, stand-ins.
struct Ops {
    reflink: fn(&Path, &Path) -> io::Result<()>,
    hard_link: fn(&Path, &Path) -> io::Result<()>,
}

const OS: Ops = Ops {
    reflink: |from, to| reflink_copy::reflink(from, to),
    hard_link: |from, to| std::fs::hard_link(from, to),
};

fn beside_with(src: &Path, dir: &Path, prefix: &str, link: bool, ops: &Ops) -> Result<Placed> {
    let src = std::fs::canonicalize(long_path(src)?)
        .with_context(|| format!("failed to resolve {}", src.display()))?;
    let dir = long_path(dir)?;
    let made = tempfile::Builder::new()
        .prefix(prefix)
        .rand_bytes(8)
        .make_in(&dir, |to| create(&src, to, link, ops))
        .with_context(|| format!("failed to place {} in {}", src.display(), dir.display()))?;
    let (method, path) = (*made.as_file(), made.into_temp_path());
    let mut file =
        File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let meta = std::fs::symlink_metadata(&path)?;
    anyhow::ensure!(
        meta.is_file(),
        "{} placed as something other than a regular file",
        path.display()
    );
    let md5 = crate::hash::hash_reader(&mut file, HashFunction::Md5)
        .with_context(|| format!("failed to read back {}", path.display()))?;
    // Durable before anyone names it. A read-only handle syncs on unix;
    // on Windows only a copy is synced (by `create`), with its write handle.
    #[cfg(unix)]
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    Ok(Placed {
        tmp: tempfile::NamedTempFile::from_parts(file, path),
        method,
        md5,
        size: meta.len(),
    })
}

/// Create `to`, which must not exist, from `from`. An error of kind
/// `AlreadyExists` makes `make_in` try another name; nothing is left at
/// `to` after any other error.
fn create(from: &Path, to: &Path, link: bool, ops: &Ops) -> io::Result<Method> {
    if link {
        match (ops.reflink)(from, to) {
            Ok(()) => return Ok(Method::Cloned),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Err(e),
            Err(_) => {}
        }
        match (ops.hard_link)(from, to) {
            Ok(()) => return Ok(Method::Linked),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Err(e),
            Err(_) => {}
        }
    }
    let mut source = File::open(from)?;
    let mut copy = File::options().write(true).create_new(true).open(to)?;
    let copied = io::copy(&mut source, &mut copy).and_then(|_| copy.sync_all());
    if let Err(e) = copied {
        drop(copy);
        let _ = std::fs::remove_file(to);
        return Err(e);
    }
    Ok(Method::Copied)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn unsupported(_: &Path, _: &Path) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    fn cross_device(_: &Path, _: &Path) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::CrossesDevices))
    }

    /// `body` written to a file, then placed beside it with `ops`.
    fn place(body: &[u8], link: bool, ops: &Ops) -> (tempfile::TempDir, File, Placed) {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("table.parquet");
        std::fs::write(&src, body).unwrap();
        let placed = beside_with(&src, dir.path(), "table.parquet#", link, ops).unwrap();
        let src = File::open(&src).unwrap();
        (dir, src, placed)
    }

    fn ino(file: &File) -> u64 {
        file.metadata().unwrap().ino()
    }

    #[test]
    fn without_reflink_a_file_is_hard_linked_and_read_back() {
        let ops = Ops {
            reflink: unsupported,
            ..OS
        };
        let (_dir, src, placed) = place(b"PAR1 rows", true, &ops);
        assert_eq!(placed.method, Method::Linked);
        assert_eq!(ino(placed.tmp.as_file()), ino(&src));
        let want = crate::hash::hash_reader(&mut &b"PAR1 rows"[..], HashFunction::Md5).unwrap();
        assert_eq!(placed.md5, want);
        assert_eq!(placed.size, 9);
    }

    #[test]
    fn across_devices_a_file_is_copied_into_a_new_file() {
        let ops = Ops {
            reflink: cross_device,
            hard_link: cross_device,
        };
        let (_dir, src, placed) = place(b"PAR1 rows", true, &ops);
        assert_eq!(placed.method, Method::Copied);
        assert_ne!(ino(placed.tmp.as_file()), ino(&src));
        assert_eq!(std::fs::read(placed.tmp.path()).unwrap(), b"PAR1 rows");
    }

    #[test]
    fn a_file_not_to_be_linked_is_copied_whatever_links_are_possible() {
        let (_dir, src, placed) = place(b"{}", false, &OS);
        assert_eq!(placed.method, Method::Copied);
        assert_ne!(ino(placed.tmp.as_file()), ino(&src));
    }

    #[test]
    fn a_placement_is_never_made_into_an_existing_name() {
        // A name already taken is skipped for another, never written into.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::write(&src, b"new").unwrap();
        let taken = dir.path().join("t");
        std::fs::write(&taken, b"old").unwrap();
        for link in [true, false] {
            let err = create(&src, &taken, link, &OS).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(std::fs::read(&taken).unwrap(), b"old");
        }
    }
}
