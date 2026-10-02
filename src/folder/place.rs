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
    /// hard link, else a copy; smaller ones are copied, and so is a symlink
    /// inside the output (its target's bytes: it may point at any file).
    /// A hard link shares the file: writing the working file afterwards
    /// changes the store's object, which a scrub then finds damaged.
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
    reflink,
    hard_link: |from, to| std::fs::hard_link(from, to),
};

/// A reflink of `from` as the new file `to`: `FICLONE` into a file created
/// here, and nothing else. No mode is copied or set, so the clone keeps the
/// mode the file system gives a new file (an NFSv4 ACL that refuses every
/// chmod gives it its own).
#[cfg(target_os = "linux")]
fn reflink(from: &Path, to: &Path) -> io::Result<()> {
    let src = File::open(from)?;
    let dest = File::options().write(true).create_new(true).open(to)?;
    if let Err(e) = rustix::fs::ioctl_ficlone(&dest, &src) {
        drop(dest);
        let _ = discard(to);
        return Err(e.into());
    }
    Ok(())
}

/// A reflink of `from` as the new file `to`: `clonefile` on macOS (which
/// makes `to` atomically, or nothing), block cloning on Windows ReFS.
#[cfg(not(target_os = "linux"))]
fn reflink(from: &Path, to: &Path) -> io::Result<()> {
    reflink_copy::reflink(from, to)
}

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
/// `AlreadyExists` makes `make_in` try another name. After any other
/// error, whatever a method left at `to` is removed before the next is
/// tried (`to` did not exist before, so it is ours), so a failed attempt
/// never turns the next one into `AlreadyExists`; if it cannot be
/// removed, placing stops with that error. A new file is never read-only
/// (see [`writable`]).
fn create(from: &Path, to: &Path, link: bool, ops: &Ops) -> io::Result<Method> {
    if link {
        match (ops.reflink)(from, to).and_then(|()| writable(to)) {
            Ok(()) => return Ok(Method::Cloned),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Err(e),
            Err(_) => discard(to)?,
        }
        if hard_linkable(from) {
            match (ops.hard_link)(from, to) {
                Ok(()) => return Ok(Method::Linked),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Err(e),
                Err(_) => discard(to)?,
            }
        }
    }
    let mut source = File::open(from)?;
    let mut copy = File::options().write(true).create_new(true).open(to)?;
    let copied = io::copy(&mut source, &mut copy).and_then(|_| copy.sync_all());
    if let Err(e) = copied {
        drop(copy);
        let _ = discard(to);
        return Err(e);
    }
    Ok(Method::Copied)
}

/// Remove what a failed method left at `to`, if anything. A failure to
/// remove it is never `AlreadyExists`, so it stops `make_in`.
fn discard(to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    let _ = writable(to);
    match std::fs::remove_file(to) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(io::Error::other(format!(
            "cannot remove a failed placement: {e}"
        ))),
        _ => Ok(()),
    }
}

/// On Windows, clear the read-only attribute a clone copied from its
/// source (reflink-copy copies it), so the temp file can be removed if it
/// is refused, and renamed over a target. A no-op elsewhere: a unix mode
/// does not stop the owner removing or renaming a file.
fn writable(to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let mut perms = std::fs::metadata(to)?.permissions();
        if perms.readonly() {
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            std::fs::set_permissions(to, perms)?;
        }
    }
    #[cfg(not(windows))]
    let _ = to;
    Ok(())
}

/// Whether `from` may be hard-linked. On Windows a read-only file is not:
/// its link would be read-only too, and clearing that would change the
/// working file, so a refused temp could not be removed. It is copied.
fn hard_linkable(from: &Path) -> bool {
    #[cfg(windows)]
    {
        std::fs::metadata(from).is_ok_and(|m| !m.permissions().readonly())
    }
    #[cfg(not(windows))]
    {
        let _ = from;
        true
    }
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

    /// A method that leaves a file at `to` and then fails, as reflink-copy
    /// did on Linux when its chmod after the clone was refused.
    fn leaves_and_fails(_: &Path, to: &Path) -> io::Result<()> {
        std::fs::write(to, b"partial")?;
        Err(io::Error::from_raw_os_error(1))
    }

    /// The files in `dir` other than `keep`.
    fn others(dir: &Path, keep: &Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p != keep)
            .collect()
    }

    #[test]
    fn what_a_failed_method_leaves_is_removed_before_the_next_is_tried() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("table.parquet");
        std::fs::write(&src, b"PAR1 rows").unwrap();
        let to = dir.path().join("to");

        // Reflink leaves a file and fails: the hard link still lands.
        let ops = Ops {
            reflink: leaves_and_fails,
            ..OS
        };
        assert_eq!(create(&src, &to, true, &ops).unwrap(), Method::Linked);
        assert_eq!(others(dir.path(), &src), vec![to.clone()]);
        std::fs::remove_file(&to).unwrap();

        // Both links leave a file and fail: the copy lands, alone.
        let ops = Ops {
            reflink: leaves_and_fails,
            hard_link: leaves_and_fails,
        };
        assert_eq!(create(&src, &to, true, &ops).unwrap(), Method::Copied);
        assert_eq!(others(dir.path(), &src), vec![to.clone()]);
        assert_eq!(std::fs::read(&to).unwrap(), b"PAR1 rows");
        std::fs::remove_file(&to).unwrap();

        // Everything fails (a directory cannot be copied): nothing is left.
        let not_a_file = dir.path().join("d");
        std::fs::create_dir(&not_a_file).unwrap();
        create(&not_a_file, &to, true, &ops).unwrap_err();
        assert!(!to.exists());

        // Through `beside`, one temp file and no retries.
        let placed = beside_with(&src, dir.path(), "table.parquet#", true, &ops).unwrap();
        assert_eq!(placed.method, Method::Copied);
        let mut left = others(dir.path(), &src);
        left.retain(|p| p != &not_a_file);
        assert_eq!(left, vec![placed.tmp.path().to_path_buf()]);
    }
}
