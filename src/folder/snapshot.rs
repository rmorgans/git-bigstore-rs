//! Point-in-time copies of files that may be appended to while we read them.

use std::io::{Read, Write};
use std::path::Path;
use std::time::SystemTime;

use crate::hash::Hasher;
use crate::types::{HashFunction, Hexdigest};

/// A private copy of a file and the md5 of exactly that copy's bytes. Only
/// [`snapshot`] builds one, so the digest can never describe different bytes
/// from the ones that get uploaded.
///
/// The copy is closed once written (a push may hold thousands of snapshots;
/// holding each one open would exhaust file descriptors) and deleted on drop.
/// Nothing else writes it: it lives in the push's private temp directory.
pub struct Snapshot {
    file: tempfile::TempPath,
    md5: Hexdigest,
    size: u64,
    unterminated_line: bool,
}

impl Snapshot {
    pub fn path(&self) -> &Path {
        &self.file
    }

    pub fn md5(&self) -> &Hexdigest {
        &self.md5
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The copy is non-empty and does not end in `\n`.
    pub fn unterminated_line(&self) -> bool {
        self.unterminated_line
    }
}

/// Why a snapshot could not be taken.
#[derive(Debug)]
pub enum SnapshotError {
    /// The file vanished or kept changing: retry the push.
    Changed(String),
    Io(anyhow::Error),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Changed(msg) => f.write_str(msg),
            Self::Io(e) => write!(f, "{e:#}"),
        }
    }
}

const ATTEMPTS: usize = 3;

/// Copy `path` into a temp file in `tmp_dir`, hashing the same bytes as they
/// are written. The file's length and mtime are checked before and after:
/// if either changed (append, truncation, replacement) the copy is retried,
/// and after [`ATTEMPTS`] tries it is reported as [`SnapshotError::Changed`].
/// Every snapshot returned is a state the file really was in.
pub fn snapshot(path: &Path, tmp_dir: &Path) -> std::result::Result<Snapshot, SnapshotError> {
    for _ in 0..ATTEMPTS {
        match try_snapshot(path, tmp_dir)? {
            Some(s) => return Ok(s),
            None => continue,
        }
    }
    Err(SnapshotError::Changed(format!(
        "{} kept changing while being read",
        path.display()
    )))
}

fn stamp(path: &Path) -> std::result::Result<(u64, SystemTime), SnapshotError> {
    match std::fs::metadata(path) {
        Ok(m) => Ok((
            m.len(),
            m.modified().map_err(|e| SnapshotError::Io(e.into()))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(SnapshotError::Changed(format!(
            "{} disappeared while being read",
            path.display()
        ))),
        Err(e) => Err(SnapshotError::Io(
            anyhow::Error::from(e).context(format!("failed to stat {}", path.display())),
        )),
    }
}

fn try_snapshot(
    path: &Path,
    tmp_dir: &Path,
) -> std::result::Result<Option<Snapshot>, SnapshotError> {
    let before = stamp(path)?;
    let mut source = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(SnapshotError::Changed(format!(
                "{} disappeared while being read",
                path.display()
            )))
        }
        Err(e) => {
            return Err(SnapshotError::Io(
                anyhow::Error::from(e).context(format!("failed to open {}", path.display())),
            ))
        }
    };
    let io = |e: std::io::Error| SnapshotError::Io(e.into());
    let mut file = tempfile::NamedTempFile::new_in(tmp_dir).map_err(io)?;
    let mut hasher = Hasher::new(HashFunction::Md5);
    let mut buf = vec![0u8; 64 * 1024];
    let mut size = 0u64;
    let mut last = None;
    loop {
        let n = source.read(&mut buf).map_err(io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).map_err(io)?;
        size += n as u64;
        last = Some(buf[n - 1]);
    }
    file.flush().map_err(io)?;
    let after = stamp(path)?;
    if before != after || after.0 != size {
        return Ok(None);
    }
    Ok(Some(Snapshot {
        file: file.into_temp_path(),
        md5: hasher.finalize(),
        size,
        unterminated_line: last.is_some_and(|b| b != b'\n'),
    }))
}

impl From<SnapshotError> for anyhow::Error {
    fn from(e: SnapshotError) -> Self {
        match e {
            SnapshotError::Changed(msg) => anyhow::anyhow!(msg),
            SnapshotError::Io(e) => e,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash;

    #[test]
    fn digest_is_of_the_copied_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("labels.jsonl");
        std::fs::write(&src, b"{\"a\":1}\n{\"a\":2}").unwrap();
        let s = snapshot(&src, dir.path()).unwrap();
        assert_eq!(
            *s.md5(),
            hash::hash_file(s.path(), HashFunction::Md5).unwrap()
        );
        assert_eq!(s.size(), 15);
        assert!(s.unterminated_line());
    }

    #[test]
    fn missing_file_is_changed_not_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = snapshot(&dir.path().join("gone"), dir.path())
            .err()
            .unwrap();
        assert!(matches!(err, SnapshotError::Changed(_)), "{err}");
    }

    /// The writer's `i`th change, applied to `content`: the byte range to
    /// write out at its offset, or `None` to truncate the file to
    /// `content.len()`.
    fn change(i: u64, content: &mut Vec<u8>) -> Option<std::ops::Range<usize>> {
        if i % 7 == 6 {
            // Cut a torn tail, as Track Inspector does.
            content.truncate(content.len().saturating_sub(3));
            None
        } else if i % 11 == 10 {
            // `{"old":0}` <-> `{"new":0}` in place: the length stays the
            // same, so only the mtime shows this change.
            let word: &[u8] = if &content[2..5] == b"old" {
                b"new"
            } else {
                b"old"
            };
            content[2..5].copy_from_slice(word);
            Some(2..5)
        } else {
            let start = content.len();
            content.extend_from_slice(format!("{{\"row\":{i}}}\n").as_bytes());
            Some(start..content.len())
        }
    }

    /// A writer appends, cuts torn tails and rewrites the first line in
    /// place while snapshots run. Every snapshot's digest must describe its
    /// own bytes, and every snapshot must be a state the file was in: one of
    /// the states the writer's changes produce, never a splice of two (say,
    /// the first line from before a rewrite with a tail appended after it).
    #[test]
    fn concurrent_changes_never_yield_an_inconsistent_snapshot() {
        use std::collections::HashSet;
        use std::io::{Seek, SeekFrom};
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let digest = |bytes: &[u8]| {
            let mut h = Hasher::new(HashFunction::Md5);
            h.update(bytes);
            h.finalize()
        };
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("labels.jsonl");
        // Several copy buffers long, so a snapshot takes several reads and a
        // change between them could splice two states.
        let initial: Vec<u8> = (0..12_000)
            .flat_map(|r| format!("{{\"old\":{r}}}\n").into_bytes())
            .collect();
        std::fs::write(&src, &initial).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (src, stop, mut content) = (src.clone(), stop.clone(), initial.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // Open for write, like Track Inspector (Windows refuses
                    // set_len on an append-only handle). Each change is one
                    // call, so the file goes from one state to the next.
                    let mut f = std::fs::OpenOptions::new().write(true).open(&src).unwrap();
                    match change(i, &mut content) {
                        None => f.set_len(content.len() as u64).unwrap(),
                        Some(r) => {
                            f.seek(SeekFrom::Start(r.start as u64)).unwrap();
                            f.write_all(&content[r]).unwrap();
                        }
                    }
                    i += 1;
                    // Bursts of changes, then quiet: a writer that never
                    // pauses would (correctly) make every snapshot "kept
                    // changing".
                    let pause = if i.is_multiple_of(5) { 15_000 } else { 100 };
                    std::thread::sleep(std::time::Duration::from_micros(pause));
                }
                i
            })
        };
        let mut taken = Vec::new();
        for _ in 0..200 {
            match snapshot(&src, dir.path()) {
                Ok(s) => {
                    let bytes = std::fs::read(s.path()).unwrap();
                    assert_eq!(*s.md5(), digest(&bytes));
                    assert_eq!(bytes.len() as u64, s.size());
                    taken.push((s.size(), s.md5().clone()));
                }
                Err(SnapshotError::Changed(_)) => {}
                Err(SnapshotError::Io(e)) => panic!("{e:#}"),
            }
        }
        stop.store(true, Ordering::Relaxed);
        let changes = writer.join().unwrap();
        assert!(!taken.is_empty(), "no snapshot ever succeeded");
        assert!(
            taken.iter().any(|(size, _)| *size != initial.len() as u64),
            "every snapshot was of the file before any change"
        );

        // Replay the writer; digest only states of a length some snapshot has.
        let sizes: HashSet<u64> = taken.iter().map(|(size, _)| *size).collect();
        let mut content = initial;
        let mut states = HashSet::new();
        for i in 0..=changes {
            if i > 0 {
                change(i - 1, &mut content);
            }
            let len = content.len() as u64;
            if sizes.contains(&len) {
                states.insert((len, digest(&content)));
            }
        }
        for (size, md5) in &taken {
            assert!(
                states.contains(&(*size, md5.clone())),
                "snapshot of {size} bytes ({md5}) is no state the writer produced"
            );
        }
    }
}
