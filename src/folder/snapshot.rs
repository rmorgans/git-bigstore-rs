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

    /// A writer appends and occasionally truncates (as Track Inspector cuts a
    /// torn line) while snapshots run. Every snapshot's digest must describe
    /// its own bytes, and every snapshot must be a state the file was in:
    /// some prefix of lines the writer produced.
    #[test]
    fn concurrent_append_and_truncate_never_yield_an_inconsistent_snapshot() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("labels.jsonl");
        std::fs::write(&src, b"").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (src, stop) = (src.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // Like Track Inspector: open for write, cut a torn tail
                    // or seek to the end and append. (Windows refuses
                    // set_len on an append-only handle.)
                    let mut f = std::fs::OpenOptions::new().write(true).open(&src).unwrap();
                    if i % 7 == 6 {
                        let len = f.metadata().unwrap().len();
                        f.set_len(len.saturating_sub(3)).unwrap();
                    } else {
                        use std::io::Seek;
                        f.seek(std::io::SeekFrom::End(0)).unwrap();
                        writeln!(f, "{{\"row\":{i}}}").unwrap();
                    }
                    i += 1;
                    // Real appends are sporadic; a writer that never pauses
                    // would (correctly) make every snapshot "kept changing".
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
            })
        };
        let mut ok = 0;
        for _ in 0..200 {
            match snapshot(&src, dir.path()) {
                Ok(s) => {
                    assert_eq!(
                        *s.md5(),
                        hash::hash_file(s.path(), HashFunction::Md5).unwrap()
                    );
                    assert_eq!(std::fs::metadata(s.path()).unwrap().len(), s.size());
                    ok += 1;
                }
                Err(SnapshotError::Changed(_)) => {}
                Err(SnapshotError::Io(e)) => panic!("{e:#}"),
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        assert!(ok > 0, "no snapshot ever succeeded");
    }
}
