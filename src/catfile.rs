//! A long-lived `git cat-file --batch` process for reading many blobs cheaply.

use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use crate::types::{Pointer, MAX_POINTER_BYTES};

/// Protocol: write `<object>\n`, read `<oid> <type> <size>\n<content>\n`, or
/// `<object> missing\n` / `<object> ambiguous\n`.
pub struct CatFileBatch {
    // Fields drop in declaration order: both pipes close before the child is
    // reaped. Closing stdin ends cat-file's input; closing stdout ends an
    // object a failed read left unread, which cat-file would otherwise block
    // writing forever while we waited for it.
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    _child: Reaped,
}

/// A child that is waited for when dropped, so it never lingers as a zombie.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.wait();
    }
}

impl CatFileBatch {
    pub fn start(repo_root: &Path) -> Result<Self> {
        let mut child = Command::new("git")
            .args(["cat-file", "--batch"])
            .current_dir(repo_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to start git cat-file --batch")?;
        let stdin = child.stdin.take().context("cat-file stdin not piped")?;
        let stdout = BufReader::new(child.stdout.take().context("cat-file stdout not piped")?);
        Ok(Self {
            stdin,
            stdout,
            _child: Reaped(child),
        })
    }

    /// Read the blob named by `object` (an object id, or `<rev>:<path>`) and
    /// classify it with [`Pointer::parse`]. `None` if the object is missing,
    /// not a blob, or not a pointer.
    ///
    /// Reads at most [`MAX_POINTER_BYTES`] of the blob and drains the rest
    /// unbuffered, so walking large non-pointer blobs costs no memory.
    pub fn read_pointer(&mut self, object: &str) -> Result<Option<Pointer>> {
        anyhow::ensure!(
            !object.contains('\n'),
            "object name contains a newline: {object:?}"
        );
        self.stdin.write_all(object.as_bytes())?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;

        let mut header = String::new();
        self.stdout.read_line(&mut header)?;
        let header = header.trim_end();
        // "<object> missing" / "<object> ambiguous": nothing follows. Matched
        // against the name we sent, so names containing spaces are safe.
        if matches!(header.strip_prefix(object), Some(" missing" | " ambiguous")) {
            return Ok(None);
        }
        // "<oid> <type> <size>"
        let mut fields = header.rsplitn(3, ' ');
        let (size, kind) = match (fields.next(), fields.next(), fields.next()) {
            (Some(size), Some(kind), Some(_oid)) => (
                size.parse::<usize>()
                    .with_context(|| format!("bad cat-file header: {header:?}"))?,
                kind,
            ),
            _ => anyhow::bail!("bad cat-file header: {header:?}"),
        };

        let head_len = size.min(MAX_POINTER_BYTES + 1);
        let mut head = vec![0u8; head_len];
        self.stdout.read_exact(&mut head)?;
        // Remainder of the object plus its trailing LF keeps the pipe aligned.
        std::io::copy(
            &mut (&mut self.stdout).take((size - head_len + 1) as u64),
            &mut std::io::sink(),
        )?;

        Ok((kind == "blob").then(|| Pointer::parse(&head)).flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }

    /// A request abandoned after its header, as when `read_pointer` fails
    /// mid-object: cat-file is still writing a blob bigger than the pipe
    /// buffer. Dropping must not wait for it forever.
    #[test]
    fn drop_returns_with_a_blob_left_unread() {
        let tmp = tempfile::TempDir::new().unwrap();
        git(tmp.path(), &["init", "-q"]);
        let blob = tmp.path().join("blob");
        std::fs::write(&blob, vec![b'x'; 1 << 20]).unwrap();
        let oid = git(tmp.path(), &["hash-object", "-w", "blob"]);

        let mut batch = CatFileBatch::start(tmp.path()).unwrap();
        writeln!(batch.stdin, "{oid}").unwrap();
        batch.stdin.flush().unwrap();
        let mut header = String::new();
        batch.stdout.read_line(&mut header).unwrap();
        assert!(header.ends_with(" blob 1048576\n"), "{header:?}");

        let (done, dropped) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(batch);
            done.send(()).unwrap();
        });
        dropped
            .recv_timeout(Duration::from_secs(10))
            .expect("dropping CatFileBatch hung on an undrained blob");
    }
}
