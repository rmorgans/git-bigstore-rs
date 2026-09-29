//! A long-lived `git cat-file --batch` process for reading many blobs cheaply.

use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use crate::types::{Pointer, MAX_POINTER_BYTES};

/// Protocol: write `<object>\n`, read `<oid> <type> <size>\n<content>\n`, or
/// `<object> missing\n` / `<object> ambiguous\n`.
pub struct CatFileBatch {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
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
            child,
            stdin: Some(stdin),
            stdout,
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
        let stdin = self.stdin.as_mut().context("cat-file already closed")?;
        stdin.write_all(object.as_bytes())?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;

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

impl Drop for CatFileBatch {
    fn drop(&mut self) {
        // Closing stdin lets cat-file see EOF and exit.
        self.stdin.take();
        let _ = self.child.wait();
    }
}
