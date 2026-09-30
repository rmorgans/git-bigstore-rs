//! git's long-running filter process protocol (gitattributes(5), "Long
//! Running Filter Process"): one `git-bigstore filter-process` serves every
//! clean and smudge of a git command over pkt-line on stdin/stdout.
//!
//! The protocol order is enforced by types: a [`Session`] exists only after a
//! completed handshake, a [`Request`] borrows the session until it is
//! answered, and an answer drains the request's content first, so git never
//! sees a reply before it has sent the whole file.
//!
//! Per-file failures are answered with `status=error` and the stream stays in
//! sync. I/O and protocol errors end the process; git then fails the file
//! (`required`) and starts a fresh filter for the next one. `status=abort` is
//! never sent: with `required`, git stops at the first failed file either way.
//!
//! Why a process: a one-shot clean/smudge costs about 19 ms per file (git's
//! `sh -c`, our start-up, a `git rev-parse`), this about 0.4 ms. `delay` is not
//! offered: `git checkout-index`, which pull uses so it never overwrites a file
//! and leaves stat data fresh, never enables delayed checkout; and a filter
//! that downloads would need the backend and credentials inside every git
//! command. git also excludes process-filtered entries from parallel checkout.
//!
//! Nothing but the [`PktWriter`] may write to stdout.

use anyhow::{Context, Result};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::cache;
use crate::catfile::CatFileBatch;
use crate::filter::{self, Head};
use crate::git;
use crate::pktline::{Content, ContentWriter, PktReader, PktWriter};
use crate::types::Pointer;

/// Non-pointer content smudged through the filter is held in memory up to
/// this size, then in a file in [`cache::spool_dir`]: it must be read
/// completely before the reply starts.
const SPOOL_LIMIT: usize = 8 << 20;

/// Serve git on stdin/stdout until it closes the stream.
pub fn run() -> Result<()> {
    let git_dir = git::common_dir()?;
    remove_stale_spools(&cache::spool_dir(&git_dir));
    let mut handler = Handler {
        git_dir,
        worktree: PathBuf::from("."),
        spool_limit: SPOOL_LIMIT,
        index: None,
    };
    serve(&mut handler, io::stdin().lock(), io::stdout().lock())
}

/// On Unix a spool file is unlinked the moment it is created, so a named
/// file in the spool directory is one a crashed filter left behind. Removing
/// one that a running filter has only just created is harmless too: it holds
/// the file open and ignores the failure of its own unlink. (On Windows an
/// open spool cannot be removed, and the OS deletes it when closed, even by a
/// crash.) Best effort: leftovers cost space, not correctness.
fn remove_stale_spools(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|t| t.is_file()) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn serve<R: Read, W: Write>(handler: &mut Handler, input: R, output: W) -> Result<()> {
    let mut session = Handshake {
        r: PktReader::new(input),
        w: PktWriter::new(output),
    }
    .negotiate()?;
    while let Some(request) = session.next()? {
        handler.handle(request)?;
    }
    Ok(())
}

struct Handshake<R, W: Write> {
    r: PktReader<R>,
    w: PktWriter<W>,
}

impl<R: Read, W: Write> Handshake<R, W> {
    /// Welcome and version 2, then the capabilities both sides support.
    fn negotiate(mut self) -> Result<Session<R, W>> {
        let welcome = self
            .r
            .text_list()?
            .context("git closed the filter stream before the handshake")?;
        anyhow::ensure!(
            welcome.first().map(Vec::as_slice) == Some(&b"git-filter-client"[..]),
            "not a git filter client: {:?}",
            welcome.first().map(Vec::as_slice).map(lossy)
        );
        anyhow::ensure!(
            welcome[1..].iter().any(|l| l == b"version=2"),
            "git did not offer filter protocol version 2"
        );
        self.w.text("git-filter-server")?;
        self.w.text("version=2")?;
        self.w.flush_pkt()?;
        self.w.send()?;

        let offered = self
            .r
            .text_list()?
            .context("git closed the filter stream during the handshake")?;
        let offers = |cap: &[u8]| offered.iter().any(|l| l == cap);
        let caps =
            Capabilities::from_offer(offers(b"capability=clean"), offers(b"capability=smudge"))
                .context("git offered neither the clean nor the smudge capability")?;
        for command in caps.commands() {
            self.w.text(&format!("capability={}", command.name()))?;
        }
        self.w.flush_pkt()?;
        self.w.send()?;

        Ok(Session {
            r: self.r,
            w: self.w,
            caps,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Clean,
    Smudge,
}

impl Command {
    fn name(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::Smudge => "smudge",
        }
    }
}

/// What was negotiated: never nothing (the handshake fails instead).
#[derive(Debug, Clone, Copy)]
enum Capabilities {
    Clean,
    Smudge,
    Both,
}

impl Capabilities {
    fn from_offer(clean: bool, smudge: bool) -> Option<Self> {
        match (clean, smudge) {
            (true, true) => Some(Self::Both),
            (true, false) => Some(Self::Clean),
            (false, true) => Some(Self::Smudge),
            (false, false) => None,
        }
    }

    fn commands(self) -> &'static [Command] {
        match self {
            Self::Clean => &[Command::Clean],
            Self::Smudge => &[Command::Smudge],
            Self::Both => &[Command::Clean, Command::Smudge],
        }
    }
}

struct Session<R, W: Write> {
    r: PktReader<R>,
    w: PktWriter<W>,
    caps: Capabilities,
}

impl<R: Read, W: Write> Session<R, W> {
    /// The next request, or `None` when git closed the stream between
    /// requests. The request borrows the session until it is answered.
    fn next(&mut self) -> Result<Option<Request<'_, R, W>>> {
        let Some(lines) = self.r.text_list()? else {
            return Ok(None);
        };
        let mut command = None;
        let mut pathname = None;
        for line in &lines {
            let eq = line
                .iter()
                .position(|&b| b == b'=')
                .with_context(|| format!("malformed request line {:?}", lossy(line)))?;
            let (key, value) = (&line[..eq], &line[eq + 1..]);
            match key {
                b"command" => {
                    let wanted = match value {
                        b"clean" => Command::Clean,
                        b"smudge" => Command::Smudge,
                        other => anyhow::bail!("unsupported filter command {:?}", lossy(other)),
                    };
                    anyhow::ensure!(
                        self.caps.commands().contains(&wanted),
                        "git sent {} without negotiating it",
                        wanted.name()
                    );
                    command = Some(wanted);
                }
                b"pathname" => pathname = Some(value.to_vec()),
                // ref=, treeish=, blob=, and whatever git adds later.
                _ => {}
            }
        }
        Ok(Some(Request {
            command: command.context("filter request without command=")?,
            pathname: pathname.context("filter request without pathname=")?,
            content: self.r.content(),
            w: &mut self.w,
        }))
    }
}

/// One file to filter. Answer it with [`Request::fail`] or
/// [`Request::succeed`]; both read the rest of its content first.
#[must_use]
struct Request<'s, R: Read, W: Write> {
    command: Command,
    pathname: Vec<u8>,
    content: Content<'s, R>,
    w: &'s mut PktWriter<W>,
}

impl<'s, R: Read, W: Write> Request<'s, R, W> {
    fn content(&mut self) -> &mut Content<'s, R> {
        &mut self.content
    }

    /// Reject this file (`status=error`); the stream stays usable.
    fn fail(mut self, err: &anyhow::Error) -> Result<()> {
        self.content.drain()?;
        report(&self.pathname, err);
        self.w.text("status=error")?;
        self.w.flush_pkt()?;
        self.w.send()?;
        Ok(())
    }

    /// Accept this file; its filtered content goes to the [`Response`].
    fn succeed(mut self) -> Result<Response<'s, W>> {
        self.content.drain()?;
        self.w.text("status=success")?;
        self.w.flush_pkt()?;
        Ok(Response {
            pathname: self.pathname,
            body: self.w.content(),
        })
    }
}

/// The filtered content of an accepted file.
#[must_use]
struct Response<'s, W: Write> {
    pathname: Vec<u8>,
    body: ContentWriter<'s, W>,
}

impl<W: Write> Response<'_, W> {
    /// End the content, then confirm it, or retract it (`status=error`: git
    /// discards what it received) if producing it failed.
    fn finish(self, outcome: Result<()>) -> Result<()> {
        let w = self.body.finish()?;
        if let Err(err) = outcome {
            report(&self.pathname, &err);
            w.text("status=error")?;
        }
        w.flush_pkt()?;
        w.send()?;
        Ok(())
    }
}

impl<W: Write> Write for Response<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.body.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.body.flush()
    }
}

fn report(pathname: &[u8], err: &anyhow::Error) {
    eprintln!("git-bigstore filter-process: {}: {err:#}", lossy(pathname));
}

fn lossy(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// The clean and smudge behaviour, shared with the one-shot filters.
struct Handler {
    git_dir: PathBuf,
    /// Where `git cat-file` runs for index lookups.
    worktree: PathBuf,
    spool_limit: usize,
    /// Started on the first clean of content, then reused.
    index: Option<CatFileBatch>,
}

impl Handler {
    fn handle<R: Read, W: Write>(&mut self, mut request: Request<'_, R, W>) -> Result<()> {
        let head = filter::read_head(request.content())?;
        match (request.command, head) {
            // Already a pointer: pass it through, as the one-shot filter does.
            (Command::Clean, Head::Pointer { raw, .. }) => reply(request, &raw),
            (Command::Clean, Head::Content { head }) => {
                let stored = self.index_pointer(&request.pathname).and_then(|indexed| {
                    filter::store_content(&self.git_dir, &head, request.content(), indexed)
                });
                match stored {
                    Ok(pointer) => reply(request, &pointer.encode()),
                    Err(err) => request.fail(&err),
                }
            }
            (Command::Smudge, Head::Pointer { pointer, raw }) => {
                match filter::open_object(&self.git_dir, &pointer) {
                    Ok(Some(mut object)) => {
                        let mut response = request.succeed()?;
                        let copied = io::copy(&mut object, &mut response);
                        response.finish(copied.map(drop).map_err(anyhow::Error::from))
                    }
                    Ok(None) => reply(request, &raw),
                    Err(err) => request.fail(&err),
                }
            }
            (Command::Smudge, Head::Content { head }) => {
                // Content passes through, but git's content must be read
                // completely before the reply starts.
                let mut spool = match self.spool(&head, request.content()) {
                    Ok(spool) => spool,
                    Err(err) => return request.fail(&err),
                };
                let mut response = request.succeed()?;
                let copied = io::copy(&mut spool, &mut response);
                response.finish(copied.map(drop).map_err(anyhow::Error::from))
            }
        }
    }

    /// `head` and the rest of `content`, rewound: in memory up to the spool
    /// limit, then in an unnamed file on the cache's filesystem. Not the
    /// system temp dir, which may be a small tmpfs.
    fn spool(&self, head: &[u8], content: &mut impl Read) -> Result<tempfile::SpooledTempFile> {
        let dir = cache::spool_dir(&self.git_dir);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let mut spool = tempfile::spooled_tempfile_in(self.spool_limit, dir);
        spool.write_all(head)?;
        io::copy(content, &mut spool)?;
        spool.rewind()?;
        Ok(spool)
    }

    /// The pointer the index holds for `pathname`; `None` for a path
    /// `git cat-file` cannot name (not UTF-8).
    fn index_pointer(&mut self, pathname: &[u8]) -> Result<Option<Pointer>> {
        let Ok(path) = std::str::from_utf8(pathname) else {
            return Ok(None);
        };
        let index = match &mut self.index {
            Some(index) => index,
            empty => empty.insert(CatFileBatch::start(&self.worktree)?),
        };
        let found = filter::index_pointer(index, path);
        if found.is_err() {
            // Out of sync or gone: start a fresh one next time.
            self.index = None;
        }
        found
    }
}

/// Accept `request` with `body` as its content.
fn reply<R: Read, W: Write>(request: Request<'_, R, W>, body: &[u8]) -> Result<()> {
    let mut response = request.succeed()?;
    let written = response.write_all(body);
    response.finish(written.map_err(anyhow::Error::from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hasher;
    use crate::types::HashFunction;
    use std::io::Cursor;
    use std::process::Command as Git;

    /// The handshake reply, written by hand from gitattributes(5).
    const REPLY: &[u8] = b"0016git-filter-server\n000eversion=2\n0000\
                           0015capability=clean\n0016capability=smudge\n0000";

    fn pkt(payload: &[u8]) -> Vec<u8> {
        let mut out = format!("{:04x}", payload.len() + 4).into_bytes();
        out.extend_from_slice(payload);
        out
    }

    fn list(lines: &[&str]) -> Vec<u8> {
        let mut out: Vec<u8> = lines
            .iter()
            .flat_map(|l| pkt(format!("{l}\n").as_bytes()))
            .collect();
        out.extend_from_slice(b"0000");
        out
    }

    fn content(data: &[u8]) -> Vec<u8> {
        let mut out: Vec<u8> = data.chunks(65516).flat_map(pkt).collect();
        out.extend_from_slice(b"0000");
        out
    }

    fn handshake(caps: &[&str]) -> Vec<u8> {
        let mut out = list(&["git-filter-client", "version=2"]);
        out.extend(list(caps));
        out
    }

    fn git_handshake() -> Vec<u8> {
        handshake(&["capability=clean", "capability=smudge", "capability=delay"])
    }

    fn request(command: &str, path: &str, data: &[u8]) -> Vec<u8> {
        let mut out = list(&[&format!("command={command}"), &format!("pathname={path}")]);
        out.extend(content(data));
        out
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        repo: PathBuf,
        handler: Handler,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        let handler = Handler {
            git_dir: repo.join(".git"),
            worktree: repo.clone(),
            spool_limit: SPOOL_LIMIT,
            index: None,
        };
        Fixture {
            _tmp: tmp,
            repo,
            handler,
        }
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = Git::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    fn serve_bytes(handler: &mut Handler, input: Vec<u8>) -> (Result<()>, Vec<u8>) {
        let mut out = Vec::new();
        let result = serve(handler, Cursor::new(input), &mut out);
        (result, out)
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Reply {
        /// `status=error` instead of content.
        Rejected,
        /// Content, then `status=error`: git discards it.
        Retracted,
        Success(Vec<u8>),
    }

    /// Parse everything after the handshake reply.
    fn replies(out: &[u8]) -> Vec<Reply> {
        let rest = out.strip_prefix(REPLY).expect("handshake reply");
        let mut r = PktReader::new(rest);
        let mut replies = Vec::new();
        while let Some(status) = r.text_list().unwrap() {
            if status == [b"status=error".to_vec()] {
                replies.push(Reply::Rejected);
                continue;
            }
            assert_eq!(status, [b"status=success".to_vec()]);
            let mut body = Vec::new();
            r.content().read_to_end(&mut body).unwrap();
            let trailer = r.text_list().unwrap().unwrap();
            replies.push(match trailer.as_slice() {
                [] => Reply::Success(body),
                [s] if s == b"status=error" => Reply::Retracted,
                other => panic!("unexpected trailer {other:?}"),
            });
        }
        replies
    }

    fn sha256_pointer(data: &[u8]) -> Pointer {
        let mut h = Hasher::new(HashFunction::Sha256);
        h.update(data);
        Pointer::new(h.finalize())
    }

    fn md5_pointer(data: &[u8]) -> Pointer {
        let mut h = Hasher::new(HashFunction::Md5);
        h.update(data);
        Pointer::new(h.finalize())
    }

    #[test]
    fn handshake_answers_the_spec_example() {
        let mut f = fixture();
        let mut input = list(&["git-filter-client", "version=2", "version=42"]);
        input.extend(list(&[
            "capability=clean",
            "capability=smudge",
            "capability=not-yet-invented",
        ]));
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        assert_eq!(out, REPLY);
    }

    #[test]
    fn only_offered_capabilities_are_claimed_and_accepted() {
        let mut f = fixture();
        let mut input = handshake(&["capability=smudge"]);
        input.extend(request("clean", "a.bin", b"data"));
        let (result, out) = serve_bytes(&mut f.handler, input);
        assert!(result.is_err(), "clean was not negotiated");
        assert_eq!(
            out,
            b"0016git-filter-server\n000eversion=2\n00000016capability=smudge\n0000"
        );
    }

    #[test]
    fn bad_handshakes_are_errors() {
        let version_reply = b"0016git-filter-server\n000eversion=2\n0000".to_vec();
        let cases: [(Vec<u8>, Vec<u8>); 4] = [
            (list(&["git-foo-client", "version=2"]), Vec::new()),
            (list(&["git-filter-client", "version=3"]), Vec::new()),
            (handshake(&["capability=delay"]), version_reply),
            (Vec::new(), Vec::new()),
        ];
        for (input, expected) in cases {
            let mut f = fixture();
            let (result, out) = serve_bytes(&mut f.handler, input);
            assert!(result.is_err());
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn malformed_requests_are_errors_with_no_reply() {
        let requests = [
            list(&["pathname=a.bin"]),
            list(&["command=list_available_blobs"]),
            list(&["command=smudge", "pathname=a.bin", "command=frobnicate"]),
            list(&["command=smudge"]),
            list(&["command=smudge", "pathname=a.bin", "can-delay"]),
        ];
        for req in requests {
            let mut f = fixture();
            let mut input = git_handshake();
            input.extend(req);
            input.extend(content(b"data"));
            let (result, out) = serve_bytes(&mut f.handler, input);
            assert!(result.is_err());
            assert_eq!(out, REPLY);
        }
    }

    #[test]
    fn unknown_metadata_is_ignored_and_values_keep_their_equals_signs() {
        let mut input = git_handshake();
        input.extend(list(&[
            "command=smudge",
            "pathname=a=b.bin",
            "ref=refs/heads/main",
            "treeish=0123",
            "blob=4567",
        ]));
        input.extend(content(b"hello"));
        let mut session = Handshake {
            r: PktReader::new(Cursor::new(input)),
            w: PktWriter::new(Vec::new()),
        }
        .negotiate()
        .unwrap();
        let mut request = session.next().unwrap().unwrap();
        assert_eq!(request.command, Command::Smudge);
        assert_eq!(request.pathname, b"a=b.bin");
        let mut body = Vec::new();
        request.content().read_to_end(&mut body).unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn content_cut_off_before_its_flush_gets_no_reply() {
        let mut f = fixture();
        let mut input = git_handshake();
        input.extend(list(&["command=smudge", "pathname=a.bin"]));
        input.extend(pkt(b"partial"));
        let (result, out) = serve_bytes(&mut f.handler, input);
        assert!(result.is_err());
        assert_eq!(out, REPLY);
    }

    #[test]
    fn requests_are_answered_like_the_one_shot_filters() {
        let mut f = fixture();
        f.handler.spool_limit = 1024;
        let cached = b"cached object\n".to_vec();
        let cached_ptr = sha256_pointer(&cached);
        let object = cache::object_path(&f.handler.git_dir, cached_ptr.hexdigest());
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(&object, &cached).unwrap();

        let uncached = sha256_pointer(b"never cached").encode();
        let crlf = String::from_utf8(cached_ptr.encode())
            .unwrap()
            .replace('\n', "\r\n")
            .into_bytes();
        let mut trailing = cached_ptr.encode();
        trailing.extend_from_slice(b"more\n");
        let big: Vec<u8> = (0..200 * 1024u32).map(|i| (i % 253) as u8).collect();

        let cases: Vec<(&str, Vec<u8>, Vec<u8>)> = vec![
            ("smudge", cached_ptr.encode(), cached.clone()),
            ("smudge", crlf.clone(), cached.clone()),
            ("smudge", uncached.clone(), uncached.clone()),
            (
                "smudge",
                b"plain content\n".to_vec(),
                b"plain content\n".to_vec(),
            ),
            ("smudge", trailing.clone(), trailing.clone()),
            ("smudge", b"bigstore\n".to_vec(), b"bigstore\n".to_vec()),
            ("smudge", Vec::new(), Vec::new()),
            // Several packets, and past the (lowered) spool limit.
            ("smudge", big.clone(), big.clone()),
            ("clean", cached_ptr.encode(), cached_ptr.encode()),
            ("clean", crlf.clone(), crlf.clone()),
            (
                "clean",
                b"bigstore\n".to_vec(),
                sha256_pointer(b"bigstore\n").encode(),
            ),
            (
                "clean",
                trailing.clone(),
                sha256_pointer(&trailing).encode(),
            ),
            ("clean", Vec::new(), sha256_pointer(b"").encode()),
            ("clean", big.clone(), sha256_pointer(&big).encode()),
        ];
        let mut input = git_handshake();
        for (command, data, _) in &cases {
            input.extend(request(command, "f.bin", data));
        }
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        let expected: Vec<Reply> = cases
            .into_iter()
            .map(|(_, _, want)| Reply::Success(want))
            .collect();
        assert_eq!(replies(&out), expected);

        // Clean stored what it hashed.
        let stored = cache::object_path(&f.handler.git_dir, sha256_pointer(&big).hexdigest());
        assert_eq!(std::fs::read(stored).unwrap(), big);
    }

    #[test]
    fn clean_keeps_the_index_md5_pointer_while_content_matches() {
        let mut f = fixture();
        let data = b"dvc content\n";
        let md5 = md5_pointer(data);
        std::fs::write(f.repo.join("m.bin"), md5.encode()).unwrap();
        git(&f.repo, &["add", "m.bin"]);

        let mut input = git_handshake();
        input.extend(request("clean", "m.bin", data));
        input.extend(request("clean", "m.bin", b"edited\n"));
        input.extend(request("clean", "other.bin", data));
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        assert_eq!(
            replies(&out),
            [
                Reply::Success(md5.encode()),
                Reply::Success(sha256_pointer(b"edited\n").encode()),
                Reply::Success(sha256_pointer(data).encode()),
            ]
        );
        let object = cache::object_path(&f.handler.git_dir, md5.hexdigest());
        assert_eq!(std::fs::read(object).unwrap(), data);
    }

    #[test]
    fn a_failed_file_leaves_the_stream_in_sync() {
        let mut f = fixture();
        let ok = b"fine\n".to_vec();
        let ok_ptr = sha256_pointer(&ok);
        let ok_obj = cache::object_path(&f.handler.git_dir, ok_ptr.hexdigest());
        std::fs::create_dir_all(ok_obj.parent().unwrap()).unwrap();
        std::fs::write(&ok_obj, &ok).unwrap();
        // An object that opens but cannot be read: a directory.
        let bad_ptr = sha256_pointer(b"unreadable");
        std::fs::create_dir_all(cache::object_path(&f.handler.git_dir, bad_ptr.hexdigest()))
            .unwrap();

        let mut input = git_handshake();
        input.extend(request("smudge", "bad.bin", &bad_ptr.encode()));
        input.extend(request("smudge", "ok.bin", &ok_ptr.encode()));
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        assert_eq!(replies(&out), [Reply::Retracted, Reply::Success(ok)]);
    }

    #[cfg(unix)]
    #[test]
    fn an_unopenable_object_is_rejected_and_the_stream_stays_in_sync() {
        use std::os::unix::fs::PermissionsExt;
        let mut f = fixture();
        let ptr = sha256_pointer(b"locked");
        let object = cache::object_path(&f.handler.git_dir, ptr.hexdigest());
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(&object, b"locked").unwrap();
        std::fs::set_permissions(&object, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::File::open(&object).is_ok() {
            return; // running as root: permissions do not apply
        }

        let mut input = git_handshake();
        input.extend(request("smudge", "locked.bin", &ptr.encode()));
        input.extend(request("smudge", "plain.bin", b"plain"));
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        assert_eq!(
            replies(&out),
            [Reply::Rejected, Reply::Success(b"plain".to_vec())]
        );
    }

    #[test]
    fn a_failed_spool_rejects_the_file_and_the_stream_stays_in_sync() {
        let mut f = fixture();
        // A file where the spool directory should be.
        let spool = cache::spool_dir(&f.handler.git_dir);
        std::fs::create_dir_all(spool.parent().unwrap()).unwrap();
        std::fs::write(&spool, b"not a directory").unwrap();
        let big: Vec<u8> = (0..200 * 1024u32).map(|i| (i % 253) as u8).collect();
        let ptr = sha256_pointer(b"x");

        let mut input = git_handshake();
        input.extend(request("smudge", "raw.bin", &big));
        input.extend(request("smudge", "p.bin", &ptr.encode()));
        let (result, out) = serve_bytes(&mut f.handler, input);
        result.unwrap();
        assert_eq!(
            replies(&out),
            [Reply::Rejected, Reply::Success(ptr.encode())]
        );
    }

    /// A framing error is not a per-file failure: past it, payload bytes
    /// could pass for packets, so no reply is safe.
    #[test]
    fn a_framing_error_inside_content_ends_the_process_without_a_reply() {
        let mut f = fixture();
        let mut input = git_handshake();
        input.extend(list(&["command=clean", "pathname=a.bin"]));
        // Past the pointer-sized head, then a bad header, then bytes that
        // read as a flush if the reader resynchronised on them.
        input.extend(pkt(&[b'a'; 4000]));
        input.extend(b"zzzz0000");
        input.extend(request("smudge", "b.bin", b"plain"));
        let (result, out) = serve_bytes(&mut f.handler, input);
        let err = result.unwrap_err();
        assert!(format!("{err:#}").contains("zzzz"), "{err:#}");
        assert_eq!(out, REPLY);
    }
}
