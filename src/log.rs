//! `git bigstore log`: file-level history of bigstore pointers.

use anyhow::{bail, Context, Result};
use globset::{Glob, GlobMatcher};
use std::fmt;
use std::process::Command;

use crate::catfile::CatFileBatch;
use crate::git;
use crate::types::{Pointer, RepoPath};

pub fn run(paths: &[String]) -> Result<()> {
    let matchers: Vec<GlobMatcher> = paths
        .iter()
        .map(|p| {
            Glob::new(p)
                .with_context(|| format!("invalid path pattern: {p:?}"))
                .map(|g| g.compile_matcher())
        })
        .collect::<Result<_>>()?;

    // First-parent only to avoid merge noise; merges are diffed against their
    // first parent.
    let history = git_stdout(&[
        "rev-list",
        "--first-parent",
        "--format=%P%n%h %ai %s",
        "HEAD",
    ])?;
    let history = String::from_utf8_lossy(&history);

    // Single long-lived process for all blob reads
    let mut cat_file = CatFileBatch::start(&git::repo_root()?)?;
    let mut found_any = false;

    for commit in parse_history(&history)? {
        let diff = git_stdout(&commit.diff_tree_args())?;
        let mut changes = Vec::new();
        for delta in parse_raw_diff(&diff)
            .with_context(|| format!("reading the diff of commit {}", commit.id))?
        {
            if matchers.is_empty() || delta.matches(&matchers) {
                changes.extend(delta.resolve(&mut cat_file)?);
            }
        }
        if changes.is_empty() {
            continue;
        }

        if found_any {
            println!();
        }
        println!("  {}", commit.summary);
        for change in &changes {
            println!("    {change}");
        }
        found_any = true;
    }

    if !found_any {
        eprintln!("No bigstore file changes found in history.");
    }

    Ok(())
}

fn git_stdout(args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(args)
        .output()
        .context("failed to run git")?;
    anyhow::ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

struct Commit<'a> {
    id: &'a str,
    /// `None` for a root commit, which is diffed against the empty tree.
    first_parent: Option<&'a str>,
    /// `<short hash> <date> <subject>`
    summary: &'a str,
}

impl<'a> Commit<'a> {
    fn diff_tree_args(&self) -> Vec<&'a str> {
        let mut args = vec![
            "diff-tree",
            "-r",
            "-M",
            "-C",
            "--raw",
            "-z",
            "--no-abbrev",
            "--no-commit-id",
        ];
        match self.first_parent {
            Some(parent) => args.extend([parent, self.id]),
            None => args.extend(["--root", self.id]),
        }
        args
    }
}

/// Parse `git rev-list --format=%P%n%h %ai %s`: per commit a `commit <id>`
/// header, the parent ids, and the summary (`%s` is always a single line).
fn parse_history(text: &str) -> Result<Vec<Commit<'_>>> {
    let mut lines = text.lines();
    let mut commits = Vec::new();
    while let Some(header) = lines.next() {
        let id = header
            .strip_prefix("commit ")
            .with_context(|| format!("unexpected git rev-list output: {header:?}"))?;
        let (Some(parents), Some(summary)) = (lines.next(), lines.next()) else {
            bail!("git rev-list output ends inside commit {id}");
        };
        commits.push(Commit {
            id,
            first_parent: parents.split(' ').next().filter(|p| !p.is_empty()),
            summary: summary.trim(),
        });
    }
    Ok(commits)
}

/// One record of `git diff-tree --raw -z`: what happened to which path(s),
/// with the blob id of each side that exists.
enum Delta<'a> {
    Added {
        path: RepoPath,
        blob: &'a str,
    },
    Deleted {
        path: RepoPath,
        blob: &'a str,
    },
    /// Content or type change in place.
    Modified {
        path: RepoPath,
        old_blob: &'a str,
        new_blob: &'a str,
    },
    Renamed {
        from: RepoPath,
        to: RepoPath,
        old_blob: &'a str,
        new_blob: &'a str,
    },
    /// The source still exists, so only the new blob matters.
    Copied {
        from: RepoPath,
        to: RepoPath,
        blob: &'a str,
    },
}

/// Parse `git diff-tree --raw -z` output. Each record is
/// `:<old mode> <new mode> <old id> <new id> <status>\0<path>\0`, with a
/// second path (the destination) for renames and copies. Paths are raw bytes,
/// never C-quoted.
fn parse_raw_diff(out: &[u8]) -> Result<Vec<Delta<'_>>> {
    let Some(body) = out.strip_suffix(b"\0") else {
        anyhow::ensure!(out.is_empty(), "diff-tree output is not NUL-terminated");
        return Ok(Vec::new());
    };
    let mut fields = body.split(|&b| b == 0);
    let mut deltas = Vec::new();
    while let Some(record) = fields.next() {
        let record = std::str::from_utf8(record)
            .ok()
            .and_then(|r| r.strip_prefix(':'))
            .with_context(|| {
                format!(
                    "unexpected diff-tree record: {:?}",
                    String::from_utf8_lossy(record)
                )
            })?;
        let mut parts = record.split(' ');
        let (Some(_), Some(_), Some(old_blob), Some(new_blob), Some(status), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            bail!("unexpected diff-tree record: {record:?}");
        };
        let mut path = || -> Result<RepoPath> {
            RepoPath::from_git_bytes(fields.next().context("diff-tree record has no path")?)
        };
        deltas.push(match status.as_bytes().first() {
            Some(b'A') => Delta::Added {
                path: path()?,
                blob: new_blob,
            },
            Some(b'D') => Delta::Deleted {
                path: path()?,
                blob: old_blob,
            },
            Some(b'M' | b'T') => Delta::Modified {
                path: path()?,
                old_blob,
                new_blob,
            },
            Some(b'R') => Delta::Renamed {
                from: path()?,
                to: path()?,
                old_blob,
                new_blob,
            },
            Some(b'C') => Delta::Copied {
                from: path()?,
                to: path()?,
                blob: new_blob,
            },
            _ => bail!("unexpected diff-tree status in record: {record:?}"),
        });
    }
    Ok(deltas)
}

impl Delta<'_> {
    fn matches(&self, matchers: &[GlobMatcher]) -> bool {
        let is_match = |p: &RepoPath| matchers.iter().any(|m| m.is_match(p.as_str()));
        match self {
            Self::Added { path, .. } | Self::Deleted { path, .. } | Self::Modified { path, .. } => {
                is_match(path)
            }
            Self::Renamed { from, to, .. } | Self::Copied { from, to, .. } => {
                is_match(from) || is_match(to)
            }
        }
    }

    /// Read the pointers on each side; `None` when neither side is a pointer.
    fn resolve(self, cat_file: &mut CatFileBatch) -> Result<Option<Change>> {
        let mut read = |blob: &str| cat_file.read_pointer(blob);
        Ok(match self {
            Self::Added { path, blob } => {
                read(blob)?.map(|pointer| Change::Added { path, pointer })
            }
            Self::Deleted { path, blob } => {
                read(blob)?.map(|pointer| Change::Deleted { path, pointer })
            }
            Self::Modified {
                path,
                old_blob,
                new_blob,
            } => match (read(old_blob)?, read(new_blob)?) {
                (None, None) => None,
                // Non-pointer converted to a pointer
                (None, Some(pointer)) => Some(Change::Added { path, pointer }),
                // Pointer converted to a non-pointer
                (Some(pointer), None) => Some(Change::Deleted { path, pointer }),
                (Some(old), Some(new)) => Some(Change::Modified { path, old, new }),
            },
            Self::Renamed {
                from,
                to,
                old_blob,
                new_blob,
            } => match (read(old_blob)?, read(new_blob)?) {
                (None, None) => None,
                (None, Some(pointer)) => Some(Change::RenamedAdded { from, to, pointer }),
                (Some(pointer), None) => Some(Change::RenamedDeleted { from, to, pointer }),
                (Some(old), Some(new)) if old == new => Some(Change::Renamed {
                    from,
                    to,
                    pointer: new,
                }),
                (Some(old), Some(new)) => Some(Change::RenamedModified { from, to, old, new }),
            },
            Self::Copied { from, to, blob } => {
                read(blob)?.map(|pointer| Change::Copied { from, to, pointer })
            }
        })
    }
}

/// A bigstore event, carrying exactly the data its log line shows.
enum Change {
    /// `+ path`: pointer added, or a non-pointer became one.
    Added { path: RepoPath, pointer: Pointer },
    /// `- path`: pointer deleted, or it became a non-pointer.
    Deleted { path: RepoPath, pointer: Pointer },
    /// `~ path  old -> new`
    Modified {
        path: RepoPath,
        old: Pointer,
        new: Pointer,
    },
    /// `R from -> to`: same pointer under a new path.
    Renamed {
        from: RepoPath,
        to: RepoPath,
        pointer: Pointer,
    },
    /// `~ from -> to  old -> new`: renamed and its pointer changed.
    RenamedModified {
        from: RepoPath,
        to: RepoPath,
        old: Pointer,
        new: Pointer,
    },
    /// `+ from -> to`: renamed and became a pointer.
    RenamedAdded {
        from: RepoPath,
        to: RepoPath,
        pointer: Pointer,
    },
    /// `- from -> to`: renamed and stopped being a pointer.
    RenamedDeleted {
        from: RepoPath,
        to: RepoPath,
        pointer: Pointer,
    },
    /// `C from -> to`: copy produced a pointer (the source still exists).
    Copied {
        from: RepoPath,
        to: RepoPath,
        pointer: Pointer,
    },
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Added { path, pointer } => write!(f, "+ {path}  {}", Short(pointer)),
            Self::Deleted { path, pointer } => write!(f, "- {path}  {}", Short(pointer)),
            Self::Modified { path, old, new } => {
                write!(f, "~ {path}  {} -> {}", Short(old), Short(new))
            }
            Self::Renamed { from, to, pointer } => {
                write!(f, "R {from} -> {to}  {}", Short(pointer))
            }
            Self::RenamedModified { from, to, old, new } => {
                write!(f, "~ {from} -> {to}  {} -> {}", Short(old), Short(new))
            }
            Self::RenamedAdded { from, to, pointer } => {
                write!(f, "+ {from} -> {to}  {}", Short(pointer))
            }
            Self::RenamedDeleted { from, to, pointer } => {
                write!(f, "- {from} -> {to}  {}", Short(pointer))
            }
            Self::Copied { from, to, pointer } => {
                write!(f, "C {from} -> {to}  {}", Short(pointer))
            }
        }
    }
}

/// `<hash fn>:<first 6>..<last 6>`; digests are always longer than 12 hex chars.
struct Short<'a>(&'a Pointer);

impl fmt::Display for Short<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.0.hexdigest().to_string();
        write!(
            f,
            "{}:{}..{}",
            self.0.hash_fn(),
            &hex[..6],
            &hex[hex.len() - 6..]
        )
    }
}
