use anyhow::{Context, Result};
use globset::{Glob, GlobMatcher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::catfile::CatFileBatch;
use crate::types::{Pointer, RepoPath};

/// The git directory shared by all linked worktrees, as an absolute path.
/// The object cache lives here, so every worktree of a clone shares it and a
/// commit made in one worktree can be checked out in another.
pub fn common_dir() -> Result<PathBuf> {
    rev_parse(&["--path-format=absolute", "--git-common-dir"])
}

/// This worktree's own git directory, as an absolute path (differs from
/// [`common_dir`] in a linked worktree). Per-worktree state lives here.
pub fn worktree_git_dir() -> Result<PathBuf> {
    rev_parse(&["--path-format=absolute", "--git-dir"])
}

pub fn repo_root() -> Result<PathBuf> {
    rev_parse(&["--show-toplevel"])
}

fn rev_parse(args: &[&str]) -> Result<PathBuf> {
    let output = Command::new("git").arg("rev-parse").args(args).output()?;
    anyhow::ensure!(output.status.success(), "not a git repository");
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(PathBuf::from(path))
}

/// Run git in `repo_root`, feeding `input` on stdin, and return stdout.
/// stdin is written from a separate thread so large inputs cannot deadlock
/// against a full stdout pipe.
fn git_with_input(repo_root: &Path, args: &[&str], input: Vec<u8>) -> Result<Vec<u8>> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to run git {}", args.join(" ")))?;
    let mut stdin = child.stdin.take().context("git stdin not piped")?;
    let writer = std::thread::spawn(move || stdin.write_all(&input));
    let output = child.wait_with_output()?;
    writer
        .join()
        .map_err(|_| anyhow::anyhow!("git stdin writer panicked"))??;
    anyhow::ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output.stdout)
}

fn nul_joined<'a>(paths: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    for p in paths {
        out.extend_from_slice(p);
        out.push(0);
    }
    out
}

/// What git's index holds for a file the bigstore filter applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBlob {
    /// A bigstore pointer: the file's content lives in the cache/remote.
    Pointer(Pointer),
    /// Raw content, e.g. committed before the filter was configured and not
    /// yet renormalised. Nothing to transfer.
    Content,
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub path: RepoPath,
    pub blob: IndexBlob,
    /// Excluded from the working tree (sparse checkout or
    /// `update-index --skip-worktree`): never fetched or checked out.
    pub skip_worktree: bool,
}

/// Every regular file in the index whose `filter` attribute is `bigstore`,
/// narrowed to those matching any of `patterns` (root-relative globs) when
/// patterns are given.
///
/// Git decides the attribute (`git check-attr`), so nested `.gitattributes`,
/// `.git/info/attributes` and macros count exactly as they do for the clean
/// filter itself. Paths are read NUL-separated and root-relative, so
/// non-ASCII names and the caller's cwd cannot change the result.
/// Unmerged (conflicted) entries are skipped.
pub fn bigstore_entries(repo_root: &Path, patterns: &[String]) -> Result<Vec<IndexEntry>> {
    let matchers: Vec<GlobMatcher> = patterns
        .iter()
        .map(|p| {
            Glob::new(p)
                .with_context(|| format!("invalid pattern: {p:?}"))
                .map(|g| g.compile_matcher())
        })
        .collect::<Result<_>>()?;

    // "<tag> <mode> <oid> <stage>\t<path>\0". Paths stay raw bytes until git
    // says the file is ours: an unrelated file with an odd name must not matter.
    let listing = git_with_input(repo_root, &["ls-files", "-s", "-t", "-z"], Vec::new())?;
    let mut files: Vec<(&[u8], &str, bool)> = Vec::new();
    for record in listing.split(|&b| b == 0).filter(|r| !r.is_empty()) {
        let tab = record
            .iter()
            .position(|&b| b == b'\t')
            .context("malformed git ls-files record")?;
        let meta = std::str::from_utf8(&record[..tab])?;
        let [tag, mode, oid, stage] = meta.split(' ').collect::<Vec<_>>()[..] else {
            anyhow::bail!("malformed git ls-files record: {meta:?}");
        };
        // Filters only ever apply to regular files at stage 0.
        if stage != "0" || !matches!(mode, "100644" | "100755") {
            continue;
        }
        files.push((&record[tab + 1..], oid, tag == "S"));
    }

    // "<path>\0filter\0<value>\0", in input order.
    let attrs = git_with_input(
        repo_root,
        &["check-attr", "-z", "--stdin", "filter"],
        nul_joined(files.iter().map(|(p, ..)| *p)),
    )?;
    let values: Vec<&[u8]> = attrs.split(|&b| b == 0).collect();
    let (records, _trailing) = values.as_chunks::<3>();
    anyhow::ensure!(
        records.len() == files.len(),
        "git check-attr returned {} records for {} paths",
        records.len(),
        files.len()
    );

    let mut cat_file = CatFileBatch::start(repo_root)?;
    let mut entries = Vec::new();
    for ((raw_path, oid, skip_worktree), [attr_path, _name, value]) in
        files.into_iter().zip(records)
    {
        anyhow::ensure!(
            *attr_path == raw_path,
            "git check-attr output out of order at {}",
            String::from_utf8_lossy(raw_path)
        );
        if *value != b"bigstore" {
            continue;
        }
        let path = RepoPath::from_git_bytes(raw_path)?;
        if !(matchers.is_empty() || matchers.iter().any(|m| m.is_match(path.as_str()))) {
            continue;
        }
        let blob = match cat_file.read_pointer(oid)? {
            Some(p) => IndexBlob::Pointer(p),
            None => IndexBlob::Content,
        };
        entries.push(IndexEntry {
            path,
            blob,
            skip_worktree,
        });
    }
    Ok(entries)
}

/// Write `paths` from the index into the working tree — through the smudge
/// filter, with the index's file mode — and refresh their index stat data so
/// git does not report them as modified (`git checkout-index -u`).
///
/// Without `-f`: git refuses to replace a file that exists, so this never
/// overwrites something written at a path after the caller checked it.
/// If any path fails, git writes no index at all: paths it did write are
/// left with stale stat data.
pub fn checkout_index(repo_root: &Path, paths: &[&RepoPath]) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    git_with_input(
        repo_root,
        &["checkout-index", "-u", "-z", "--stdin"],
        nul_joined(paths.iter().map(|p| p.as_str().as_bytes())),
    )?;
    Ok(())
}

pub fn config_get(key: &str) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["config", "--get", key])
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

fn config_set(key: &str, value: &str) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(["config", key, value])
        .status()?;
    anyhow::ensure!(status.success(), "failed to set git config {key}");
    Ok(())
}

// ──────────────────────────────────────────────────
// Filter configuration
// ──────────────────────────────────────────────────
//
// Git's clean/smudge filter has three config keys: clean, smudge, and
// required. This type models them as a unit and enforces:
//
//   1. Presence-consistency: all three must be set, or none.
//   2. Command-shape: clean must end with "filter-clean %f" (or plain
//      "filter-clean", written by older versions), smudge with
//      "filter-smudge", and both must share the same binary prefix.
//   3. Required must be "true".
//
// Partial or malformed config is rejected on load() with repair guidance.

/// A valid, complete filter configuration.
///
/// Invariants (enforced by load/new):
/// - `binary` is the shared command prefix (e.g. "git-bigstore" or "/full/path/to/git-bigstore")
/// - clean = "{binary} filter-clean %f", smudge = "{binary} filter-smudge"
/// - required is always true (set on save, checked on load)
pub struct FilterConfig {
    binary: String,
}

impl FilterConfig {
    /// Default config using bare binary name (requires git-bigstore in PATH).
    pub fn default_commands() -> Self {
        Self {
            binary: "git-bigstore".to_string(),
        }
    }

    /// Read the current filter config from git.
    ///
    /// Returns:
    /// - `Ok(None)` — not configured (all three keys absent)
    /// - `Ok(Some(config))` — valid, complete config
    /// - `Err` — partial, malformed, or inconsistent config
    pub fn load() -> Result<Option<Self>> {
        let clean = config_get("filter.bigstore.clean");
        let smudge = config_get("filter.bigstore.smudge");
        let required = config_get("filter.bigstore.required");

        // All absent = unconfigured
        if clean.is_none() && smudge.is_none() && required.is_none() {
            return Ok(None);
        }

        // Partial presence
        let clean = clean.ok_or_else(|| {
            anyhow::anyhow!(
                "filter.bigstore.smudge is set but filter.bigstore.clean is missing.\n\
             Fix: git config filter.bigstore.clean \"git-bigstore filter-clean %f\""
            )
        })?;
        let smudge = smudge.ok_or_else(|| {
            anyhow::anyhow!(
                "filter.bigstore.clean is set but filter.bigstore.smudge is missing.\n\
             Fix: git config filter.bigstore.smudge \"git-bigstore filter-smudge\""
            )
        })?;

        // Required must be "true"
        match required.as_deref() {
            Some("true") => {}
            Some(other) => anyhow::bail!(
                "filter.bigstore.required is {other:?}, expected \"true\".\n\
                 Fix: git config filter.bigstore.required true"
            ),
            None => anyhow::bail!(
                "filter.bigstore.required is not set.\n\
                 Fix: git config filter.bigstore.required true"
            ),
        }

        // Command shape: must end with "filter-clean [%f]" / "filter-smudge".
        // `%f` hands clean the path, so it can keep an md5 pointer.
        let clean_bin = clean
            .strip_suffix(" filter-clean %f")
            .or_else(|| clean.strip_suffix(" filter-clean"))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "filter.bigstore.clean has unexpected format: {clean:?}\n\
             Expected: \"<binary> filter-clean %f\"\n\
             Fix: git config filter.bigstore.clean \"git-bigstore filter-clean %f\""
                )
            })?;
        let smudge_bin = smudge.strip_suffix(" filter-smudge").ok_or_else(|| {
            anyhow::anyhow!(
                "filter.bigstore.smudge has unexpected format: {smudge:?}\n\
             Expected: \"<binary> filter-smudge\"\n\
             Fix: git config filter.bigstore.smudge \"git-bigstore filter-smudge\""
            )
        })?;

        // Same binary prefix
        anyhow::ensure!(
            clean_bin == smudge_bin,
            "filter.bigstore.clean and smudge point at different binaries:\n\
             clean:  {clean:?}\n\
             smudge: {smudge:?}\n\
             Both must use the same binary prefix."
        );

        Ok(Some(Self {
            binary: clean_bin.to_string(),
        }))
    }

    /// Write this filter config to git.
    pub fn save(&self) -> Result<()> {
        config_set(
            "filter.bigstore.clean",
            &format!("{} filter-clean %f", self.binary),
        )?;
        config_set(
            "filter.bigstore.smudge",
            &format!("{} filter-smudge", self.binary),
        )?;
        config_set("filter.bigstore.required", "true")?;
        Ok(())
    }

    /// The binary path/name used by this config.
    #[cfg(test)]
    pub fn binary(&self) -> &str {
        &self.binary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_commands_roundtrip() {
        let cfg = FilterConfig::default_commands();
        assert_eq!(cfg.binary(), "git-bigstore");
    }
}
