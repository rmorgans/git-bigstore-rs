//! `git bigstore log`: file-level history of bigstore pointers.

use anyhow::{Context, Result};
use globset::Glob;
use std::process::{Command, Stdio};

use crate::catfile::CatFileBatch;
use crate::git;
use crate::types;

pub fn run(paths: &[String]) -> Result<()> {
    // Get commit list (first-parent only to avoid merge noise)
    let rev_output = Command::new("git")
        .args(["rev-list", "--first-parent", "HEAD"])
        .output()?;
    anyhow::ensure!(rev_output.status.success(), "git rev-list failed");
    let commits = String::from_utf8(rev_output.stdout)?;

    // Optional path filter matchers
    let path_matchers: Vec<_> = paths
        .iter()
        .map(|p| {
            Glob::new(p)
                .with_context(|| format!("invalid path pattern: {p:?}"))
                .map(|g| g.compile_matcher())
        })
        .collect::<Result<_>>()?;

    // Single long-lived process for all blob reads
    let mut cat_file = CatFileBatch::start(&git::repo_root()?)?;
    let mut found_any = false;

    for commit in commits.lines() {
        // Check if this is a root commit (no parents)
        let parent_check = Command::new("git")
            .args(["rev-parse", "--verify", &format!("{commit}^")])
            .stderr(Stdio::null())
            .output()?;
        let is_root = !parent_check.status.success();

        // For root commits: diff against empty tree (--root)
        // For all others (including merges): diff against first parent explicitly
        let diff_output = if is_root {
            Command::new("git")
                .args([
                    "diff-tree",
                    "--root",
                    "-r",
                    "-M",
                    "-C",
                    "--name-status",
                    commit,
                ])
                .output()?
        } else {
            let parent = format!("{commit}~1");
            Command::new("git")
                .args([
                    "diff-tree",
                    "-r",
                    "-M",
                    "-C",
                    "--name-status",
                    &parent,
                    commit,
                ])
                .output()?
        };
        if !diff_output.status.success() {
            continue;
        }
        let diff_text = String::from_utf8_lossy(&diff_output.stdout);

        let mut changes: Vec<LogChange> = Vec::new();

        for line in diff_text.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                continue;
            }

            let status = parts[0];
            let (old_path, new_path) = if status.starts_with('R') || status.starts_with('C') {
                if parts.len() < 3 {
                    continue;
                }
                (Some(parts[1]), parts[2])
            } else {
                (None, parts[1])
            };

            // Path filter
            if !path_matchers.is_empty() {
                let matches = path_matchers.iter().any(|m| m.is_match(new_path))
                    || old_path.is_some_and(|op| path_matchers.iter().any(|m| m.is_match(op)));
                if !matches {
                    continue;
                }
            }

            let status_char = status.chars().next().unwrap_or('M');

            let new_pointer = if status_char == 'D' {
                None
            } else {
                cat_file.read_pointer(&format!("{commit}:{new_path}"))?
            };

            let old_pointer = if status_char == 'A' {
                None
            } else {
                let old_ref = format!("{commit}~1");
                let check_path = old_path.unwrap_or(new_path);
                cat_file.read_pointer(&format!("{old_ref}:{check_path}"))?
            };

            if new_pointer.is_none() && old_pointer.is_none() {
                continue;
            }

            // For copies, old path still exists — only the new path matters.
            // If the copy didn't produce a pointer, it's not a bigstore event.
            if status_char == 'C' && new_pointer.is_none() {
                continue;
            }

            let kind = match (status_char, &old_pointer, &new_pointer) {
                // File added as pointer, or non-pointer converted to pointer
                ('A', _, Some(_)) | ('M' | 'T', None, Some(_)) => ChangeKind::Added,
                // File deleted, or pointer converted to non-pointer
                ('D', Some(_), _) | ('M' | 'T', Some(_), None) => ChangeKind::Deleted,
                // Copy produced a pointer (old path still exists, this is a new pointer)
                ('C', _, Some(_)) => ChangeKind::Copied,
                // Rename where bigstore tracking was added
                ('R', None, Some(_)) => ChangeKind::RenamedAdded,
                // Rename where bigstore tracking was removed
                ('R', Some(_), None) => ChangeKind::RenamedDeleted,
                // Pure rename (same content hash)
                ('R', _, _)
                    if old_pointer.as_ref().map(|p| p.hexdigest())
                        == new_pointer.as_ref().map(|p| p.hexdigest()) =>
                {
                    ChangeKind::Renamed
                }
                // Everything else: content change
                _ => ChangeKind::Modified,
            };

            changes.push(LogChange {
                kind,
                path: new_path.to_string(),
                old_path: old_path.map(String::from),
                old_pointer,
                new_pointer,
            });
        }

        if changes.is_empty() {
            continue;
        }

        // Get commit metadata
        let meta_output = Command::new("git")
            .args(["log", "-1", "--format=%h %ai %s", commit])
            .output()?;
        let meta = String::from_utf8_lossy(&meta_output.stdout)
            .trim()
            .to_string();

        if found_any {
            println!();
        }
        println!("  {meta}");

        for c in &changes {
            let symbol = match c.kind {
                ChangeKind::Added | ChangeKind::RenamedAdded => "+",
                ChangeKind::Deleted | ChangeKind::RenamedDeleted => "-",
                ChangeKind::Modified => "~",
                ChangeKind::Renamed => "R",
                ChangeKind::Copied => "C",
            };

            match c.kind {
                ChangeKind::Added => {
                    if let Some(p) = &c.new_pointer {
                        println!(
                            "    {symbol} {}  {}:{}",
                            c.path,
                            p.hash_fn(),
                            short_hash(p.hexdigest())
                        );
                    }
                }
                ChangeKind::Deleted => {
                    if let Some(p) = &c.old_pointer {
                        println!(
                            "    {symbol} {}  {}:{}",
                            c.path,
                            p.hash_fn(),
                            short_hash(p.hexdigest())
                        );
                    }
                }
                ChangeKind::RenamedAdded | ChangeKind::Copied => {
                    let old = c.old_path.as_deref().unwrap_or("?");
                    if let Some(p) = &c.new_pointer {
                        println!(
                            "    {symbol} {old} -> {}  {}:{}",
                            c.path,
                            p.hash_fn(),
                            short_hash(p.hexdigest())
                        );
                    }
                }
                ChangeKind::RenamedDeleted => {
                    let old = c.old_path.as_deref().unwrap_or("?");
                    if let Some(p) = &c.old_pointer {
                        println!(
                            "    {symbol} {old} -> {}  {}:{}",
                            c.path,
                            p.hash_fn(),
                            short_hash(p.hexdigest())
                        );
                    }
                }
                ChangeKind::Modified => {
                    let old_desc = c
                        .old_pointer
                        .as_ref()
                        .map(|p| format!("{}:{}", p.hash_fn(), short_hash(p.hexdigest())))
                        .unwrap_or_else(|| "(not a pointer)".to_string());
                    let new_desc = c
                        .new_pointer
                        .as_ref()
                        .map(|p| format!("{}:{}", p.hash_fn(), short_hash(p.hexdigest())))
                        .unwrap_or_else(|| "(not a pointer)".to_string());
                    let path_str = if let Some(op) = &c.old_path {
                        format!("{op} -> {}", c.path)
                    } else {
                        c.path.clone()
                    };
                    println!("    {symbol} {path_str}  {old_desc} -> {new_desc}");
                }
                ChangeKind::Renamed => {
                    let old = c.old_path.as_deref().unwrap_or("?");
                    if let Some(p) = &c.new_pointer {
                        println!(
                            "    {symbol} {old} -> {}  {}:{}",
                            c.path,
                            p.hash_fn(),
                            short_hash(p.hexdigest())
                        );
                    }
                }
            }
        }

        found_any = true;
    }

    drop(cat_file);

    if !found_any {
        eprintln!("No bigstore file changes found in history.");
    }

    Ok(())
}

fn short_hash(hexdigest: &types::Hexdigest) -> String {
    let s = hexdigest.to_string();
    if s.len() > 12 {
        format!("{}..{}", &s[..6], &s[s.len() - 6..])
    } else {
        s
    }
}

enum ChangeKind {
    Added,
    Deleted,
    Modified,
    Renamed,
    RenamedAdded,   // Rename + became a pointer
    RenamedDeleted, // Rename + stopped being a pointer
    Copied,         // Copy produced a pointer (source still exists)
}

struct LogChange {
    kind: ChangeKind,
    path: String,
    old_path: Option<String>,
    old_pointer: Option<types::Pointer>,
    new_pointer: Option<types::Pointer>,
}
