use anyhow::Result;
use clap::{Parser, Subcommand};
use globset::Glob;
use std::path::Path;

mod backend;
mod cache;
mod config;
mod dvc;
mod filter;
mod git;
mod transfer;
mod types;

#[derive(Parser)]
#[command(name = "git-bigstore", version, about = "Large files in git, your bucket, one binary.")]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize bigstore in this repository
    Init {
        /// Storage URL: s3://bucket, t3://bucket, r2://bucket, gs://bucket, az://container
        url: String,

        /// S3-compatible endpoint override (for R2, MinIO, B2, etc.)
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// Upload cached objects to remote storage
    Push {
        /// Only push files matching these patterns
        patterns: Vec<String>,
    },

    /// Download objects from remote storage (with integrity verification)
    Pull {
        /// Only pull files matching these patterns
        patterns: Vec<String>,
    },

    /// Show status of tracked large files
    Status,

    /// Migrate .bigstore config to .bigstore.toml
    MigrateConfig {
        /// Overwrite existing .bigstore.toml
        #[arg(long)]
        force: bool,
    },

    /// Show history of bigstore-tracked large files
    Log {
        /// Only show history for these paths
        paths: Vec<String>,
    },

    /// Create a bigstore pointer from a .dvc file
    Ref {
        /// Path to .dvc file
        source: String,
        /// Destination path for the pointer file
        dest: String,
    },

    /// Internal: clean filter (stdin -> stdout)
    #[command(name = "filter-clean", hide = true)]
    FilterClean,

    /// Internal: smudge filter (stdin -> stdout)
    #[command(name = "filter-smudge", hide = true)]
    FilterSmudge,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init { url, endpoint } => cmd_init(&url, endpoint.as_deref()).await,
        Commands::Push { patterns } => cmd_push(&patterns).await,
        Commands::Pull { patterns } => cmd_pull(&patterns).await,
        Commands::Status => cmd_status().await,
        Commands::MigrateConfig { force } => cmd_migrate_config(force),
        Commands::Log { paths } => cmd_log(&paths),
        Commands::Ref { source, dest } => cmd_ref(&source, &dest),
        Commands::FilterClean => filter::clean(),
        Commands::FilterSmudge => filter::smudge(),
    }
}

async fn cmd_init(url: &str, endpoint: Option<&str>) -> Result<()> {
    let git_dir = git::git_dir()?;
    let repo_root = git::repo_root()?;

    let cfg = config::BigstoreConfig::from_url(url, endpoint)?;
    let config_path = repo_root.join(".bigstore.toml");
    cfg.save(&config_path)?;

    git::config_set("filter.bigstore.clean", "git-bigstore filter-clean")?;
    git::config_set("filter.bigstore.smudge", "git-bigstore filter-smudge")?;
    git::config_set("filter.bigstore.required", "true")?;

    cache::ensure_cache_dir(&git_dir)?;

    eprintln!("Initialized bigstore with backend: {}", cfg.backend_type());
    eprintln!("Config written to .bigstore.toml");
    eprintln!();
    eprintln!("Add patterns to .gitattributes:");
    eprintln!("  echo '*.bin filter=bigstore' >> .gitattributes");
    eprintln!("  echo 'assets/** filter=bigstore' >> .gitattributes");

    Ok(())
}

async fn cmd_push(patterns: &[String]) -> Result<()> {
    let repo_root = git::repo_root()?;
    let tracked = tracked_files(&repo_root, patterns)?;
    let summary = transfer::push(&tracked).await?;
    summary.print();
    if !summary.failed.is_empty() {
        anyhow::bail!("{} file(s) failed", summary.failed.len());
    }
    Ok(())
}

async fn cmd_pull(patterns: &[String]) -> Result<()> {
    let repo_root = git::repo_root()?;
    let tracked = tracked_files(&repo_root, patterns)?;
    let summary = transfer::pull(&tracked).await?;
    summary.print();
    if !summary.failed.is_empty() {
        anyhow::bail!("{} file(s) failed", summary.failed.len());
    }
    Ok(())
}

async fn cmd_status() -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;
    let _cfg = config::BigstoreConfig::find_and_load(&repo_root)?;

    let tracked = tracked_files(&repo_root, &[])?;

    for (path, _filter) in &tracked {
        let pointer = filter::read_pointer_from_git(path);
        let status = match pointer {
            Ok(Some(p)) => {
                let cached = cache::object_path(&git_dir, &p.hexdigest, p.hash_fn).exists();
                let full_path = repo_root.join(path);
                let smudged = full_path.exists() && !filter::is_pointer_file(&full_path);
                match (cached, smudged) {
                    (true, true) => "ok",
                    (true, false) => "cached (not checked out)",
                    (false, true) => "local only (not cached)",
                    (false, false) => "pointer only (needs pull)",
                }
            }
            _ => "not a bigstore file",
        };
        println!("{status:>30}  {path}");
    }

    Ok(())
}

fn cmd_migrate_config(force: bool) -> Result<()> {
    let repo_root = git::repo_root()?;
    let legacy_path = repo_root.join(".bigstore");
    let toml_path = repo_root.join(".bigstore.toml");

    anyhow::ensure!(
        legacy_path.exists(),
        "no .bigstore file found — nothing to migrate"
    );

    if toml_path.exists() && !force {
        anyhow::bail!(
            ".bigstore.toml already exists. Use --force to overwrite."
        );
    }

    // Load from legacy, save as toml (validates + normalizes)
    let cfg = config::BigstoreConfig::load(&legacy_path)?;
    cfg.save(&toml_path)?;

    eprintln!("Migrated .bigstore -> .bigstore.toml");
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  git add .bigstore.toml");
    eprintln!("  git rm .bigstore        # remove the old config");
    eprintln!("  git commit -m 'migrate bigstore config to toml'");

    Ok(())
}

fn cmd_log(paths: &[String]) -> Result<()> {
    // Get commit list (first-parent only to avoid merge noise)
    let mut rev_list_cmd = std::process::Command::new("git");
    rev_list_cmd.args(["rev-list", "--first-parent", "HEAD"]);
    let rev_output = rev_list_cmd.output()?;
    anyhow::ensure!(rev_output.status.success(), "git rev-list failed");
    let commits = String::from_utf8(rev_output.stdout)?;

    // Optional path filter matchers
    let path_matchers: Vec<_> = paths
        .iter()
        .filter_map(|p| Glob::new(p).ok().map(|g| g.compile_matcher()))
        .collect();

    let mut found_any = false;

    for commit in commits.lines() {
        // Get changed files with rename detection
        // --root handles the initial commit (diffs against empty tree)
        // Check if this is a root commit (no parents)
        let parent_check = std::process::Command::new("git")
            .args(["rev-parse", "--verify", &format!("{commit}^")])
            .output()?;
        let is_root = !parent_check.status.success();

        // For root commits: diff against empty tree (--root)
        // For all others (including merges): diff against first parent explicitly
        let diff_output = if is_root {
            std::process::Command::new("git")
                .args(["diff-tree", "--root", "-r", "-M", "--name-status", commit])
                .output()?
        } else {
            // Explicit two-tree diff: first-parent vs this commit
            let parent = format!("{commit}~1");
            std::process::Command::new("git")
                .args(["diff-tree", "-r", "-M", "--name-status", &parent, commit])
                .output()?
        };
        if !diff_output.status.success() {
            continue;
        }
        let diff_text = String::from_utf8_lossy(&diff_output.stdout);

        // Parse diff-tree output into file changes
        let mut changes: Vec<LogChange> = Vec::new();

        for line in diff_text.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                continue;
            }

            let status = parts[0];
            let (old_path, new_path) = if status.starts_with('R') || status.starts_with('C') {
                // Rename/Copy: R100\told\tnew
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

            // Read blob content to detect bigstore pointers.
            // Only read sides that exist for the given status.
            let status_char = status.chars().next().unwrap_or('M');

            let new_pointer = if status_char == 'D' {
                None // Deleted — blob doesn't exist at this commit
            } else {
                read_blob_pointer(commit, new_path)
            };

            let old_pointer = if status_char == 'A' {
                None // Added — no previous version
            } else {
                let old_ref = format!("{commit}~1");
                let check_path = old_path.unwrap_or(new_path);
                read_blob_pointer(&old_ref, check_path)
            };

            // Only emit if at least one side is a bigstore pointer
            if new_pointer.is_none() && old_pointer.is_none() {
                continue;
            }

            let kind = match (status_char, &old_pointer, &new_pointer) {
                // File added as pointer, or normal file converted to pointer
                ('A', _, Some(_)) | ('M', None, Some(_)) => ChangeKind::Added,
                // File deleted, or pointer replaced with normal file
                ('D', Some(_), _) | ('M', Some(_), None) => ChangeKind::Deleted,
                // Pure rename (same content hash)
                ('R' | 'C', _, _) if old_pointer.as_ref().map(|p| &p.hexdigest)
                    == new_pointer.as_ref().map(|p| &p.hexdigest) => ChangeKind::Renamed,
                // Rename + content change, or regular modification
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
        let meta_output = std::process::Command::new("git")
            .args(["log", "-1", "--format=%h %ai %s", commit])
            .output()?;
        let meta = String::from_utf8_lossy(&meta_output.stdout).trim().to_string();

        if found_any {
            println!();
        }
        println!("  {meta}");

        for c in &changes {
            let symbol = match c.kind {
                ChangeKind::Added => "+",
                ChangeKind::Deleted => "-",
                ChangeKind::Modified => "~",
                ChangeKind::Renamed => "R",
            };

            match c.kind {
                ChangeKind::Added => {
                    if let Some(p) = &c.new_pointer {
                        println!("    {symbol} {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
                ChangeKind::Deleted => {
                    if let Some(p) = &c.old_pointer {
                        println!("    {symbol} {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
                ChangeKind::Modified => {
                    let old_desc = c.old_pointer.as_ref()
                        .map(|p| format!("{}:{}", p.hash_fn, short_hash(&p.hexdigest)))
                        .unwrap_or_else(|| "(not a pointer)".to_string());
                    let new_desc = c.new_pointer.as_ref()
                        .map(|p| format!("{}:{}", p.hash_fn, short_hash(&p.hexdigest)))
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
                        println!("    {symbol} {old} -> {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
            }
        }

        found_any = true;
    }

    if !found_any {
        eprintln!("No bigstore file changes found in history.");
    }

    Ok(())
}

/// Try to read a blob at commit:path and parse it as a bigstore pointer.
/// Returns None if the blob doesn't exist or isn't a pointer.
fn read_blob_pointer(commit: &str, path: &str) -> Option<types::Pointer> {
    let output = std::process::Command::new("git")
        .args(["show", &format!("{commit}:{path}")])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    types::Pointer::parse(&output.stdout).ok().flatten()
}

fn short_hash(hexdigest: &types::Hexdigest) -> String {
    let s = hexdigest.to_string();
    if s.len() > 12 {
        format!("{}..{}", &s[..6], &s[s.len()-6..])
    } else {
        s
    }
}

enum ChangeKind {
    Added,
    Deleted,
    Modified,
    Renamed,
}

struct LogChange {
    kind: ChangeKind,
    path: String,
    old_path: Option<String>,
    old_pointer: Option<types::Pointer>,
    new_pointer: Option<types::Pointer>,
}

fn cmd_ref(source: &str, dest: &str) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;

    // Reject paths that escape the repository
    for (label, p) in [("source", source), ("dest", dest)] {
        let path = Path::new(p);
        anyhow::ensure!(!path.is_absolute(), "{label} must be a relative path: {p:?}");
        anyhow::ensure!(
            !path.components().any(|c| matches!(c, std::path::Component::ParentDir)),
            "{label} must not contain '..': {p:?}"
        );
    }

    let source_path = repo_root.join(source);
    let (pointer, dvc_out_path) = dvc::parse_dvc_pointer(&source_path)?;

    // Try to import the object from DVC cache into bigstore cache
    let dvc_cache = cache::dvc_cache_path(&repo_root, &pointer.hexdigest);
    let bs_cache = cache::object_path(&git_dir, &pointer.hexdigest, pointer.hash_fn);

    if dvc_cache.exists() && !bs_cache.exists() {
        // Verify hash before importing
        let actual = transfer::hash_file(&dvc_cache, pointer.hash_fn)?;
        anyhow::ensure!(
            actual == pointer.hexdigest,
            "DVC cache integrity check failed: expected {}, got {actual}",
            pointer.hexdigest
        );

        if let Some(parent) = bs_cache.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Atomic persist — never leave partial files in cache
        match cache::copy_atomically_noclobber(&dvc_cache, &bs_cache) {
            Ok(()) => {}
            Err(e) if bs_cache.exists() => {}
            Err(e) => return Err(e),
        }
        eprintln!("Imported from DVC cache (verified): {dvc_out_path}");
    } else if bs_cache.exists() {
        eprintln!("Already in bigstore cache: {dvc_out_path}");
    } else {
        anyhow::bail!(
            "object not found in DVC cache at {}\n\
             Run `dvc pull {source}` first to populate the DVC cache, then retry.",
            cache::dvc_cache_path(&repo_root, &pointer.hexdigest).display()
        );
    }

    // Write the pointer file
    let dest_path = repo_root.join(dest);
    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dest_path, pointer.encode())?;

    eprintln!("Created pointer: {dest} -> {source} (md5:{})", pointer.hexdigest);
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. Ensure {dest} is tracked: echo '{dest} filter=bigstore' >> .gitattributes");
    eprintln!("  2. git add {dest} .gitattributes");
    eprintln!("  3. git commit -m 'add {dest}'");
    eprintln!("  4. git bigstore push");

    Ok(())
}

/// Parse .gitattributes for bigstore filter patterns, then list matching tracked files.
fn tracked_files(repo_root: &Path, patterns: &[String]) -> Result<Vec<(String, String)>> {
    let attrs_path = repo_root.join(".gitattributes");
    let filter_patterns = filter::parse_gitattributes(&attrs_path)?;

    if filter_patterns.is_empty() {
        anyhow::bail!("no bigstore filters found in .gitattributes");
    }

    let attr_matchers: Vec<_> = filter_patterns
        .iter()
        .map(|(pattern, filter_name)| {
            let matcher = Glob::new(pattern)
                .unwrap_or_else(|_| Glob::new("*").unwrap())
                .compile_matcher();
            (matcher, filter_name.clone())
        })
        .collect();

    let user_matchers: Vec<_> = patterns
        .iter()
        .filter_map(|p| Glob::new(p).ok().map(|g| g.compile_matcher()))
        .collect();

    let output = std::process::Command::new("git")
        .args(["ls-files"])
        .output()?;
    let files = String::from_utf8(output.stdout)?;

    let mut results = Vec::new();
    for file in files.lines() {
        for (matcher, filter_name) in &attr_matchers {
            if matcher.is_match(file)
                && (user_matchers.is_empty() || user_matchers.iter().any(|m| m.is_match(file)))
            {
                results.push((file.to_string(), filter_name.clone()));
                break;
            }
        }
    }

    Ok(results)
}
