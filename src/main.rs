use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use globset::Glob;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};

mod backend;
mod cache;
mod config;
mod dvc;
mod filter;
mod git;
mod transfer;
mod types;

#[derive(Parser)]
#[command(name = "git-bigstore", version, about = "Large files in git, your bucket, one binary.", long_about = None)]
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

        /// Number of concurrent transfers (default: 8, env: BIGSTORE_JOBS)
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Download objects from remote storage (with integrity verification)
    Pull {
        /// Only pull files matching these patterns
        patterns: Vec<String>,

        /// Number of concurrent transfers (default: 8, env: BIGSTORE_JOBS)
        #[arg(short, long)]
        jobs: Option<usize>,
    },

    /// Show status of tracked large files
    Status {
        /// Verify integrity of cached objects by re-hashing
        #[arg(long)]
        verify: bool,
    },

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

    /// List files in a DVC .dir manifest
    #[command(name = "dvc-ls")]
    DvcLs {
        /// Path to .dvc file (must be a .dir type)
        source: String,
    },

    /// Import files from a DVC .dir manifest into bigstore
    #[command(name = "import-dvc-dir")]
    ImportDvcDir {
        /// Path to .dvc file (must be a .dir type)
        source: String,

        /// Destination root directory for pointer files
        dest_root: String,

        /// Only import files matching these glob patterns (default: all)
        patterns: Vec<String>,

        /// Overwrite existing destination files
        #[arg(long)]
        force: bool,
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
        Commands::Push { patterns, jobs } => cmd_push(&patterns, jobs).await,
        Commands::Pull { patterns, jobs } => cmd_pull(&patterns, jobs).await,
        Commands::Status { verify } => cmd_status(verify).await,
        Commands::MigrateConfig { force } => cmd_migrate_config(force),
        Commands::Log { paths } => cmd_log(&paths),
        Commands::Ref { source, dest } => cmd_ref(&source, &dest),
        Commands::DvcLs { source } => cmd_dvc_ls(&source),
        Commands::ImportDvcDir {
            source,
            dest_root,
            patterns,
            force,
        } => cmd_import_dvc_dir(&source, &dest_root, &patterns, force),
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

    // Only set filter config if not already configured (preserve custom paths)
    let had_filter = git::config_get("filter.bigstore.clean").is_some();
    if !had_filter {
        git::config_set("filter.bigstore.clean", "git-bigstore filter-clean")?;
        git::config_set("filter.bigstore.smudge", "git-bigstore filter-smudge")?;
    }
    git::config_set("filter.bigstore.required", "true")?;

    cache::ensure_cache_dir(&git_dir)?;

    eprintln!("Initialized bigstore with backend: {}", cfg.backend_type());
    eprintln!("Config written to .bigstore.toml");
    if had_filter {
        eprintln!("Filter config preserved (already configured)");
    }
    eprintln!();
    eprintln!("Add patterns to .gitattributes:");
    eprintln!("  echo '*.bin filter=bigstore' >> .gitattributes");
    eprintln!("  echo 'assets/** filter=bigstore' >> .gitattributes");

    Ok(())
}

async fn cmd_push(patterns: &[String], jobs: Option<usize>) -> Result<()> {
    let repo_root = git::repo_root()?;
    let tracked = tracked_files(&repo_root, patterns)?;
    let concurrency = resolve_jobs(jobs)?;
    let summary = transfer::push(&tracked, concurrency).await?;
    summary.print();
    if !summary.failed.is_empty() {
        anyhow::bail!("{} file(s) failed", summary.failed.len());
    }
    Ok(())
}

async fn cmd_pull(patterns: &[String], jobs: Option<usize>) -> Result<()> {
    let repo_root = git::repo_root()?;
    let tracked = tracked_files(&repo_root, patterns)?;
    let concurrency = resolve_jobs(jobs)?;
    let summary = transfer::pull(&tracked, concurrency).await?;
    summary.print();
    if !summary.failed.is_empty() {
        anyhow::bail!("{} file(s) failed", summary.failed.len());
    }
    Ok(())
}

async fn cmd_status(verify: bool) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;
    let _cfg = config::BigstoreConfig::find_and_load(&repo_root)?;

    let tracked = tracked_files(&repo_root, &[])?;
    let mut corrupted: Vec<String> = Vec::new();

    for (path, _filter) in &tracked {
        let pointer = filter::read_pointer_from_git(path);
        let status = match pointer {
            Ok(Some(p)) => {
                let cache_path = cache::object_path(&git_dir, &p.hexdigest, p.hash_fn);
                let cached = cache_path.exists();
                let full_path = repo_root.join(path);
                let smudged = full_path.exists() && !filter::is_pointer_file(&full_path);

                if verify && cached {
                    match transfer::hash_file(&cache_path, p.hash_fn) {
                        Ok(actual) if actual == p.hexdigest => match (true, smudged) {
                            (true, true) => "ok (verified)",
                            (true, false) => "cached (not checked out, verified)",
                            _ => unreachable!(),
                        },
                        Ok(_) => {
                            corrupted.push(path.clone());
                            "CORRUPTED (hash mismatch)"
                        }
                        Err(_) => {
                            corrupted.push(path.clone());
                            "CORRUPTED (unreadable)"
                        }
                    }
                } else {
                    match (cached, smudged) {
                        (true, true) => "ok",
                        (true, false) => "cached (not checked out)",
                        (false, true) => "local only (not cached)",
                        (false, false) => "pointer only (needs pull)",
                    }
                }
            }
            _ => "not a bigstore file",
        };
        println!("{status:>40}  {path}");
    }

    if !corrupted.is_empty() {
        eprintln!();
        eprintln!(
            "{} corrupted cache object(s) found. To repair:",
            corrupted.len()
        );
        for path in &corrupted {
            eprintln!("  {path}");
        }
        eprintln!();
        eprintln!("Delete corrupted cache and re-pull:");
        eprintln!("  rm -rf .git/bigstore/objects");
        eprintln!("  git bigstore pull");
        anyhow::bail!("{} corrupted object(s)", corrupted.len());
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
    let rev_output = Command::new("git")
        .args(["rev-list", "--first-parent", "HEAD"])
        .output()?;
    anyhow::ensure!(rev_output.status.success(), "git rev-list failed");
    let commits = String::from_utf8(rev_output.stdout)?;

    // Optional path filter matchers
    let path_matchers: Vec<_> = paths
        .iter()
        .filter_map(|p| Glob::new(p).ok().map(|g| g.compile_matcher()))
        .collect();

    // Single long-lived process for all blob reads
    let mut cat_file = CatFileBatch::start()?;
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
                .args(["diff-tree", "--root", "-r", "-M", "-C", "--name-status", commit])
                .output()?
        } else {
            let parent = format!("{commit}~1");
            Command::new("git")
                .args(["diff-tree", "-r", "-M", "-C", "--name-status", &parent, commit])
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
                cat_file.read_pointer(commit, new_path)?
            };

            let old_pointer = if status_char == 'A' {
                None
            } else {
                let old_ref = format!("{commit}~1");
                let check_path = old_path.unwrap_or(new_path);
                cat_file.read_pointer(&old_ref, check_path)?
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
                ('R', _, _) if old_pointer.as_ref().map(|p| &p.hexdigest)
                    == new_pointer.as_ref().map(|p| &p.hexdigest) => ChangeKind::Renamed,
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
        let meta = String::from_utf8_lossy(&meta_output.stdout).trim().to_string();

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
                        println!("    {symbol} {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
                ChangeKind::Deleted => {
                    if let Some(p) = &c.old_pointer {
                        println!("    {symbol} {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
                ChangeKind::RenamedAdded | ChangeKind::Copied => {
                    let old = c.old_path.as_deref().unwrap_or("?");
                    if let Some(p) = &c.new_pointer {
                        println!("    {symbol} {old} -> {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
                    }
                }
                ChangeKind::RenamedDeleted => {
                    let old = c.old_path.as_deref().unwrap_or("?");
                    if let Some(p) = &c.old_pointer {
                        println!("    {symbol} {old} -> {}  {}:{}", c.path, p.hash_fn, short_hash(&p.hexdigest));
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

    drop(cat_file);

    if !found_any {
        eprintln!("No bigstore file changes found in history.");
    }

    Ok(())
}

// ──────────────────────────────────────────────────
// git cat-file --batch wrapper
// ──────────────────────────────────────────────────
//
// Single long-lived process for all blob reads during log.
// Protocol: write "<ref>\n" to stdin, read response from stdout.
// Response is either:
//   <sha> blob <size>\n<content>\n   (object found)
//   <ref> missing\n                  (object not found)

struct CatFileBatch {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
}

impl CatFileBatch {
    fn start() -> Result<Self> {
        let mut child = Command::new("git")
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to start git cat-file --batch")?;

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = BufReader::new(child.stdout.take().expect("stdout was piped"));

        Ok(Self { child, stdin: Some(stdin), stdout })
    }

    /// Read a blob and try to parse it as a bigstore pointer.
    /// Returns Ok(None) if the blob doesn't exist or isn't a pointer.
    fn read_pointer(&mut self, rev: &str, path: &str) -> Result<Option<types::Pointer>> {
        let stdin = self.stdin.as_mut().context("cat-file already closed")?;
        let ref_spec = format!("{rev}:{path}\n");
        stdin.write_all(ref_spec.as_bytes())?;
        stdin.flush()?;

        // Read header line: "<sha> <type> <size>\n" or "<ref> missing\n"
        let mut header = String::new();
        self.stdout.read_line(&mut header)?;

        if header.trim_end().ends_with("missing") {
            return Ok(None);
        }

        let size: usize = header
            .trim_end()
            .rsplit_once(' ')
            .and_then(|(_, s)| s.parse().ok())
            .context("failed to parse cat-file header")?;

        // Read content + trailing newline.
        // Pointers are ~81 bytes. For tracked files, blobs are always pointers
        // (clean filter ensures this), so size is always small.
        let mut buf = vec![0u8; size + 1]; // +1 for trailing LF
        std::io::Read::read_exact(&mut self.stdout, &mut buf)?;
        buf.truncate(size); // drop trailing LF

        Ok(types::Pointer::parse(&buf).ok().flatten())
    }
}

impl Drop for CatFileBatch {
    fn drop(&mut self) {
        // Close stdin so cat-file sees EOF and exits
        self.stdin.take();
        let _ = self.child.wait();
    }
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

fn cmd_ref(source: &str, dest: &str) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;

    // Reject paths that escape the repository
    validate_relative_path("source", source)?;
    validate_relative_path("dest", dest)?;

    let source_path = repo_root.join(source);
    let (pointer, dvc_out_path) = dvc::parse_dvc_pointer(&source_path)?;

    // Try to import the object from DVC cache into bigstore cache
    match cache::import_md5_from_dvc_cache(&repo_root, &git_dir, &pointer.hexdigest)? {
        cache::DvcImportResult::Imported => {
            eprintln!("Imported from DVC cache (verified): {dvc_out_path}");
        }
        cache::DvcImportResult::AlreadyCached => {
            eprintln!("Already in bigstore cache: {dvc_out_path}");
        }
        cache::DvcImportResult::NotInDvcCache => {
            anyhow::bail!(
                "object not found in DVC cache at {}\n\
                 Run `dvc pull {source}` first to populate the DVC cache, then retry.",
                cache::dvc_cache_path(&repo_root, &pointer.hexdigest).display()
            );
        }
    }

    // Write the pointer file
    let dest_path = repo_root.join(dest);
    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dest_path, pointer.encode())?;

    // Restore content from cache so working tree has real data (not pointer text).
    // The clean filter will convert back to pointer on `git add`.
    let cache_path = cache::object_path(&git_dir, &pointer.hexdigest, pointer.hash_fn);
    if cache_path.exists() {
        cache::copy_to_working_tree(&cache_path, &dest_path)?;
        eprintln!("Created: {dest} (content restored from cache)");
    } else {
        eprintln!("Created pointer: {dest} (run `git bigstore pull` to restore content)");
    }

    eprintln!("  Source: {source} (md5:{})", pointer.hexdigest);
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. Ensure {dest} is tracked: echo '{dest} filter=bigstore' >> .gitattributes");
    eprintln!("  2. git add {dest} .gitattributes");
    eprintln!("  3. git commit -m 'add {dest}'");
    eprintln!("  4. git bigstore push");

    Ok(())
}

fn cmd_dvc_ls(source: &str) -> Result<()> {
    validate_relative_path("source", source)?;
    let repo_root = git::repo_root()?;
    let source_path = repo_root.join(source);
    let (manifest_hash, entries) = resolve_dir_manifest(&repo_root, &source_path)?;

    eprintln!(
        "{} entries in {} (manifest md5:{manifest_hash})",
        entries.len(),
        source,
    );
    eprintln!();
    for entry in &entries {
        println!("  {}  {}", entry.md5, entry.relpath);
    }

    Ok(())
}

fn cmd_import_dvc_dir(
    source: &str,
    dest_root: &str,
    patterns: &[String],
    force: bool,
) -> Result<()> {
    validate_relative_path("source", source)?;
    validate_relative_path("dest_root", dest_root)?;
    let repo_root = git::repo_root()?;
    let git_dir = git::git_dir()?;

    let source_path = repo_root.join(source);
    let (_manifest_hash, entries) = resolve_dir_manifest(&repo_root, &source_path)?;

    // Filter entries by patterns (if any)
    let entries = if patterns.is_empty() {
        entries
    } else {
        let matchers: Vec<_> = patterns
            .iter()
            .map(|p| {
                Glob::new(p)
                    .with_context(|| format!("invalid glob pattern: {p:?}"))
                    .map(|g| g.compile_matcher())
            })
            .collect::<Result<_>>()?;
        entries
            .into_iter()
            .filter(|e| matchers.iter().any(|m| m.is_match(&e.relpath)))
            .collect()
    };

    if entries.is_empty() {
        eprintln!("No matching entries to import.");
        return Ok(());
    }

    // Pre-check: fail if any destination exists (unless --force)
    if !force {
        let mut conflicts = Vec::new();
        for entry in &entries {
            let dest = repo_root.join(dest_root).join(&entry.relpath);
            if dest.exists() {
                conflicts.push(entry.relpath.clone());
            }
        }
        if !conflicts.is_empty() {
            eprintln!("Destination files already exist (use --force to overwrite):");
            for c in &conflicts {
                eprintln!("  {dest_root}/{c}");
            }
            anyhow::bail!(
                "{} destination file(s) already exist",
                conflicts.len()
            );
        }
    }

    // Import each entry
    let mut imported = 0u64;
    let mut cached = 0u64;
    let mut failed: Vec<(String, String)> = Vec::new();

    for entry in &entries {
        let hexdigest = &entry.md5;
        let relpath = &entry.relpath;
        let dest_path = repo_root.join(dest_root).join(relpath);

        // Import from DVC cache into bigstore cache
        match cache::import_md5_from_dvc_cache(&repo_root, &git_dir, hexdigest) {
            Ok(cache::DvcImportResult::Imported) => imported += 1,
            Ok(cache::DvcImportResult::AlreadyCached) => cached += 1,
            Ok(cache::DvcImportResult::NotInDvcCache) => {
                failed.push((
                    relpath.clone(),
                    format!(
                        "not found in DVC cache at {}",
                        cache::dvc_cache_path(&repo_root, hexdigest).display()
                    ),
                ));
                continue;
            }
            Err(e) => {
                failed.push((relpath.clone(), format!("{e:#}")));
                continue;
            }
        }

        // Write pointer file, then restore content from cache
        let pointer = types::Pointer::new(types::HashFunction::Md5, hexdigest.clone());
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest_path, pointer.encode())?;

        // Restore real content so working tree has data, not pointer text.
        // The clean filter will convert back to pointer on `git add`.
        let cache_path =
            cache::object_path(&git_dir, hexdigest, types::HashFunction::Md5);
        if cache_path.exists() {
            cache::copy_to_working_tree(&cache_path, &dest_path)?;
        }
    }

    // Summary
    let total = imported + cached;
    eprintln!();
    if imported > 0 {
        eprintln!("{imported} file(s) imported from DVC cache (verified)");
    }
    if cached > 0 {
        eprintln!("{cached} file(s) already in bigstore cache");
    }
    eprintln!("{total} pointer(s) written under {dest_root}/");

    if !failed.is_empty() {
        eprintln!();
        for (path, err) in &failed {
            eprintln!("FAILED: {path} — {err}");
        }
        anyhow::bail!("{} of {} entries failed", failed.len(), failed.len() + total as usize);
    }

    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. echo '{dest_root}/** filter=bigstore' >> .gitattributes");
    eprintln!("  2. git add {dest_root}/ .gitattributes");
    eprintln!("  3. git commit -m 'import {dest_root} from DVC'");
    eprintln!("  4. git bigstore push");

    Ok(())
}

/// Parse a .dvc file as a .dir type and load its manifest entries from the DVC cache.
fn resolve_dir_manifest(
    repo_root: &Path,
    source_path: &Path,
) -> Result<(String, Vec<dvc::DirEntry>)> {
    let kind = dvc::parse_dvc_file(source_path)?;
    let (manifest_hash, _output_path) = match kind {
        dvc::DvcKind::Dir {
            manifest_hash,
            output_path,
        } => (manifest_hash, output_path),
        dvc::DvcKind::File(..) => {
            anyhow::bail!(
                "{} is a single-file .dvc — use `git bigstore ref` instead",
                source_path.display()
            );
        }
    };

    // Find the manifest in DVC cache
    let manifest_digest = types::Hexdigest::new(&manifest_hash, types::HashFunction::Md5)?;
    let manifest_path = cache::dvc_cache_path(repo_root, &manifest_digest);

    // Also try with .dir suffix (some DVC versions store it this way)
    let manifest_path_dir = manifest_path.with_extension("dir");

    let actual_path = if manifest_path.exists() {
        manifest_path
    } else if manifest_path_dir.exists() {
        manifest_path_dir
    } else {
        anyhow::bail!(
            "DVC .dir manifest not found in cache.\n\
             Expected at: {}\n\
             Run `dvc pull {}` first to populate the DVC cache.",
            manifest_path.display(),
            source_path.display()
        );
    };

    let entries = dvc::parse_dir_manifest(&actual_path)?;
    Ok((manifest_hash, entries))
}

/// Reject absolute paths and path traversal.
fn validate_relative_path(label: &str, p: &str) -> Result<()> {
    let path = Path::new(p);
    anyhow::ensure!(!path.is_absolute(), "{label} must be a relative path: {p:?}");
    anyhow::ensure!(
        !path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir)),
        "{label} must not contain '..': {p:?}"
    );
    Ok(())
}

/// Resolve concurrency: --jobs flag > BIGSTORE_JOBS env > default (8).
fn resolve_jobs(flag: Option<usize>) -> Result<usize> {
    let jobs = match flag {
        Some(n) => n,
        None => match std::env::var("BIGSTORE_JOBS") {
            Ok(s) => s
                .parse::<usize>()
                .context("BIGSTORE_JOBS must be a positive integer")?,
            Err(_) => transfer::DEFAULT_CONCURRENCY,
        },
    };
    anyhow::ensure!(jobs >= 1, "--jobs must be at least 1");
    Ok(jobs)
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
