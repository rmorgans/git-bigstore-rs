use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use globset::Glob;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use bigstore::backend;
use bigstore::cache::{self, DvcImportResult};
use bigstore::config::BigstoreConfig;
use bigstore::filter::{self, WorktreeFile};
use bigstore::git::{self, IndexBlob};
use bigstore::transfer::{self, Remote};
use bigstore::types::{self, Hexdigest, RepoPath};
use bigstore::{dvc, hash};

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
        /// Only push files matching these patterns (relative to the repository root)
        patterns: Vec<String>,

        /// Number of concurrent transfers (default: 8, env: BIGSTORE_JOBS)
        #[arg(short, long)]
        jobs: Option<NonZeroUsize>,
    },

    /// Download objects from remote storage (with integrity verification)
    Pull {
        /// Only pull files matching these patterns (relative to the repository root)
        patterns: Vec<String>,

        /// Number of concurrent transfers (default: 8, env: BIGSTORE_JOBS)
        #[arg(short, long)]
        jobs: Option<NonZeroUsize>,
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

    /// Create a bigstore file from a single-file .dvc file
    Ref {
        /// Path to .dvc file (relative to the repository root)
        #[arg(value_parser = RepoPath::new)]
        source: RepoPath,
        /// Destination path (relative to the repository root)
        #[arg(value_parser = RepoPath::new)]
        dest: RepoPath,
    },

    /// List files in a DVC .dir manifest
    #[command(name = "dvc-ls")]
    DvcLs {
        /// Path to .dvc file (must be a .dir type)
        #[arg(value_parser = RepoPath::new)]
        source: RepoPath,
    },

    /// Import files from a DVC .dir manifest into bigstore
    #[command(name = "import-dvc-dir")]
    ImportDvcDir {
        /// Path to .dvc file (must be a .dir type)
        #[arg(value_parser = RepoPath::new)]
        source: RepoPath,

        /// Destination root directory (relative to the repository root)
        #[arg(value_parser = RepoPath::new)]
        dest_root: RepoPath,

        /// Only import files matching these glob patterns (default: all)
        patterns: Vec<String>,

        /// Overwrite existing destination files
        #[arg(long)]
        force: bool,
    },

    /// Back up plain folders in DVC 3's format, without git
    #[command(subcommand)]
    Folder(FolderCommand),

    /// Internal: clean filter (stdin -> stdout)
    #[command(name = "filter-clean", hide = true)]
    FilterClean,

    /// Internal: smudge filter (stdin -> stdout)
    #[command(name = "filter-smudge", hide = true)]
    FilterSmudge,

    /// Internal: Git LFS custom transfer adapter (stdin/stdout JSON protocol)
    #[command(name = "lfs-adapter", hide = true)]
    LfsAdapter,
}

/// Remote options shared by every `folder` command.
#[derive(clap::Args)]
struct RemoteArgs {
    /// Remote URL: s3://bucket/prefix, local:///path or rclone://remote:path
    #[arg(long, env = "BIGSTORE_FOLDER_REMOTE")]
    remote: String,
    /// S3 endpoint (required for s3://; also read from AWS_ENDPOINT_URL)
    #[arg(long)]
    endpoint: Option<String>,
    /// S3 region (also read from AWS_REGION)
    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,
}

#[derive(Subcommand)]
enum FolderCommand {
    /// Back up a directory or file; writes <name>.dvc beside it
    Push {
        /// Directory or file to back up
        path: PathBuf,
        /// History key, e.g. <survey>/<dataset>/<output path>
        #[arg(long)]
        history: String,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(short, long)]
        jobs: Option<NonZeroUsize>,
    },
    /// Restore from a .dvc file, or from history with --history
    Pull {
        /// The .dvc file to restore (omit with --history)
        pointer: Option<PathBuf>,
        /// Restore a version of this history key instead of a .dvc file
        #[arg(long, conflicts_with = "pointer", requires = "into")]
        history: Option<String>,
        /// Version: latest, an id prefix (8+ hex), or a time (RFC 3339)
        #[arg(long, default_value = "latest", requires = "history")]
        at: String,
        /// Where to restore (default: beside the .dvc file)
        #[arg(long)]
        into: Option<PathBuf>,
        /// Replace local files that differ
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(short, long)]
        jobs: Option<NonZeroUsize>,
    },
    /// List the versions of a history key
    Log {
        history: String,
        #[command(flatten)]
        remote: RemoteArgs,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    match cli.command {
        Commands::Init { url, endpoint } => cmd_init(&url, endpoint.as_deref()),
        Commands::Push { patterns, jobs } => cmd_push(&patterns, jobs),
        Commands::Pull { patterns, jobs } => cmd_pull(&patterns, jobs),
        Commands::Status { verify } => cmd_status(verify),
        Commands::MigrateConfig { force } => cmd_migrate_config(force),
        Commands::Log { paths } => bigstore::log::run(&paths),
        Commands::Ref { source, dest } => cmd_ref(&source, &dest),
        Commands::DvcLs { source } => cmd_dvc_ls(&source),
        Commands::ImportDvcDir {
            source,
            dest_root,
            patterns,
            force,
        } => cmd_import_dvc_dir(&source, &dest_root, &patterns, force),
        Commands::Folder(cmd) => cmd_folder(cmd),
        Commands::FilterClean => filter::clean(),
        Commands::FilterSmudge => filter::smudge(),
        Commands::LfsAdapter => bigstore::lfs_adapter::run(),
    }
}

/// Only push and pull do async I/O; everything else stays synchronous and
/// never pays for a runtime (the filters run once per file).
fn block_on<F: Future>(fut: F) -> Result<F::Output> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start async runtime")?;
    Ok(rt.block_on(fut))
}

fn cmd_init(url: &str, endpoint: Option<&str>) -> Result<()> {
    let git_dir = git::common_dir()?;
    let repo_root = git::repo_root()?;

    let cfg = BigstoreConfig::from_url(url, endpoint)?;
    cfg.save(&repo_root.join(".bigstore.toml"))?;

    // Read existing filter config as a unit — detects partial/broken state
    let existing = git::FilterConfig::load()?;
    if existing.is_none() {
        git::FilterConfig::default_commands().save()?;
    }

    cache::ensure_cache_dir(&git_dir)?;

    eprintln!("Initialized bigstore with backend: {}", cfg.backend_type());
    eprintln!("Config written to .bigstore.toml");
    if existing.is_some() {
        eprintln!("Filter config preserved (already configured)");
    }
    eprintln!();
    eprintln!("Add patterns to .gitattributes:");
    eprintln!("  echo '*.bin filter=bigstore' >> .gitattributes");
    eprintln!("  echo 'assets/** filter=bigstore' >> .gitattributes");

    Ok(())
}

fn cmd_push(patterns: &[String], jobs: Option<NonZeroUsize>) -> Result<()> {
    let jobs = resolve_jobs(jobs)?;
    let repo_root = git::repo_root()?;
    let git_dir = git::common_dir()?;
    let cfg = BigstoreConfig::find_and_load(&repo_root)?;
    let entries = git::bigstore_entries(&repo_root, patterns)?;
    let objects = transfer::objects(&entries);
    let store = backend::from_config(&cfg)?;
    let remote = Remote {
        store: &store,
        cfg: &cfg,
        git_dir: &git_dir,
    };

    let report = block_on(transfer::push(&remote, &objects, jobs.get()))?;
    report.print();
    anyhow::ensure!(
        report.failed.is_empty(),
        "{} file(s) failed",
        report.failed.iter().map(|f| f.paths.len()).sum::<usize>()
    );
    Ok(())
}

fn cmd_pull(patterns: &[String], jobs: Option<NonZeroUsize>) -> Result<()> {
    let jobs = resolve_jobs(jobs)?;
    let repo_root = git::repo_root()?;
    let git_dir = git::common_dir()?;
    let cfg = BigstoreConfig::find_and_load(&repo_root)?;

    // Checkout goes through the smudge filter, so a fresh clone needs it.
    if git::FilterConfig::load()?.is_none() {
        git::FilterConfig::default_commands().save()?;
        eprintln!("Configured bigstore git filters for this clone");
    }

    let entries = git::bigstore_entries(&repo_root, patterns)?;
    let objects = transfer::objects(entries.iter().filter(|e| !e.skip_worktree));
    let dvc_cache_root = match cache::find_dvc_project_root(&repo_root) {
        Some(dvc_root) => cache::resolve_dvc_cache_root(&dvc_root)?,
        None => repo_root.join(".dvc/cache"),
    };
    let store = backend::from_config(&cfg)?;
    let remote = Remote {
        store: &store,
        cfg: &cfg,
        git_dir: &git_dir,
    };

    let report = block_on(transfer::pull(
        &remote,
        &dvc_cache_root,
        &objects,
        jobs.get(),
    ))?;
    report.print();
    // Check out whatever did arrive, even if some objects failed.
    let journal = git::worktree_git_dir()?.join("bigstore-pull-journal");
    let checkout = transfer::checkout(&repo_root, &git_dir, &journal, &entries)?;
    if checkout.recovered > 0 {
        eprintln!(
            "{} file(s) left by an interrupted pull recovered",
            checkout.recovered
        );
    }
    if checkout.checked_out > 0 {
        eprintln!("{} file(s) checked out", checkout.checked_out);
    }
    for f in &checkout.failed {
        let paths: Vec<&str> = f.paths.iter().map(RepoPath::as_str).collect();
        eprintln!("FAILED checkout: {} — {}", paths.join(", "), f.error);
    }
    let failed = report.failed.iter().chain(&checkout.failed);
    let failed_files: usize = failed.map(|f| f.paths.len()).sum();
    anyhow::ensure!(failed_files == 0, "{failed_files} file(s) failed");
    Ok(())
}

fn cmd_status(verify: bool) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::common_dir()?;
    BigstoreConfig::find_and_load(&repo_root)?;

    let mut corrupted: Vec<(RepoPath, PathBuf)> = Vec::new();
    for entry in git::bigstore_entries(&repo_root, &[])? {
        let IndexBlob::Pointer(pointer) = &entry.blob else {
            println!(
                "{:>40}  {}",
                "not a pointer in git (git add --renormalize)", entry.path
            );
            continue;
        };
        let cache_path = cache::object_path(&git_dir, pointer.hexdigest());
        let cached = cache_path.is_file();
        if entry.skip_worktree {
            let label = if cached {
                "outside sparse checkout (cached)"
            } else {
                "outside sparse checkout"
            };
            println!("{label:>40}  {}", entry.path);
            continue;
        }
        let worktree = filter::worktree_file(&entry.path.to_fs_path(&repo_root))?;

        let verified = if verify && cached {
            match hash::hash_file(&cache_path, pointer.hash_fn()) {
                Ok(actual) if actual == *pointer.hexdigest() => true,
                Ok(_) => {
                    println!("{:>40}  {}", "CORRUPTED (hash mismatch)", entry.path);
                    corrupted.push((entry.path, cache_path));
                    continue;
                }
                Err(_) => {
                    println!("{:>40}  {}", "CORRUPTED (unreadable)", entry.path);
                    corrupted.push((entry.path, cache_path));
                    continue;
                }
            }
        } else {
            false
        };
        let v = if verified { ", verified" } else { "" };

        let label = match (&worktree, cached) {
            (WorktreeFile::Missing, _) => "missing from working tree".to_string(),
            (WorktreeFile::Content, true) if verified => "ok (verified)".to_string(),
            (WorktreeFile::Content, true) => "ok".to_string(),
            (WorktreeFile::Content, false) => "local only (not cached)".to_string(),
            (WorktreeFile::Pointer(p), true) if p == pointer => {
                format!("cached (not checked out{v})")
            }
            (WorktreeFile::Pointer(p), false) if p == pointer => {
                "pointer only (needs pull)".to_string()
            }
            (WorktreeFile::Pointer(_), _) => "pointer differs from git".to_string(),
        };
        println!("{label:>40}  {}", entry.path);
    }

    if !corrupted.is_empty() {
        eprintln!();
        eprintln!(
            "{} corrupted cache object(s) found. Delete them and re-pull:",
            corrupted.len()
        );
        for (path, object) in &corrupted {
            eprintln!("  rm {}    # {path}", object.display());
        }
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
        anyhow::bail!(".bigstore.toml already exists. Use --force to overwrite.");
    }

    // Load from legacy, save as toml (validates + normalizes)
    let cfg = BigstoreConfig::load(&legacy_path)?;
    cfg.save(&toml_path)?;

    eprintln!("Migrated .bigstore -> .bigstore.toml");
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  git add .bigstore.toml");
    eprintln!("  git rm .bigstore        # remove the old config");
    eprintln!("  git commit -m 'migrate bigstore config to toml'");

    Ok(())
}

fn cmd_ref(source: &RepoPath, dest: &RepoPath) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::common_dir()?;

    let source_path = source.to_fs_path(&repo_root);
    let dvc::DvcPointer {
        output: dvc::DvcOutput::File { md5, .. },
        path: dvc_out_path,
    } = dvc::DvcPointer::load(&source_path)?
    else {
        anyhow::bail!("{source} is a .dir .dvc file — use `git bigstore import-dvc-dir` instead");
    };
    let pointer = types::Pointer::new(md5);
    let dvc_cache_root = resolve_dvc_cache(&repo_root, &source_path)?;

    match cache::import_from_dvc_cache(&dvc_cache_root, &git_dir, pointer.hexdigest())? {
        DvcImportResult::Imported => {
            eprintln!("Imported from DVC cache (verified): {dvc_out_path}");
        }
        DvcImportResult::AlreadyCached => {
            eprintln!("Already in bigstore cache: {dvc_out_path}");
        }
        DvcImportResult::NotInDvcCache => {
            anyhow::bail!(
                "object not found in DVC cache at {}\n\
                 Run `dvc pull {source}` first to populate the DVC cache, then retry.",
                cache::dvc_cache_path(&dvc_cache_root, pointer.hexdigest()).display()
            );
        }
    }

    // Write the real content; the clean filter turns it into the pointer on `git add`.
    let cache_path = cache::object_path(&git_dir, pointer.hexdigest());
    cache::copy_to_worktree(&cache_path, &dest.to_fs_path(&repo_root))?;

    eprintln!("Created: {dest} (content restored from cache)");
    eprintln!("  Source: {source} (md5:{})", pointer.hexdigest());
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. Ensure {dest} is tracked: echo '{dest} filter=bigstore' >> .gitattributes");
    eprintln!("  2. git add {dest} .gitattributes");
    eprintln!("  3. git commit -m 'add {dest}'");
    eprintln!("  4. git bigstore push");

    Ok(())
}

fn cmd_dvc_ls(source: &RepoPath) -> Result<()> {
    let repo_root = git::repo_root()?;
    let source_path = source.to_fs_path(&repo_root);
    let dvc_cache_root = resolve_dvc_cache(&repo_root, &source_path)?;
    let (manifest, entries) = resolve_dir_manifest(&dvc_cache_root, &source_path)?;

    eprintln!(
        "{} entries in {source} (manifest md5:{manifest})",
        entries.len()
    );
    eprintln!();
    for entry in &entries {
        println!("  {}  {}", entry.md5, entry.relpath);
    }

    Ok(())
}

fn cmd_import_dvc_dir(
    source: &RepoPath,
    dest_root: &RepoPath,
    patterns: &[String],
    force: bool,
) -> Result<()> {
    let repo_root = git::repo_root()?;
    let git_dir = git::common_dir()?;

    let source_path = source.to_fs_path(&repo_root);
    let dvc_cache_root = resolve_dvc_cache(&repo_root, &source_path)?;
    let (_manifest, entries) = resolve_dir_manifest(&dvc_cache_root, &source_path)?;

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
            .filter(|e| matchers.iter().any(|m| m.is_match(e.relpath.as_str())))
            .collect()
    };

    if entries.is_empty() {
        eprintln!("No matching entries to import.");
        return Ok(());
    }

    // Pre-check: fail if any destination exists (unless --force)
    if !force {
        let conflicts: Vec<RepoPath> = entries
            .iter()
            .map(|e| dest_root.join(&e.relpath))
            .filter(|dest| dest.to_fs_path(&repo_root).exists())
            .collect();
        if !conflicts.is_empty() {
            eprintln!("Destination files already exist (use --force to overwrite):");
            for c in &conflicts {
                eprintln!("  {c}");
            }
            anyhow::bail!("{} destination file(s) already exist", conflicts.len());
        }
    }

    let mut imported = 0u64;
    let mut cached = 0u64;
    let mut failed: Vec<(&RepoPath, String)> = Vec::new();

    for entry in &entries {
        match cache::import_from_dvc_cache(&dvc_cache_root, &git_dir, &entry.md5) {
            Ok(DvcImportResult::Imported) => imported += 1,
            Ok(DvcImportResult::AlreadyCached) => cached += 1,
            Ok(DvcImportResult::NotInDvcCache) => {
                failed.push((
                    &entry.relpath,
                    format!(
                        "not found in DVC cache at {}",
                        cache::dvc_cache_path(&dvc_cache_root, &entry.md5).display()
                    ),
                ));
                continue;
            }
            Err(e) => {
                failed.push((&entry.relpath, format!("{e:#}")));
                continue;
            }
        }

        // Write the real content; the clean filter turns it into a pointer on `git add`.
        let dest = dest_root.join(&entry.relpath).to_fs_path(&repo_root);
        cache::copy_to_worktree(&cache::object_path(&git_dir, &entry.md5), &dest)?;
    }

    let total = imported + cached;
    eprintln!();
    if imported > 0 {
        eprintln!("{imported} file(s) imported from DVC cache (verified)");
    }
    if cached > 0 {
        eprintln!("{cached} file(s) already in bigstore cache");
    }
    eprintln!("{total} file(s) written under {dest_root}/");

    if !failed.is_empty() {
        eprintln!();
        for (path, err) in &failed {
            eprintln!("FAILED: {path} — {err}");
        }
        anyhow::bail!(
            "{} of {} entries failed",
            failed.len(),
            failed.len() + total as usize
        );
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
    dvc_cache_root: &Path,
    source_path: &Path,
) -> Result<(Hexdigest, Vec<dvc::ManifestEntry>)> {
    let dvc::DvcOutput::Dir { manifest, .. } = dvc::DvcPointer::load(source_path)?.output else {
        anyhow::bail!(
            "{} is a single-file .dvc — use `git bigstore ref` instead",
            source_path.display()
        );
    };

    // DVC stores the manifest either bare or with a .dir suffix.
    let manifest_path = cache::dvc_cache_path(dvc_cache_root, &manifest);
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
    Ok((manifest, entries))
}

/// Find the DVC project root from a .dvc source file and resolve its cache directory.
/// If no DVC project exists (no `.dvc/` directory), falls back to repo-local `.dvc/cache`.
fn resolve_dvc_cache(repo_root: &Path, source_path: &Path) -> Result<PathBuf> {
    match cache::find_dvc_project_root(source_path) {
        Some(dvc_root) => cache::resolve_dvc_cache_root(&dvc_root),
        None => Ok(repo_root.join(".dvc/cache")),
    }
}

fn open_folder_remote(args: &RemoteArgs) -> Result<bigstore::folder::Remote> {
    use bigstore::folder::{Credentials, Remote, RemoteConfig};
    Remote::open(&RemoteConfig {
        url: args.remote.clone(),
        endpoint: args
            .endpoint
            .clone()
            .or_else(backend::store::env_s3_endpoint),
        region: args.region.clone(),
        credentials: Credentials::FromEnv,
    })
}

fn cmd_folder(cmd: FolderCommand) -> Result<()> {
    use bigstore::folder::{self, HistoryKey, Overwrite, PointerSource, Selector};
    match cmd {
        FolderCommand::Push {
            path,
            history,
            remote,
            jobs,
        } => {
            let remote = open_folder_remote(&remote)?;
            let r = folder::push(
                &remote,
                &path,
                &folder::PushOptions {
                    history: HistoryKey::new(&history)?,
                    jobs: resolve_jobs(jobs)?.get(),
                },
            )?;
            for w in &r.warnings {
                eprintln!("warning: {w}");
            }
            if r.empty_dirs > 0 {
                eprintln!("{} empty dir(s) not recorded (DVC cannot)", r.empty_dirs);
            }
            eprintln!(
                "{} file(s): {} uploaded, {} already on the remote; wrote {}{}",
                r.files,
                r.uploaded,
                r.already_present,
                r.pointer_path.display(),
                if r.history_record.is_some() {
                    ""
                } else {
                    " (unchanged since the last version)"
                }
            );
        }
        FolderCommand::Pull {
            pointer,
            history,
            at,
            into,
            force,
            remote,
            jobs,
        } => {
            let remote = open_folder_remote(&remote)?;
            let source = match (pointer, history) {
                (Some(p), None) => PointerSource::File(p),
                (None, Some(key)) => PointerSource::History {
                    key: HistoryKey::new(&key)?,
                    at: match at.as_str() {
                        "latest" => Selector::Latest,
                        s if s.len() >= 8 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                            Selector::Id(s.to_string())
                        }
                        s => Selector::AtOrBefore(s.to_string()),
                    },
                },
                _ => anyhow::bail!("give a .dvc file or --history"),
            };
            let r = folder::pull(
                &remote,
                &source,
                &folder::PullOptions {
                    into,
                    overwrite: if force {
                        Overwrite::Force
                    } else {
                        Overwrite::Refuse
                    },
                    jobs: resolve_jobs(jobs)?.get(),
                },
            )?;
            eprintln!(
                "{} file(s) written, {} already up to date, {} local file(s) not in this version (kept)",
                r.written, r.unchanged, r.extra_local
            );
        }
        FolderCommand::Log { history, remote } => {
            let remote = open_folder_remote(&remote)?;
            for r in folder::log(&remote, &HistoryKey::new(&history)?)? {
                let (kind, detail) = match &r.pointer.output {
                    dvc::DvcOutput::Dir { size, nfiles, .. } => {
                        ("dir", format!("{nfiles} files, {size} bytes"))
                    }
                    dvc::DvcOutput::File { size, .. } => ("file", format!("{size} bytes")),
                };
                println!(
                    "{}  {}  {kind}  {detail}",
                    r.time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    r.id()
                );
            }
        }
    }
    Ok(())
}

/// Resolve concurrency: --jobs flag > BIGSTORE_JOBS env > default (8).
fn resolve_jobs(flag: Option<NonZeroUsize>) -> Result<NonZeroUsize> {
    if let Some(n) = flag {
        return Ok(n);
    }
    match std::env::var("BIGSTORE_JOBS") {
        Ok(s) => s
            .parse()
            .context("BIGSTORE_JOBS must be a positive integer"),
        Err(_) => Ok(NonZeroUsize::new(transfer::DEFAULT_CONCURRENCY)
            .expect("default concurrency is non-zero")),
    }
}
