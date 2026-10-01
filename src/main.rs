use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use globset::Glob;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use bigstore::backend;
use bigstore::cache::{self, DvcImportResult, WorktreeMode};
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
        #[arg(value_parser = RepoPath::new_to_create)]
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
        #[arg(value_parser = RepoPath::new_to_create)]
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

    /// Internal: clean filter (stdin -> stdout); git passes the path as `%f`
    #[command(name = "filter-clean", hide = true, disable_help_flag = true)]
    FilterClean {
        /// The file's path relative to the repository root
        #[arg(allow_hyphen_values = true)]
        path: Option<std::ffi::OsString>,
    },

    /// Internal: smudge filter (stdin -> stdout)
    #[command(name = "filter-smudge", hide = true)]
    FilterSmudge,

    /// Internal: git's long-running filter process (pkt-line on stdin/stdout)
    #[command(name = "filter-process", hide = true)]
    FilterProcess,

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

/// What `folder push` backs up, and how; `folder status` takes the same.
#[derive(clap::Args)]
struct FolderPushArgs {
    /// Directory or file to back up
    path: PathBuf,
    /// History key, e.g. <survey>/<dataset>/<output path>
    #[arg(long)]
    history: String,
    #[command(flatten)]
    remote: RemoteArgs,
    #[arg(short, long)]
    jobs: Option<NonZeroUsize>,
    /// Also skip entries matching this .gitignore-style pattern, relative
    /// to the directory (repeatable). .DS_Store, ._*, Thumbs.db and
    /// desktop.ini are always skipped.
    #[arg(long, value_name = "PATTERN")]
    exclude: Vec<String>,
}

impl FolderPushArgs {
    fn open(&self) -> Result<(bigstore::folder::Remote, bigstore::folder::PushOptions)> {
        use bigstore::folder::{Excludes, HistoryKey, PushOptions};
        let opts = PushOptions {
            jobs: resolve_jobs(self.jobs)?.get(),
            exclude: Excludes::new(&self.exclude)?,
            cancel: cancel_on_ctrl_c()?,
            progress: progress_bars(),
            ..PushOptions::new(HistoryKey::new(&self.history)?)
        };
        Ok((open_folder_remote(&self.remote)?, opts))
    }
}

/// One bar on stderr per phase of a folder command, in bytes when the
/// phase's size is known and in files otherwise. Nothing is drawn when
/// stderr is not a terminal; the last bar is cleared when the options
/// holding it are dropped.
fn progress_bars() -> bigstore::folder::Progress {
    use bigstore::folder::{Phase, Progress, ProgressEvent};
    use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};
    // The bar, and whether it counts bytes (else files).
    let current = std::sync::Mutex::new(None::<(ProgressBar, bool)>);
    Progress::new(move |event| {
        let mut current = current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match event {
            ProgressEvent::Started {
                phase,
                files,
                bytes,
            } => {
                if let Some((done, _)) = current.take() {
                    done.finish_and_clear();
                }
                let label = match phase {
                    Phase::Hashing => "hashing",
                    Phase::Uploading => "uploading",
                    Phase::Downloading => "downloading",
                    _ => "working",
                };
                let (len, counts, by_bytes) = match bytes {
                    Some(b) => (b, "{bytes}/{total_bytes}", true),
                    None => (files, "{pos}/{len} files", false),
                };
                let style = ProgressStyle::with_template(&format!(
                    "{{prefix:>11}} [{{bar:30.cyan/blue}}] {counts}"
                ))
                .expect("progress template is valid")
                .progress_chars("#>-");
                let bar = ProgressBar::new(len)
                    .with_style(style)
                    .with_prefix(label)
                    .with_finish(ProgressFinish::AndClear);
                *current = Some((bar, by_bytes));
            }
            ProgressEvent::Advanced { files, bytes, .. } => {
                if let Some((bar, by_bytes)) = &*current {
                    bar.inc(if *by_bytes { bytes } else { files });
                }
            }
            _ => {}
        }
    })
}

/// A token the first Ctrl-C cancels, so a folder command stops cleanly
/// (a push publishes nothing); a second Ctrl-C exits at once.
fn cancel_on_ctrl_c() -> Result<bigstore::folder::CancelToken> {
    let token = bigstore::folder::CancelToken::new();
    let cancel = token.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to start the Ctrl-C watcher")?;
    std::thread::spawn(move || {
        rt.block_on(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("cancelling; Ctrl-C again to stop at once");
                cancel.cancel();
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            }
        })
    });
    Ok(token)
}

#[derive(Subcommand)]
enum FolderCommand {
    /// Back up a directory or file; writes <name>.dvc beside it
    Push {
        #[command(flatten)]
        args: FolderPushArgs,
        /// When history has forked: refuse, or merge every head into one
        /// version (the output's .dvc must name one of them as its base)
        #[arg(long, value_enum, default_value = "refuse")]
        resolve: ResolveArg,
    },
    /// Say what push would upload and whether the output is the latest
    /// version in its history, without writing anything
    Status(FolderPushArgs),
    /// Restore from a .dvc file, or from history with --history
    Pull {
        /// The .dvc file to restore (omit with --history)
        pointer: Option<PathBuf>,
        /// Restore a version of this history key instead of a .dvc file
        #[arg(long, conflicts_with = "pointer", requires = "into")]
        history: Option<String>,
        /// Version: latest, an id prefix as `folder log` prints it (8+ hex;
        /// a 0.2 version's content id too), or a time (RFC 3339). A .dvc
        /// naming it is written beside --into
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
    /// List the versions of a history key, oldest first, with the versions
    /// each follows; marks heads, forks and merges
    Log {
        history: String,
        #[command(flatten)]
        remote: RemoteArgs,
        #[arg(short, long)]
        jobs: Option<NonZeroUsize>,
    },
    /// List the history keys on the remote, optionally only those under PREFIX
    Keys {
        /// Only keys equal to or below this one, e.g. <survey>/<dataset>/annotations
        prefix: Option<String>,
        #[command(flatten)]
        remote: RemoteArgs,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum ResolveArg {
    Refuse,
    Merge,
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
        Commands::FilterClean { path } => filter::clean(path.as_deref()),
        Commands::FilterSmudge => filter::smudge(),
        Commands::FilterProcess => bigstore::filter_process::run(),
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
    let filters = git::ensure_filter_config()?;

    cache::ensure_cache_dir(&git_dir)?;

    eprintln!("Initialized bigstore with backend: {}", cfg.backend_type());
    eprintln!("Config written to .bigstore.toml");
    match filters {
        git::Ensured::Configured => {}
        git::Ensured::AddedProcess => eprintln!("Enabled the bigstore filter process"),
        git::Ensured::Unchanged => eprintln!("Filter config preserved (already configured)"),
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
    let store = backend::Store::open(&cfg)?;
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

    // Checkout goes through the filter, so a fresh clone needs it; legacy
    // config gains the (much faster) filter process.
    match git::ensure_filter_config()? {
        git::Ensured::Configured => eprintln!("Configured bigstore git filters for this clone"),
        git::Ensured::AddedProcess => {
            eprintln!("Enabled the bigstore filter process for this clone")
        }
        git::Ensured::Unchanged => {}
    }

    let entries = git::bigstore_entries(&repo_root, patterns)?;
    let objects = transfer::objects(entries.iter().filter(|e| !e.skip_worktree));
    let dvc_cache_root = match cache::find_dvc_project_root(&repo_root) {
        Some(dvc_root) => cache::resolve_dvc_cache_root(&dvc_root)?,
        None => repo_root.join(".dvc/cache"),
    };
    let store = backend::Store::open(&cfg)?;
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
    let dvc::LenientPointer {
        pointer:
            dvc::DvcPointer {
                output: dvc::DvcOutput::File { md5, .. },
                path: dvc_out_path,
                ..
            },
        isexec,
    } = dvc::DvcPointer::load_lenient(&source_path)?
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
    let mode = if isexec {
        WorktreeMode::Executable
    } else {
        WorktreeMode::Regular
    };
    cache::copy_to_worktree(&cache_path, &dest.to_fs_path(&repo_root), mode)?;

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

    // Refuse names this OS cannot write before writing anything.
    let dests: Vec<RepoPath> = entries
        .iter()
        .map(|e| Ok(dest_root.join(&e.relpath.to_repo_path()?)))
        .collect::<Result<_>>()?;

    // Pre-check: fail if any destination exists (unless --force)
    if !force {
        let conflicts: Vec<&RepoPath> = dests
            .iter()
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
    let mut failed: Vec<(&types::ManifestPath, String)> = Vec::new();

    for (entry, dest) in entries.iter().zip(&dests) {
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
        // A `.dir` manifest records no file modes, so DVC restores none.
        let dest = dest.to_fs_path(&repo_root);
        cache::copy_to_worktree(
            &cache::object_path(&git_dir, &entry.md5),
            &dest,
            WorktreeMode::Regular,
        )?;
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
    let dvc::DvcOutput::Dir { manifest, .. } =
        dvc::DvcPointer::load_lenient(source_path)?.pointer.output
    else {
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
        FolderCommand::Push { args, resolve } => {
            let r = {
                let (remote, opts) = args.open()?;
                let opts = folder::PushOptions {
                    resolve: match resolve {
                        ResolveArg::Refuse => folder::Resolve::Refuse,
                        ResolveArg::Merge => folder::Resolve::Merge,
                    },
                    ..opts
                };
                folder::push(&remote, &args.path, &opts)?
            };
            for w in &r.warnings {
                eprintln!("warning: {w}");
            }
            if r.empty_dirs > 0 {
                eprintln!("{} empty dir(s) not recorded (DVC cannot)", r.empty_dirs);
            }
            eprintln!(
                "{} file(s): {} uploaded, {} already on the remote; wrote {} ({} {})",
                r.files,
                r.uploaded,
                r.already_present,
                r.pointer_path.display(),
                if r.history_record.is_some() {
                    "new version"
                } else {
                    "already the latest version,"
                },
                r.version
            );
            if !r.forked_with.is_empty() {
                eprintln!(
                    "warning: history has forked: {} pushed from the same base meanwhile; \
                     reconcile, then push --resolve merge",
                    join_ids(&r.forked_with)
                );
            }
        }
        FolderCommand::Status(args) => {
            let s = {
                let (remote, opts) = args.open()?;
                folder::status(&remote, &args.path, &opts)?
            };
            for w in &s.warnings {
                eprintln!("warning: {w}");
            }
            if s.empty_dirs > 0 {
                eprintln!(
                    "{} empty dir(s) would not be recorded (DVC cannot)",
                    s.empty_dirs
                );
            }
            println!(
                "{} file(s): {} to upload ({} bytes), {} already on the remote ({} bytes)",
                s.files, s.to_upload, s.to_upload_bytes, s.already_present, s.already_present_bytes
            );
            let when = |r: &folder::HistoryRecord| {
                r.time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            };
            match &s.sync {
                folder::SyncState::NoHistory => println!("no version in history yet"),
                folder::SyncState::InSync => println!("in sync: this is the latest version"),
                folder::SyncState::LocalAhead => {
                    println!("changed since the latest version; push to record it")
                }
                folder::SyncState::RemoteAhead { latest } => println!(
                    "history has a newer version, {} pushed {}; pull to update",
                    latest.id,
                    when(latest)
                ),
                folder::SyncState::Stale { base, head } => println!(
                    "stale: changed locally, but the latest version, {} pushed {}, is not this \
                     output's base ({}); push would refuse: set changes aside, pull, redo them",
                    head.id,
                    when(head),
                    base.as_ref()
                        .map_or("none".to_string(), ToString::to_string)
                ),
                folder::SyncState::Diverged { heads } => println!(
                    "diverged: history has forked into versions {}; pull one with --at <id>, \
                     reconcile, then push --resolve merge",
                    join_ids(heads)
                ),
                _ => println!("unknown sync state"),
            }
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
                    cancel: cancel_on_ctrl_c()?,
                    progress: progress_bars(),
                },
            )?;
            eprintln!(
                "{} file(s) written, {} already up to date, {} local file(s) not in this version (kept)",
                r.written, r.unchanged, r.extra_local
            );
        }
        FolderCommand::Log {
            history,
            remote,
            jobs,
        } => {
            let remote = open_folder_remote(&remote)?;
            let key = HistoryKey::new(&history)?;
            let opts = folder::LogOptions {
                jobs: resolve_jobs(jobs)?.get(),
                cancel: cancel_on_ctrl_c()?,
            };
            let records = folder::log(&remote, &key, &opts)?;
            let mut children: std::collections::HashMap<&dvc::RecordId, usize> =
                std::collections::HashMap::new();
            for p in records.iter().flat_map(|r| &r.parents) {
                *children.entry(p).or_default() += 1;
            }
            let mut heads = Vec::new();
            for r in &records {
                let (kind, detail) = match &r.pointer.output {
                    dvc::DvcOutput::Dir { size, nfiles, .. } => {
                        ("dir", format!("{nfiles} files, {size} bytes"))
                    }
                    dvc::DvcOutput::File { size, .. } => ("file", format!("{size} bytes")),
                };
                let by = r
                    .writer
                    .as_ref()
                    .map_or(String::new(), |w| format!("  by {w}"));
                let from = match r.parents.as_slice() {
                    [] => "root".to_string(),
                    ps => join_ids(ps),
                };
                let mut marks = String::new();
                if r.parents.len() > 1 {
                    marks.push_str("  [merge]");
                }
                match children.get(&r.id) {
                    None => {
                        marks.push_str("  [head]");
                        heads.push(&r.id);
                    }
                    Some(&n) if n > 1 => marks.push_str(&format!("  [fork: {n} children]")),
                    Some(_) => {}
                }
                println!(
                    "{}  {}  {kind}  {detail}{by}  <- {from}{marks}",
                    r.time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    r.id
                );
            }
            if heads.len() > 1 {
                eprintln!(
                    "history has forked: {} heads; pull one with --at <id>, reconcile, then \
                     push --resolve merge",
                    heads.len()
                );
            }
        }
        FolderCommand::Keys { prefix, remote } => {
            let remote = open_folder_remote(&remote)?;
            let prefix = prefix.as_deref().map(HistoryKey::new).transpose()?;
            for key in folder::keys(&remote, prefix.as_ref())? {
                println!("{}", key.as_str());
            }
        }
    }
    Ok(())
}

/// Record ids, comma-separated.
fn join_ids<'a>(ids: impl IntoIterator<Item = &'a bigstore::dvc::RecordId>) -> String {
    ids.into_iter()
        .map(|id| id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
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
