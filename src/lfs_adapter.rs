//! Git LFS custom standalone transfer adapter for bigstore object storage.
//!
//! Lets Git LFS clients upload/download blobs from the same bucket/prefix
//! that bigstore uses for SHA-256 objects. Storage-layer bridge only —
//! no pointer-format bridging, no LFS API server.
//!
//! Git config:
//!   [lfs "customtransfer.bigstore"]
//!       path = git-bigstore
//!       args = lfs-adapter
//!   [lfs]
//!       standalonetransferagent = bigstore
//!
//! Config resolution:
//!   1. .bigstore.toml (if present)
//!   2. git config bigstore-lfs.url (fallback for LFS-only repos)
//!
//! Protocol: <https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md>.
//! Every per-object failure — malformed request, invalid oid, missing init,
//! storage or verification error — is reported as that object's `complete`
//! event with an `error`; only stdin/stdout failures end the adapter.

use crate::{backend, config, git, hash, types};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

// ──────────────────────────────────────────────────
// LFS custom transfer protocol types
// ──────────────────────────────────────────────────

/// A message from Git LFS. Fields LFS sends that bigstore does not use
/// (`remote`, `concurrent`, `action`, ...) are ignored.
#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum Request {
    Init { operation: Operation },
    Upload { oid: Oid, size: u64, path: PathBuf },
    Download { oid: Oid, size: u64 },
    Terminate,
}

/// Fallback view of a message that failed to parse as a [`Request`], so a
/// malformed transfer request can still be answered for its oid.
#[derive(Deserialize)]
struct Envelope {
    event: Option<String>,
    oid: Option<String>,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Upload,
    Download,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Upload => "upload",
            Self::Download => "download",
        })
    }
}

/// An LFS object id, validated as a SHA-256 digest when the request is
/// parsed. `raw` is echoed back verbatim so LFS can match the response.
#[derive(Deserialize)]
#[serde(try_from = "String")]
struct Oid {
    raw: String,
    digest: types::Hexdigest,
}

impl TryFrom<String> for Oid {
    type Error = String;

    fn try_from(raw: String) -> Result<Self, String> {
        match types::Hexdigest::new(&raw, types::HashFunction::Sha256) {
            Ok(digest) => Ok(Self { raw, digest }),
            Err(e) => Err(format!("LFS oid is not a SHA-256 hex digest: {e:#}")),
        }
    }
}

#[derive(Serialize)]
struct InitResponse {}

#[derive(Serialize)]
struct ProgressResponse<'a> {
    event: &'static str,
    oid: &'a str,
    #[serde(rename = "bytesSoFar")]
    bytes_so_far: u64,
    #[serde(rename = "bytesSinceLast")]
    bytes_since_last: u64,
}

#[derive(Serialize)]
struct CompleteResponse<'a> {
    event: &'static str,
    oid: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a Path>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<TransferError>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: TransferError,
}

#[derive(Serialize)]
struct TransferError {
    code: i32,
    message: String,
}

/// Error code for a failed init.
const INIT_ERROR: i32 = 32;
/// Error code for a failed object transfer.
const TRANSFER_ERROR: i32 = 2;

// ──────────────────────────────────────────────────
// Config resolution
// ──────────────────────────────────────────────────

/// Adapter state after a successful `init`.
struct Adapter {
    operation: Operation,
    config: config::BigstoreConfig,
    backend: backend::Backend,
}

impl Adapter {
    fn load(operation: Operation) -> Result<Self> {
        let config = load_bigstore_config()?;

        // Fail init rather than every object when the layout has no SHA-256 keys.
        let probe = types::Hexdigest::new(&"ab".repeat(32), types::HashFunction::Sha256)?;
        config
            .remote_object_key(&probe)
            .context("bigstore layout does not support SHA-256 — incompatible with LFS")?;

        let backend = backend::from_config(&config)?;
        Ok(Self {
            operation,
            config,
            backend,
        })
    }

    /// Download `oid` into `work_dir`, verified; returns the file's path.
    fn download(
        &self,
        rt: &tokio::runtime::Runtime,
        oid: &types::Hexdigest,
        work_dir: &Path,
    ) -> Result<PathBuf> {
        let key = self.config.remote_object_key(oid)?;
        // `oid` is validated hex, so it is a safe file name inside the
        // private per-run directory.
        let tmp_path = work_dir.join(oid.to_string());

        let result = rt
            .block_on(backend::download(&self.backend, &key, &tmp_path))
            .with_context(|| format!("download failed for oid {oid}"))
            .and_then(|()| verify_oid(&tmp_path, oid));

        match result {
            Ok(()) => Ok(tmp_path),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp_path);
                Err(e)
            }
        }
    }

    /// Upload the file at `path` as `oid` unless storage already has it.
    fn upload(
        &self,
        rt: &tokio::runtime::Runtime,
        oid: &types::Hexdigest,
        path: &Path,
    ) -> Result<()> {
        let key = self.config.remote_object_key(oid)?;

        let already_exists = rt
            .block_on(backend::exists(&self.backend, &key))
            .with_context(|| format!("failed to check storage for oid {oid}"))?;
        if already_exists {
            return Ok(());
        }

        // Verify the bytes hash to the claimed OID before writing them to shared
        // storage under that key — a mismatched upload would poison the bucket
        // for every consumer (bigstore and LFS alike).
        verify_oid(path, oid)?;
        rt.block_on(backend::upload(&self.backend, path, &key))
            .with_context(|| format!("upload failed for oid {oid}"))
    }
}

fn load_bigstore_config() -> Result<config::BigstoreConfig> {
    // Try .bigstore.toml first
    if let Ok(repo_root) = git::repo_root() {
        let toml_path = repo_root.join(".bigstore.toml");
        if toml_path.exists() {
            return config::BigstoreConfig::load(&toml_path);
        }
    }

    // Fallback: git config bigstore-lfs.*
    let url = git::config_get("bigstore-lfs.url")
        .context("no .bigstore.toml and no git config bigstore-lfs.url")?;
    let endpoint = git::config_get("bigstore-lfs.endpoint");

    config::BigstoreConfig::from_url(&url, endpoint.as_deref())
}

/// The adapter to serve an `operation` transfer, or why there is none.
fn ready(adapter: Option<&Adapter>, operation: Operation) -> Result<&Adapter> {
    let adapter = adapter
        .with_context(|| format!("{operation} request received before a successful init"))?;
    anyhow::ensure!(
        adapter.operation == operation,
        "{operation} request in a session initialised for {}",
        adapter.operation
    );
    Ok(adapter)
}

/// Verify a file's contents against an LFS OID (a SHA-256 digest). Used on both
/// sides: a corrupt download is never reported `complete`, and a mismatched
/// upload is never written to content-addressed storage under the wrong key.
/// Git LFS also verifies, but bigstore checks every transfer itself.
fn verify_oid(path: &Path, oid: &types::Hexdigest) -> Result<()> {
    let actual = hash::hash_file(path, oid.hash_fn())
        .with_context(|| format!("failed to hash oid {oid}"))?;
    anyhow::ensure!(
        actual == *oid,
        "integrity check failed for oid {oid}: got {actual}"
    );
    Ok(())
}

// ──────────────────────────────────────────────────
// Responses
// ──────────────────────────────────────────────────

fn send(w: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let line = serde_json::to_string(value)?;
    writeln!(w, "{line}")?;
    w.flush()?;
    Ok(())
}

/// Report a finished transfer: progress then `complete` on success (with the
/// downloaded file's path, if any), or an error `complete`.
fn send_outcome(
    out: &mut impl Write,
    oid: &str,
    size: u64,
    outcome: Result<Option<PathBuf>>,
) -> Result<()> {
    match outcome {
        Ok(path) => {
            send(
                out,
                &ProgressResponse {
                    event: "progress",
                    oid,
                    bytes_so_far: size,
                    bytes_since_last: size,
                },
            )?;
            send(
                out,
                &CompleteResponse {
                    event: "complete",
                    oid,
                    path: path.as_deref(),
                    error: None,
                },
            )
        }
        Err(e) => send_failure(out, oid, &e),
    }
}

fn send_failure(out: &mut impl Write, oid: &str, e: &anyhow::Error) -> Result<()> {
    send(
        out,
        &CompleteResponse {
            event: "complete",
            oid,
            path: None,
            error: Some(TransferError {
                code: TRANSFER_ERROR,
                message: format!("{e:#}"),
            }),
        },
    )
}

fn send_init_failure(out: &mut impl Write, message: String) -> Result<()> {
    send(
        out,
        &ErrorResponse {
            error: TransferError {
                code: INIT_ERROR,
                message,
            },
        },
    )
}

/// Answer a message that is not a valid [`Request`]: an error `complete` when
/// it names a transfer's oid, an init error for a bad init, otherwise a report
/// on stderr (there is nothing to answer).
fn reject(out: &mut impl Write, line: &str, err: serde_json::Error) -> Result<()> {
    let envelope = serde_json::from_str::<Envelope>(line).ok();
    match envelope
        .as_ref()
        .map(|e| (e.event.as_deref(), e.oid.as_deref()))
    {
        Some((Some(event @ ("upload" | "download")), Some(oid))) => {
            send_failure(out, oid, &anyhow::anyhow!("invalid {event} request: {err}"))
        }
        Some((Some("init"), _)) => send_init_failure(out, format!("invalid init request: {err}")),
        _ => {
            eprintln!("git-bigstore lfs-adapter: ignoring unrecognised message ({err}): {line}");
            Ok(())
        }
    }
}

// ──────────────────────────────────────────────────
// Main loop
// ──────────────────────────────────────────────────

pub fn run() -> Result<()> {
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut stdout = std::io::stdout().lock();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    // Private scratch dir for downloaded objects; removed when the adapter exits.
    let work_dir = tempfile::Builder::new()
        .prefix("bigstore-lfs-")
        .tempdir()
        .context("failed to create temp dir")?;
    // Download paths are reported to LFS as JSON strings.
    anyhow::ensure!(
        work_dir.path().to_str().is_some(),
        "temp dir path is not valid UTF-8: {}",
        work_dir.path().display()
    );

    let mut adapter: Option<Adapter> = None;

    for line in reader.lines() {
        let line = line.context("failed to read stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        let request = match serde_json::from_str::<Request>(&line) {
            Ok(request) => request,
            Err(e) => {
                reject(&mut stdout, &line, e)?;
                continue;
            }
        };

        match request {
            Request::Init { operation } => match Adapter::load(operation) {
                Ok(a) => {
                    adapter = Some(a);
                    send(&mut stdout, &InitResponse {})?;
                }
                Err(e) => {
                    adapter = None;
                    send_init_failure(&mut stdout, format!("failed to load config: {e:#}"))?;
                }
            },

            Request::Download { oid, size } => {
                let outcome = ready(adapter.as_ref(), Operation::Download)
                    .and_then(|a| a.download(&rt, &oid.digest, work_dir.path()))
                    .map(Some);
                send_outcome(&mut stdout, &oid.raw, size, outcome)?;
            }

            Request::Upload { oid, size, path } => {
                let outcome = ready(adapter.as_ref(), Operation::Upload)
                    .and_then(|a| a.upload(&rt, &oid.digest, &path))
                    .map(|()| None);
                send_outcome(&mut stdout, &oid.raw, size, outcome)?;
            }

            Request::Terminate => break,
        }
    }

    Ok(())
}
