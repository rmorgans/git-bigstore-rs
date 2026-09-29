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

use crate::{backend, config, git, transfer, types};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

// ──────────────────────────────────────────────────
// LFS custom transfer protocol types
// ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct Event {
    event: String,
    #[serde(default)]
    oid: String,
    #[serde(default)]
    #[allow(dead_code)]
    size: u64,
    #[serde(default)]
    path: Option<String>,
}

#[derive(Serialize)]
struct InitResponse {}

#[derive(Serialize)]
struct ProgressResponse {
    event: &'static str,
    oid: String,
    #[serde(rename = "bytesSoFar")]
    bytes_so_far: u64,
    #[serde(rename = "bytesSinceLast")]
    bytes_since_last: u64,
}

#[derive(Serialize)]
struct CompleteResponse {
    event: &'static str,
    oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
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

// ──────────────────────────────────────────────────
// Config resolution
// ──────────────────────────────────────────────────

struct AdapterConfig {
    backend: backend::Backend,
    prefix: String,
    layout: types::Layout,
}

fn load_config() -> Result<AdapterConfig> {
    let cfg = load_bigstore_config()?;

    // Verify layout supports SHA-256
    let test_hex = "ab".repeat(32);
    let test_digest = types::Hexdigest::new(&test_hex, types::HashFunction::Sha256)?;
    cfg.layout
        .object_key(&test_digest, types::HashFunction::Sha256)
        .context("bigstore layout does not support SHA-256 — incompatible with LFS")?;

    let b = backend::from_config(&cfg)?;
    let prefix = cfg.bucket_prefix().to_string();

    Ok(AdapterConfig {
        backend: b,
        prefix,
        layout: cfg.layout.clone(),
    })
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

// ──────────────────────────────────────────────────
// Object key mapping
// ──────────────────────────────────────────────────

fn oid_to_remote_key(cfg: &AdapterConfig, oid: &str) -> Result<String> {
    let hexdigest = types::Hexdigest::new(oid, types::HashFunction::Sha256)
        .context("LFS OID is not a valid SHA-256 hex digest")?;

    let key = cfg
        .layout
        .object_key(&hexdigest, types::HashFunction::Sha256)?;

    if cfg.prefix.is_empty() {
        Ok(key)
    } else {
        Ok(format!("{}/{key}", cfg.prefix))
    }
}

// ──────────────────────────────────────────────────
// Transfer operations (delegates to shared backend)
// ──────────────────────────────────────────────────

fn send(w: &mut impl Write, value: &impl Serialize) -> Result<()> {
    let line = serde_json::to_string(value)?;
    writeln!(w, "{line}")?;
    w.flush()?;
    Ok(())
}

/// Verify a file's contents against an LFS OID (a SHA-256 digest). Used on both
/// sides: a corrupt download is never reported `complete`, and a mismatched
/// upload is never written to content-addressed storage under the wrong key.
/// Git LFS also verifies, but bigstore checks every transfer itself.
fn verify_oid(path: &Path, oid: &str) -> Result<()> {
    let expected = types::Hexdigest::new(oid, types::HashFunction::Sha256)?;
    let actual = transfer::hash_file(path, types::HashFunction::Sha256)
        .with_context(|| format!("failed to hash oid {oid}"))?;
    anyhow::ensure!(
        actual == expected,
        "integrity check failed for oid {oid}: got {actual}"
    );
    Ok(())
}

fn handle_download(
    rt: &tokio::runtime::Runtime,
    cfg: &AdapterConfig,
    oid: &str,
    work_dir: &Path,
    out: &mut impl Write,
) -> Result<()> {
    let key = oid_to_remote_key(cfg, oid)?;

    // Private per-run directory (random name, cleaned up on exit) avoids the
    // predictable shared-temp path the previous implementation used.
    let tmp_path = work_dir.join(oid);

    let result = rt
        .block_on(async {
            backend::download(&cfg.backend, &key, &tmp_path)
                .await
                .with_context(|| format!("download failed for oid {oid}"))
        })
        .and_then(|()| verify_oid(&tmp_path, oid));

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }

    match result {
        Ok(()) => {
            let file_size = std::fs::metadata(&tmp_path)?.len();
            send(
                out,
                &ProgressResponse {
                    event: "progress",
                    oid: oid.to_string(),
                    bytes_so_far: file_size,
                    bytes_since_last: file_size,
                },
            )?;
            send(
                out,
                &CompleteResponse {
                    event: "complete",
                    oid: oid.to_string(),
                    path: Some(tmp_path.to_string_lossy().to_string()),
                    error: None,
                },
            )?;
        }
        Err(e) => {
            send(
                out,
                &CompleteResponse {
                    event: "complete",
                    oid: oid.to_string(),
                    path: None,
                    error: Some(TransferError {
                        code: 2,
                        message: format!("{e:#}"),
                    }),
                },
            )?;
        }
    }

    Ok(())
}

fn handle_upload(
    rt: &tokio::runtime::Runtime,
    cfg: &AdapterConfig,
    oid: &str,
    path: &str,
    out: &mut impl Write,
) -> Result<()> {
    let key = oid_to_remote_key(cfg, oid)?;

    // Check if already exists (proper error propagation — only NotFound is false)
    let already_exists = rt.block_on(async { backend::exists(&cfg.backend, &key).await })?;

    let result: Result<()> = if already_exists {
        Ok(())
    } else {
        let local_path = Path::new(path);
        // Verify the bytes hash to the claimed OID before writing them to shared
        // storage under that key — a mismatched upload would poison the bucket
        // for every consumer (bigstore and LFS alike).
        verify_oid(local_path, oid).and_then(|()| {
            rt.block_on(async {
                backend::upload(&cfg.backend, local_path, &key)
                    .await
                    .with_context(|| format!("upload failed for oid {oid}"))
            })
        })
    };

    match result {
        Ok(()) => {
            let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            send(
                out,
                &ProgressResponse {
                    event: "progress",
                    oid: oid.to_string(),
                    bytes_so_far: file_size,
                    bytes_since_last: file_size,
                },
            )?;
            send(
                out,
                &CompleteResponse {
                    event: "complete",
                    oid: oid.to_string(),
                    path: None,
                    error: None,
                },
            )?;
        }
        Err(e) => {
            send(
                out,
                &CompleteResponse {
                    event: "complete",
                    oid: oid.to_string(),
                    path: None,
                    error: Some(TransferError {
                        code: 2,
                        message: format!("{e:#}"),
                    }),
                },
            )?;
        }
    }

    Ok(())
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

    let mut cfg: Option<AdapterConfig> = None;

    for line in reader.lines() {
        let line = line.context("failed to read stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        let event: Event = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON from LFS: {line}"))?;

        match event.event.as_str() {
            "init" => match load_config() {
                Ok(c) => {
                    cfg = Some(c);
                    send(&mut stdout, &InitResponse {})?;
                }
                Err(e) => {
                    send(
                        &mut stdout,
                        &ErrorResponse {
                            error: TransferError {
                                code: 32,
                                message: format!("failed to load config: {e:#}"),
                            },
                        },
                    )?;
                }
            },

            "download" => {
                let c = cfg.as_ref().expect("init must precede download");
                handle_download(&rt, c, &event.oid, work_dir.path(), &mut stdout)?;
            }

            "upload" => {
                let c = cfg.as_ref().expect("init must precede upload");
                let path = event.path.as_deref().expect("upload must have path");
                handle_upload(&rt, c, &event.oid, path, &mut stdout)?;
            }

            "terminate" => break,

            other => {
                eprintln!("git-bigstore lfs-adapter: unknown event: {other}");
            }
        }
    }

    Ok(())
}
