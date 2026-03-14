//! Git LFS custom standalone transfer adapter for bigstore object storage.
//!
//! Lets Git LFS clients upload/download blobs from the same bucket/prefix
//! that bigstore uses for SHA-256 objects. Storage-layer bridge only —
//! no pointer-format bridging, no LFS API server.
//!
//! Git config:
//!   [lfs "customtransfer.bigstore"]
//!       path = git-bigstore-lfs-adapter
//!   [lfs]
//!       standalonetransferagent = bigstore
//!
//! Config resolution:
//!   1. .bigstore.toml (if present)
//!   2. git config bigstore-lfs.url (fallback for LFS-only repos)

use anyhow::{Context, Result};
use bigstore::{backend, config, types};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

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
    if let Ok(repo_root) = git_repo_root() {
        let toml_path = repo_root.join(".bigstore.toml");
        if toml_path.exists() {
            return config::BigstoreConfig::load(&toml_path);
        }
    }

    // Fallback: git config bigstore-lfs.*
    let url = git_config_get("bigstore-lfs.url")
        .context("no .bigstore.toml and no git config bigstore-lfs.url")?;
    let endpoint = git_config_get("bigstore-lfs.endpoint");

    config::BigstoreConfig::from_url(&url, endpoint.as_deref())
}

fn git_repo_root() -> Result<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()?;
    anyhow::ensure!(output.status.success(), "not a git repository");
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(PathBuf::from(path))
}

fn git_config_get(key: &str) -> Option<String> {
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

fn handle_download(
    rt: &tokio::runtime::Runtime,
    cfg: &AdapterConfig,
    oid: &str,
    out: &mut impl Write,
) -> Result<()> {
    let key = oid_to_remote_key(cfg, oid)?;

    let tmp_dir = std::env::temp_dir();
    let tmp_path = tmp_dir.join(format!("bigstore-lfs-{oid}"));

    let result = rt.block_on(async {
        backend::download(&cfg.backend, &key, &tmp_path)
            .await
            .with_context(|| format!("download failed for oid {oid}"))
    });

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
        let local_path = std::path::Path::new(path);
        rt.block_on(async {
            backend::upload(&cfg.backend, local_path, &key)
                .await
                .with_context(|| format!("upload failed for oid {oid}"))
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

fn main() -> Result<()> {
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut stdout = std::io::stdout().lock();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;

    let mut cfg: Option<AdapterConfig> = None;

    for line in reader.lines() {
        let line = line.context("failed to read stdin")?;
        if line.trim().is_empty() {
            continue;
        }

        let event: Event =
            serde_json::from_str(&line).with_context(|| format!("invalid JSON from LFS: {line}"))?;

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
                handle_download(&rt, c, &event.oid, &mut stdout)?;
            }

            "upload" => {
                let c = cfg.as_ref().expect("init must precede upload");
                let path = event.path.as_deref().expect("upload must have path");
                handle_upload(&rt, c, &event.oid, path, &mut stdout)?;
            }

            "terminate" => break,

            other => {
                eprintln!("git-bigstore-lfs-adapter: unknown event: {other}");
            }
        }
    }

    Ok(())
}
