# Changelog

## Unreleased

### Fixed

- Upload now uses `object_store`'s `BufWriter`, which sizes multipart parts
  correctly (single PUT for small objects, 10 MiB parts above). The previous
  raw `put_part` loop could forward short reads as sub-5 MiB parts and risk an
  `EntityTooSmall` rejection on large files.
- `log` no longer buffers entire blobs into memory to check the pointer header;
  it reads a bounded header and drains the rest, capping per-blob memory.
- Invalid globs in `.gitattributes` and in CLI patterns now error instead of
  silently matching everything (or nothing).
- LFS adapter verifies every transfer against its OID — downloads before
  reporting `complete`, uploads before writing to shared storage (so a corrupt
  upload can't poison the bucket) — writes downloads to a private per-run temp
  dir (no predictable shared path), and cleans up on failure.
- `Pointer::parse` rejects any trailing non-whitespace content after the digest
  line, including content that follows a blank line.
- Push/pull progress bar now advances for skipped and not-found objects.

### Changed

- Core modules (`cache`, `dvc`, `filter`, `git`, `transfer`) moved into the
  library crate; the binary is now a thin CLI shell. Removes duplicated git
  helpers in the LFS adapter and makes the core unit-testable.
- Replaced unmaintained `serde_yaml` (RUSTSEC-2024-0370) with `serde_yaml_ng`.
- Bumped `object_store` to 0.14 and refreshed the lockfile, clearing
  advisories in `rustls-webpki`, `quick-xml` (RUSTSEC-2026-0194/0195), `h2`
  (RUSTSEC-2026-0258) and `rustls` (RUSTSEC-2026-0285). TLS now uses
  `aws-lc-rs` with the platform certificate verifier. Added `deny.toml` for
  `cargo deny` supply-chain gating.
- Removed the unimplemented `bigstore-compress` filter recognition.
- `rust-version` raised to 1.89, the floor the dependency tree now requires.

## 0.1.0

First release. Validated against a real monorepo with 80 files across 5 ML
models, pushed to Tigris, and verified on fresh clone.

### Core

- Content-addressed storage with SHA-256 (default) and MD5 (DVC interop)
- Clean/smudge git filter with idempotent pointer format (3 lines, ~81 bytes)
- Concurrent push/pull with configurable `--jobs` (default 8, env `BIGSTORE_JOBS`)
- Integrity verification on every download
- `status --verify` for cache integrity checking

### Backends

- S3, GCS, Azure, Cloudflare R2, Tigris (`t3://`), rclone, local filesystem
- DVC-compatible storage layout (`files/{hash_fn}/{prefix}/{rest}`)

### DVC migration

- `ref` — import single-file .dvc pointers with hash verification
- `dvc-ls` — inspect .dir manifest contents
- `import-dvc-dir` — batch import with selective glob patterns, `--force` overwrite
- Content auto-restored to working tree after import (no manual checkout needed)
- DVC cache discovery via `dvc cache dir` (supports global/shared caches)
- Pull fallback: automatically imports from local DVC cache when remote object missing

### History and diagnostics

- `log` — file-level history with change classification (+/-/~/R/C)
- `status` — shows cached/checked-out/pointer-only state per file
- `status --verify` — re-hashes cached objects, reports corruption with repair guidance

### LFS interop

- `git bigstore lfs-adapter` — LFS custom transfer agent (hidden subcommand)
- Lets Git LFS clients upload/download from bigstore's bucket (no LFS server needed)
- SHA-256 object keys shared between LFS and bigstore
- Storage-layer bridge only — no pointer-format bridging, no locking

### Configuration safety

- `init` preserves existing filter config on re-run
- `FilterConfig` type enforces clean/smudge/required consistency
- Partial or malformed filter config detected and rejected with fix instructions
- `migrate-config` upgrades legacy `.bigstore` to `.bigstore.toml`
