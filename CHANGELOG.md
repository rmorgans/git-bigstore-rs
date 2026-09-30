# Changelog

## Unreleased

### Added

- **Folder mode** (`git bigstore folder push|pull|log`, library
  `bigstore::folder`): Git-free backup of plain directories and single files
  in DVC 3's exact format, checked against real DVC 3.67.1 in CI on Linux and
  Windows. Includes snapshot-while-hashing (safe for files being appended to),
  an append-only history log on the remote with restore by id or time,
  non-destructive pull with a typed `PullConflict`, a required S3 endpoint
  with no AWS/IMDS fallback, and a blocking API for sync callers.
- `cli` cargo feature (default). `default-features = false` builds only the
  library.
- Windows CI job that builds and uploads `git-bigstore.exe`.
- `progress` cargo feature (enabled by `cli`). Without it the library does
  not depend on `indicatif` and `bigstore::transfer` draws no progress bars.

### Security

- On Windows, `RepoPath` rejects `\` and `:`. Before this, a hostile DVC
  manifest could make `import-dvc-dir` write outside the repository via
  `a\..\..\x` or a drive-relative `C:x`.

### Changed

- DVC pointers are parsed strictly as DVC 3. A pointer without `hash: md5`
  comes from DVC 2 (md5-dos2unix, a different cache layout) and is refused
  with a message saying so, instead of being reported as a missing object.
- **git runs one bigstore filter process per command** instead of one
  clean/smudge process per file. `init` and `pull` set
  `filter.bigstore.process = "<bin> filter-process"` (git's long-running
  filter protocol) beside the one-shot `clean`/`smudge`/`required` keys, which
  stay as the fallback for git older than 2.11; clones configured by an older
  version gain the key on their next `pull` or `init`. For 500 files of 1 KiB
  (local backend, macOS arm64): `pull` 11 s → 0.4 s, `pull` with a warm cache
  9.7 s → 0.3 s, `git add` 13.7 s → 0.4 s. A `process` key with another
  binary or subcommand, or without the one-shot keys, is rejected with a fix.
- **Downgrading** to a version without `filter-process` needs, in each clone
  first, `git config --unset filter.bigstore.process` and
  `git config filter.bigstore.clean "git-bigstore filter-clean"` (older
  versions reject the `%f` now on the clean command); otherwise every
  checkout, `git add` and `git status` of a tracked file fails (nothing is
  corrupted).

### Fixed

- `pull` no longer overwrites uncommitted edits. It fills the cache, then
  replaces only files that are still the index's pointer, via
  `git checkout-index`: restored files keep their committed mode (executables
  stay executable, no more 0600), `git status` is clean afterwards, and missing
  files (deleted or outside a sparse checkout) are left alone. A fresh clone
  gets the bigstore filters configured on first `pull`.
- Files with non-ASCII names were silently never pushed or pulled (`git
  ls-files` quoted them), and running from a subdirectory acted on the wrong
  files. Paths are now read NUL-separated and root-relative.
- Which files bigstore handles is now decided by git (`git check-attr`), so
  files tracked through nested `.gitattributes` or `.git/info/attributes` are
  pushed and pulled instead of being silently skipped.
- A DVC manifest with `\` or `:` in a file name (valid DVC data made on
  Unix) failed to parse on Windows, so even read-only commands such as
  `dvc-ls` refused it. Parsing now checks content only (relative,
  `/`-separated, no empty, `.` or `..` components); `import-dvc-dir` and
  `folder pull` refuse, naming the file, only the names this OS would misread
  (`\` and `:` on Windows), before writing anything.
- `folder pull` restores any name this OS can create, e.g. non-ASCII or `\`
  names from a manifest `dvc push` wrote on Linux. `folder push` still
  refuses names that are not portable, so what it writes can be pulled
  everywhere.
- `folder pull --at <id>` with a prefix that matches several versions lists
  each candidate's id and push time instead of only saying it is ambiguous.
- `folder pull <x.dvc>` restored to `<dir>/<path>` with `path` taken as
  written, so a hostile `.dvc` with `path: ../x` or an absolute path wrote
  outside the pointer's directory. `path` must now be one name this OS can
  write (as push writes); anything else is refused, naming the value and the
  `.dvc` file, before anything is written.
- `folder pull` of a directory output followed a symlink at the output root
  (`out -> elsewhere` committed beside `out.dvc`) and wrote there. A
  symlinked output root is now refused, like any symlinked directory below
  it and like `folder push` already did.
- One blob that is not a pointer (e.g. committed before its `filter=bigstore`
  rule) no longer aborts the whole push or pull; `status` reports it as
  `not a pointer in git (git add --renormalize)`.
- Content that merely starts like a pointer (first line `bigstore`, or a
  pointer followed by more data) is now stored as a large file. Previously the
  clean filter passed it through unchanged and checkout then failed or
  truncated it. A single rule (`Pointer::parse`) now decides pointer vs content
  for both filters, the index and the working tree.
- `push` fails for an object that is neither on the remote nor in the local
  cache, instead of exiting 0 and leaving a committed pointer with no content
  anywhere.
- The object cache lives in the common git directory, so linked worktrees
  share it; a commit made in one worktree can be checked out in another.
  Objects an older version cached from inside a linked worktree
  (`.git/worktrees/<name>/bigstore/objects`) are no longer read: already
  pushed ones are simply re-downloaded; for unpushed ones, move that
  directory's contents into `.git/bigstore/objects` (or run
  `git add --renormalize <path>` in that worktree), then push.
- A pull whose checkout fails part-way (e.g. an unreadable cache object) puts
  the affected pointer files back and reports them; the other files are still
  checked out and the index blobs are never changed. A pull that is killed
  (Ctrl-C, SIGKILL) leaves a journal in the worktree's git directory, and the
  next pull repairs exactly the files it had touched — files the user deleted
  stay deleted. Pull never replaces a file that appears at a path while it
  runs.
- Files whose committed pointer is md5 (DVC pointer text) no longer show as
  modified, or get re-staged as sha256, after git re-checks their timestamps.
  The clean filter now gets the path (`filter-clean %f`) and keeps the index's
  md5 pointer when the content still matches it. `init` and `pull` write the
  new command; an existing `filter-clean` without `%f` keeps working as
  before.
- Files outside a sparse checkout (skip-worktree) are no longer downloaded;
  `status` shows them as `outside sparse checkout`.
- `status --verify` repair advice names the corrupted object files instead of
  suggesting deleting the whole cache (via a path that is wrong in linked
  worktrees).
- `ref` and `import-dvc-dir` write files with normal permissions (0666 minus
  umask) and no longer write a pointer only to overwrite it.
- Pull and push transfer each object once, however many paths share it, and
  hash DVC-cache imports off the async runtime.
- A failed large (multipart) upload is now aborted instead of leaving billed
  incomplete parts in the bucket.
- `local://` backends create the storage directory on first use.
- rclone backend: `exists` no longer reports auth, network or config failures
  as "not found"; rclone runs asynchronously (so `--jobs` applies), its errors
  include rclone's stderr, and non-UTF-8 local paths work.
- LFS adapter: every per-object failure (invalid oid, remote error, request
  before `init`) is reported as an error `complete` for that object instead of
  killing the adapter and every queued transfer. Requests are parsed into a
  typed enum; object keys come from the shared config.
- `log` shows files with non-ASCII or quoted names (previously dropped), reads
  pointers by blob id, spawns two git processes per commit fewer, and reports
  a failing `git diff-tree` instead of skipping the commit.
- Upload now uses `object_store`'s `BufWriter`, which sizes multipart parts
  correctly (single PUT for small objects, 10 MiB parts above). The previous
  raw `put_part` loop could forward short reads as sub-5 MiB parts and risk an
  `EntityTooSmall` rejection on large files.
- `log` no longer buffers entire blobs into memory to check the pointer header;
  it reads a bounded header and drains the rest, capping per-blob memory.
- Invalid globs in CLI patterns now error instead of silently matching
  nothing.
- LFS adapter verifies every transfer against its OID — downloads before
  reporting `complete`, uploads before writing to shared storage (so a corrupt
  upload can't poison the bucket) — writes downloads to a private per-run temp
  dir (no predictable shared path), and cleans up on failure.
- Push/pull progress bar now advances for skipped and not-found objects.

### Changed

- Library types carry their invariants: `Hexdigest` includes its hash
  function (a digest can no longer be paired with the wrong algorithm, and
  `Pointer::new` cannot panic), `Pointer::parse` is total (`Option`), paths
  are validated `RepoPath`s, `--jobs` is a `NonZeroUsize`, and CLI paths are
  validated by the argument parser.
- The clean/smudge filters and other synchronous commands no longer start an
  async runtime; only `push` and `pull` do.
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
