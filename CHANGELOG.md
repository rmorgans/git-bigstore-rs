# Changelog

## Unreleased

### Added

- **Archived objects are a typed error.** A read the store refuses with
  S3's `InvalidObjectState` (an object in GLACIER or DEEP_ARCHIVE that was
  not restored) fails with `bigstore::backend::Error::Archived { key }`,
  and in folder mode with the new `bigstore::folder::Error::Archived { key }`
  (pull, log, and the history reads of push and status). Any other 403
  stays the permission error it was. object_store exposes neither S3's
  error code nor the storage class as data, so the code is read from the
  error response it reports.
- **Folder history is a graph of parent-linked versions.** Each record is
  a DVC 3 `.dvc` pointer plus `meta: {bigstore: {parents, writer, time}}`,
  stored create-once as `bigstore-history/<key>/<parents>/<id>.dvc`
  (`<id>`: the first 32 hex characters of the SHA-256 of the record;
  `<parents>`: `root`, or the parent ids sorted and `+`-joined, at most 8).
  One listing gives the graph; the latest version is the only *head* (a
  record no other follows). A record whose bytes do not hash to its name's
  id, or whose parents differ from its name's, is refused as a bad record.
- **Push checks the output's base**, the version its `.dvc` records in
  `meta: {bigstore: {base: <id>}}`, before uploading anything. An output
  equal to the only head adopts it and publishes nothing (this also
  repairs a crash between the record and the `.dvc`). A `.dvc` without a
  base that records the only head's content counts as based on it.
  Otherwise push is refused with the new
  `folder::Error::StaleBase { base, heads }` if there is no `.dvc` while
  history has versions, or the base is not the only head;
  and with `folder::Error::Diverged { heads }` if the history has forked,
  unless `PushOptions::resolve` is `Resolve::Merge` (CLI:
  `folder push --resolve merge`) and the base is one of the heads. The
  record is published before the `.dvc`, which is written last. History is
  then listed again: pushes that raced from the same base are reported in
  `PushReport::forked_with` (a CLI warning).
- `PushOptions::writer` (the host name by default) is recorded in each
  version; `PushReport::version` is the version the output now is.
- **Pull from history writes `<into>.dvc`** with the pulled version as its
  base, last, once every file is in place; a cancelled or failed pull
  leaves the old `.dvc`. A `.dvc` there that push would refuse to replace
  is refused first. Pulling an older version makes the next push of it
  `StaleBase`. `Selector::Latest` in a forked history is `Diverged`.
- `SyncState::Stale { base, head }` and `SyncState::Diverged { heads }`;
  `folder status` prints both. `folder log` prints each version's parents
  and writer, and marks heads, fork points and merges.
- `HistoryRecord` has `id`, `parents` and `writer` fields;
  `HistoryRecord::output_id()` is the content id (manifest id or md5).
- `bigstore::dvc::{RecordId, BigstoreMeta}`, and `DvcPointer::meta`: the
  strict parser accepts exactly bigstore's two `meta:` shapes, and still
  refuses any other `meta:` (so push never replaces a `.dvc` carrying
  someone else's annotations).

### Changed

- **Breaking (library): `bigstore::backend::Store`** replaces the `Backend`
  enum and the free functions `from_config`, `exists`, `put_bytes`,
  `get_bytes`, `list`, `upload`, `download` and `download_verified`. Build
  one with `Store::open(&cfg)` or `Store::from_object_store(client)`; its
  methods are `head(key)` (size and modification time, `None` if absent;
  replaces `exists`), `get(key, limit)`, `put(key, bytes)`,
  `put_file(key, path)` (streamed, multipart above 10 MiB, aborted on a
  failed read), `list(prefix)` (sorted `Listed { key, size, modified }`)
  and `download_verified(key, expected, dir)`. The unverified `download` is
  gone: the LFS adapter and `git bigstore pull` both download through
  `download_verified`. `transfer::Remote::store` is now a `&Store`.
- rclone remotes are listed with `rclone lsjson` instead of `rclone lsf`,
  for object sizes and times; the keys listed are unchanged.
- **Breaking (folder history): a hard cutover per history key.** 0.3 reads
  0.2's `<time>-<content id>.dvc` records as a straight line in time order
  (each with the id of its file name) and continues it, but never writes
  that form; 0.2 clients do not see 0.3 records, so a 0.2 writer left on a
  key forks it. Upgrade every writer of a key together. A `.dvc` 0.2 wrote
  has no base; if it records the latest version's content, its output
  counts as based on that version, so a changed output's first push
  follows it. If it records an older version, the push is `StaleBase`.
- **Breaking (library):** `HistoryRecord::id()` is gone: `id` is now the
  record id, and `Selector::Id` (`--at <hex>`) matches record ids as
  `folder log` prints them, not content ids. `Selector::AtOrBefore` uses
  each record's own time and fetches every record. `SyncState::Diverged {
  latest }` (changed locally and remotely) is now `Stale { base, head }`.
  `PushOptions::cancel` stops a push before its record is published (it
  was: before its `.dvc` was written).

### Fixed

- A read whose body ends before the object's length now fails with
  `incomplete download of <key> (<got>/<expected> bytes)`. Before, a small
  object (a `.dir` manifest or history record) was returned truncated, and
  a download failed only as an integrity check.

## 0.2.1 — 2026-09-30

### Added

- **Async folder API**: `bigstore::folder::{push_async, status_async,
  pull_async, log_async, keys_async}` take the same arguments and return the
  same results as the blocking functions, on the caller's tokio runtime
  (I/O and time drivers enabled; `current_thread` or `multi_thread`). Their
  futures are `Send`, so they can be `tokio::spawn`ed with owned arguments.
  Walking, snapshotting, hashing, `.dvc` reads and writes, pull's
  classification and placing, and snapshot cleanup run on the blocking pool,
  never on the thread polling the future. Dropping a future stops the
  hashing it started at the next file. The blocking functions are now thin
  wrappers over these; the CLI still uses them.

### Changed

- A blocking `bigstore::folder` function called inside a tokio runtime
  still returns an error, which now names the async function to await
  instead (`… await bigstore::folder::push_async instead`).

## 0.2.0 — 2026-09-30

### Added

- **Folder mode** (`git bigstore folder push|pull|log`, library
  `bigstore::folder`): Git-free backup of plain directories and single files
  in DVC 3's exact format, checked against real DVC 3.67.1 in CI on Linux and
  Windows. Includes snapshot-while-hashing (safe for files being appended to),
  an append-only history log on the remote with restore by id or time,
  non-destructive pull, a required S3 endpoint
  with no AWS/IMDS fallback, and a blocking API for sync callers.
- `cli` cargo feature (default). `default-features = false` builds only the
  library.
- Windows CI job that builds and uploads `git-bigstore.exe`, and runs the
  folder-mode tests, including the round trip through an rclone remote.
- `bigstore::folder::Error` (with `folder::Refusal`): every folder-mode
  refusal is typed, so library callers can `downcast_ref` and match instead
  of parsing messages: `Refused { path, reason }` (symlinks, special files,
  nested `.git`/`.dvc`, `*.dvc` inside, non-portable names, foreign pointers,
  a pointer path leaving its directory, symlinked or blocked destinations,
  case collisions, …), `OutputChanged`, `PullConflict { paths }` (replaces the
  `PullConflict` struct), `NoSuchVersion`, `AmbiguousId { candidates }`,
  `InvalidVersionId`, `InvalidTime`, `EndpointRequired` and
  `UnsupportedRemote`. Messages are unchanged, except for non-UTF-8 names:
  one inside a pushed directory is now shown relative to it, like every other
  refused entry, and an output whose own name is not UTF-8 says so instead of
  "has no usable file name". Any URL scheme other than `s3`, `local`/`file`
  and `rclone` is now `UnsupportedRemote` in folder mode (an unknown scheme
  such as `ftp://` used to fail with the generic "unsupported scheme"
  message). A pull whose final rename fails for any reason but a file
  appearing there now says `failed to write <path>` instead of claiming one
  appeared.
- `progress` cargo feature (enabled by `cli`). Without it the library does
  not depend on `indicatif` and `bigstore::transfer` draws no progress bars.
- `gcp` and `azure` cargo features (enabled by `cli`). Library users with
  `default-features = false` no longer build object_store's GCS and Azure
  clients; a `gs://` or `az://` URL then fails with an error naming the
  missing feature.
- `aws-lc-rs` (enabled by `cli`) and `ring` cargo features choose the crypto
  behind TLS and S3/GCS/Azure request signing, so a library user on ring
  (`default-features = false, features = ["ring"]`) no longer builds
  aws-lc-rs. With neither, cloud URLs fail with an error naming both
  features; `local://` and `rclone://` still work.
- `folder::keys(&remote, under)` and `git bigstore folder keys [PREFIX]` list
  the history keys on a remote (all, or those equal to or below a prefix, by
  whole path components), so a host can find other writers' outputs without
  a pointer. One listing; nested keys (`k` and `k/sub`) are both reported and
  objects that are not records are ignored, as are keys `HistoryKey::new`
  rejects (left out silently).
- `folder::status(&remote, output, &push_options)` and `git bigstore folder
  status` (push's arguments): what a push would upload (distinct contents and
  bytes, and what the remote already has), the pointer it would write, and
  `SyncState` against the latest history version (`NoHistory`, `InSync`,
  `LocalAhead`, `RemoteAhead`, `Diverged`, judged by the `.dvc` beside the
  output). It shares push's walk, snapshot and hashing, refuses what push
  refuses, and writes nothing to the remote or beside the output. `InSync`
  means no new version; push may still rewrite a missing or stale `.dvc`.
- `PushReport::already_present` counts what the remote already had when its
  manifest was there too; a push of an unchanged directory used to report
  `0 uploaded, 0 already on the remote`.
- Cancellation for folder mode: `folder::CancelToken` (cloneable, shared
  flag), as `PushOptions::cancel`, `PullOptions::cancel` and
  `LogOptions::cancel`, checked between files and between objects by push,
  status and pull, and between record fetches by log; the call returns
  `folder::Error::Cancelled`. A cancelled push writes no `.dvc` and no
  history record; a cancelled pull leaves no partly written file.
  `git bigstore folder push|status|pull|log` cancel this way on the first
  Ctrl-C. `PushOptions::new(history)`, `PullOptions::default()` and
  `LogOptions::default()` give the defaults (8 jobs), so new options fields
  no longer break struct literals written as
  `PushOptions { jobs: 4, ..PushOptions::new(key) }`.
- Progress for folder mode: `folder::Progress::new(|event| …)` as
  `PushOptions::progress` and `PullOptions::progress` receives
  `ProgressEvent::Started { phase, files, bytes }` and
  `Advanced { phase, files, bytes }` per finished file, for the phases
  `Hashing`, `Uploading` and `Downloading`. The callback is `Send + Sync`,
  usable from sync callers, and free when unset; the library needs no
  `indicatif`. `git bigstore folder push|status|pull` draw a bar per phase
  on a terminal.
- `folder pull` refuses, as `Refusal::CaseCollision`, manifest names that
  differ only by Unicode normalization (NFC `é` vs NFD `e` + accent) or by
  non-ASCII case (`Ä`/`ä`), on every OS, before writing anything. Only ASCII
  case was folded before, so on macOS such a pair restored one file and then
  failed with "appeared while pulling". New dependency:
  `unicode-normalization` (std has no NFC).

### Security

- On Windows, `RepoPath` rejects `\` and `:`. Before this, a hostile DVC
  manifest could make `import-dvc-dir` write outside the repository via
  `a\..\..\x` or a drive-relative `C:x`.
- `folder push` and `folder status` create their snapshot temp directory
  mode 0700 on unix; it was 0755 under the usual umask. The copies inside
  were already 0600; now other users cannot list or enter the directory
  either.

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
- **`folder push` skips OS junk**: `.DS_Store`, `._*` (AppleDouble),
  `Thumbs.db` and `desktop.ini`, at any depth. Finder writes `.DS_Store` just
  by showing a folder, which made the next push a new manifest and history
  record. `--exclude PATTERN` (`PushOptions::exclude`, `folder::Excludes`)
  skips more, with `.gitignore` rules relative to the pushed directory; a
  directory-only pattern (`scratch/`) also skips a symlink to a directory,
  as `.dvcignore` does, instead of refusing it, and never follows it.
  Anyone also running `dvc add` on the folder needs the same patterns in
  `.dvcignore`; the README gives the lines.
- **Folder history is read by listing.** A record's name holds its time and
  id, so `folder push` and `folder pull --history` now fetch only the one
  record they need instead of every record of the key (a push onto 50
  versions made 50 GETs; now 1). `folder::log(&remote, &key, &LogOptions)`
  takes options like push and pull (`jobs`, `cancel`) and fetches records
  `jobs` at a time (`folder log -j`). A record whose
  pointer is not the version its name says is refused as a bad record, and
  a malformed `--at` is refused before the remote is contacted.
- **`folder pull` reads any single-output DVC 3 `.dvc`**, ignoring stage
  fields and annotations (`dvc import-url`'s `deps`/`frozen`/`md5`,
  `dvc add --desc`), since it never rewrites the file; before, it refused
  them ("has fields bigstore does not write"). A `.dvc` with nothing to
  restore (`cache: false`, etag-only, several outputs, `wdir:`) is now the
  typed `Refusal::UnrestorablePointer`, the parse error below it.
- An invalid `HistoryKey` is `folder::Error::InvalidHistoryKey { key }` and
  a history pull without `into` is `folder::Error::DestinationRequired`;
  both were untyped. Messages are unchanged apart from naming the key.

### Fixed

- Downloads from an `rclone://` remote failed on Windows ("Access is
  denied"): bigstore kept its temp file open while rclone renamed its
  partial download over it, which Windows refuses. The temp file is now
  closed while rclone writes (folder pull and history, and git-mode pull).
  The Windows CI job now runs the rclone round trip.
- On Windows, paths bigstore builds from a repository or manifest path
  (pull reports and conflicts, restore targets) used `/` inside an
  otherwise `\`-separated path. They now use `\` throughout; the
  destination you passed is still shown as given.
- `ref`, `dvc-ls` and `import-dvc-dir` refused legal DVC 3 `.dvc` files
  with fields beyond the output's hash (a regression since 0.1.0):
  `dvc import-url`'s `md5:`/`frozen:`/`deps:`, `meta:`/`desc:` annotations,
  `isexec:`, and per-output `remote:`/`push:`/`cloud:`. They are read for
  their output again. Outputs with no md5-addressed object are refused
  saying why (`cache: false`, not yet downloaded, etag/version_id only), as
  is a `wdir:` other than `.`. `folder push` still refuses to overwrite such
  a file, now naming the fields it would drop.
- `ref` of a `.dvc` with `isexec: true` (DVC's mark for an executable
  file) wrote the file without its execute bit. On unix it is now written
  0777 minus umask, as git checks out an executable; DVC's `.dir`
  manifests record no modes, so `import-dvc-dir` is unchanged.
- `folder pull` of such a `.dvc` (a single-file output) dropped the mark
  the same way. It now restores the file executable on unix, and makes an
  identical copy already in place executable instead of leaving it as it
  was. A `.dir` manifest entry marked `isexec` (only manifests hashed with
  per-file metadata have one; `dvc add` writes none) is refused instead of
  restored without its mode: `folder pull` says so as
  `Refusal::ExecutableInDirectory`, as it does for a directory output
  marked `isexec` in its `.dvc`, and `import-dvc-dir` and `dvc-ls` fail
  naming the entry (`dvc::ExecutableEntry`).
- On Windows, `import-dvc-dir`, `folder pull` and every other path bigstore
  writes refuse names Windows cannot create before writing anything, naming
  the path: device names (`CON`, `nul.txt`, `com1.log`, `con .txt`,
  `CONIN$`), a trailing `.` or space, `* ? " < > |` and control characters.
  Before, such a name from a manifest made on Unix failed at the final
  rename. Folder push's portability check refuses the same device names.
  Only paths about to be created are checked (manifest entries, `ref`'s
  destination, `import-dvc-dir`'s destination root): paths git reports
  are read as before, so `log` of a history that once held `docs/aux.md`
  works on Windows.
- On Windows, `folder pull` into a path past the 260-character `MAX_PATH`,
  and `folder push` of an output whose `.dvc` lands past it, failed at the
  final rename unless both the machine (`LongPathsEnabled`) and the program
  (its manifest) opted into long paths: std lifts the limit for its own
  calls, but tempfile's `persist` hands paths to `MoveFileExW` as they are.
  Those renames and their temp files now use verbatim `\\?\` paths, so any
  depth works on any machine; paths in reports and errors stay as given. CI
  checks it on Windows with `LongPathsEnabled` off. Bare relative arguments
  (`folder push data`, `folder pull store.toml.dvc`, `--into out.bin`, run
  from the directory holding them) still work: their parent, the empty
  path, is the current directory.
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
- `folder push` kept one file open per unique file until the upload, so a
  folder of a few hundred files failed with `Too many open files` under a
  256-descriptor limit (the macOS launchd default). Snapshots are now closed
  once hashed; open files are bounded by `--jobs`.
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
- The filter process no longer spools checkouts of large non-pointer files
  (committed before their `filter=bigstore` rule) to the system temp dir,
  which could fill a small `/tmp` or tmpfs, or fail when `TMPDIR` is
  unusable. Content over 8 MiB now goes to an unnamed file in
  `.git/bigstore/tmp`, on the cache's filesystem; files a crashed filter left
  there are removed when the next one starts.
- A failed index lookup during clean (or a failed blob read in `log`,
  `status`, `push`, `pull`) could hang the filter or command for good:
  closing the `git cat-file` helper waited for it to exit while it was still
  blocked writing the rest of a blob over 64 KiB. Its output pipe is now closed
  first, so it exits.
- A malformed packet inside a file's content no longer lets the filter
  process answer `status=error` and carry on, reading payload bytes as packet
  headers. It now exits with the error, and git starts a fresh filter.

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
