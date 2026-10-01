# git-bigstore-rs

Large files in git, your bucket, one binary.

A Rust rewrite of Dan Loewenherz's
[git-bigstore](https://github.com/lionheart/git-bigstore) (2013), which got
one thing right that everything else got wrong: large file storage should use
git's own clean/smudge filters and your own bucket, not a vendor-hosted server
with its own protocol and billing.

Dan's original insight was that git-media (and later Git LFS) broke
idempotency — running the clean filter twice produced different output, which
corrupted repos during collaboration. bigstore fixed that with a simple,
idempotent pointer format and direct cloud storage. It was a Python script, a
`.gitattributes` line, and your S3 credentials. Nothing else.

This rewrite is an exploration of the modern state of git and big data — what's
possible now with Rust, async object-store crates, and the DVC/LFS ecosystem
that didn't exist when Dan wrote the original. Git itself may absorb much of
this soon. Until then: one binary, no server, no lock-in, and a storage-layer
bridge that lets Git LFS clients pull from the same bucket without knowing
bigstore exists.

## Install

```bash
cargo install --path .
```

The binary is called `git-bigstore`. Git discovers it automatically as a
subcommand (`git bigstore ...`).

### Cargo features

The defaults build the binary with everything. A library user (the crate is
`bigstore`) turns them off with `default-features = false` and picks what it
needs:

| Feature | Default | Enables |
|---------|---------|---------|
| `cli` | yes | The `git-bigstore` binary (clap, tracing-subscriber) and everything below |
| `progress` | via `cli` | Progress bars on stderr during `bigstore::transfer` push and pull |
| `gcp` | via `cli` | `gs://` remotes (Google Cloud Storage) |
| `azure` | via `cli` | `az://` remotes (Azure Blob Storage) |
| `aws-lc-rs` | via `cli` | aws-lc-rs as the crypto for TLS and request signing |
| `ring` | no | ring as the crypto for TLS and request signing |

`s3://` (and R2, Tigris), `local://` and `rclone://` are always built in. A
URL whose backend is left out fails with an error naming the feature.

Cloud remotes (`s3://`, `gs://`, `az://`) need one crypto provider:
`aws-lc-rs` or `ring`. With neither they fail with an error, and only
`local://` and `rclone://` work. With both, aws-lc-rs is used. An application
already built on ring (the folder-mode library, S3 only):

```toml
bigstore = { package = "git-bigstore-rs", git = "…", rev = "…", default-features = false, features = ["ring"] }
```

With `ring` (and not `aws-lc-rs`), bigstore installs ring as the process's
default rustls `CryptoProvider` when it builds its first cloud client
(in folder mode, `Remote::open` of an `s3://` remote), unless the
application installed one already. An application that installs its own
provider must do so before that call: afterwards a default is set, so its
`CryptoProvider::install_default()` returns `Err`, and the usual
`.expect(…)` on it panics.

## Quick start

```bash
# Initialize with your storage backend
git bigstore init s3://my-bucket/bigstore

# Tell git which files to track
echo '*.bin filter=bigstore' >> .gitattributes
git add .gitattributes .bigstore.toml

# Use git normally — large files are transparently replaced with pointers
cp ~/large-model.bin .
git add large-model.bin
git commit -m "add model"

# Upload to remote storage
git bigstore push

# On another machine: clone and pull
git clone ...
git bigstore pull
```

## Backends

| Scheme | Example | Notes |
|--------|---------|-------|
| `s3://` | `s3://bucket/prefix` | AWS S3 (uses standard AWS credentials) |
| `gs://` | `gs://bucket/prefix` | Google Cloud Storage |
| `az://` | `az://container/prefix` | Azure Blob Storage |
| `r2://` | `r2://bucket/prefix` | Cloudflare R2 (requires `--endpoint`) |
| `t3://` or `tigris://` | `t3://bucket` | Tigris (auto-configures endpoint) |
| `rclone://` | `rclone://remote:path` | Any rclone remote |
| `local://` or `file://` | `local:///tmp/store` | Local filesystem (testing) |

```bash
# R2 requires an explicit endpoint
git bigstore init r2://my-bucket --endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com

# Tigris auto-configures
git bigstore init t3://my-bucket
```

## Commands

### `git bigstore init <url>`

Initialize bigstore in the current repository. Creates `.bigstore.toml` and
configures the git filter: one long-running filter process per git command
(`filter.bigstore.process`), with the one-shot clean/smudge filters kept as
the fallback for git older than 2.11. Existing filter config with a custom
binary path is preserved; config written by an older version gains the
process key.

### `git bigstore push [patterns...]`

Upload cached objects to remote storage. Skips objects already present on the
remote. Optional glob patterns (relative to the repository root, whatever
directory you run from) filter which files to push. An object that is
neither on the remote nor in the local cache is reported as a failure — its
content exists nowhere this clone can reach.

```bash
git bigstore push              # push all tracked files
git bigstore push "models/*"   # push only models
git bigstore push --jobs 16    # use 16 concurrent uploads
```

### `git bigstore pull [patterns...]`

Download objects from remote storage with integrity verification. Every
downloaded object is hash-verified before entering the local cache.

Pull then checks files out through git, so they get their committed file mode
and `git status` stays clean. Only files that are still unsmudged pointers are
replaced: local edits are never overwritten, and missing files (deleted, or
outside a sparse checkout) stay missing. In a fresh clone, pull configures the
bigstore git filters if they are not set yet, and it adds the filter process
to a clone configured by an older version. If pull is interrupted, the
next pull repairs the files it had started checking out.

```bash
git bigstore pull              # pull all tracked files
git bigstore pull "*.bin"      # pull only .bin files
git bigstore pull --jobs 4     # limit to 4 concurrent downloads
```

### `git bigstore status [--verify]`

Show the state of each tracked large file:

```
                            ok  models/bert.bin
        cached (not checked out)  models/gpt2.bin
       pointer only (needs pull)  data/train.bin
       missing from working tree  data/old.bin
not a pointer in git (git add --renormalize)  legacy/raw.bin
```

Use `--verify` to re-hash cached objects and detect corruption:

```bash
git bigstore status --verify
```

Reports `CORRUPTED (hash mismatch)` for bad cache entries and exits non-zero
with repair guidance.

### `git bigstore log [paths...]`

Show history of bigstore-tracked files with change classification:

```
  a1b2c3d 2024-01-15 12:00:00 +0000 update model
    ~ models/bert.bin  sha256:abc123..def456 -> sha256:789abc..def012

  d4e5f6a 2024-01-14 10:00:00 +0000 add training data
    + data/train.bin  sha256:111222..333444
```

Symbols: `+` added, `-` deleted, `~` modified, `R` renamed, `C` copied.

### `git bigstore ref <source.dvc> <dest>`

Create a bigstore pointer from a DVC file. Imports the object from the DVC
cache (`.dvc/cache/`) into the bigstore cache with hash verification.

```bash
git bigstore ref model.bin.dvc model.bin
echo 'model.bin filter=bigstore' >> .gitattributes
git add model.bin .gitattributes
git commit -m "migrate model from DVC"
git bigstore push
```

### `git bigstore dvc-ls <source.dvc>`

List files in a DVC `.dir` manifest:

```bash
git bigstore dvc-ls models.dvc
# 17 entries in models.dvc (manifest md5:0f0d92...)
#   28a6a97b...  exports/model.onnx
#   46ce4109...  exports/model.onnx.data
```

### `git bigstore import-dvc-dir <source.dvc> <dest> [patterns...]`

Import files from a DVC `.dir` manifest into bigstore. Content is restored to
the working tree automatically.

```bash
# Import everything
git bigstore import-dvc-dir models.dvc models/

# Import selectively
git bigstore import-dvc-dir models.dvc models/ "exports/*.onnx"

# Overwrite existing files
git bigstore import-dvc-dir models.dvc models/ --force
```

### `git bigstore migrate-config`

Migrate legacy `.bigstore` config to `.bigstore.toml`.

```bash
git bigstore migrate-config
git add .bigstore.toml
git rm .bigstore
git commit -m "migrate config to toml"
```

## Configuration

### `.bigstore.toml`

Created by `init`. Committed to the repo so all collaborators share the same
backend.

```toml
layout = "files/{hash_fn}/{prefix}/{rest}"

[backend]
type = "s3"
bucket = "my-bucket"
prefix = "bigstore"
```

The `layout` field controls how objects are stored remotely. The default layout
is DVC-compatible (`files/{hash_fn}/{prefix}/{rest}`).

### `.gitattributes`

Standard git mechanism for declaring which files use the bigstore filter.
bigstore asks git which files have `filter=bigstore` (`git check-attr`), so
nested `.gitattributes` files, `.git/info/attributes` and macros apply exactly
as they do for the filter itself:

```gitattributes
*.bin filter=bigstore
*.safetensors filter=bigstore
models/** filter=bigstore
```

### Pointers

Tracked files are replaced in git with small pointer files:

```
bigstore
sha256
a1b2c3d4e5f6...  (64-character hex digest)
```

Pointers are 3 lines, ~81 bytes. The clean filter creates them on `git add`;
the smudge filter restores the real content on checkout (if cached locally).
Only an exact pointer is treated as one: any other content — including text
that happens to start with `bigstore` — is stored as a large file.

New content always gets a sha256 pointer. A file whose committed pointer is
md5 (committed DVC pointer text) keeps that md5 pointer as long as its content
matches it, so re-running the clean filter — as git does after checkout —
never shows it as modified. git passes the path to the filter (`pathname` in
the filter process, `%f` for one-shot clean) so it can look the pointer up in
the index.

The object cache lives in the repository's common git directory
(`.git/bigstore/objects`), shared by all linked worktrees.

## Concurrency

Push and pull run up to 8 transfers concurrently by default. Override with
`--jobs`:

```bash
git bigstore push --jobs 16
git bigstore pull --jobs 1     # sequential
```

Or set `BIGSTORE_JOBS` as a default:

```bash
export BIGSTORE_JOBS=16
git bigstore push              # uses 16
git bigstore push --jobs 4     # CLI flag wins
```

## DVC migration

bigstore can import files tracked by DVC, verified against the DVC cache.

### Cache discovery

bigstore resolves the DVC cache location by running `dvc cache dir`. This means
shared/global caches (`dvc cache dir --global ~/.dvc/cache`) work automatically.
If `dvc` is not installed, bigstore falls back to `.dvc/cache` in the DVC
project directory.

### Which `.dvc` files

`ref`, `dvc-ls` and `import-dvc-dir` read any single-output DVC 3 `.dvc`
(`hash: md5`), including stage fields (`dvc import-url`'s `md5:`, `frozen:`,
`deps:`) and annotations (`meta:`, `desc:`, `labels:`...), which they
ignore. They refuse, saying why, what has no object in the DVC cache:
`cache: false`, outputs not yet downloaded (`--no-download`), cloud outputs
tracked only by etag/version_id, and DVC 2 pointers (no `hash:` field).

### Single-file migration

```bash
git bigstore ref model.bin.dvc model.bin
echo 'model.bin filter=bigstore' >> .gitattributes
git add model.bin .gitattributes
git commit -m "migrate model from DVC"
git bigstore push
```

### Directory migration

Most DVC repos use `.dir` tracking. Inspect first, then import:

```bash
# List contents
git bigstore dvc-ls models.dvc

# Import all (or use glob patterns for selective import)
git bigstore import-dvc-dir models.dvc models/

# Stage, commit, push
echo 'models/** filter=bigstore' >> .gitattributes
git add models/ .gitattributes
git commit -m "migrate models from DVC"
git bigstore push
```

### Migration playbook

Tested against a real monorepo with 34 .dvc files across nested DVC projects.

**Prerequisites:**

1. **Consolidate DVC cache** (recommended for multi-worktree repos):
   ```bash
   dvc cache dir --global ~/.dvc/cache
   # Move per-project caches into global cache
   ```

2. **Populate the DVC cache** — objects must be pulled locally before import:
   ```bash
   dvc pull path/to/file.dvc
   ```

3. **Set credentials** — `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for push.

**Per-artifact workflow:**

1. Classify: `single-file` (use `ref`) or `.dir` (use `import-dvc-dir`)
2. Import — objects are md5-verified from DVC cache
3. **Edit DVC `.gitignore`** — DVC auto-generates `.gitignore` files next to
   `.dvc` files that ignore the output paths. Remove the relevant entries so
   `git add` can stage the bigstore-tracked files.
4. `git add` — the clean filter re-hashes content as sha256 (bigstore's native
   hash). The md5 cache entries from DVC import remain for deduplication.
5. `git bigstore status --verify` — confirm all files are ok
6. Commit and push

**What to watch for:**

- DVC sibling `.gitignore` files must be edited per migrated output path.
  Without this, `git add` silently ignores the imported files.
- Content is auto-restored to the working tree after import (real data, not
  pointer text). The clean filter converts back to pointers on `git add`.
- If `git-bigstore` is not in PATH, set full filter paths before `git add`:
  ```bash
  git config filter.bigstore.clean "/path/to/git-bigstore filter-clean %f"
  git config filter.bigstore.smudge "/path/to/git-bigstore filter-smudge"
  git config filter.bigstore.required true
  git config filter.bigstore.process "/path/to/git-bigstore filter-process"
  ```

### Pull fallback

During `git bigstore pull`, if an md5-hashed object is not on the remote but
exists in the local DVC cache, bigstore imports it automatically with
verification.

### Storage compatibility

The default storage layout (`files/{hash_fn}/{prefix}/{rest}`) is
DVC-compatible. Objects uploaded by bigstore can coexist with DVC objects in the
same bucket.

## Folder mode: DVC-compatible backup without git

`git bigstore folder` (and the `bigstore::folder` library) backs up plain
folders, with no git repository involved, in DVC 3's exact format. Real DVC can
`dvc pull`/`dvc status` what it writes, and it can pull what `dvc push` wrote.

```bash
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
export AWS_ENDPOINT_URL=https://s3.ap-southeast-2.wasabisys.com AWS_REGION=ap-southeast-2
R=s3://my-bucket/dvc

# Back up a directory or a single file; writes <name>.dvc beside it, naming
# the version pushed as its base.
git bigstore folder push ds/annotations/reviewer=rick/host=mac \
    --history ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R
git bigstore folder push ds/store.toml --history ST032/Beatons/store.toml --remote $R

# What push would upload, and whether this is the latest version; writes
# nothing (same arguments as push).
git bigstore folder status ds/annotations/reviewer=rick/host=mac \
    --history ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R

# Every version ever pushed, oldest first, with the versions each follows.
git bigstore folder log ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R

# Every history key, or those under a prefix (whole path components):
# here, every reviewer and host that pushed annotations.
git bigstore folder keys ST032/Beatons/annotations --remote $R

# Restore: from a .dvc file, or any version from history (which writes
# /tmp/v1.dvc naming it, once every file is in place).
git bigstore folder pull ds/store.toml.dvc --remote $R
git bigstore folder pull --history ST032/Beatons/annotations/reviewer=rick/host=mac \
    --at 9f9acd0a --into /tmp/v1 --remote $R

# Catch up with another host's push: replace (or remove) only the files
# still as the .dvc beside the output records them; any local change is
# refused, and nothing is written.
git bigstore folder pull --history ST032/Beatons/views/default --if-unchanged \
    --into ds/views/default --remote $R

# After a fork: reconcile the heads into one output whose .dvc names one of
# them, then record a version that follows them all.
git bigstore folder push ds/annotations/reviewer=rick/host=mac --resolve merge \
    --history ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R
```

What it guarantees:

- **Byte-exact DVC 3.** It writes `.dir` manifests and `.dvc` pointers exactly
  as DVC 3.67.1 does, including `hash: md5`. Objects go to `files/md5/xx/rest`,
  and a manifest is uploaded only after every object it lists. CI checks this
  against the real DVC, in both directions, on Linux and Windows.
- **Safe with files being appended to.** Each file is copied to a private
  snapshot while being hashed, so an uploaded object always matches its key.
  Files that change or vanish mid-push are retried; after 3 tries push fails.
  A `.jsonl` without a final newline is a warning.
- **History without git.** Every push that changes an output records a
  version under `bigstore-history/<key>/` on the remote; a push that changes
  nothing adds nothing. History is a graph, not a timeline: each version
  names the versions it follows, and the latest version is the one no other
  follows (the *head*). See [History](#history-bases-forks-and-merges)
  below. It is kept as records rather than S3 bucket versioning because
  object_store cannot list object versions, a bucket's versioning setting
  can't be checked cheaply or tested on `local://`, and a lifecycle rule or
  delete marker would drop history silently.
- **Pull never destroys local work.** It refuses to replace a file that
  differs unless told to (`--if-unchanged` replaces only files still as the
  `.dvc`'s base has them; `--force` replaces any), deletes nothing but what
  `--if-unchanged` proves unchanged since the base (it is in that version
  on the remote), never writes through a symlink, and writes via temp file
  plus rename (never a link). A single file DVC marked executable
  (`isexec: true` in its `.dvc`) is restored executable on unix (0777
  minus umask), and an identical copy
  already there is made executable. Push records no modes, so a version
  pulled from history never is. DVC's `dvc add` writes no modes into a
  `.dir` manifest; an entry that has one (a manifest hashed with per-file
  metadata) is refused, not restored without its mode.
- **Refuses ambiguity instead of guessing.** Push refuses directory and
  broken symlinks, nested `.git`/`.dvc`, `*.dvc` inside an output, and names
  that aren't portable (non-ASCII, or not allowed on Windows), so anything
  it pushes can be pulled on every OS. Empty directories are reported; DVC
  cannot record them.
- **Pull restores what this OS can create.** A manifest `dvc push` wrote may
  hold names push would refuse (`back\slash.txt`, non-ASCII). Pull restores
  them where the OS allows it; on Windows it refuses by name, before writing
  anything, `\` and `:` (they would change the path) and names Windows
  cannot create (`nul.txt`, `com1`, a trailing `.` or space, `*?"<>|`).
  Names that differ only by case (`Ä`/`ä` as well as `A`/`a`) or by Unicode
  normalization (`é` as one code point or as `e` plus an accent, as macOS
  and Linux may each write it) are refused everywhere, before anything is
  written: macOS and Windows would store them as one file.
- **Any path length, Windows included.** Push and pull work with paths
  longer than Windows' 260-character `MAX_PATH` whether or not the machine
  enables long paths (`LongPathsEnabled`): the renames that bypass std's own
  long-path handling are given verbatim `\\?\` paths. Paths in reports and
  errors stay in the form you passed.
- **S3 needs an endpoint** (`--endpoint` or `AWS_ENDPOINT_URL`). It never
  defaults to AWS and never falls back to instance-metadata credentials.
- **Skips OS junk.** `.DS_Store` (Finder writes one just by showing a
  folder), `._*` (AppleDouble files macOS writes beside files on FAT,
  exFAT and network volumes), `Thumbs.db` and `desktop.ini` are never backed
  up, at any depth, so opening a folder never makes a new version. Add more
  with `--exclude PATTERN` (repeatable; `PushOptions::exclude` in the
  library), using `.gitignore` rules relative to the pushed directory: `*.tmp`
  matches at any depth, `/cache` only at the top, `scratch/` only
  directories (a symlink to one included, as in DVC: it is skipped, never
  followed); `!` is not supported. A directory holding only skipped files
  counts as empty. Pull is unaffected.

Push records exactly what DVC 3 would, so if you also run `dvc add` on the
same folder, DVC must ignore the same files. Add these lines to the DVC
project's `.dvcignore`:

```gitignore
.DS_Store
._*
Thumbs.db
desktop.ini
```

plus any `--exclude` patterns: unanchored ones (`*.tmp`, `scratch/`) as they
are, and anchored ones prefixed with the folder's path from the project root
(`--exclude /cache` on `data/views` is `/data/views/cache`). With that, `dvc
add` gives the same `.dir` md5 as `folder push` (CI checks this).

Do not run `dvc gc --cloud` against this remote: DVC only knows the latest
`.dvc` files, and would delete the objects of older versions.

### History: bases, forks and merges

A version is a *record*: a valid DVC 3 `.dvc` pointer plus
`meta: {bigstore: {parents, writer, time}}`, stored as
`bigstore-history/<key>/<parents>/<id>.dvc`. `<id>` is 32 hex characters,
the first half of the SHA-256 of the record's bytes; `<parents>` is `root`
for a first version, else the ids it follows, sorted and `+`-joined (a merge
joins at most 8). Every name is new, so a record is written once and never
overwritten, which works on any S3-compatible store with no conditional
writes. One listing gives the whole graph; a record is fetched only for its
pointer, writer (`PushOptions::writer`, the host name by default) and time.
A record whose bytes do not hash to its id, or whose parents differ from
its name's, is refused as a bad record.

The `.dvc` beside an output records its *base*, the version it was last
pushed or pulled as, in `meta: {bigstore: {base: <id>}}` (DVC keeps and
ignores `meta:`). Push compares it with the history, from one listing,
before uploading anything:

1. Output equal to the only head: no new version; the head becomes the base.
   This also repairs a `.dvc` a crash left behind (see below).
2. A `.dvc` without a base (0.2 wrote it, or `dvc add` did) that records
   the only head's content counts as based on the head: the output was last
   synced to it.
3. No base while the history has versions, or a base that is not the only
   head: refused, `folder::Error::StaleBase { base, heads }`. Nothing is
   published. Set local changes aside, pull the latest version, redo them.
   An output unchanged since its base (`SyncState::RemoteAhead`) catches up
   with a pull of `Overwrite::IfUnchanged` (`--if-unchanged`), below.
4. Several heads: refused, `folder::Error::Diverged { heads }`, unless
   `--resolve merge` (`Resolve::Merge`) and the base is one of the heads;
   the new version then follows every head (at most 8, else
   `folder::Error::TooManyHeads`).
5. Otherwise objects, then the `.dir` manifest, then the record, then the
   `.dvc` with the new base: the base is written last.
6. History is listed again. If another push published from the same base
   meanwhile, both versions stand: that is a *fork*. The push is not a plain
   success: `PushReport::outcome` is `Pushed::Forked { record, with }`
   rather than `Pushed::Published { record }`, and the CLI exits non-zero.

Forks are detected, not prevented: plain S3 has no compare-and-swap that
every store honours (Wasabi accepted `If-None-Match` and overwrote anyway).
Until a merge joins the fork, `pull --at latest` is `Diverged` and so is
every push. `folder log` marks each head, fork point and merge:

```
2026-10-01T02:00:00.000Z  6f1c…  dir  4 files, 37 bytes  by mac  <- root  [fork: 2 children]
2026-10-01T02:05:00.000Z  24a7…  dir  4 files, 40 bytes  by mac  <- 6f1c…  [head]
2026-10-01T02:05:01.000Z  c925…  dir  5 files, 52 bytes  by pc  <- 6f1c…  [head]
```

To resolve it, pull one head by id over the output (`--at 24a7…`, which
makes it the base), bring in the other head's changes, and push with
`--resolve merge`.

Pull writes the base last, only once every file is in place, so a
cancelled or failed pull leaves the old base. Pulling an older version
(`--at <id or time>`) makes that version the base, so a push of changes
made to it is `StaleBase` rather than silently replacing the latest. A
crash after a push published its record but before it wrote the `.dvc`
leaves a stale base; the next push finds the output equal to the head and
adopts it, publishing nothing.

**Catching up.** Pull with `Overwrite::IfUnchanged` (`--if-unchanged`)
brings an output that has not changed since its base to the version
pulled. It replaces a differing file only if the file still has the
content the base records, and removes a file the base had, still
unchanged, that the version pulled does not; every other differing file,
and a file the version removed but that changed locally, is
`PullConflict`, before anything is written. Each file is hashed again just
before it is replaced or removed, and one written meanwhile is left as it
is (`Refusal::ChangedWhilePulling`). Files the base never had are kept.
Only a pull from history has a base (the `.dvc` beside `into`); from a
`.dvc` file, or with none beside `into`, this refuses like
`Overwrite::Refuse`. It never falls back to `Force`.

`Selector::Id` (`--at <hex>`) matches a record id prefix as `folder log`
prints it, or the content id (md5) in a 0.2 record's name, which 0.2's log
printed; a prefix matching more than one version, of either kind, is
refused with every candidate listed. `Selector::AtOrBefore` (`--at
<time>`) picks the newest version by the time each record holds, fetching
every record.

**Upgrading from 0.2 is a hard cutover per key.** 0.2 named records
`<time>-<content id>.dvc` directly under the key. 0.3 reads them as a
straight line in time order (a 0.2 record's id is that of its file name)
and continues it, but never writes that form. A 0.2 client does not see
0.3 records (they sit one directory deeper), so it would keep extending the
0.2 line: 0.3 then reports a fork. Upgrade every writer of a key together.
A `.dvc` 0.2 wrote has no base. If it records the latest version's content,
its output counts as based on it (step 2 above), so the first 0.3 push of
local changes simply follows the head; if it records an older version, the
push is `StaleBase`.

What to push, and what push assumes:

- **Push the writer's directory.** The unit of push is one output: the
  directory one writer owns, e.g. `annotations/reviewer=rick/host=h` with
  every recording below it (`site=…/date=…/src_…`), under a history key that
  names it. Each version is then the writer's whole state at one moment, one
  `.dvc` and one history per writer. Every push reads and hashes every file
  of the output; unchanged files are not uploaded again.
- **Choose that granularity once.** Push writes `<name>.dvc` beside the
  output and refuses an output with any `*.dvc` inside it (DVC forbids
  nested outputs). After pushing `…/host=h/site=s1`, pushing `…/host=h` is
  refused until `site=s1.dvc` is deleted; after pushing `…/host=h`, its
  parent (which now holds `host=h.dvc`) is refused. Give a different output
  a different history key (one key's versions should all be one output), so
  switching granularity starts a new history.
- **A manifest on the remote means its objects are there.** When the
  directory's `.dir` manifest already exists on the remote, push uploads
  nothing and checks no objects (the report shows 0 uploaded, 0 already
  present): bigstore and DVC both upload a manifest only after every object
  it lists. That is wrong if objects were deleted behind their manifest: by
  hand, by a bucket lifecycle rule, or by a copy or sync of the bucket that
  stopped part way. Push then succeeds, and pulling that version fails on
  the missing object. To repair, delete the manifest
  (`files/md5/xx/<rest>.dir`, named by the `.dvc`'s `md5`) and push again:
  push then checks each object and uploads the missing ones. Single-file
  outputs always check their object.
- **Change detection is length plus mtime.** A file's length and mtime are
  read before and after it is copied, and the copy is retried if either
  moved, or if the bytes copied are not the length the file ended at.
  Appending always changes the length, so a file that is only appended to
  is always captured as a state it really had. A change that keeps the
  length (a rewrite in place, or a cut and an append of the same size) is
  seen only through the mtime: where the mtime is coarser than the change
  (FAT's 2 s, or a kernel clock tick of a few ms on some Linux
  filesystems), such a change during the copy can go unnoticed and the
  snapshot can mix old and new bytes of that file. Its object still matches
  its key, because the digest is taken from the copied bytes as they are
  written, and the next push, which reads every file again, records the
  file as it then is.

As a library (`default-features = false, features = ["ring"]` or
`["aws-lc-rs"]` drops the CLI's dependencies and keeps S3; see
[Cargo features](#cargo-features)):

```rust
use bigstore::folder::{
    self, Credentials, Excludes, HistoryKey, PointerSource, PullOptions, PushOptions, Remote,
    RemoteConfig,
};

let remote = Remote::open(&RemoteConfig {
    url: "s3://my-bucket/dvc".into(),
    endpoint: Some("https://s3.ap-southeast-2.wasabisys.com".into()),
    region: Some("ap-southeast-2".into()),
    credentials: Credentials::Static { access_key_id, secret_access_key },
})?;
let key = HistoryKey::new("ST032/Beatons/annotations/reviewer=rick/host=mac")?;
let report = folder::push(&remote, dir, &PushOptions {
    exclude: Excludes::new(["*.tmp", "/cache/"])?, // default: Excludes::default()
    ..PushOptions::new(key) // 8 jobs, default excludes, never cancelled
})?;
match report.outcome {
    folder::Pushed::Forked { with, .. } => { /* published, but the history forked: merge */ }
    _ => {} // Published, AlreadyLatest
}
let pulled = folder::pull(&remote, &PointerSource::File(dvc_file), &PullOptions::default())?;
```

`PushReport` is `#[must_use]`: a push that forked the history published
its version but is not a plain success, so look at `outcome`.

**Confined to a root.** With `PushOptions::root` or `PullOptions::root`
set, the output (and pull's `into`, or its `.dvc` source) is a path
relative to the root, of plain names only (`Refusal::OutsideRoot`
otherwise), and nothing from the root down to the output, nor the output
itself or its `.dvc`, may be a symlink or, on Windows, a junction or any
other reparse point (`Refusal::SymlinkedComponent`). The root itself may
be one. Use it when the caller owns a boundary, such as a dataset folder,
that an output named inside it must not leave. Below the output nothing
changes: pull never writes through a symlink, and push backs up a symlink
to a file with its target's content, as DVC does.

Build options from `PushOptions::new(history)`, `PullOptions::default()` or
`LogOptions::default()` and override fields with `..`, as above: options
added later get their defaults there, so a caller pinned to a revision
keeps compiling when it moves to the next one. A struct literal naming
every field does not.

Cancelling from another thread (a request handler, a UI button): every
clone of a `CancelToken` shares one flag. Push, status and pull check it
between files and between objects, and log between history records;
whatever is being hashed, uploaded or downloaded at that moment finishes
first. The call then returns
`folder::Error::Cancelled`. A cancelled push has published no history
record and written no `.dvc` (objects already uploaded stay; they are
content-addressed, and the next push skips them); the check before the
record is published is the last, after which the push completes. A
cancelled pull leaves every file as it was or fully restored, never partly
written, no temp files, and the `.dvc` beside it untouched.
`git bigstore folder` (push, status, pull and log) cancels this way on the
first Ctrl-C (a second one exits at once).

```rust
let cancel = folder::CancelToken::new();
let opts = PushOptions { cancel: cancel.clone(), ..PushOptions::new(key) };
std::thread::spawn(move || { /* later */ cancel.cancel() });
match folder::push(&remote, dir, &opts) {
    Err(e) if matches!(e.downcast_ref(), Some(folder::Error::Cancelled)) => { /* nothing published */ }
    other => { other?; }
}
```

Progress, for a bar or a job status: a `Progress` callback gets
`ProgressEvent::Started { phase, files, bytes }` when a phase begins
(`Phase::Hashing`, `Uploading` or `Downloading`; `bytes` is `None` when the
size is unknown up front, as for a directory's download) and
`Advanced { phase, files, bytes }` per finished file. It is called from
worker threads, possibly concurrently, so it must be `Send + Sync`; the
default does nothing and costs nothing. The library does not depend on
`indicatif`; the CLI draws its bars from these events.

```rust
use std::sync::atomic::{AtomicU64, Ordering};
let done = std::sync::Arc::new(AtomicU64::new(0));
let seen = done.clone();
let opts = PushOptions {
    progress: folder::Progress::new(move |event| {
        if let folder::ProgressEvent::Advanced { phase: folder::Phase::Uploading, bytes, .. } = event {
            seen.fetch_add(bytes, Ordering::Relaxed);
        }
    }),
    ..PushOptions::new(key)
};
```

History, from the library:

```rust
// Other writers' outputs, without holding any pointer: every key equal to
// or below the prefix (`None` lists all). One listing, no record fetched.
let under = HistoryKey::new("ST032/Beatons/annotations")?;
for key in folder::keys(&remote, Some(&under))? {
    // Every version of it, oldest first, records fetched 8 at a time.
    for v in folder::log(&remote, &key, &folder::LogOptions::default())? {
        println!("{}  {}  {}  <- {:?}", key.as_str(), v.time, v.id, v.parents);
    }
}
```

`keys` leaves out, silently, any key that `HistoryKey::new` would reject
(records another tool, or a hand, put under a non-portable name); nothing
bigstore pushes is affected.

Dry run: `folder::status` walks, snapshots and hashes the output exactly as
`push` does (and refuses what push refuses), asks the remote which contents
it has and fetches the history's heads, and writes nothing, on the remote or
beside the output. Snapshots go to a private temp directory one file at a
time. `sync` compares the output with the latest version, using the `.dvc`
beside it for its base and the content it had then:

```rust
let s = folder::status(&remote, dir, &opts)?; // the PushOptions push would get
println!("{} to upload ({} bytes)", s.to_upload, s.to_upload_bytes);
match s.sync {
    folder::SyncState::NoHistory => {}              // never pushed
    folder::SyncState::InSync => {}                 // push would add no version
    folder::SyncState::LocalAhead => {}             // changed since its base: push
    folder::SyncState::RemoteAhead { latest } => {} // newer version, no local change: pull IfUnchanged
    folder::SyncState::Stale { base, head } => {}   // changed, but the base is old: StaleBase
    folder::SyncState::Diverged { heads } => {}     // forked: merge
    _ => {}                                         // #[non_exhaustive]
}
```

`InSync` says push would record no new version. It may still rewrite the
`.dvc` beside the output, if that is missing or records another base.

Each function above blocks and runs its own tokio runtime; called from inside
a runtime it returns an error naming its async twin. The twins,
`push_async`, `status_async`, `pull_async`, `log_async` and `keys_async`,
take the same arguments and return the same results on the caller's tokio
runtime, which needs the I/O and time drivers (`#[tokio::main]` and
`Builder::enable_all` enable both); `current_thread` and `multi_thread` both
work. Their futures are `Send`, so they can be spawned with owned arguments,
and their file and hashing work runs on the runtime's blocking pool, never
on the thread polling them. `Remote::open` has no twin: it makes no request.

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let remote = std::sync::Arc::new(Remote::open(&config)?);
    let key = HistoryKey::new("ST032/Beatons/store.toml")?;
    let push = tokio::spawn({
        let (remote, key) = (remote.clone(), key.clone());
        async move { folder::push_async(&remote, Path::new("ds/store.toml"), &PushOptions::new(key)).await }
    });
    let report = push.await??;
    let versions = folder::log_async(&remote, &key, &folder::LogOptions::default()).await?;
    let source = PointerSource::File(report.pointer_path);
    folder::pull_async(&remote, &source, &PullOptions::default()).await?;
    Ok(())
}
```

Dropping an async call's future stops it like a crash at that point, never
leaving a partial file or object: hashing stops at the next file, a file
already downloaded is placed whole, and transfers under way are abandoned
(an S3 multipart upload may be left for the bucket's lifecycle rule). A push
dropped after publishing its record may not have written its `.dvc`; the
next push adopts the record. A `CancelToken` stops more gently: transfers
under way finish.

Errors are `anyhow::Error`. Every refusal, and every other outcome a caller
may want to act on, carries a `bigstore::folder::Error` in its chain, found
with `err.downcast_ref::<folder::Error>()` whatever context was added above
it. Match on it instead of on message text. Its `Display` says what
happened, never which flag to pass, a request URL or the remote's answer:
the remedy is the caller's to name (the CLI adds a `hint:` line), and the
ids and paths are fields. Both enums are `#[non_exhaustive]`.

| `folder::Error` | When |
| --- | --- |
| `Refused { path, reason }` | push or pull will not touch `path`; `reason` is a `folder::Refusal` (below). Nothing was published or written. |
| `OutputChanged { detail }` | files kept changing or vanishing through every retry; push again later |
| `PullConflict { paths }` | local files differ from the version and `PullOptions::overwrite` does not allow replacing them (`Refuse`: any; `IfUnchanged`: changed since the base); nothing written |
| `NoSuchVersion` | no version in history matches the selector, or there is none |
| `AmbiguousId { prefix, candidates }` | a version id prefix matches several versions (`candidates`, oldest first) |
| `StaleBase { base, heads }` | push: the output's base is not the latest version (another push landed, an older version was pulled, or there is no base while history has versions); nothing published |
| `Diverged { heads }` | the history has forked: pull of `Latest`, or push without `Resolve::Merge`; nothing written or published |
| `TooManyHeads { key, heads, max }` | a merge push of a history with more heads than one version can follow; nothing published |
| `NoHead { key }` | the history has versions but none is the latest (each follows another): a damaged remote |
| `InvalidVersionId { prefix }`, `InvalidTime { time }` | a `Selector::Id` that is not 8+ hex characters; a `Selector::AtOrBefore` that is not RFC 3339 |
| `EndpointRequired` | an `s3://` remote without an endpoint |
| `CredentialsMissing` | an `s3://` remote without both an access key id and a secret (unset or empty); no request made |
| `RemoteUnusable { url }` | `Remote::open` cannot make a client for `url` (a URL it cannot parse, no TLS crypto compiled in, a `local://` directory it cannot create); no request made |
| `UnsupportedRemote { url }` | anything but `s3://`, `local://` (`file://`) and `rclone://` |
| `InvalidExclude { pattern }` | an exclude pattern that does not compile (or uses `!`) |
| `InvalidHistoryKey { key }` | `HistoryKey::new` of a key that is not a relative `/`-separated path of portable names |
| `DestinationRequired` | a pull from history without `PullOptions::into` |
| `Cancelled` | the caller's `CancelToken` was cancelled; a push published no history record or `.dvc`, a pull wrote no partial file and left its `.dvc` |
| `Archived { key }` | the remote says object `key` (file, `.dir` manifest or history record) is archived and not restored (S3 `InvalidObjectState`); restore it and retry |

| `folder::Refusal` | Refused by | `path` is |
| --- | --- | --- |
| `NoFileName`, `NotUtf8Name` (of the output), `NonPortableName { detail }` (of the output), `DvcFile`, `NotFileOrDirectory` (a symlink or special file) | push, the output itself | the output |
| `ForeignPointer` (not a plain DVC 3 pointer: a stage, annotations, or a `meta:` that is not bigstore's own), `PointerForOtherOutput { other }` | push, and pull from history, the `.dvc` beside the output | the `.dvc` |
| `ControlFile` (`.git`, `.hg`, `.dvc`, `.dvcignore`, `*.dvc`), `SymlinkToDirectory`, `BrokenSymlink`, `SpecialFile`, `NonPortableName { detail }`, `NotUtf8Name` | push, inside a directory | relative to the output, `/`-separated |
| `PointerPathEscapes { output }` | pull, a `.dvc` naming an output outside its directory | the `.dvc` |
| `UnrestorablePointer` (not a DVC 3 pointer to one md5-addressed output: `cache: false`, etag-only, several outputs, `wdir:`…; stage fields and annotations are fine) | pull, the `.dvc` | the `.dvc` |
| `SymlinkedOutput`, `NotADirectory`, `NotRegularFile`, `AppearedWhilePulling`, `ChangedWhilePulling` (`IfUnchanged`: written between check and replace or remove) | pull, the destination | the filesystem path |
| `OutsideRoot` | push and pull with a `root`, a path that is absolute or holds `.`/`..` | the path as given |
| `SymlinkedComponent` | push and pull with a `root`: a symlink or reparse point from the root to the output, at the output or at its `.dvc` | the filesystem path |
| `CaseCollision { other }`, `UnwritableName` (`\` or `:` on Windows) | pull, the manifest | the manifest name |
| `ExecutableInDirectory` (`isexec` on a manifest entry, or on a directory output in its `.dvc`) | pull | the manifest name, or the `.dvc` |

```rust
match folder::pull(&remote, &source, &opts) {
    Ok(report) => { /* … */ }
    Err(e) => match e.downcast_ref::<folder::Error>() {
        Some(folder::Error::PullConflict { paths }) => { /* ask the user */ }
        Some(folder::Error::Refused { path, reason }) => { /* a stable code per reason */ }
        _ => return Err(e),
    },
}
```

## Comparison: bigstore vs Git LFS vs DVC

All three solve "large files in git." They differ in where control sits.

|  | **bigstore** | **Git LFS** | **DVC** |
|--|-------------|-------------|---------|
| Mechanism | Git clean/smudge filter | Git clean/smudge filter | Separate CLI, `.dvc` metafiles |
| Storage | Any S3-compatible bucket you own | Host's LFS server | Any remote (S3, GCS, SSH, etc.) |
| Pointer format | 3-line (`bigstore\nsha256\n<hex>`) | `version`, `oid`, `size` | YAML `.dvc` files |
| Server required | No (direct bucket access) | Yes (LFS HTTP API on host) | No |
| Billing | Your bucket costs | Host LFS quotas + bandwidth | Your bucket costs |
| DVC migration | Built-in (`ref`, `import-dvc-dir`) | None | N/A |
| File locking | No | Yes | No |
| Ecosystem support | Custom tooling | Broad (GitHub, GitLab, etc.) | ML/data pipelines |
| Integrity verification | Hash-verified on every transfer | Hash-verified | Hash-verified |

### When to use what

**Git LFS** when you want standard tooling with broad hosting support and don't
mind host-managed storage. Best for teams on GitHub/GitLab who want minimal
operational burden.

**DVC** when your large files are part of ML pipelines with versioned
experiments, parameters, and metrics. DVC is a data pipeline tool that happens
to store files, not a git extension.

**bigstore** when you want the git-native clean/smudge workflow with full
control over your object storage. No LFS server needed, no host quotas, works
with any S3-compatible bucket. Best for teams that already manage their own
infrastructure.

### Interop

**bigstore + DVC**: Content-level interop via `ref` and `import-dvc-dir`.
DVC-compatible storage layout allows coexistence in the same bucket. DVC is a
byte source for bigstore, not a shared pointer layer.

**bigstore + Git LFS**: Can coexist in one repo on different path patterns.
Migration from LFS: `git lfs pull`, change `.gitattributes` to
`filter=bigstore`, `git add`, push. No protocol-level interop — different
pointer formats, different transfer mechanisms.

**Same file path cannot use both filters.** Both bigstore and LFS use git
clean/smudge, so applying both to the same path will break.

### LFS transfer adapter

`git bigstore lfs-adapter` is a Git LFS custom transfer agent that lets LFS
clients upload/download from bigstore's bucket. No LFS API server needed — LFS
talks directly to your object store.

**Setup** (in an LFS-configured repo):

```bash
git config lfs.standalonetransferagent bigstore
git config lfs.customtransfer.bigstore.path git-bigstore
git config lfs.customtransfer.bigstore.args lfs-adapter
```

**Config resolution:**

1. `.bigstore.toml` (if present in repo)
2. `git config bigstore-lfs.url` + `bigstore-lfs.endpoint` (for LFS-only repos)

```bash
# LFS-only repo without .bigstore.toml:
git config bigstore-lfs.url s3://my-bucket/bigstore
git config bigstore-lfs.endpoint https://t3.storage.dev
```

**Object mapping:** LFS `oid sha256:<hex>` maps to `files/sha256/<2>/<rest>` —
the same key bigstore uses natively. Shared bytes, separate pointer formats.

**Scope and limits:**

- SHA-256 only (LFS OIDs are SHA-256; bigstore's native hash)
- Shared remote objects, separate local caches (LFS cache != bigstore cache)
- No locking support
- No pointer-format bridging — LFS pointers stay LFS, bigstore pointers stay bigstore
- Credentials via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (same as bigstore)

**When to use it:**

- Migrating a team from hosted LFS to owned bucket storage
- Letting LFS-native collaborators pull from a bigstore-managed bucket
- Coexisting LFS and bigstore in one org with shared object storage

## Legacy config

If your repo has a `.bigstore` file (no `.toml` extension), bigstore will load
it with a deprecation warning. Run `git bigstore migrate-config` to upgrade.

Repos with layout templates that omit `{hash_fn}` (e.g.,
`files/sha256/{prefix}/{rest}`) continue to work for SHA-256 objects. MD5/DVC
objects require the `{hash_fn}` placeholder — bigstore will error with a clear
message if the layout doesn't support the hash function.

## Downgrading

`init` and `pull` set `filter.bigstore.process` and add `%f` to
`filter.bigstore.clean`; versions before the filter process understand
neither. With such a binary every checkout, `git add` and `git status` of a
tracked file fails (`smudge filter bigstore failed`, `clean filter 'bigstore'
failed`, `unexpected argument`). Nothing is corrupted. Before running an
older binary, undo both in each clone, using the same binary path as
`filter.bigstore.smudge`:

```bash
git config --unset filter.bigstore.process
git config filter.bigstore.clean "git-bigstore filter-clean"
```

git then uses the older one-shot clean/smudge filters again.

## Troubleshooting

**"no bigstore config found"** — Run `git bigstore init <url>` first, or check
that `.bigstore.toml` is committed.

**"not found on remote"** — The object hasn't been pushed yet. Run
`git bigstore push` from a machine that has the file cached.

**"not in the local cache and not on the remote"** (push) — The pointer was
committed but its content was never uploaded. Push from the clone that
committed it.

**"not a pointer in git (git add --renormalize)"** — The file was committed
before its `filter=bigstore` rule existed, so git holds its raw content. Run
`git add --renormalize <path>` and commit to move it into bigstore. Until
then, checkout passes it through unchanged, buffering content over 8 MiB in
`.git/bigstore/tmp` (not the system temp dir).

**"pointer only (needs pull)"** — The file is tracked but not downloaded. Run
`git bigstore pull`.

**"integrity check failed"** — A downloaded or cached object doesn't match its
expected hash. This indicates corruption in transit or at rest. Delete the
corrupted cache entry and re-pull.

**"layout template does not contain {hash_fn}"** — Your `.bigstore.toml` uses a
legacy layout that only supports SHA-256. Update the layout to
`files/{hash_fn}/{prefix}/{rest}` to support MD5/DVC objects.

**"smudge filter bigstore failed"** or **"clean filter 'bigstore' failed"** on
every file — The configured binary has no `filter-process` command (an older
version, see [Downgrading](#downgrading)), or is not on PATH.
