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
configures git clean/smudge filters.

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
bigstore git filters if they are not set yet. If pull is interrupted, the
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
never shows it as modified. git passes the path to the clean filter (`%f`) so
it can look the pointer up in the index.

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

# Back up a directory or a single file; writes <name>.dvc beside it.
git bigstore folder push ds/annotations/reviewer=rick/host=mac \
    --history ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R
git bigstore folder push ds/store.toml --history ST032/Beatons/store.toml --remote $R

# Every version ever pushed, oldest first.
git bigstore folder log ST032/Beatons/annotations/reviewer=rick/host=mac --remote $R

# Restore: from a .dvc file, or any version from history.
git bigstore folder pull ds/store.toml.dvc --remote $R
git bigstore folder pull --history ST032/Beatons/annotations/reviewer=rick/host=mac \
    --at 9f9acd0a --into /tmp/v1 --remote $R
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
- **History without git.** Every push that changes an output appends its
  pointer to `bigstore-history/<key>/` on the remote. A push that changes
  nothing adds nothing. Versions are ordered by push time; each record is a
  valid `.dvc` file.
- **Pull never destroys local work.** It refuses to replace a file that
  differs unless forced, never deletes files missing from the version, never
  writes through a symlink, and writes via temp file plus rename (never a
  link).
- **Refuses ambiguity instead of guessing.** It refuses directory and broken
  symlinks, nested `.git`/`.dvc`, `*.dvc` inside an output, and names that
  aren't portable (non-ASCII, or not allowed on Windows). Empty directories
  are reported; DVC cannot record them.
- **S3 needs an endpoint** (`--endpoint` or `AWS_ENDPOINT_URL`). It never
  defaults to AWS and never falls back to instance-metadata credentials.

Do not run `dvc gc --cloud` against this remote: DVC only knows the latest
`.dvc` files, and would delete the objects of older versions.

As a library (`default-features = false` drops the CLI's dependencies):

```rust
use bigstore::folder::{self, Credentials, HistoryKey, PushOptions, Remote, RemoteConfig};

let remote = Remote::open(&RemoteConfig {
    url: "s3://my-bucket/dvc".into(),
    endpoint: Some("https://s3.ap-southeast-2.wasabisys.com".into()),
    region: Some("ap-southeast-2".into()),
    credentials: Credentials::Static { access_key_id, secret_access_key },
})?;
let report = folder::push(&remote, dir, &PushOptions {
    history: HistoryKey::new("ST032/Beatons/annotations/reviewer=rick/host=mac")?,
    jobs: 8,
})?;
```

The functions block and run their own tokio runtime. Calling them from inside
a runtime returns an error; use `spawn_blocking`. A pull refused because of
differing local files returns a typed `folder::PullConflict` in the error
chain.

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
`git add --renormalize <path>` and commit to move it into bigstore.

**"pointer only (needs pull)"** — The file is tracked but not downloaded. Run
`git bigstore pull`.

**"integrity check failed"** — A downloaded or cached object doesn't match its
expected hash. This indicates corruption in transit or at rest. Delete the
corrupted cache entry and re-pull.

**"layout template does not contain {hash_fn}"** — Your `.bigstore.toml` uses a
legacy layout that only supports SHA-256. Update the layout to
`files/{hash_fn}/{prefix}/{rest}` to support MD5/DVC objects.
