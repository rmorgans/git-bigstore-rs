# bigstore

Large files in git, your bucket, one binary.

bigstore is a single-binary alternative to Git LFS. It stores large files in
user-owned cloud storage (S3, GCS, Azure, R2, Tigris, or any rclone remote)
using git clean/smudge filters. Files are content-addressed, integrity-verified,
and deduplicated.

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
remote. Optional glob patterns filter which files to push.

```bash
git bigstore push              # push all tracked files
git bigstore push "models/*"   # push only models
git bigstore push --jobs 16    # use 16 concurrent uploads
```

### `git bigstore pull [patterns...]`

Download objects from remote storage with integrity verification. Every
downloaded object is hash-verified before entering the local cache.

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

Standard git mechanism for declaring which files use the bigstore filter:

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
  git config filter.bigstore.clean "/path/to/git-bigstore filter-clean"
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

**"pointer only (needs pull)"** — The file is tracked but not downloaded. Run
`git bigstore pull`.

**"integrity check failed"** — A downloaded or cached object doesn't match its
expected hash. This indicates corruption in transit or at rest. Delete the
corrupted cache entry and re-pull.

**"layout template does not contain {hash_fn}"** — Your `.bigstore.toml` uses a
legacy layout that only supports SHA-256. Update the layout to
`files/{hash_fn}/{prefix}/{rest}` to support MD5/DVC objects.
