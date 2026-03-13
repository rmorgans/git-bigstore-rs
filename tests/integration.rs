use md5::Digest;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Helper to run git commands in a directory, panicking on failure.
fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("failed to run git {}: {e}", args.join(" ")));

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("git {} failed: {stderr}", args.join(" "));
    }

    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// Helper to run git-bigstore in a directory.
fn bigstore(dir: &Path, args: &[&str]) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_git-bigstore");
    Command::new(bin)
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("failed to run git-bigstore {}: {e}", args.join(" ")))
}

fn bigstore_ok(dir: &Path, args: &[&str]) -> String {
    let output = bigstore(dir, args);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "git-bigstore {} failed:\nstdout: {stdout}\nstderr: {stderr}",
            args.join(" ")
        );
    }
    String::from_utf8(output.stdout).unwrap()
}

struct TestRepo {
    repo_dir: PathBuf,
    storage_dir: PathBuf,
    _tmp: tempfile::TempDir,
}

impl TestRepo {
    fn new() -> Self {
        let tmp = tempfile::TempDir::new().unwrap();
        let repo_dir = tmp.path().join("repo");
        let storage_dir = tmp.path().join("storage");

        std::fs::create_dir_all(&repo_dir).unwrap();
        std::fs::create_dir_all(&storage_dir).unwrap();

        // Init git repo
        git(&repo_dir, &["init"]);
        git(&repo_dir, &["config", "user.email", "test@test.com"]);
        git(&repo_dir, &["config", "user.name", "Test"]);

        // Init bigstore with local backend
        let storage_url = format!("local://{}", storage_dir.display());
        bigstore_ok(&repo_dir, &["init", &storage_url]);

        // Override the git filter config to use the full path to the test binary
        // (git can't find "git-bigstore" in PATH during tests)
        let bin = env!("CARGO_BIN_EXE_git-bigstore");
        git(
            &repo_dir,
            &[
                "config",
                "filter.bigstore.clean",
                &format!("{bin} filter-clean"),
            ],
        );
        git(
            &repo_dir,
            &[
                "config",
                "filter.bigstore.smudge",
                &format!("{bin} filter-smudge"),
            ],
        );

        Self {
            repo_dir,
            storage_dir,
            _tmp: tmp,
        }
    }

    fn write_file(&self, name: &str, content: &[u8]) {
        let path = self.repo_dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    fn read_file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.repo_dir.join(name)).unwrap()
    }

    fn file_exists(&self, name: &str) -> bool {
        self.repo_dir.join(name).exists()
    }
}

// ──────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────

#[test]
fn init_creates_config_and_sets_git_filters() {
    let t = TestRepo::new();

    // .bigstore.toml should exist
    assert!(t.file_exists(".bigstore.toml"));

    // Git filters should be configured (we override to full path in TestRepo::new,
    // so just check they contain "filter-clean" / "filter-smudge")
    let clean = git(&t.repo_dir, &["config", "filter.bigstore.clean"]);
    assert!(clean.contains("filter-clean"), "clean filter not set: {clean}");
    let smudge = git(&t.repo_dir, &["config", "filter.bigstore.smudge"]);
    assert!(smudge.contains("filter-smudge"), "smudge filter not set: {smudge}");
}

#[test]
fn clean_filter_produces_pointer_file() {
    let t = TestRepo::new();

    // Set up .gitattributes
    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Create a large-ish file
    let content = vec![42u8; 1024 * 100]; // 100KB
    t.write_file("data.bin", &content);

    // Stage it — this triggers the clean filter
    git(&t.repo_dir, &["add", "data.bin"]);

    // What git stored should be a pointer, not the raw content
    let stored = git(&t.repo_dir, &["show", ":data.bin"]);
    let lines: Vec<&str> = stored.lines().collect();
    assert_eq!(lines[0], "bigstore", "first line should be 'bigstore'");
    assert_eq!(lines[1], "sha256", "second line should be 'sha256'");
    assert_eq!(
        lines[2].len(),
        64,
        "third line should be a 64-char hex digest"
    );
}

#[test]
fn full_lifecycle_push_pull_with_verification() {
    let t = TestRepo::new();

    // Set up .gitattributes
    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Create and commit a tracked file
    let original_content = b"hello bigstore! this is test content for verification.\n";
    t.write_file("test.bin", original_content);
    git(&t.repo_dir, &["add", "test.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add test.bin"]);

    // Push to remote (local filesystem backend)
    bigstore_ok(&t.repo_dir, &["push"]);

    // Verify something was uploaded to storage
    let storage_files: Vec<_> = walkdir::WalkDir::new(&t.storage_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();
    assert!(
        !storage_files.is_empty(),
        "should have uploaded at least one object to storage"
    );

    // Now simulate a fresh clone: delete the local cache and working tree file
    let git_dir_str = git(&t.repo_dir, &["rev-parse", "--git-dir"]);
    let cache_dir = PathBuf::from(&git_dir_str).join("bigstore");
    if cache_dir.exists() {
        std::fs::remove_dir_all(&cache_dir).unwrap();
    }

    // Write the pointer file back (simulating what checkout would show without smudge cache)
    let pointer_content = git(&t.repo_dir, &["show", "HEAD:test.bin"]);
    t.write_file("test.bin", pointer_content.as_bytes());

    // Pull — this should download, verify hash, and restore the file
    bigstore_ok(&t.repo_dir, &["pull"]);

    // The file should be restored with original content
    let restored = t.read_file("test.bin");
    assert_eq!(
        restored, original_content,
        "restored content should match original"
    );
}

#[test]
fn push_is_idempotent() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("test.bin", b"some content here\n");
    git(&t.repo_dir, &["add", "test.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add"]);

    // First push
    bigstore_ok(&t.repo_dir, &["push"]);
    // Second push — should skip (already uploaded)
    let output = bigstore(& t.repo_dir, &["push"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already up to date") || stderr.contains("0 file(s) uploaded"),
        "second push should skip: {stderr}"
    );
}

#[test]
fn clean_filter_is_idempotent() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("test.bin", b"content\n");
    git(&t.repo_dir, &["add", "test.bin"]);

    // Get the pointer
    let pointer1 = git(&t.repo_dir, &["show", ":test.bin"]);

    // Write the pointer as the file content and re-add
    // The clean filter should pass it through unchanged
    t.write_file("test.bin", pointer1.as_bytes());
    git(&t.repo_dir, &["add", "test.bin"]);

    let pointer2 = git(&t.repo_dir, &["show", ":test.bin"]);

    assert_eq!(pointer1, pointer2, "clean filter should be idempotent");
}

#[test]
fn status_shows_file_states() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("test.bin", b"content for status\n");
    git(&t.repo_dir, &["add", "test.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add"]);

    let output = bigstore_ok(&t.repo_dir, &["status"]);
    assert!(
        output.contains("test.bin"),
        "status should show tracked file"
    );
}

#[test]
fn multiple_files_tracked() {
    let t = TestRepo::new();

    t.write_file(
        ".gitattributes",
        b"*.bin filter=bigstore\nassets/** filter=bigstore\n",
    );
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("a.bin", b"file a\n");
    t.write_file("b.bin", b"file b\n");
    t.write_file("assets/model.dat", b"model data\n");
    git(
        &t.repo_dir,
        &["add", "a.bin", "b.bin", "assets/model.dat"],
    );
    git(&t.repo_dir, &["commit", "-m", "add files"]);

    // Push all
    bigstore_ok(&t.repo_dir, &["push"]);

    // Count uploaded objects
    let storage_files: Vec<_> = walkdir::WalkDir::new(&t.storage_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();
    assert_eq!(
        storage_files.len(),
        3,
        "should have 3 objects in storage (one per unique file)"
    );
}

// ──────────────────────────────────────────────────
// DVC interop tests
// ──────────────────────────────────────────────────

#[test]
fn ref_creates_md5_pointer_from_dvc_file() {
    let t = TestRepo::new();

    // Create content and compute its md5
    let content = b"model weights for ref test\n";
    let md5_hash = format!("{:x}", md5::Md5::digest(content));

    // Populate DVC cache so ref can import
    let dvc_cache_dir = t.repo_dir.join(".dvc/cache/files/md5");
    let shard = &md5_hash[..2];
    let rest = &md5_hash[2..];
    let cache_obj_dir = dvc_cache_dir.join(shard);
    std::fs::create_dir_all(&cache_obj_dir).unwrap();
    std::fs::write(cache_obj_dir.join(rest), content).unwrap();

    // Create a .dvc file
    t.write_file(
        "model.bin.dvc",
        format!("outs:\n- md5: {md5_hash}\n  size: {}\n  path: model.bin\n", content.len())
            .as_bytes(),
    );

    // Run ref command
    bigstore_ok(&t.repo_dir, &["ref", "model.bin.dvc", "model.bin"]);

    // Verify the pointer was created with correct content
    let pointer_content = t.read_file("model.bin");
    let pointer_str = String::from_utf8(pointer_content).unwrap();
    let lines: Vec<&str> = pointer_str.lines().collect();
    assert_eq!(lines[0], "bigstore");
    assert_eq!(lines[1], "md5");
    assert_eq!(lines[2], md5_hash);
}

#[test]
fn ref_rejects_missing_dvc_cache() {
    let t = TestRepo::new();

    // Create a .dvc file with a hash but NO corresponding DVC cache object
    let md5 = "ab".repeat(16);
    t.write_file(
        "model.bin.dvc",
        format!("outs:\n- md5: {md5}\n  size: 1234\n  path: model.bin\n").as_bytes(),
    );

    let output = bigstore(&t.repo_dir, &["ref", "model.bin.dvc", "model.bin"]);
    assert!(
        !output.status.success(),
        "ref should fail when DVC cache is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found in DVC cache"),
        "should tell user to dvc pull: {stderr}"
    );
}

#[test]
fn ref_imports_from_dvc_cache_with_verification() {
    let t = TestRepo::new();

    // Create content and compute its md5
    let content = b"hello from dvc cache\n";
    let md5_hash = format!("{:x}", md5::Md5::digest(content));

    // Populate DVC cache
    let dvc_cache_dir = t.repo_dir.join(".dvc/cache/files/md5");
    let shard = &md5_hash[..2];
    let rest = &md5_hash[2..];
    let cache_obj_dir = dvc_cache_dir.join(shard);
    std::fs::create_dir_all(&cache_obj_dir).unwrap();
    std::fs::write(cache_obj_dir.join(rest), content).unwrap();

    // Create .dvc file
    t.write_file(
        "data.bin.dvc",
        format!("outs:\n- md5: {md5_hash}\n  size: {}\n  path: data.bin\n", content.len())
            .as_bytes(),
    );

    // Run ref — should import from DVC cache
    let output = bigstore(&t.repo_dir, &["ref", "data.bin.dvc", "data.bin"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "ref should succeed: {stderr}"
    );
    assert!(
        stderr.contains("Imported from DVC cache"),
        "should report DVC cache import: {stderr}"
    );

    // Verify bigstore cache now has the object
    let bs_cache = t.repo_dir
        .join(".git/bigstore/objects/md5")
        .join(shard)
        .join(rest);
    assert!(bs_cache.exists(), "object should be in bigstore cache");

    // Verify the cached content matches
    let cached = std::fs::read(&bs_cache).unwrap();
    assert_eq!(cached, content);
}

#[test]
fn pull_falls_back_to_dvc_cache() {
    let t = TestRepo::new();

    // Set up .gitattributes
    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Create content and compute md5
    let content = b"content for dvc cache fallback test\n";
    let md5_hash = format!("{:x}", md5::Md5::digest(content));

    // Write an md5 pointer directly (simulating what ref + commit produces)
    let pointer = format!("bigstore\nmd5\n{md5_hash}\n");
    t.write_file("test.bin", pointer.as_bytes());
    git(&t.repo_dir, &["add", "test.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add md5 pointer"]);

    // Populate DVC cache (but NOT bigstore cache)
    let dvc_cache_dir = t.repo_dir.join(".dvc/cache/files/md5");
    let shard = &md5_hash[..2];
    let rest = &md5_hash[2..];
    let cache_obj_dir = dvc_cache_dir.join(shard);
    std::fs::create_dir_all(&cache_obj_dir).unwrap();
    std::fs::write(cache_obj_dir.join(rest), content).unwrap();

    // Write pointer back to working tree (simulating checkout without cache)
    t.write_file("test.bin", pointer.as_bytes());

    // Pull — should find it in DVC cache
    bigstore_ok(&t.repo_dir, &["pull"]);

    // The file should be restored
    let restored = t.read_file("test.bin");
    assert_eq!(restored, content, "should restore from DVC cache");
}

#[test]
fn ref_rejects_invalid_dvc_file() {
    let t = TestRepo::new();

    t.write_file("bad.dvc", b"this is not yaml at all: [[[");

    let output = bigstore(&t.repo_dir, &["ref", "bad.dvc", "out.bin"]);
    assert!(
        !output.status.success(),
        "ref should fail on invalid .dvc file"
    );
}

#[test]
fn pull_rejects_corrupted_dvc_cache() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Create content and compute md5
    let content = b"real content for corruption test\n";
    let md5_hash = format!("{:x}", md5::Md5::digest(content));

    // Write an md5 pointer
    let pointer = format!("bigstore\nmd5\n{md5_hash}\n");
    t.write_file("test.bin", pointer.as_bytes());
    git(&t.repo_dir, &["add", "test.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add md5 pointer"]);

    // Populate DVC cache with WRONG content
    let dvc_cache_dir = t.repo_dir.join(".dvc/cache/files/md5");
    let shard = &md5_hash[..2];
    let rest = &md5_hash[2..];
    let cache_obj_dir = dvc_cache_dir.join(shard);
    std::fs::create_dir_all(&cache_obj_dir).unwrap();
    std::fs::write(cache_obj_dir.join(rest), b"corrupted data!").unwrap();

    // Write pointer back to working tree
    t.write_file("test.bin", pointer.as_bytes());

    // Pull should fail integrity check
    let output = bigstore(&t.repo_dir, &["pull"]);
    assert!(
        !output.status.success(),
        "pull should fail with corrupted DVC cache"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("integrity") || stderr.contains("failed"),
        "should report integrity failure: {stderr}"
    );
}

#[test]
fn ref_rejects_path_traversal() {
    let t = TestRepo::new();

    let md5 = "ab".repeat(16);
    t.write_file(
        "model.dvc",
        format!("outs:\n- md5: {md5}\n  size: 100\n  path: model.bin\n").as_bytes(),
    );

    // dest with path traversal should be rejected
    let output = bigstore(&t.repo_dir, &["ref", "model.dvc", "../../etc/shadow"]);
    assert!(
        !output.status.success(),
        "ref should reject path traversal in dest"
    );

    // source with path traversal should be rejected
    let output = bigstore(&t.repo_dir, &["ref", "../../../etc/passwd", "out.bin"]);
    assert!(
        !output.status.success(),
        "ref should reject path traversal in source"
    );
}

#[test]
fn ref_rejects_multi_output_dvc_file() {
    let t = TestRepo::new();

    let md5 = "ab".repeat(16);
    t.write_file(
        "multi.dvc",
        format!(
            "outs:\n- md5: {md5}\n  size: 100\n  path: a.bin\n- md5: {md5}\n  size: 200\n  path: b.bin\n"
        ).as_bytes(),
    );

    let output = bigstore(&t.repo_dir, &["ref", "multi.dvc", "out.bin"]);
    assert!(
        !output.status.success(),
        "ref should reject multi-output .dvc files"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("multi-output"),
        "error should mention multi-output: {stderr}"
    );
}

#[test]
fn legacy_layout_sha256_works_md5_rejected() {
    let t = TestRepo::new();

    // Overwrite .bigstore.toml with a legacy layout (no {hash_fn})
    let legacy_config = format!(
        "layout = \"files/sha256/{{prefix}}/{{rest}}\"\n\n\
         [backend]\n\
         type = \"local\"\n\
         path = \"{}\"\n",
        t.storage_dir.display()
    );
    t.write_file(".bigstore.toml", legacy_config.as_bytes());

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // SHA-256 push/pull should work end-to-end
    let original_content = b"legacy layout sha256 content\n";
    t.write_file("data.bin", original_content);
    git(&t.repo_dir, &["add", "data.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add data.bin"]);

    bigstore_ok(&t.repo_dir, &["push"]);

    // Simulate fresh clone: clear cache, write pointer back
    let cache_dir = t.repo_dir.join(".git/bigstore");
    if cache_dir.exists() {
        std::fs::remove_dir_all(&cache_dir).unwrap();
    }
    let pointer_content = git(&t.repo_dir, &["show", "HEAD:data.bin"]);
    t.write_file("data.bin", pointer_content.as_bytes());

    bigstore_ok(&t.repo_dir, &["pull"]);
    let restored = t.read_file("data.bin");
    assert_eq!(restored, original_content, "sha256 should work with legacy layout");

    // MD5 pointer should fail with layout-migration error
    let content = b"md5 content\n";
    let md5_hash = format!("{:x}", md5::Md5::digest(content));
    let md5_pointer = format!("bigstore\nmd5\n{md5_hash}\n");
    t.write_file("md5.bin", md5_pointer.as_bytes());
    git(&t.repo_dir, &["add", "md5.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add md5 pointer"]);

    // Populate DVC cache so pull actually tries to resolve the key
    let dvc_cache_dir = t.repo_dir.join(".dvc/cache/files/md5");
    let shard = &md5_hash[..2];
    let rest = &md5_hash[2..];
    std::fs::create_dir_all(dvc_cache_dir.join(shard)).unwrap();
    std::fs::write(dvc_cache_dir.join(shard).join(rest), content).unwrap();

    t.write_file("md5.bin", md5_pointer.as_bytes());

    let output = bigstore(&t.repo_dir, &["pull"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("hash_fn") || stderr.contains("layout"),
        "md5 with legacy layout should fail with layout error: {stderr}"
    );
}

// ──────────────────────────────────────────────────
// Log tests
// ──────────────────────────────────────────────────

#[test]
fn log_shows_bigstore_file_history() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Commit 1: add a tracked file
    t.write_file("model.bin", b"version 1\n");
    git(&t.repo_dir, &["add", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add model v1"]);

    // Commit 2: update it
    t.write_file("model.bin", b"version 2\n");
    git(&t.repo_dir, &["add", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "update model v2"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // Should show both commits
    assert!(output.contains("update model v2"), "should show v2 commit: {output}");
    assert!(output.contains("add model v1"), "should show v1 commit: {output}");

    // Should show the file path
    assert!(output.contains("model.bin"), "should show file path: {output}");

    // Should show + for add and ~ for modify
    assert!(output.contains("+ model.bin"), "should show + for add: {output}");
    assert!(output.contains("~ model.bin"), "should show ~ for modify: {output}");
}

#[test]
fn log_filters_by_path() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("a.bin", b"file a\n");
    t.write_file("b.bin", b"file b\n");
    git(&t.repo_dir, &["add", "a.bin", "b.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add both"]);

    // Filter to only a.bin
    let output = bigstore_ok(&t.repo_dir, &["log", "a.bin"]);
    assert!(output.contains("a.bin"), "should show a.bin: {output}");
    assert!(!output.contains("b.bin"), "should NOT show b.bin: {output}");
}

#[test]
fn log_detects_renames() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("old.bin", b"some content for rename test\n");
    git(&t.repo_dir, &["add", "old.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add old.bin"]);

    // Rename via git mv
    git(&t.repo_dir, &["mv", "old.bin", "new.bin"]);
    git(&t.repo_dir, &["commit", "-m", "rename to new.bin"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // Should show R symbol with both paths
    assert!(output.contains("R old.bin -> new.bin"), "should show R with old -> new: {output}");
}

#[test]
fn log_shows_delete_as_minus() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    t.write_file("temp.bin", b"temporary data\n");
    git(&t.repo_dir, &["add", "temp.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add temp"]);

    git(&t.repo_dir, &["rm", "temp.bin"]);
    git(&t.repo_dir, &["commit", "-m", "delete temp"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);
    assert!(output.contains("- temp.bin"), "should show - for delete: {output}");
    assert!(output.contains("+ temp.bin"), "should also show + for the add: {output}");
}

#[test]
fn log_shows_changes_from_merge_commits() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Add a file on main
    t.write_file("base.bin", b"base content\n");
    git(&t.repo_dir, &["add", "base.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add base"]);

    // Create a feature branch, add a file there
    git(&t.repo_dir, &["checkout", "-b", "feature"]);
    t.write_file("feature.bin", b"feature content\n");
    git(&t.repo_dir, &["add", "feature.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add feature.bin on branch"]);

    // Switch back to main, merge feature
    git(&t.repo_dir, &["checkout", "main"]);
    git(&t.repo_dir, &["merge", "feature", "--no-ff", "-m", "merge feature"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // The merge commit should show the feature.bin addition
    assert!(
        output.contains("feature.bin"),
        "merge commit should show feature.bin: {output}"
    );
    assert!(
        output.contains("merge feature"),
        "should show the merge commit message: {output}"
    );
}

#[test]
fn log_ignores_non_bigstore_files() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Add a non-bigstore file
    t.write_file("readme.txt", b"hello\n");
    git(&t.repo_dir, &["add", "readme.txt"]);
    git(&t.repo_dir, &["commit", "-m", "add readme"]);

    // Add a bigstore file
    t.write_file("data.bin", b"binary data\n");
    git(&t.repo_dir, &["add", "data.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add data"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // Should show data.bin but not readme.txt
    assert!(output.contains("data.bin"), "should show data.bin: {output}");
    assert!(!output.contains("readme.txt"), "should NOT show readme.txt: {output}");
}

#[test]
fn log_nonpointer_to_pointer_shows_add() {
    let t = TestRepo::new();

    // Commit a regular file first (no bigstore filter)
    t.write_file("model.bin", b"plain content\n");
    git(&t.repo_dir, &["add", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add as plain file"]);

    // Now add .gitattributes to track *.bin, re-add the file so the
    // clean filter converts it to a pointer
    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    bigstore_ok(&t.repo_dir, &["init", &format!("local://{}", t.storage_dir.display())]);
    // Override filter paths for test binary
    let bin = env!("CARGO_BIN_EXE_git-bigstore");
    git(&t.repo_dir, &["config", "filter.bigstore.clean", &format!("{bin} filter-clean")]);
    git(&t.repo_dir, &["config", "filter.bigstore.smudge", &format!("{bin} filter-smudge")]);

    t.write_file("model.bin", b"plain content\n"); // same content, but now filtered
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "convert to bigstore"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // The conversion commit should show + (not ~) since old side wasn't a pointer
    assert!(
        output.contains("+ model.bin"),
        "non-pointer -> pointer should show + (add): {output}"
    );
}

#[test]
fn log_pointer_to_nonpointer_shows_delete() {
    let t = TestRepo::new();

    t.write_file(".gitattributes", b"*.bin filter=bigstore\n");
    git(&t.repo_dir, &["add", ".gitattributes", ".bigstore.toml"]);
    git(&t.repo_dir, &["commit", "-m", "init"]);

    // Add a bigstore file
    t.write_file("model.bin", b"tracked content\n");
    git(&t.repo_dir, &["add", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "add as bigstore"]);

    // Remove the filter and re-add as a plain file
    t.write_file(".gitattributes", b"# no filters\n");
    t.write_file("model.bin", b"now just a regular file\n");
    git(&t.repo_dir, &["add", ".gitattributes", "model.bin"]);
    git(&t.repo_dir, &["commit", "-m", "convert to plain file"]);

    let output = bigstore_ok(&t.repo_dir, &["log"]);

    // The conversion commit should show - (pointer removed)
    assert!(
        output.contains("- model.bin"),
        "pointer -> non-pointer should show - (delete): {output}"
    );
}

#[test]
fn log_root_commit_with_bigstore_file() {
    let tmp = tempfile::TempDir::new().unwrap();
    let repo_dir = tmp.path().join("repo");
    let storage_dir = tmp.path().join("storage");
    std::fs::create_dir_all(&repo_dir).unwrap();
    std::fs::create_dir_all(&storage_dir).unwrap();

    git(&repo_dir, &["init"]);
    git(&repo_dir, &["config", "user.email", "test@test.com"]);
    git(&repo_dir, &["config", "user.name", "Test"]);

    let storage_url = format!("local://{}", storage_dir.display());
    bigstore_ok(&repo_dir, &["init", &storage_url]);

    let bin = env!("CARGO_BIN_EXE_git-bigstore");
    git(&repo_dir, &["config", "filter.bigstore.clean", &format!("{bin} filter-clean")]);
    git(&repo_dir, &["config", "filter.bigstore.smudge", &format!("{bin} filter-smudge")]);

    // First (root) commit includes a bigstore file
    std::fs::write(repo_dir.join(".gitattributes"), b"*.bin filter=bigstore\n").unwrap();
    std::fs::write(repo_dir.join("initial.bin"), b"root commit data\n").unwrap();
    git(&repo_dir, &["add", ".gitattributes", ".bigstore.toml", "initial.bin"]);
    git(&repo_dir, &["commit", "-m", "root with bigstore file"]);

    let output = bigstore_ok(&repo_dir, &["log"]);
    assert!(
        output.contains("+ initial.bin"),
        "root commit should show + for bigstore file: {output}"
    );
}
