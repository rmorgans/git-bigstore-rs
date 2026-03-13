use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Component, Path};

use crate::types::{HashFunction, Hexdigest, Pointer};

#[derive(Debug, Deserialize)]
struct DvcFile {
    outs: Vec<DvcOut>,
}

#[derive(Debug, Deserialize)]
struct DvcOut {
    md5: String,
    #[allow(dead_code)]
    size: u64,
    path: String,
}

/// Parse a .dvc file and return a single Pointer + output path.
/// Rejects multi-output .dvc files.
pub fn parse_dvc_pointer(path: &Path) -> Result<(Pointer, String)> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let dvc_file: DvcFile = serde_yaml::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    anyhow::ensure!(
        dvc_file.outs.len() == 1,
        "multi-output .dvc files not supported (found {} outputs in {})",
        dvc_file.outs.len(),
        path.display()
    );

    let out = &dvc_file.outs[0];
    let hash_fn = HashFunction::Md5;
    let hexdigest = Hexdigest::new(&out.md5, hash_fn)
        .with_context(|| format!("invalid md5 in {}", path.display()))?;
    let pointer = Pointer::new(hash_fn, hexdigest);

    Ok((pointer, out.path.clone()))
}

/// A validated entry from a `.dir` manifest.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub md5: Hexdigest,
    pub relpath: String,
}

/// Result of parsing a `.dvc` file — either a single file or a directory manifest.
#[derive(Debug)]
#[allow(dead_code)] // File variant fields used in pattern matching
pub enum DvcKind {
    /// Single file output: pointer + output path.
    File(Pointer, String),
    /// Directory output: manifest hash, output path, path to the `.dir` manifest in DVC cache.
    Dir {
        manifest_hash: String,
        output_path: String,
    },
}

/// Parse a `.dvc` file and classify it as single-file or `.dir`.
pub fn parse_dvc_file(path: &Path) -> Result<DvcKind> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let dvc_file: DvcFile = serde_yaml::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    anyhow::ensure!(
        dvc_file.outs.len() == 1,
        "multi-output .dvc files not supported (found {} outputs in {})",
        dvc_file.outs.len(),
        path.display()
    );

    let out = &dvc_file.outs[0];

    if let Some(manifest_hash) = out.md5.strip_suffix(".dir") {
        // Validate the hash portion (without .dir suffix)
        let _ = Hexdigest::new(manifest_hash, HashFunction::Md5)
            .with_context(|| format!("invalid md5 in {}", path.display()))?;
        Ok(DvcKind::Dir {
            manifest_hash: manifest_hash.to_string(),
            output_path: out.path.clone(),
        })
    } else {
        let hexdigest = Hexdigest::new(&out.md5, HashFunction::Md5)
            .with_context(|| format!("invalid md5 in {}", path.display()))?;
        let pointer = Pointer::new(HashFunction::Md5, hexdigest);
        Ok(DvcKind::File(pointer, out.path.clone()))
    }
}

/// Parse a `.dir` manifest JSON file, returning validated entries.
///
/// Every `relpath` is checked for path traversal (no `..`, no absolute paths).
pub fn parse_dir_manifest(manifest_path: &Path) -> Result<Vec<DirEntry>> {
    let content = std::fs::read(manifest_path)
        .with_context(|| format!("failed to read manifest {}", manifest_path.display()))?;

    #[derive(Deserialize)]
    struct RawEntry {
        md5: String,
        relpath: String,
    }

    let raw: Vec<RawEntry> = serde_json::from_slice(&content)
        .with_context(|| format!("failed to parse manifest JSON {}", manifest_path.display()))?;

    let mut entries = Vec::with_capacity(raw.len());
    for entry in &raw {
        validate_relpath(&entry.relpath)?;
        let md5 = Hexdigest::new(&entry.md5, HashFunction::Md5)
            .with_context(|| format!("invalid md5 for relpath {:?}", entry.relpath))?;
        entries.push(DirEntry {
            md5,
            relpath: entry.relpath.clone(),
        });
    }

    Ok(entries)
}

/// Reject relpaths that could escape the destination directory.
fn validate_relpath(relpath: &str) -> Result<()> {
    anyhow::ensure!(!relpath.is_empty(), "manifest entry has empty relpath");

    let path = Path::new(relpath);
    anyhow::ensure!(
        !path.is_absolute(),
        "manifest relpath must be relative, got: {relpath:?}"
    );
    for component in path.components() {
        match component {
            Component::ParentDir => {
                anyhow::bail!("manifest relpath must not contain '..': {relpath:?}");
            }
            Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("manifest relpath must be relative: {relpath:?}");
            }
            Component::CurDir => {
                anyhow::bail!("manifest relpath must not contain '.': {relpath:?}");
            }
            Component::Normal(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_dvc_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "ab".repeat(16);
        std::fs::write(
            tmp.path(),
            format!("outs:\n- md5: {md5}\n  size: 12345\n  path: model.bin\n"),
        )
        .unwrap();
        let (pointer, out_path) = parse_dvc_pointer(tmp.path()).unwrap();
        assert_eq!(pointer.hash_fn, HashFunction::Md5);
        assert_eq!(pointer.hexdigest.to_string(), md5);
        assert_eq!(out_path, "model.bin");
    }

    #[test]
    fn rejects_multi_output() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "ab".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(
                "outs:\n- md5: {md5}\n  size: 100\n  path: a.bin\n- md5: {md5}\n  size: 200\n  path: b.bin\n"
            ),
        )
        .unwrap();
        assert!(parse_dvc_pointer(tmp.path()).is_err());
    }

    #[test]
    fn rejects_invalid_md5() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "outs:\n- md5: not-a-valid-hash\n  size: 100\n  path: a.bin\n",
        )
        .unwrap();
        assert!(parse_dvc_pointer(tmp.path()).is_err());
    }

    // --- DvcKind / .dir tests ---

    #[test]
    fn parse_dvc_file_single() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "ab".repeat(16);
        std::fs::write(
            tmp.path(),
            format!("outs:\n- md5: {md5}\n  size: 100\n  path: model.bin\n"),
        )
        .unwrap();
        match parse_dvc_file(tmp.path()).unwrap() {
            DvcKind::File(p, path) => {
                assert_eq!(p.hexdigest.to_string(), md5);
                assert_eq!(path, "model.bin");
            }
            DvcKind::Dir { .. } => panic!("expected File"),
        }
    }

    #[test]
    fn parse_dvc_file_dir() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "ab".repeat(16);
        std::fs::write(
            tmp.path(),
            format!("outs:\n- md5: {md5}.dir\n  size: 12345\n  path: models\n"),
        )
        .unwrap();
        match parse_dvc_file(tmp.path()).unwrap() {
            DvcKind::Dir {
                manifest_hash,
                output_path,
            } => {
                assert_eq!(manifest_hash, md5);
                assert_eq!(output_path, "models");
            }
            DvcKind::File(..) => panic!("expected Dir"),
        }
    }

    #[test]
    fn parse_dir_manifest_valid() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5a = "aa".repeat(16);
        let md5b = "bb".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(
                r#"[{{"md5":"{md5a}","relpath":"weights/model.pt"}},{{"md5":"{md5b}","relpath":"exports/out.onnx"}}]"#
            ),
        )
        .unwrap();
        let entries = parse_dir_manifest(tmp.path()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].relpath, "weights/model.pt");
        assert_eq!(entries[1].relpath, "exports/out.onnx");
        assert_eq!(entries[0].md5.to_string(), md5a);
    }

    #[test]
    fn parse_dir_manifest_rejects_parent_dir() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "aa".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(r#"[{{"md5":"{md5}","relpath":"../etc/passwd"}}]"#),
        )
        .unwrap();
        let err = parse_dir_manifest(tmp.path()).unwrap_err();
        assert!(err.to_string().contains(".."), "{err}");
    }

    #[test]
    fn parse_dir_manifest_rejects_absolute_path() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "aa".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(r#"[{{"md5":"{md5}","relpath":"/etc/passwd"}}]"#),
        )
        .unwrap();
        let err = parse_dir_manifest(tmp.path()).unwrap_err();
        assert!(
            err.to_string().contains("relative"),
            "expected 'relative' in error: {err}"
        );
    }

    #[test]
    fn parse_dir_manifest_rejects_empty_relpath() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "aa".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(r#"[{{"md5":"{md5}","relpath":""}}]"#),
        )
        .unwrap();
        assert!(parse_dir_manifest(tmp.path()).is_err());
    }

    #[test]
    fn parse_dir_manifest_rejects_dot_relpath() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "aa".repeat(16);
        std::fs::write(
            tmp.path(),
            format!(r#"[{{"md5":"{md5}","relpath":"."}}]"#),
        )
        .unwrap();
        let err = parse_dir_manifest(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("."), "{err}");
    }

    #[test]
    fn parse_dir_manifest_rejects_bad_md5() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            r#"[{"md5":"not-valid","relpath":"file.bin"}]"#,
        )
        .unwrap();
        assert!(parse_dir_manifest(tmp.path()).is_err());
    }
}
