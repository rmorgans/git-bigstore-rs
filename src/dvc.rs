use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

use crate::types::{HashFunction, Hexdigest, Pointer, RepoPath};

#[derive(Debug, Deserialize)]
struct DvcFile {
    outs: Vec<DvcOut>,
}

#[derive(Debug, Deserialize)]
struct DvcOut {
    md5: String,
    path: String,
}

/// A validated entry from a `.dir` manifest.
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub md5: Hexdigest,
    pub relpath: RepoPath,
}

/// A parsed single-output `.dvc` file.
#[derive(Debug)]
pub enum DvcKind {
    /// Single file output: its md5 pointer and the output path DVC recorded.
    File { pointer: Pointer, path: String },
    /// Directory output: the md5 of its `.dir` manifest in the DVC cache.
    Dir { manifest: Hexdigest },
}

/// Parse a `.dvc` file and classify it as single-file or `.dir`.
/// Rejects multi-output `.dvc` files.
pub fn parse_dvc_file(path: &Path) -> Result<DvcKind> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let dvc_file: DvcFile = serde_yaml_ng::from_str(&content)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    let [out] = &dvc_file.outs[..] else {
        anyhow::bail!(
            "multi-output .dvc files not supported (found {} outputs in {})",
            dvc_file.outs.len(),
            path.display()
        );
    };

    let (hex, is_dir) = match out.md5.strip_suffix(".dir") {
        Some(hex) => (hex, true),
        None => (out.md5.as_str(), false),
    };
    let md5 = Hexdigest::new(hex, HashFunction::Md5)
        .with_context(|| format!("invalid md5 in {}", path.display()))?;
    Ok(if is_dir {
        DvcKind::Dir { manifest: md5 }
    } else {
        DvcKind::File {
            pointer: Pointer::new(md5),
            path: out.path.clone(),
        }
    })
}

/// Parse a `.dir` manifest JSON file, returning validated entries.
///
/// Every `relpath` must be a relative path that stays inside the directory.
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

    raw.into_iter()
        .map(|entry| {
            let relpath = RepoPath::new(&entry.relpath).with_context(|| {
                format!(
                    "manifest relpath must be a relative path inside the directory: {:?}",
                    entry.relpath
                )
            })?;
            let md5 = Hexdigest::new(&entry.md5, HashFunction::Md5)
                .with_context(|| format!("invalid md5 for relpath {:?}", entry.relpath))?;
            Ok(DirEntry { md5, relpath })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dvc_file_single() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "ab".repeat(16);
        std::fs::write(
            tmp.path(),
            format!("outs:\n- md5: {md5}\n  size: 12345\n  path: model.bin\n"),
        )
        .unwrap();
        match parse_dvc_file(tmp.path()).unwrap() {
            DvcKind::File { pointer, path } => {
                assert_eq!(pointer.hash_fn(), HashFunction::Md5);
                assert_eq!(pointer.hexdigest().to_string(), md5);
                assert_eq!(path, "model.bin");
            }
            DvcKind::Dir { .. } => panic!("expected File"),
        }
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
        assert!(parse_dvc_file(tmp.path()).is_err());
    }

    #[test]
    fn rejects_invalid_md5() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "outs:\n- md5: not-a-valid-hash\n  size: 100\n  path: a.bin\n",
        )
        .unwrap();
        assert!(parse_dvc_file(tmp.path()).is_err());
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
            DvcKind::Dir { manifest } => {
                assert_eq!(manifest.to_string(), md5);
                assert_eq!(manifest.hash_fn(), HashFunction::Md5);
            }
            DvcKind::File { .. } => panic!("expected Dir"),
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
        assert_eq!(entries[0].relpath.as_str(), "weights/model.pt");
        assert_eq!(entries[1].relpath.as_str(), "exports/out.onnx");
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
        std::fs::write(tmp.path(), format!(r#"[{{"md5":"{md5}","relpath":""}}]"#)).unwrap();
        assert!(parse_dir_manifest(tmp.path()).is_err());
    }

    #[test]
    fn parse_dir_manifest_rejects_dot_relpath() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let md5 = "aa".repeat(16);
        std::fs::write(tmp.path(), format!(r#"[{{"md5":"{md5}","relpath":"."}}]"#)).unwrap();
        let err = parse_dir_manifest(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("."), "{err}");
    }

    #[test]
    fn parse_dir_manifest_rejects_bad_md5() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), r#"[{"md5":"not-valid","relpath":"file.bin"}]"#).unwrap();
        assert!(parse_dir_manifest(tmp.path()).is_err());
    }
}
