use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

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
        assert_eq!(pointer.hexdigest.as_str(), md5);
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
}
