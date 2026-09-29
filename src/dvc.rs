//! DVC 3 file formats: `.dvc` pointers and `.dir` manifests.
//!
//! Writers are byte-exact with DVC 3.67.1 (goldens in
//! `tests/fixtures/dvc-3.67.1`): a `.dir` manifest's id is the md5 of its
//! bytes, so any formatting difference makes DVC report the data modified.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::path::Path;

use crate::hash::Hasher;
use crate::types::{HashFunction, Hexdigest, RepoPath};

/// An md5 digest; DVC 3 addresses everything with plain md5.
fn md5(hex: &str) -> Result<Hexdigest> {
    Hexdigest::new(hex, HashFunction::Md5)
}

/// One file in a `.dir` manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub relpath: RepoPath,
    /// Always md5.
    pub md5: Hexdigest,
}

/// A DVC directory manifest. Invariants (enforced by every constructor):
/// entries sorted by relpath string, relpaths unique, no entry is a directory
/// prefix of another (`a` and `a/b`), every digest md5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn from_entries(mut entries: Vec<ManifestEntry>) -> Result<Self> {
        entries.sort_by(|a, b| a.relpath.as_str().cmp(b.relpath.as_str()));
        for e in &entries {
            anyhow::ensure!(
                e.md5.hash_fn() == HashFunction::Md5,
                "manifest entry {} is not md5",
                e.relpath
            );
        }
        for pair in entries.windows(2) {
            let (a, b) = (pair[0].relpath.as_str(), pair[1].relpath.as_str());
            anyhow::ensure!(a != b, "duplicate manifest entry {a:?}");
        }
        // With string sort, "a/..." does not always sort right after "a"
        // ("a-x" < "a/b"), so check prefixes against a set of files.
        let files: std::collections::HashSet<&str> =
            entries.iter().map(|e| e.relpath.as_str()).collect();
        for e in &entries {
            let mut p = e.relpath.as_str();
            while let Some((parent, _)) = p.rsplit_once('/') {
                anyhow::ensure!(
                    !files.contains(parent),
                    "manifest has both a file and a directory named {parent:?}"
                );
                p = parent;
            }
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    /// DVC's serialisation: CPython `json.dumps(entries, sort_keys=True)`,
    /// i.e. `", "`/`": "` separators, `ensure_ascii` escapes, no newline.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = String::from("[");
        for (i, e) in self.entries.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{{\"md5\": \"{}\", \"relpath\": \"", e.md5);
            json_escape_ascii(&mut out, e.relpath.as_str());
            out.push_str("\"}");
        }
        out.push(']');
        out.into_bytes()
    }

    /// The manifest's id: md5 of [`Self::to_bytes`].
    pub fn id(&self) -> Hexdigest {
        let mut h = Hasher::new(HashFunction::Md5);
        h.update(&self.to_bytes());
        h.finalize()
    }

    /// Parse manifest bytes fetched by id. The bytes must hash to `id`
    /// (checked before anything else), and are never re-serialised.
    pub fn parse(raw: &[u8], id: &Hexdigest) -> Result<Self> {
        let mut h = Hasher::new(HashFunction::Md5);
        h.update(raw);
        let actual = h.finalize();
        anyhow::ensure!(
            actual == *id,
            "manifest integrity check failed: expected {id}, got {actual}"
        );
        Self::parse_unverified(raw)
    }

    /// Parse manifest bytes read from a local DVC cache, where the file name
    /// is the id but DVC may have stored it in a different formatting.
    pub fn parse_unverified(raw: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct RawEntry {
            md5: String,
            relpath: String,
        }
        let raw: Vec<RawEntry> =
            serde_json::from_slice(raw).context("manifest is not a JSON list")?;
        let entries = raw
            .into_iter()
            .map(|e| {
                let relpath = RepoPath::new(&e.relpath).with_context(|| {
                    format!(
                        "manifest relpath must be a relative path inside the directory: {:?}",
                        e.relpath
                    )
                })?;
                let md5 =
                    md5(&e.md5).with_context(|| format!("invalid md5 for {:?}", e.relpath))?;
                Ok(ManifestEntry { relpath, md5 })
            })
            .collect::<Result<_>>()?;
        Self::from_entries(entries)
    }
}

/// CPython `json.dumps` string escaping with `ensure_ascii=True`.
fn json_escape_ascii(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(ch),
            _ => {
                let mut buf = [0u16; 2];
                for unit in ch.encode_utf16(&mut buf) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
}

/// What a `.dvc` file points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DvcOutput {
    /// A directory: its manifest id, total size and file count.
    Dir {
        manifest: Hexdigest,
        size: u64,
        nfiles: u64,
    },
    /// A single file.
    File { md5: Hexdigest, size: u64 },
}

/// A single-output DVC 3 pointer (`hash: md5`). `path` is the output's name
/// relative to the `.dvc` file's directory — one component for pointers
/// bigstore writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DvcPointer {
    pub output: DvcOutput,
    pub path: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct YamlFile {
    outs: Vec<YamlOut>,
}

/// Field order is DVC's output order.
#[derive(Serialize, Deserialize)]
struct YamlOut {
    md5: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nfiles: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
    path: String,
}

impl DvcPointer {
    /// DVC 3's YAML for this pointer, byte-exact.
    pub fn to_yaml(&self) -> String {
        let (md5, size, nfiles) = match &self.output {
            DvcOutput::Dir {
                manifest,
                size,
                nfiles,
            } => (format!("{manifest}.dir"), *size, Some(*nfiles)),
            DvcOutput::File { md5, size } => (md5.to_string(), *size, None),
        };
        let file = YamlFile {
            outs: vec![YamlOut {
                md5,
                size: Some(size),
                nfiles,
                hash: Some("md5".into()),
                path: self.path.clone(),
            }],
        };
        serde_yaml_ng::to_string(&file).expect("pointer YAML serialises")
    }

    /// Parse a DVC 3 pointer that bigstore can act on: exactly one output,
    /// `hash: md5`, and no stage fields (`deps`, `cmd`, `wdir`, ...).
    /// A pointer without `hash:` is DVC 2 (md5-dos2unix, legacy layout) and
    /// is refused rather than misread.
    pub fn parse(text: &str) -> Result<Self> {
        let file: YamlFile = serde_yaml_ng::from_str(text).context("not a DVC 3 pointer")?;
        let [out] = &file.outs[..] else {
            anyhow::bail!(
                "multi-output .dvc files not supported (found {} outputs)",
                file.outs.len()
            );
        };
        match out.hash.as_deref() {
            Some("md5") => {}
            None => anyhow::bail!(
                "pointer has no `hash:` field: it is from DVC 2 (md5-dos2unix), \
                 which bigstore does not read; re-add it with DVC 3"
            ),
            Some(other) => anyhow::bail!("unsupported pointer hash {other:?}"),
        }
        let size = out.size.context("pointer has no size")?;
        let output = match out.md5.strip_suffix(".dir") {
            Some(hex) => DvcOutput::Dir {
                manifest: md5(hex)?,
                size,
                nfiles: out.nfiles.context("directory pointer has no nfiles")?,
            },
            None => {
                anyhow::ensure!(out.nfiles.is_none(), "file pointer has nfiles");
                DvcOutput::File {
                    md5: md5(&out.md5)?,
                    size,
                }
            }
        };
        Ok(Self {
            output,
            path: out.path.clone(),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("failed to parse {}", path.display()))
    }
}

/// Parse a `.dir` manifest file from a local DVC cache.
pub fn parse_dir_manifest(manifest_path: &Path) -> Result<Vec<ManifestEntry>> {
    let raw = std::fs::read(manifest_path)
        .with_context(|| format!("failed to read manifest {}", manifest_path.display()))?;
    Manifest::parse_unverified(&raw)
        .map(|m| m.entries)
        .with_context(|| format!("failed to parse manifest {}", manifest_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dvc-3.67.1");

    /// Every golden manifest DVC 3.67.1 wrote is reproduced byte for byte
    /// from its entries in reverse order (so sorting is exercised), and its
    /// id matches the `.dvc` pointer DVC wrote.
    #[test]
    fn manifests_match_dvc_byte_for_byte() {
        for (case, out) in [
            ("dataset", "tt"),
            ("names", "x"),
            ("empty", "e"),
            ("crlf", "c"),
            ("nested_scm", "n"),
            ("dirsymlink", "s"),
        ] {
            let dir = Path::new(GOLDEN).join(case);
            let raw = std::fs::read(dir.join("manifest.dir")).unwrap();
            let parsed = Manifest::parse_unverified(&raw).unwrap();
            let mut entries = parsed.entries().to_vec();
            entries.reverse();
            let rebuilt = Manifest::from_entries(entries).unwrap();
            assert_eq!(rebuilt.to_bytes(), raw, "{case}: bytes differ");

            let pointer = DvcPointer::load(&dir.join(format!("{out}.dvc"))).unwrap();
            let DvcOutput::Dir { manifest, .. } = &pointer.output else {
                panic!("{case}: expected a directory pointer");
            };
            assert_eq!(rebuilt.id(), *manifest, "{case}: id");
            Manifest::parse(&raw, manifest).unwrap();
            let yaml = std::fs::read_to_string(dir.join(format!("{out}.dvc"))).unwrap();
            assert_eq!(pointer.to_yaml(), yaml, "{case}: .dvc bytes");
        }
    }

    #[test]
    fn single_file_pointer_matches_dvc() {
        // `dvc add store.toml` (DVC 3.67.1) on "x = 1\n".
        let yaml = "outs:\n- md5: 3253b41059cac6e987c5a5e9233ea5d0\n  size: 6\n  hash: md5\n  path: store.toml\n";
        let p = DvcPointer::parse(yaml).unwrap();
        assert!(matches!(p.output, DvcOutput::File { size: 6, .. }));
        assert_eq!(p.to_yaml(), yaml);
    }

    #[test]
    fn pointer_parse_refuses_what_it_cannot_act_on() {
        let d = "ab".repeat(16);
        for bad in [
            // DVC 2: md5-dos2unix and a different layout
            format!("outs:\n- md5: {d}.dir\n  size: 1\n  nfiles: 1\n  path: x\n"),
            format!("outs:\n- md5: {d}\n  size: 1\n  hash: sha256\n  path: x\n"),
            format!("outs:\n- md5: {d}\n  size: 1\n  hash: md5\n  path: a\n- md5: {d}\n  size: 1\n  hash: md5\n  path: b\n"),
            format!("deps:\n- path: x\nouts:\n- md5: {d}\n  size: 1\n  hash: md5\n  path: y\n"),
            "outs:\n- md5: not-hex\n  size: 1\n  hash: md5\n  path: x\n".to_string(),
            format!("outs:\n- md5: {d}.dir\n  size: 1\n  hash: md5\n  path: x\n"),
        ] {
            assert!(DvcPointer::parse(&bad).is_err(), "accepted:\n{bad}");
        }
    }

    #[test]
    fn manifest_parse_checks_the_id_first() {
        let raw = std::fs::read(Path::new(GOLDEN).join("crlf/manifest.dir")).unwrap();
        let wrong = md5(&"00".repeat(16)).unwrap();
        let err = Manifest::parse(&raw, &wrong).unwrap_err();
        assert!(err.to_string().contains("integrity"), "{err}");
    }

    fn entry(relpath: &str) -> ManifestEntry {
        ManifestEntry {
            relpath: RepoPath::new(relpath).unwrap(),
            md5: md5(&"aa".repeat(16)).unwrap(),
        }
    }

    #[test]
    fn manifest_rejects_duplicates_and_file_dir_overlap() {
        assert!(Manifest::from_entries(vec![entry("a"), entry("a")]).is_err());
        // "a-x" sorts between "a" and "a/b": overlap must still be found.
        assert!(Manifest::from_entries(vec![entry("a/b"), entry("a-x"), entry("a")]).is_err());
        Manifest::from_entries(vec![entry("a/b"), entry("a-x"), entry("ab")]).unwrap();
    }

    #[test]
    fn manifest_rejects_unsafe_relpaths_and_bad_md5() {
        let md5 = "aa".repeat(16);
        for relpath in ["../etc/passwd", "/etc/passwd", "", "."] {
            let raw = format!(r#"[{{"md5":"{md5}","relpath":"{relpath}"}}]"#);
            assert!(
                Manifest::parse_unverified(raw.as_bytes()).is_err(),
                "{relpath:?}"
            );
        }
        let raw = r#"[{"md5":"not-valid","relpath":"file.bin"}]"#;
        assert!(Manifest::parse_unverified(raw.as_bytes()).is_err());
    }
}
