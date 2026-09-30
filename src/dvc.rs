//! DVC 3 file formats: `.dvc` pointers and `.dir` manifests.
//!
//! Writers are byte-exact with DVC 3.67.1 (goldens in
//! `tests/fixtures/dvc-3.67.1`): a `.dir` manifest's id is the md5 of its
//! bytes, so any formatting difference makes DVC report the data modified.

use anyhow::{Context, Result};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::path::Path;

use crate::hash::Hasher;
use crate::types::{HashFunction, Hexdigest, ManifestPath};

/// An md5 digest; DVC 3 addresses everything with plain md5.
fn md5(hex: &str) -> Result<Hexdigest> {
    Hexdigest::new(hex, HashFunction::Md5)
}

/// One file in a `.dir` manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub relpath: ManifestPath,
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
                let relpath = ManifestPath::new(&e.relpath)
                    .context("manifest relpath must be a relative path inside the directory")?;
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

/// What [`DvcPointer::to_yaml`] writes; field order is DVC's output order.
#[derive(Serialize)]
struct YamlFile {
    outs: [YamlOut; 1],
}

#[derive(Serialize)]
struct YamlOut {
    md5: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    nfiles: Option<u64>,
    hash: &'static str,
    path: String,
}

/// A `.dvc` file as DVC 3 reads it: every top-level key its schema allows
/// (`SINGLE_STAGE_SCHEMA` in `dvc/schema.py`); DVC refuses any other key,
/// and so does this.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DvcFile {
    outs: Option<Vec<DvcFileOut>>,
    /// The directory output paths are relative to (default: the file's own).
    wdir: Option<String>,
    // Stage fields (`dvc import-url` writes `md5`, `frozen`, `deps`) and
    // annotations: none changes which bytes the output holds.
    md5: Option<IgnoredAny>,
    deps: Option<IgnoredAny>,
    frozen: Option<IgnoredAny>,
    locked: Option<IgnoredAny>,
    always_changed: Option<IgnoredAny>,
    meta: Option<IgnoredAny>,
    desc: Option<IgnoredAny>,
}

impl DvcFile {
    /// Top-level keys [`DvcPointer::to_yaml`] does not write.
    fn extra_fields(&self) -> impl Iterator<Item = &'static str> {
        [
            ("wdir", self.wdir.is_some()),
            ("md5", self.md5.is_some()),
            ("deps", self.deps.is_some()),
            ("frozen", self.frozen.is_some()),
            ("locked", self.locked.is_some()),
            ("always_changed", self.always_changed.is_some()),
            ("meta", self.meta.is_some()),
            ("desc", self.desc.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
    }
}

/// One output as DVC 3 reads it: every key its schema allows (`SCHEMA` in
/// `dvc/output.py`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DvcFileOut {
    path: String,
    md5: Option<String>,
    size: Option<u64>,
    nfiles: Option<u64>,
    hash: Option<String>,
    /// `false`: DVC tracks the hash but never stores the data.
    cache: Option<bool>,
    // What identifies data on a cloud or external filesystem instead of md5.
    etag: Option<IgnoredAny>,
    checksum: Option<IgnoredAny>,
    version_id: Option<IgnoredAny>,
    // Neither changes which bytes the output holds nor where DVC caches
    // them: the file mode, pipeline and push settings, annotations, and the
    // per-remote ids of a cloud-versioned remote.
    isexec: Option<IgnoredAny>,
    persist: Option<IgnoredAny>,
    remote: Option<IgnoredAny>,
    push: Option<IgnoredAny>,
    desc: Option<IgnoredAny>,
    #[serde(rename = "type")]
    kind: Option<IgnoredAny>,
    labels: Option<IgnoredAny>,
    meta: Option<IgnoredAny>,
    cloud: Option<IgnoredAny>,
    files: Option<IgnoredAny>,
    fs_config: Option<IgnoredAny>,
}

impl DvcFileOut {
    /// Keys identifying the data without an md5, as found.
    fn non_md5_ids(&self) -> Vec<&'static str> {
        [
            ("etag", self.etag.is_some()),
            ("checksum", self.checksum.is_some()),
            ("version_id", self.version_id.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
        .collect()
    }

    /// Output keys [`DvcPointer::to_yaml`] does not write.
    fn extra_fields(&self) -> impl Iterator<Item = &'static str> {
        [
            ("outs.cache", self.cache.is_some()),
            ("outs.etag", self.etag.is_some()),
            ("outs.checksum", self.checksum.is_some()),
            ("outs.version_id", self.version_id.is_some()),
            ("outs.isexec", self.isexec.is_some()),
            ("outs.persist", self.persist.is_some()),
            ("outs.remote", self.remote.is_some()),
            ("outs.push", self.push.is_some()),
            ("outs.desc", self.desc.is_some()),
            ("outs.type", self.kind.is_some()),
            ("outs.labels", self.labels.is_some()),
            ("outs.meta", self.meta.is_some()),
            ("outs.cloud", self.cloud.is_some()),
            ("outs.files", self.files.is_some()),
            ("outs.fs_config", self.fs_config.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
    }
}

/// A `.dvc` file's output, and the fields it has that
/// [`DvcPointer::to_yaml`] would not write back.
struct ParsedDvcFile {
    pointer: DvcPointer,
    extra_fields: Vec<&'static str>,
}

impl ParsedDvcFile {
    /// Any single-output DVC 3 `.dvc` whose data DVC has cached under its
    /// md5. A pointer without `hash:` is DVC 2 (md5-dos2unix, legacy layout)
    /// and is refused rather than misread.
    fn parse(text: &str) -> Result<Self> {
        let file: DvcFile = serde_yaml_ng::from_str(text).context("not a DVC 3 .dvc file")?;
        let outs = file.outs.as_deref().unwrap_or_default();
        let [out] = outs else {
            anyhow::bail!(
                "multi-output .dvc files not supported (found {} outputs)",
                outs.len()
            );
        };
        let name = &out.path;
        if let Some(wdir) = file.wdir.as_deref().filter(|w| *w != ".") {
            anyhow::bail!(
                "`wdir: {wdir}` makes output {name:?} relative to another directory; \
                 bigstore reads only .dvc files whose output sits beside them"
            );
        }
        anyhow::ensure!(
            out.cache != Some(false),
            "output {name:?} has `cache: false`: DVC keeps no copy of it, \
             so there is no object to read"
        );
        let Some(out_md5) = out.md5.as_deref() else {
            let ids = out.non_md5_ids();
            anyhow::ensure!(
                !ids.is_empty(),
                "output {name:?} has no md5: DVC has not downloaded or hashed it \
                 (`dvc import-url --no-download` or `--no-exec`); fetch it with DVC first"
            );
            anyhow::bail!(
                "output {name:?} is identified by {} instead of an md5 (a cloud or \
                 external output); bigstore reads only md5-addressed DVC data",
                ids.join(" and ")
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
        let output = match out_md5.strip_suffix(".dir") {
            Some(hex) => DvcOutput::Dir {
                manifest: md5(hex)?,
                size,
                nfiles: out.nfiles.context("directory pointer has no nfiles")?,
            },
            None => {
                anyhow::ensure!(out.nfiles.is_none(), "file pointer has nfiles");
                DvcOutput::File {
                    md5: md5(out_md5)?,
                    size,
                }
            }
        };
        Ok(Self {
            pointer: DvcPointer {
                output,
                path: name.clone(),
            },
            extra_fields: file.extra_fields().chain(out.extra_fields()).collect(),
        })
    }
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
            outs: [YamlOut {
                md5,
                size,
                nfiles,
                hash: "md5",
                path: self.path.clone(),
            }],
        };
        serde_yaml_ng::to_string(&file).expect("pointer YAML serialises")
    }

    /// Parse a `.dvc` holding exactly what [`Self::to_yaml`] writes (in any
    /// formatting), so replacing the file loses nothing. A stage (`deps:`),
    /// annotations (`meta:`, `desc:`) or any other DVC field is refused,
    /// naming the fields; use this before overwriting a `.dvc`.
    pub fn parse(text: &str) -> Result<Self> {
        let ParsedDvcFile {
            pointer,
            extra_fields,
        } = ParsedDvcFile::parse(text)?;
        anyhow::ensure!(
            extra_fields.is_empty(),
            "it has fields bigstore does not write: {}",
            extra_fields.join(", ")
        );
        Ok(pointer)
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::parse(&read_dvc_file(path)?)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Read the output of any single-output DVC 3 `.dvc` whose data DVC
    /// cached under its md5, ignoring stage fields and annotations (none
    /// changes which bytes the output holds). Refuses, saying why, what
    /// has no md5-addressed object: `cache: false`, outputs not yet
    /// downloaded, etag/version_id-only cloud outputs, and a `wdir:` that
    /// moves the output. For reading only: [`Self::to_yaml`] would drop the
    /// extra fields.
    pub fn load_lenient(path: &Path) -> Result<Self> {
        ParsedDvcFile::parse(&read_dvc_file(path)?)
            .map(|parsed| parsed.pointer)
            .with_context(|| format!("failed to parse {}", path.display()))
    }
}

fn read_dvc_file(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
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
    use crate::types::PathSyntax;

    const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dvc-3.67.1");

    /// Every golden manifest DVC 3.67.1 wrote is reproduced byte for byte
    /// from its entries in reverse order (so sorting is exercised), and its
    /// id matches the `.dvc` pointer DVC wrote.
    #[test]
    fn manifests_match_dvc_byte_for_byte() {
        for (case, out) in [
            ("dataset", "tt"),
            // Names with `\`, a newline, non-ASCII: valid DVC data on every OS.
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

    const STAGE_FIELDS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/dvc-3.67.1/stage_fields"
    );

    fn file(hex: &str, size: u64) -> DvcOutput {
        DvcOutput::File {
            md5: md5(hex).unwrap(),
            size,
        }
    }

    fn dir(hex: &str, size: u64, nfiles: u64) -> DvcOutput {
        DvcOutput::Dir {
            manifest: md5(hex).unwrap(),
            size,
            nfiles,
        }
    }

    /// `.dvc` files DVC 3.67.1 wrote or accepted (`regen_stage_fields.py`)
    /// with fields beyond `outs:`: read for their output, but never taken
    /// as a pointer bigstore may overwrite, since that would drop them.
    #[test]
    fn dvc3_stage_and_annotation_fields_are_read_but_not_overwritable() {
        for (name, output, path, extras) in [
            (
                "imported.txt.dvc",
                file("b1946ac92492d2347c6235b4d2611184", 6),
                "imported.txt",
                "md5, deps, frozen",
            ),
            (
                "imported_dir.dvc",
                dir("5b94ef7ba4840901cc23311660411a1d", 2, 2),
                "imported_dir",
                "md5, deps, frozen",
            ),
            (
                "annotated.dvc",
                dir("c1aa8378201c5b38b6b109d77fbf79bc", 2, 2),
                "annotated",
                "meta, desc, outs.persist, outs.remote, outs.push, outs.desc, outs.type, \
                 outs.labels, outs.meta",
            ),
            (
                "run.sh.dvc",
                file("3e2b31c72181b87149ff995e7202c0e3", 10),
                "run.sh",
                "outs.isexec",
            ),
            (
                "versioned.bin.dvc",
                file("9e3669d19b675bd57058fd4664205d2a", 1),
                "versioned.bin",
                "outs.cloud",
            ),
        ] {
            let at = Path::new(STAGE_FIELDS).join(name);
            let pointer = DvcPointer::load_lenient(&at).unwrap();
            assert_eq!(pointer.output, output, "{name}");
            assert_eq!(pointer.path, path, "{name}");
            let err = format!("{:#}", DvcPointer::load(&at).unwrap_err());
            assert!(
                err.ends_with(&format!("bigstore does not write: {extras}")),
                "{err}"
            );
        }
    }

    #[test]
    fn dvc_files_without_an_md5_addressed_object_are_refused_saying_why() {
        for (name, why) in [
            ("uncached.bin.dvc", "\"uncached.bin\" has `cache: false`"),
            ("no_download.txt.dvc", "\"no_download.txt\" has no md5"),
            ("no_exec.txt.dvc", "\"no_exec.txt\" has no md5"),
            (
                "etag_only.bin.dvc",
                "\"etag_only.bin\" is identified by etag and version_id",
            ),
            (
                "sub__wdir.dvc",
                "`wdir: ..` makes output \"run.sh\" relative",
            ),
            ("two_outs.dvc", "found 2 outputs"),
        ] {
            let at = Path::new(STAGE_FIELDS).join(name);
            let err = format!("{:#}", DvcPointer::load_lenient(&at).unwrap_err());
            assert!(err.contains(why), "{name}: {err}");
            assert!(err.contains(name), "{name} not named: {err}");
        }
        for (text, why) in [
            ("outs: []\n", "found 0 outputs"),
            ("frozen: true\n", "found 0 outputs"),
            // DVC 1 stage files: DVC 3 refuses `cmd` in a `.dvc` too.
            ("cmd: echo\nouts: []\n", "unknown field `cmd`"),
        ] {
            let err = format!("{:#}", ParsedDvcFile::parse(text).err().unwrap());
            assert!(err.contains(why), "{text}: {err}");
        }
    }

    #[test]
    fn wdir_dot_is_the_default() {
        let text = std::fs::read_to_string(Path::new(STAGE_FIELDS).join("run.sh.dvc")).unwrap();
        let parsed = ParsedDvcFile::parse(&format!("wdir: .\n{text}")).unwrap();
        assert_eq!(parsed.pointer.path, "run.sh");
        assert_eq!(parsed.extra_fields, ["wdir", "outs.isexec"]);
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
            relpath: ManifestPath::new(relpath).unwrap(),
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

    /// A manifest made on Unix with `\` and `:` in names parses anywhere
    /// (`dvc-ls` works on Windows), but Windows refuses to write those
    /// names: `a\..\..\x` would land outside the target there.
    #[test]
    fn manifest_names_windows_misreads_parse_but_are_not_written_there() {
        let md5 = "aa".repeat(16);
        let raw = format!(
            r#"[{{"md5":"{md5}","relpath":"a\\..\\..\\x"}},{{"md5":"{md5}","relpath":"C:/y"}}]"#
        );
        let manifest = Manifest::parse_unverified(raw.as_bytes()).unwrap();
        let names: Vec<&str> = manifest
            .entries()
            .iter()
            .map(|e| e.relpath.as_str())
            .collect();
        assert_eq!(names, ["C:/y", "a\\..\\..\\x"]);
        for e in manifest.entries() {
            let err = e.relpath.to_repo_path_for(PathSyntax::Windows).unwrap_err();
            let shown = format!("{err:#}");
            assert!(
                shown.contains(&format!("{:?}", e.relpath.as_str())),
                "{shown}"
            );
        }
    }
}
