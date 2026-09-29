use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};

/// Supported hash functions. Exhaustive enum — invalid states unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HashFunction {
    Sha256,
    Md5,
}

impl HashFunction {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "sha256" => Ok(Self::Sha256),
            "md5" => Ok(Self::Md5),
            other => anyhow::bail!("unsupported hash function: {other:?}"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Md5 => "md5",
        }
    }

    pub fn digest_len(&self) -> usize {
        match self {
            Self::Sha256 => 64,
            Self::Md5 => 32,
        }
    }
}

impl fmt::Display for HashFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A validated content digest: the hash function and its lowercase hex output.
///
/// The algorithm travels with the hex, so a digest can never be paired with
/// the wrong hash function (wrong cache path, wrong remote key, wrong verifier).
/// Guarantees: hex is `[0-9a-f]` only and its length matches `hash_fn`, so
/// slicing into prefix/rest is always safe and path-traversal-free.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hexdigest {
    hash_fn: HashFunction,
    hex: String,
}

impl Hexdigest {
    pub fn new(s: &str, hash_fn: HashFunction) -> Result<Self> {
        let expected_len = hash_fn.digest_len();
        anyhow::ensure!(
            s.len() == expected_len,
            "hexdigest length {}, expected {} for {}",
            s.len(),
            expected_len,
            hash_fn
        );
        anyhow::ensure!(
            s.chars().all(|c| c.is_ascii_hexdigit()),
            "hexdigest contains non-hex characters: {s:?}"
        );
        Ok(Self {
            hash_fn,
            hex: s.to_ascii_lowercase(),
        })
    }

    pub fn hash_fn(&self) -> HashFunction {
        self.hash_fn
    }

    /// First 2 hex characters (directory shard).
    pub fn prefix(&self) -> &str {
        &self.hex[..2]
    }

    /// Remaining hex characters after the shard prefix.
    pub fn rest(&self) -> &str {
        &self.hex[2..]
    }
}

/// Displays the hex only; the hash function is shown separately where needed.
impl fmt::Display for Hexdigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex)
    }
}

/// Upper bound on the size of anything recognised as a pointer. Real pointers
/// are ~81 bytes; the headroom tolerates CRLF and trailing blank lines.
pub const MAX_POINTER_BYTES: usize = 512;

/// A bigstore pointer: the three-line text git stores in place of a large file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pointer(Hexdigest);

impl Pointer {
    pub fn new(hexdigest: Hexdigest) -> Self {
        Self(hexdigest)
    }

    pub fn hexdigest(&self) -> &Hexdigest {
        &self.0
    }

    pub fn hash_fn(&self) -> HashFunction {
        self.0.hash_fn
    }

    pub fn encode(&self) -> Vec<u8> {
        format!("bigstore\n{}\n{}\n", self.0.hash_fn, self.0.hex).into_bytes()
    }

    /// Classify `data` as a pointer. Total: anything that is not exactly a
    /// well-formed pointer — binary data, text that merely starts with
    /// `bigstore`, a pointer followed by other content — is `None`, i.e. file
    /// content. This is the single definition of "is a pointer" used by the
    /// clean and smudge filters, the working-tree check and index reads, so
    /// they can never disagree.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() > MAX_POINTER_BYTES {
            return None;
        }
        let text = std::str::from_utf8(data).ok()?;
        let mut lines = text.lines();
        if lines.next()? != "bigstore" {
            return None;
        }
        let hash_fn = HashFunction::parse(lines.next()?).ok()?;
        let hexdigest = Hexdigest::new(lines.next()?, hash_fn).ok()?;
        // Exactly three lines; trailing blank lines are tolerated, anything
        // else means this is content that happens to start like a pointer.
        if lines.any(|l| !l.trim().is_empty()) {
            return None;
        }
        Some(Self(hexdigest))
    }
}

/// A path relative to the repository root: `/`-separated, non-empty, with no
/// `..` components and no leading `/`. Every path bigstore reads from git or
/// the user, and every path it writes to, goes through this type, so paths
/// can neither escape the repository nor be misread relative to the cwd.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoPath(String);

impl RepoPath {
    /// Validate and normalise. Empty and `.` components are dropped, so
    /// `./a//b/` becomes `a/b`.
    pub fn new(s: &str) -> Result<Self> {
        anyhow::ensure!(
            !s.starts_with('/') && !Path::new(s).is_absolute(),
            "path must be relative to the repository root: {s:?}"
        );
        anyhow::ensure!(!s.contains('\0'), "path contains a NUL byte: {s:?}");
        // On Windows `\` is a separator and `C:x` is drive-relative, so either
        // could make `to_fs_path` land outside the root (`a\..\..\x`).
        #[cfg(windows)]
        anyhow::ensure!(
            !s.contains(['\\', ':']),
            "path contains '\\' or ':', which Windows would misread: {s:?}"
        );
        let mut parts = Vec::new();
        for part in s.split('/') {
            match part {
                "" | "." => {}
                ".." => anyhow::bail!("path must not contain '..': {s:?}"),
                p => parts.push(p),
            }
        }
        anyhow::ensure!(!parts.is_empty(), "path is empty: {s:?}");
        Ok(Self(parts.join("/")))
    }

    /// Parse a path as git prints it with `-z` (raw bytes, root-relative).
    pub fn from_git_bytes(bytes: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).with_context(|| {
            format!(
                "path is not valid UTF-8: {:?}",
                String::from_utf8_lossy(bytes)
            )
        })?;
        Self::new(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `self/child`, e.g. an import destination root joined with a manifest path.
    pub fn join(&self, child: &RepoPath) -> RepoPath {
        RepoPath(format!("{}/{}", self.0, child.0))
    }

    /// The on-disk location under `repo_root`.
    pub fn to_fs_path(&self, repo_root: &Path) -> PathBuf {
        repo_root.join(&self.0)
    }
}

impl fmt::Display for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Checks one path component is safe to create on Windows, macOS and Linux:
/// printable ASCII only (which also rules out Unicode NFC/NFD twins), none of
/// `\ / : * ? " < > |`, not a Windows device name (`CON`, `nul.txt`, `COM1`…),
/// no trailing `.` or space, and not `.` or `..`.
pub fn check_portable_component(c: &str) -> Result<()> {
    anyhow::ensure!(
        !c.is_empty() && c != "." && c != "..",
        "invalid path component: {c:?}"
    );
    anyhow::ensure!(
        c.bytes().all(|b| (0x20..=0x7e).contains(&b)),
        "{c:?}: only printable ASCII names are portable"
    );
    anyhow::ensure!(
        !c.contains(['\\', '/', ':', '*', '?', '"', '<', '>', '|']),
        "{c:?}: contains a character Windows does not allow"
    );
    anyhow::ensure!(
        !c.ends_with(['.', ' ']),
        "{c:?}: Windows drops a trailing '.' or space"
    );
    let stem = c.split('.').next().unwrap_or(c).to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.as_bytes()[3].is_ascii_digit());
    anyhow::ensure!(!reserved, "{c:?}: reserved device name on Windows");
    Ok(())
}

/// A [`RepoPath`] whose every component passes [`check_portable_component`],
/// so it can be created on any OS bigstore runs on. Used for everything
/// folder mode writes into a manifest or remote key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PortableRelPath(RepoPath);

impl PortableRelPath {
    pub fn new(s: &str) -> Result<Self> {
        let path = RepoPath::new(s)?;
        for c in path.as_str().split('/') {
            check_portable_component(c).with_context(|| format!("path {s:?}"))?;
        }
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_repo_path(&self) -> &RepoPath {
        &self.0
    }
}

impl fmt::Display for PortableRelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}

/// A validated storage layout template. Guarantees:
/// - Contains `{prefix}` and `{rest}` placeholders
/// - Produces deterministic, safe object keys
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout(String);

impl Layout {
    pub fn new(template: &str) -> Result<Self> {
        for required in ["{prefix}", "{rest}"] {
            anyhow::ensure!(
                template.contains(required),
                "layout template missing {required}: {template:?}"
            );
        }
        Ok(Self(template.to_string()))
    }

    /// Format an object key from a hexdigest. Safe: Hexdigest is validated.
    ///
    /// For layouts without `{hash_fn}`, only sha256 is supported — the
    /// template is used as-is (backward compatible with older configs).
    /// For layouts with `{hash_fn}`, the placeholder is replaced dynamically.
    pub fn object_key(&self, hexdigest: &Hexdigest) -> Result<String> {
        let hash_fn = hexdigest.hash_fn();
        if !self.0.contains("{hash_fn}") && hash_fn != HashFunction::Sha256 {
            anyhow::bail!(
                "layout template does not contain {{hash_fn}} — only sha256 is supported.\n\
                 Update layout in .bigstore.toml to: files/{{hash_fn}}/{{prefix}}/{{rest}}"
            );
        }
        Ok(self
            .0
            .replace("{hash_fn}", hash_fn.as_str())
            .replace("{prefix}", hexdigest.prefix())
            .replace("{rest}", hexdigest.rest()))
    }
}

/// DVC-compatible default layout.
impl Default for Layout {
    fn default() -> Self {
        Self::new("files/{hash_fn}/{prefix}/{rest}")
            .expect("default layout template is invalid — this is a bug")
    }
}

impl fmt::Display for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Layout {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Layout {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Layout::new(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hexdigest_valid_sha256() {
        let hex = "a".repeat(64);
        let d = Hexdigest::new(&hex, HashFunction::Sha256).unwrap();
        assert_eq!(d.prefix(), "aa");
        assert_eq!(d.rest().len(), 62);
    }

    #[test]
    fn hexdigest_rejects_short() {
        assert!(Hexdigest::new("deadbeef", HashFunction::Sha256).is_err());
    }

    #[test]
    fn hexdigest_rejects_non_hex() {
        let bad = format!("{}zz", "a".repeat(62));
        assert!(Hexdigest::new(&bad, HashFunction::Sha256).is_err());
    }

    #[test]
    fn hexdigest_rejects_path_traversal() {
        assert!(Hexdigest::new("../../etc/passwd", HashFunction::Sha256).is_err());
    }

    #[test]
    fn hexdigest_normalizes_to_lowercase() {
        let hex = "A".repeat(64);
        let d = Hexdigest::new(&hex, HashFunction::Sha256).unwrap();
        assert_eq!(d.to_string(), "a".repeat(64));
    }

    #[test]
    fn hash_function_parses_md5() {
        let hf = HashFunction::parse("md5").unwrap();
        assert_eq!(hf, HashFunction::Md5);
        assert_eq!(hf.digest_len(), 32);
    }

    #[test]
    fn hash_function_rejects_unknown() {
        assert!(HashFunction::parse("sha1").is_err());
        assert!(HashFunction::parse("../../etc").is_err());
    }

    #[test]
    fn hexdigest_valid_md5() {
        let hex = "a".repeat(32);
        let d = Hexdigest::new(&hex, HashFunction::Md5).unwrap();
        assert_eq!(d.prefix(), "aa");
        assert_eq!(d.rest().len(), 30);
    }

    #[test]
    fn hexdigest_rejects_sha256_length_for_md5() {
        let hex = "a".repeat(64);
        assert!(Hexdigest::new(&hex, HashFunction::Md5).is_err());
    }

    #[test]
    fn hexdigest_rejects_md5_length_for_sha256() {
        let hex = "a".repeat(32);
        assert!(Hexdigest::new(&hex, HashFunction::Sha256).is_err());
    }

    #[test]
    fn pointer_parse_valid() {
        let hex = "ab".repeat(32);
        let data = format!("bigstore\nsha256\n{hex}\n");
        let p = Pointer::parse(data.as_bytes()).unwrap();
        assert_eq!(p.hash_fn(), HashFunction::Sha256);
        assert_eq!(p.hexdigest().to_string(), hex);
    }

    #[test]
    fn pointer_parse_accepts_crlf() {
        let hex = "ab".repeat(32);
        let data = format!("bigstore\r\nmd5\r\n{}\r\n", &hex[..32]);
        assert_eq!(
            Pointer::parse(data.as_bytes()).unwrap().hash_fn(),
            HashFunction::Md5
        );
    }

    #[test]
    fn pointer_roundtrip() {
        let hex = "ab".repeat(32);
        let p = Pointer::new(Hexdigest::new(&hex, HashFunction::Sha256).unwrap());
        assert_eq!(Pointer::parse(&p.encode()), Some(p));
    }

    /// Everything that is not exactly a pointer is content — including text
    /// that starts with the pointer header. None of these may be an error:
    /// callers treat the blob as ordinary file content.
    #[test]
    fn pointer_parse_classifies_non_pointers_as_content() {
        let hex = "ab".repeat(32);
        let cases: Vec<Vec<u8>> = vec![
            b"just some regular file content\n".to_vec(),
            b"".to_vec(),
            vec![0xff, 0xfe, 0x00, b'\n'],
            b"bigstore\n".to_vec(),
            b"bigstore\nis a great tool\n".to_vec(),
            b"bigstore\n../../etc\naaaa\n".to_vec(),
            b"bigstore\nsha256\ndeadbeef\n".to_vec(),
            format!("bigstore\nsha256\n{hex}\nextra junk\n").into_bytes(),
            format!("bigstore\nsha256\n{hex}\n\npayload\n").into_bytes(),
            format!(
                "bigstore\nsha256\n{hex}\n{}",
                "\n".repeat(MAX_POINTER_BYTES)
            )
            .into_bytes(),
        ];
        for data in cases {
            assert_eq!(
                Pointer::parse(&data),
                None,
                "{:?}",
                String::from_utf8_lossy(&data)
            );
        }
    }

    #[test]
    fn pointer_allows_trailing_blank_line() {
        let hex = "ab".repeat(32);
        let data = format!("bigstore\nsha256\n{hex}\n\n");
        assert!(Pointer::parse(data.as_bytes()).is_some());
    }

    #[test]
    fn repo_path_normalises() {
        assert_eq!(RepoPath::new("./a//b/").unwrap().as_str(), "a/b");
        assert_eq!(RepoPath::new("café.bin").unwrap().as_str(), "café.bin");
    }

    #[test]
    fn repo_path_rejects_escapes_and_empty() {
        for bad in ["", ".", "/etc/passwd", "../x", "a/../../x", "a/..", "a\0b"] {
            assert!(RepoPath::new(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn repo_path_rejects_non_utf8_git_bytes() {
        let err = RepoPath::from_git_bytes(b"caf\xe9.bin").unwrap_err();
        assert!(format!("{err:#}").contains("caf"), "{err:#}");
    }

    #[test]
    fn portable_rel_path_accepts_the_annotation_layout() {
        for ok in [
            "annotations/reviewer=rick/host=xenoglossicist/site=s1/date=2026-09-01/src_01/labels.jsonl",
            "views/v1/mask.json",
            ".DS_Store",
            "a b/c-d_e.parquet",
        ] {
            PortableRelPath::new(ok).unwrap();
        }
    }

    #[test]
    fn portable_rel_path_rejects_what_some_os_cannot_create() {
        for bad in [
            "café.txt",
            "a\\b",
            "c:d",
            "q\"uote",
            "tab\there",
            "CON",
            "sub/nul.txt",
            "com1.log",
            "trailing.",
            "trailing ",
            "a/../b",
        ] {
            assert!(PortableRelPath::new(bad).is_err(), "{bad:?} accepted");
        }
        // Not reserved: only the exact device stems are.
        PortableRelPath::new("console.txt").unwrap();
        PortableRelPath::new("COM.txt").unwrap();
    }

    // Layout tests

    #[test]
    fn layout_valid() {
        let l = Layout::new("files/{hash_fn}/{prefix}/{rest}").unwrap();
        let hex = "ab".repeat(32);
        let d = Hexdigest::new(&hex, HashFunction::Sha256).unwrap();
        let key = l.object_key(&d).unwrap();
        assert_eq!(key, format!("files/sha256/{}/{}", d.prefix(), d.rest()));
    }

    #[test]
    fn layout_without_hash_fn_is_sha256_only() {
        let l = Layout::new("files/sha256/{prefix}/{rest}").unwrap();
        let sha_hex = "ab".repeat(32);
        let sha_d = Hexdigest::new(&sha_hex, HashFunction::Sha256).unwrap();
        // sha256 works
        assert!(l.object_key(&sha_d).is_ok());
        // md5 is rejected
        let md5_hex = "ab".repeat(16);
        let md5_d = Hexdigest::new(&md5_hex, HashFunction::Md5).unwrap();
        assert!(l.object_key(&md5_d).is_err());
    }

    #[test]
    fn layout_with_hash_fn_supports_both() {
        let l = Layout::new("files/{hash_fn}/{prefix}/{rest}").unwrap();
        let sha_hex = "ab".repeat(32);
        let sha_d = Hexdigest::new(&sha_hex, HashFunction::Sha256).unwrap();
        assert!(l.object_key(&sha_d).is_ok());
        let md5_hex = "ab".repeat(16);
        let md5_d = Hexdigest::new(&md5_hex, HashFunction::Md5).unwrap();
        let key = l.object_key(&md5_d).unwrap();
        assert!(key.contains("md5/"));
    }

    #[test]
    fn layout_rejects_missing_prefix() {
        assert!(Layout::new("files/{hash_fn}/{rest}").is_err());
    }

    #[test]
    fn layout_rejects_missing_rest() {
        assert!(Layout::new("files/{hash_fn}/{prefix}").is_err());
    }

    #[test]
    fn layout_rejects_empty() {
        assert!(Layout::new("oops-no-placeholders").is_err());
    }

    #[test]
    fn layout_default_is_dvc_compatible() {
        let l = Layout::default();
        assert!(l.to_string().starts_with("files/"));
    }

    #[test]
    fn layout_serde_roundtrip() {
        let l = Layout::default();
        let json = serde_json::to_string(&l).unwrap();
        let parsed: Layout = serde_json::from_str(&json).unwrap();
        assert_eq!(l, parsed);
    }

    #[test]
    fn layout_serde_rejects_invalid() {
        let json = "\"no-placeholders\"";
        let result: std::result::Result<Layout, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }
}
