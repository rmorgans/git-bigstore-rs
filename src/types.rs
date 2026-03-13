use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported hash functions. Exhaustive enum — invalid states unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// A validated hex digest. Guarantees:
/// - Only lowercase hex characters [0-9a-f]
/// - Length matches the hash function (64 for sha256)
///
/// Slicing into prefix/rest is always safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hexdigest(String);

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
        Ok(Self(s.to_ascii_lowercase()))
    }

    /// First 2 hex characters (directory shard).
    pub fn prefix(&self) -> &str {
        &self.0[..2]
    }

    /// Remaining hex characters after the shard prefix.
    pub fn rest(&self) -> &str {
        &self.0[2..]
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Hexdigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated bigstore pointer. Cannot be constructed with invalid data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer {
    pub hash_fn: HashFunction,
    pub hexdigest: Hexdigest,
}

impl Pointer {
    /// Create a pointer. Panics if the hexdigest length doesn't match the hash function.
    /// Safe: Both HashFunction and Hexdigest are validated, so a mismatch is a programmer error.
    pub fn new(hash_fn: HashFunction, hexdigest: Hexdigest) -> Self {
        assert_eq!(
            hexdigest.0.len(),
            hash_fn.digest_len(),
            "hexdigest length {} does not match {} (expected {})",
            hexdigest.0.len(),
            hash_fn,
            hash_fn.digest_len()
        );
        Self { hash_fn, hexdigest }
    }

    pub fn encode(&self) -> Vec<u8> {
        format!("bigstore\n{}\n{}\n", self.hash_fn, self.hexdigest).into_bytes()
    }

    pub fn parse(data: &[u8]) -> Result<Option<Self>> {
        let text = std::str::from_utf8(data).context("pointer is not valid UTF-8")?;
        let mut lines = text.lines();

        match lines.next() {
            Some("bigstore") => {}
            _ => return Ok(None),
        }

        let hash_fn_str = lines.next().context("pointer missing hash function")?;
        let hash_fn = HashFunction::parse(hash_fn_str)?;

        let hexdigest_str = lines.next().context("pointer missing hexdigest")?;
        let hexdigest = Hexdigest::new(hexdigest_str, hash_fn)?;

        Ok(Some(Self::new(hash_fn, hexdigest)))
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

    /// Whether this layout supports multiple hash functions.
    /// Layouts without `{hash_fn}` are sha256-only (backward compatible).
    pub fn supports_hash_fn(&self, hash_fn: HashFunction) -> bool {
        self.0.contains("{hash_fn}") || hash_fn == HashFunction::Sha256
    }

    /// Format an object key from a hexdigest. Safe: Hexdigest is validated.
    ///
    /// For layouts without `{hash_fn}`, only sha256 is supported — the
    /// template is used as-is (backward compatible with older configs).
    /// For layouts with `{hash_fn}`, the placeholder is replaced dynamically.
    pub fn object_key(&self, hexdigest: &Hexdigest, hash_fn: HashFunction) -> Result<String> {
        if !self.0.contains("{hash_fn}") && hash_fn != HashFunction::Sha256 {
            anyhow::bail!(
                "layout template does not contain {{hash_fn}} — only sha256 is supported.\n\
                 Update layout in .bigstore.toml to: files/{{hash_fn}}/{{prefix}}/{{rest}}"
            );
        }
        Ok(self.0
            .replace("{hash_fn}", hash_fn.as_str())
            .replace("{prefix}", hexdigest.prefix())
            .replace("{rest}", hexdigest.rest()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
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
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Layout {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
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
        assert_eq!(d.as_str(), "a".repeat(64));
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
    #[should_panic(expected = "does not match")]
    fn pointer_rejects_mismatched_hash_fn_and_digest() {
        let hex = "a".repeat(32);
        let digest = Hexdigest::new(&hex, HashFunction::Md5).unwrap();
        // This should panic: sha256 hash_fn with a 32-char md5 digest
        Pointer::new(HashFunction::Sha256, digest);
    }

    #[test]
    fn pointer_parse_valid() {
        let hex = "ab".repeat(32);
        let data = format!("bigstore\nsha256\n{hex}\n");
        let p = Pointer::parse(data.as_bytes()).unwrap().unwrap();
        assert_eq!(p.hash_fn, HashFunction::Sha256);
        assert_eq!(p.hexdigest.as_str(), hex);
    }

    #[test]
    fn pointer_parse_not_a_pointer() {
        let data = b"just some regular file content\n";
        assert!(Pointer::parse(data).unwrap().is_none());
    }

    #[test]
    fn pointer_roundtrip() {
        let hex = "ab".repeat(32);
        let p = Pointer::new(
            HashFunction::Sha256,
            Hexdigest::new(&hex, HashFunction::Sha256).unwrap(),
        );
        let encoded = p.encode();
        let parsed = Pointer::parse(&encoded).unwrap().unwrap();
        assert_eq!(p, parsed);
    }

    #[test]
    fn pointer_rejects_malicious_hash_fn() {
        let data = b"bigstore\n../../etc\naaaa\n";
        assert!(Pointer::parse(data).is_err());
    }

    #[test]
    fn pointer_rejects_short_digest() {
        let data = b"bigstore\nsha256\ndeadbeef\n";
        assert!(Pointer::parse(data).is_err());
    }

    // Layout tests

    #[test]
    fn layout_valid() {
        let l = Layout::new("files/{hash_fn}/{prefix}/{rest}").unwrap();
        let hex = "ab".repeat(32);
        let d = Hexdigest::new(&hex, HashFunction::Sha256).unwrap();
        let key = l.object_key(&d, HashFunction::Sha256).unwrap();
        assert_eq!(key, format!("files/sha256/{}/{}", d.prefix(), d.rest()));
    }

    #[test]
    fn layout_without_hash_fn_is_sha256_only() {
        let l = Layout::new("files/sha256/{prefix}/{rest}").unwrap();
        let sha_hex = "ab".repeat(32);
        let sha_d = Hexdigest::new(&sha_hex, HashFunction::Sha256).unwrap();
        // sha256 works
        assert!(l.object_key(&sha_d, HashFunction::Sha256).is_ok());
        // md5 is rejected
        let md5_hex = "ab".repeat(16);
        let md5_d = Hexdigest::new(&md5_hex, HashFunction::Md5).unwrap();
        assert!(l.object_key(&md5_d, HashFunction::Md5).is_err());
    }

    #[test]
    fn layout_with_hash_fn_supports_both() {
        let l = Layout::new("files/{hash_fn}/{prefix}/{rest}").unwrap();
        let sha_hex = "ab".repeat(32);
        let sha_d = Hexdigest::new(&sha_hex, HashFunction::Sha256).unwrap();
        assert!(l.object_key(&sha_d, HashFunction::Sha256).is_ok());
        let md5_hex = "ab".repeat(16);
        let md5_d = Hexdigest::new(&md5_hex, HashFunction::Md5).unwrap();
        let key = l.object_key(&md5_d, HashFunction::Md5).unwrap();
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
        assert!(l.as_str().starts_with("files/"));
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
