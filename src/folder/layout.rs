//! A folder-mode store's files, in one place: which keys are a store's own,
//! the order a transfer between stores places them in, and whether a file's
//! bytes are what its name says.
//!
//! A key is relative to the store, `/`-separated, in the form the store
//! holds it on disk, which is how object_store encodes it (a `~` in a
//! history key is `%7E`):
//!
//! | Kind | Key |
//! | --- | --- |
//! | [`Kind::Object`] | `files/md5/<2 hex>/<30 hex>`: a file's content, named by its md5 |
//! | [`Kind::Manifest`] | `files/md5/<2 hex>/<30 hex>.dir`: a directory version's manifest, named by its md5 |
//! | [`Kind::Record`] | `bigstore-history/<history key>/<parents>/<id>.dvc`: a version, `<id>` the [`RecordId`] of its bytes; or a 0.2 record, `bigstore-history/<history key>/<time>-<content id>.dvc` |
//! | [`Kind::Other`] | anything else: a temp file (`#` in its name, `.partial`), a desktop's `.DS_Store`, an absolute or traversing key (`..`, `\`, `C:`), … |
//!
//! Every name is fixed by its content, so a store only ever gains files, and
//! a file already present under its name never needs replacing.

use anyhow::Result;

use super::history::{self, Name};
use super::Error;
use crate::dvc::{BigstoreMeta, DvcPointer, RecordId};
use crate::hash::Hasher;
use crate::types::{check_portable_component, HashFunction, Hexdigest};

/// Largest `.dir` manifest read or accepted.
pub(crate) const MAX_MANIFEST_BYTES: u64 = 64 << 20;
/// Largest history record read or accepted.
pub(crate) const MAX_RECORD_BYTES: u64 = 64 << 10;

const OBJECTS: &str = "files/md5/";
const RECORDS: &str = "bigstore-history/";

/// What a store key names. The order is the order a transfer places files
/// in: every object, then every manifest, then every record. So a manifest
/// never exists without the objects it names (push relies on it: a
/// manifest on the remote means all its objects are), and a record never
/// exists without its version's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Object,
    Manifest,
    Record,
    /// Not one of a store's own files.
    Other,
}

/// What `key`, relative to a store, names. See [the module docs](self).
pub fn kind(key: &str) -> Kind {
    match parse(key) {
        Some(Named::Object(_)) => Kind::Object,
        Some(Named::Manifest(_)) => Kind::Manifest,
        Some(Named::Record { .. } | Named::Legacy) => Kind::Record,
        None => Kind::Other,
    }
}

/// Check that `bytes` are the file `key` names: an object's or manifest's
/// md5 is its name; a record's [`RecordId`] is its name's id, and the
/// versions it follows are its name's parents; a 0.2 record, whose name
/// says no id, is checked for size only. A manifest is at most 64 MiB and
/// a record 64 KiB.
///
/// A key that is not a store file's ([`Kind::Other`]) is
/// [`Error::InvalidStoreKey`]; bytes that are not what the key names are
/// [`Error::Integrity`].
pub fn verify(key: &str, bytes: &[u8]) -> Result<()> {
    let mut check = Check::new(key, bytes.len() as u64)?;
    check.update(bytes);
    check.finish()
}

/// What a key's name says its bytes are.
enum Named {
    Object(Hexdigest),
    Manifest(Hexdigest),
    Record {
        parents: Vec<RecordId>,
        id: RecordId,
    },
    /// A 0.2 record: its name says no content id.
    Legacy,
}

fn parse(key: &str) -> Option<Named> {
    if !key.split('/').all(encoded_component) {
        return None;
    }
    if let Some(rest) = key.strip_prefix(OBJECTS) {
        let (shard, name) = rest.split_once('/')?;
        let (name, manifest) = match name.strip_suffix(".dir") {
            Some(name) => (name, true),
            None => (name, false),
        };
        if shard.len() != 2 || name.len() != 30 || !lower_hex(shard) || !lower_hex(name) {
            return None;
        }
        let md5 = Hexdigest::new(&format!("{shard}{name}"), HashFunction::Md5).ok()?;
        return Some(if manifest {
            Named::Manifest(md5)
        } else {
            Named::Object(md5)
        });
    }
    let (_, name) = history::parse_record_path(key.strip_prefix(RECORDS)?)?;
    Some(match name {
        Name::Linked { parents, id } => Named::Record { parents, id },
        Name::Legacy { .. } => Named::Legacy,
    })
}

fn lower_hex(s: &str) -> bool {
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// One component of a key as object_store writes it: a name portable to
/// every OS bigstore runs on (no `.`/`..`, `\`, `:` or other character
/// Windows refuses), with none of the characters object_store
/// percent-encodes left raw, and every `%` starting a `%XX` escape.
fn encoded_component(c: &str) -> bool {
    if check_portable_component(c).is_err() {
        return false;
    }
    let b = c.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let escape = b.get(i + 1..i + 3);
                if !escape.is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit)) {
                    return false;
                }
                i += 3;
                continue;
            }
            b'{' | b'}' | b'^' | b'`' | b'[' | b']' | b'~' | b'#' => return false,
            _ => {}
        }
        i += 1;
    }
    true
}

/// Checks a file against its key as its bytes arrive: [`verify`] in
/// pieces, for files too large to hold in memory.
pub(crate) struct Check {
    key: String,
    /// The size the file is said to have.
    size: u64,
    /// Bytes seen so far.
    got: u64,
    expect: Expect,
}

enum Expect {
    Md5(Hasher, Hexdigest),
    /// The record's bytes, up to its size limit.
    Record {
        bytes: Vec<u8>,
        parents: Vec<RecordId>,
        id: RecordId,
    },
    Legacy,
}

impl Check {
    /// Start checking a file of `size` bytes at `key`, refusing a key that
    /// is not a store file's, or a size over its kind's limit.
    pub(crate) fn new(key: &str, size: u64) -> Result<Self> {
        let named = parse(key).ok_or_else(|| Error::InvalidStoreKey {
            key: key.to_string(),
        })?;
        let limit = match named {
            Named::Object(_) => u64::MAX,
            Named::Manifest(_) => MAX_MANIFEST_BYTES,
            Named::Record { .. } | Named::Legacy => MAX_RECORD_BYTES,
        };
        if size > limit {
            return Err(integrity(key));
        }
        let expect = match named {
            Named::Object(md5) | Named::Manifest(md5) => {
                Expect::Md5(Hasher::new(HashFunction::Md5), md5)
            }
            Named::Record { parents, id } => Expect::Record {
                bytes: Vec::with_capacity(size as usize),
                parents,
                id,
            },
            Named::Legacy => Expect::Legacy,
        };
        Ok(Self {
            key: key.to_string(),
            size,
            got: 0,
            expect,
        })
    }

    /// The next bytes of the file.
    pub(crate) fn update(&mut self, chunk: &[u8]) {
        self.got += chunk.len() as u64;
        match &mut self.expect {
            Expect::Md5(hasher, _) => hasher.update(chunk),
            Expect::Record { bytes, .. } => {
                // More than the size said fails in `finish`; never hold it.
                if self.got <= self.size {
                    bytes.extend_from_slice(chunk);
                }
            }
            Expect::Legacy => {}
        }
    }

    /// Whether the file was its size and what its key names.
    pub(crate) fn finish(self) -> Result<()> {
        if self.got != self.size {
            return Err(integrity(&self.key));
        }
        let matches = match self.expect {
            Expect::Md5(hasher, md5) => hasher.finalize() == md5,
            Expect::Record { bytes, parents, id } => {
                RecordId::digest(&bytes) == id && record_parents(&bytes).as_ref() == Some(&parents)
            }
            Expect::Legacy => true,
        };
        if !matches {
            return Err(integrity(&self.key));
        }
        Ok(())
    }
}

/// The versions a record says it follows, if it is a record.
fn record_parents(bytes: &[u8]) -> Option<Vec<RecordId>> {
    let pointer = DvcPointer::parse(std::str::from_utf8(bytes).ok()?).ok()?;
    match pointer.meta? {
        BigstoreMeta::Record { parents, .. } => Some(parents),
        BigstoreMeta::Base(_) => None,
    }
}

fn integrity(key: &str) -> anyhow::Error {
    Error::Integrity {
        key: key.to_string(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_components() {
        for ok in ["a", "reviewer=rick", "a%7Eb", "c%25d", ".notes", "%2E"] {
            assert!(encoded_component(ok), "{ok}");
        }
        for bad in [
            "", ".", "..", "a~b", "a#1", "a%", "a%7", "a%zz", "C:", "a\\b", "a b ", "{x}",
        ] {
            assert!(!encoded_component(bad), "{bad}");
        }
    }

    fn integrity_error(err: &anyhow::Error) -> bool {
        matches!(err.downcast_ref::<Error>(), Some(Error::Integrity { .. }))
    }

    #[test]
    fn a_legacy_record_is_a_record_checked_for_size_only() {
        let key = format!(
            "bigstore-history/a/k/20260901T000000.000000000Z-{}.dvc",
            "f".repeat(32)
        );
        assert_eq!(kind(&key), Kind::Record);
        verify(&key, b"anything at all").unwrap();
        verify(&key, &vec![0; MAX_RECORD_BYTES as usize]).unwrap();
        let over = vec![0; MAX_RECORD_BYTES as usize + 1];
        assert!(integrity_error(&verify(&key, &over).unwrap_err()));
    }

    #[test]
    fn sizes_over_a_kind_s_limit_are_refused_before_any_byte() {
        let manifest = format!("files/md5/ab/{}.dir", "c".repeat(30));
        let record = format!("bigstore-history/k/root/{}.dvc", "d".repeat(32));
        let object = format!("files/md5/ab/{}", "c".repeat(30));
        for (key, limit) in [(&manifest, MAX_MANIFEST_BYTES), (&record, MAX_RECORD_BYTES)] {
            assert!(integrity_error(&Check::new(key, limit + 1).err().unwrap()));
            assert!(Check::new(key, limit).is_ok());
        }
        assert!(Check::new(&object, u64::MAX).is_ok());
    }
}
