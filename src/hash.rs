//! Streaming content hashing for every supported [`HashFunction`].

use anyhow::{Context, Result};
use md5::Md5;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;

use crate::types::{HashFunction, Hexdigest};

pub enum Hasher {
    Sha256(Sha256),
    Md5(Md5),
}

impl Hasher {
    pub fn new(hash_fn: HashFunction) -> Self {
        match hash_fn {
            HashFunction::Sha256 => Self::Sha256(Sha256::new()),
            HashFunction::Md5 => Self::Md5(Md5::new()),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha256(h) => h.update(data),
            Self::Md5(h) => h.update(data),
        }
    }

    pub fn finalize(self) -> Hexdigest {
        let (hash_fn, hex) = match self {
            Self::Sha256(h) => (HashFunction::Sha256, hex::encode(h.finalize())),
            Self::Md5(h) => (HashFunction::Md5, hex::encode(h.finalize())),
        };
        Hexdigest::new(&hex, hash_fn).expect("hasher output is valid hex of the right length")
    }
}

/// Hash everything `reader` yields.
pub fn hash_reader(reader: &mut impl Read, hash_fn: HashFunction) -> std::io::Result<Hexdigest> {
    let mut hasher = Hasher::new(hash_fn);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(hasher.finalize());
        }
        hasher.update(&buf[..n]);
    }
}

/// Hash a file on disk.
pub fn hash_file(path: &Path, hash_fn: HashFunction) -> Result<Hexdigest> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    hash_reader(&mut file, hash_fn).with_context(|| format!("failed to read {}", path.display()))
}
