use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::{self, BufRead, Read, Write};
use std::path::Path;

use crate::cache;
use crate::git;
use crate::types::{HashFunction, Hexdigest, Pointer};

const POINTER_HEADER: &[u8] = b"bigstore\n";

/// Clean filter: file content -> pointer (stdin -> stdout).
///
/// If the input is already a pointer, pass through unchanged (idempotent).
pub fn clean() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();

    let mut first_line = Vec::new();
    reader.read_until(b'\n', &mut first_line)?;

    if first_line == POINTER_HEADER {
        writer.write_all(&first_line)?;
        io::copy(&mut reader, &mut writer)?;
        return Ok(());
    }

    let git_dir = git::git_dir()?;
    let hash_fn = HashFunction::Sha256;
    let mut hasher = Sha256::new();

    cache::ensure_cache_dir(&git_dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(cache::cache_dir(&git_dir))?;

    hasher.update(&first_line);
    tmp.write_all(&first_line)?;

    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        tmp.write_all(&buf[..n])?;
    }
    tmp.flush()?;

    let hex_str = hex::encode(hasher.finalize());
    let hexdigest = Hexdigest::new(&hex_str, hash_fn)
        .context("internal error: sha256 produced invalid hex")?;

    let dest = cache::object_path(&git_dir, &hexdigest, hash_fn);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Atomic persist, ignore AlreadyExists (concurrent clean of same content)
    match tmp.persist_noclobber(&dest) {
        Ok(_) => {}
        Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }

    let pointer = Pointer::new(hash_fn, hexdigest);
    writer.write_all(&pointer.encode())?;

    Ok(())
}

/// Smudge filter: pointer -> file content (stdin -> stdout).
///
/// If the object is in the local cache, output its content.
/// If not, pass through the pointer (user needs to `git bigstore pull`).
pub fn smudge() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();

    let mut first_line = Vec::new();
    reader.read_until(b'\n', &mut first_line)?;

    if first_line != POINTER_HEADER {
        writer.write_all(&first_line)?;
        io::copy(&mut reader, &mut writer)?;
        return Ok(());
    }

    let mut hash_fn_line = String::new();
    reader.read_line(&mut hash_fn_line)?;
    let mut hexdigest_line = String::new();
    reader.read_line(&mut hexdigest_line)?;

    // Reconstruct the raw pointer bytes for pass-through on failure
    let raw_pointer = [
        &first_line[..],
        hash_fn_line.as_bytes(),
        hexdigest_line.as_bytes(),
    ]
    .concat();

    // Validate the pointer — if invalid, pass through as-is
    let pointer = match Pointer::parse(&raw_pointer)? {
        Some(p) => p,
        None => {
            writer.write_all(&raw_pointer)?;
            return Ok(());
        }
    };

    let git_dir = git::git_dir()?;
    let cache_path = cache::object_path(&git_dir, &pointer.hexdigest, pointer.hash_fn);

    if cache_path.exists() {
        let mut file = std::fs::File::open(&cache_path)?;
        io::copy(&mut file, &mut writer)?;
    } else {
        writer.write_all(&raw_pointer)?;
    }

    Ok(())
}

/// Read a pointer from git's stored content for a tracked file.
pub fn read_pointer_from_git(path: &str) -> Result<Option<Pointer>> {
    let output = std::process::Command::new("git")
        .args(["show", &format!("HEAD:{path}")])
        .output()?;

    if !output.status.success() {
        return Ok(None);
    }

    Pointer::parse(&output.stdout)
}

/// Check if a working-tree file is a pointer (not yet smudged).
pub fn is_pointer_file(path: &Path) -> bool {
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    let mut reader = io::BufReader::new(file);
    let mut first_line = Vec::new();
    if reader.read_until(b'\n', &mut first_line).is_err() {
        return false;
    }
    first_line == POINTER_HEADER
}

/// Parse .gitattributes for bigstore filter patterns.
/// Returns Vec<(glob_pattern, filter_name)>.
pub fn parse_gitattributes(path: &Path) -> Result<Vec<(String, String)>> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut filters = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            for attr in &parts[1..] {
                if let Some(filter_name) = attr.strip_prefix("filter=") {
                    if filter_name == "bigstore" || filter_name == "bigstore-compress" {
                        filters.push((parts[0].to_string(), filter_name.to_string()));
                    }
                }
            }
        }
    }

    Ok(filters)
}
