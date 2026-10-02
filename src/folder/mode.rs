//! Setting the mode of a file bigstore just created (or, on pull, made
//! executable), where the file system may govern modes by ACL.
//!
//! A TrueNAS NFSv4-ACL dataset with `aclmode=restricted` (an SMB share)
//! refuses every `chmod` with `EPERM`, and gives each new file the mode
//! its ACL says. There the file keeps that mode: `EPERM` is not an error.
//! Every other error still is. `EPERM` also means "not the file's owner";
//! a file this process created is its own, and for any other the owner is
//! checked first, so that case still fails.

use std::fs::{File, Permissions};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// `EPERM`: 1 on every unix.
const EPERM: i32 = 1;

/// Give `file`, newly created, `mode`; see [the module docs](self).
pub(crate) fn set_file_mode(file: &File, mode: u32) -> io::Result<()> {
    tolerate_acl(chmod(|| file.set_permissions(Permissions::from_mode(mode))))
}

/// Give the existing file at `path`, owned by `owner` (a uid), `mode`. As
/// for a new file when this process's effective user owns it (the ACL
/// case); otherwise `EPERM` fails, since it means the file is someone
/// else's.
pub(crate) fn set_path_mode(path: &Path, mode: u32, owner: u32) -> io::Result<()> {
    let set = chmod(|| std::fs::set_permissions(path, Permissions::from_mode(mode)));
    if owner == rustix::process::geteuid().as_raw() {
        tolerate_acl(set)
    } else {
        set
    }
}

fn tolerate_acl(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if e.raw_os_error() == Some(EPERM) => Ok(()),
        result => result,
    }
}

/// Run `set`, the `chmod`; in tests, fail it instead with the errno
/// [`fail_chmod`] set on this thread.
fn chmod(set: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    #[cfg(test)]
    if let Some(errno) = FAIL_CHMOD.with(std::cell::Cell::get) {
        return Err(io::Error::from_raw_os_error(errno));
    }
    set()
}

#[cfg(test)]
thread_local! {
    static FAIL_CHMOD: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

/// Make every `chmod` here on this thread fail with `errno` until the
/// returned guard drops.
#[cfg(test)]
pub(crate) fn fail_chmod(errno: i32) -> impl Drop {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FAIL_CHMOD.with(|f| f.set(None));
        }
    }
    FAIL_CHMOD.with(|f| f.set(Some(errno)));
    Reset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eperm_on_an_existing_file_is_tolerated_only_for_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool");
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        let me = rustix::process::geteuid().as_raw();
        let _eperm = fail_chmod(EPERM);
        set_path_mode(&path, 0o755, me).unwrap();
        let err = set_path_mode(&path, 0o755, me.wrapping_add(1)).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(EPERM));
    }
}
