//! Setting the mode of a file bigstore just created (or, on pull, made
//! executable), where the file system may govern modes by ACL.
//!
//! A TrueNAS NFSv4-ACL dataset with `aclmode=restricted` (an SMB share)
//! refuses every `chmod` with `EPERM`, and gives each new file the mode
//! its ACL says. There the file keeps that mode: `EPERM` is not an error.
//! Every other error still is.

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

/// Give the file at `path` `mode`; see [the module docs](self).
pub(crate) fn set_path_mode(path: &Path, mode: u32) -> io::Result<()> {
    tolerate_acl(chmod(|| {
        std::fs::set_permissions(path, Permissions::from_mode(mode))
    }))
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
