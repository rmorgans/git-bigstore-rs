//! List the files of a directory output exactly as DVC 3 would, refusing
//! every case where DVC would silently drop or reinterpret data.

use std::path::{Path, PathBuf};

use crate::types::PortableRelPath;

/// One file of a directory output.
pub struct WalkedFile {
    pub relpath: PortableRelPath,
    /// Where to read it (a symlink's target is read through the link).
    pub path: PathBuf,
}

pub struct Walk {
    pub files: Vec<WalkedFile>,
    /// Empty directories: DVC cannot record them.
    pub empty_dirs: usize,
}

/// Why a walk failed.
#[derive(Debug)]
pub enum WalkError {
    /// Something appeared or vanished mid-walk: retry.
    Changed(String),
    /// The directory holds something folder mode refuses to back up.
    Refused(String),
    Io(anyhow::Error),
}

impl std::fmt::Display for WalkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Changed(m) | Self::Refused(m) => f.write_str(m),
            Self::Io(e) => write!(f, "{e:#}"),
        }
    }
}

impl From<WalkError> for anyhow::Error {
    fn from(e: WalkError) -> Self {
        match e {
            WalkError::Changed(m) | WalkError::Refused(m) => anyhow::anyhow!(m),
            WalkError::Io(e) => e,
        }
    }
}

/// Walk `root`.
///
/// - regular file → entry; symlink to a file → entry with the target's
///   content (as DVC);
/// - symlink to a directory, broken symlink, FIFO/socket/device → refused
///   (DVC drops the first silently and fails on the second);
/// - `.git`, `.hg`, `.dvc`, `.dvcignore` or a `*.dvc` file anywhere inside
///   → refused (DVC would exclude or reject them, and nested outputs are
///   illegal);
/// - names must be portable ([`PortableRelPath`]);
/// - empty directories are counted.
pub fn walk(root: &Path) -> Result<Walk, WalkError> {
    let mut files = Vec::new();
    let mut empty_dirs = 0;
    let mut stack = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, rel)) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(WalkError::Changed(format!(
                    "{} disappeared while being walked",
                    dir.display()
                )))
            }
            Err(e) => return Err(WalkError::Io(e.into())),
        };
        let mut any = false;
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(WalkError::Changed(format!(
                        "{} changed while being walked",
                        dir.display()
                    )))
                }
                Err(e) => return Err(WalkError::Io(e.into())),
            };
            any = true;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                WalkError::Refused(format!(
                    "{}: name is not valid UTF-8",
                    entry.path().display()
                ))
            })?;
            let relpath = if rel.is_empty() {
                name.to_string()
            } else {
                format!("{rel}/{name}")
            };
            if matches!(name, ".git" | ".hg" | ".dvc" | ".dvcignore") || name.ends_with(".dvc") {
                return Err(WalkError::Refused(format!(
                    "{relpath}: DVC control files and nested repositories/outputs \
                     cannot be inside a backed-up directory"
                )));
            }
            let path = entry.path();
            let kind = match std::fs::symlink_metadata(&path) {
                Ok(m) => m.file_type(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(WalkError::Changed(format!(
                        "{relpath} disappeared while being walked"
                    )))
                }
                Err(e) => return Err(WalkError::Io(e.into())),
            };
            if kind.is_dir() {
                stack.push((path, relpath));
                continue;
            }
            let is_file = if kind.is_symlink() {
                match std::fs::metadata(&path) {
                    Ok(m) if m.is_file() => true,
                    Ok(m) if m.is_dir() => {
                        return Err(WalkError::Refused(format!(
                            "{relpath}: symlink to a directory (DVC would silently skip it)"
                        )))
                    }
                    Ok(_) => false,
                    Err(_) => return Err(WalkError::Refused(format!("{relpath}: broken symlink"))),
                }
            } else {
                kind.is_file()
            };
            if !is_file {
                return Err(WalkError::Refused(format!("{relpath}: not a regular file")));
            }
            let relpath =
                PortableRelPath::new(&relpath).map_err(|e| WalkError::Refused(format!("{e:#}")))?;
            files.push(WalkedFile { relpath, path });
        }
        if !any && !rel.is_empty() {
            empty_dirs += 1;
        }
    }
    Ok(Walk { files, empty_dirs })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(w: &Walk) -> Vec<String> {
        let mut v: Vec<_> = w.files.iter().map(|f| f.relpath.to_string()).collect();
        v.sort();
        v
    }

    #[test]
    fn walks_nested_files_and_counts_empty_dirs() {
        let d = tempfile::tempdir().unwrap();
        let r = d.path();
        std::fs::create_dir_all(r.join("site=s1/date=d/src/gt_geometry")).unwrap();
        std::fs::create_dir_all(r.join("empty/inner")).unwrap();
        std::fs::write(r.join("site=s1/date=d/src/labels.jsonl"), b"{}\n").unwrap();
        std::fs::write(r.join("site=s1/date=d/src/gt_geometry/a.parquet"), b"p").unwrap();
        std::fs::write(r.join(".DS_Store"), b"x").unwrap();
        let w = walk(r).unwrap();
        assert_eq!(
            names(&w),
            [
                ".DS_Store",
                "site=s1/date=d/src/gt_geometry/a.parquet",
                "site=s1/date=d/src/labels.jsonl"
            ]
        );
        assert_eq!(w.empty_dirs, 1);
    }

    #[test]
    fn refuses_control_files_and_nested_outputs() {
        for bad in [".git/HEAD", "sub/.dvc/config", "sub/x.dvc", ".dvcignore"] {
            let d = tempfile::tempdir().unwrap();
            let p = d.path().join(bad);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, b"x").unwrap();
            assert!(
                matches!(walk(d.path()), Err(WalkError::Refused(_))),
                "{bad} accepted"
            );
        }
    }

    #[test]
    fn refuses_non_portable_names() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("café.json"), b"x").unwrap();
        assert!(matches!(walk(d.path()), Err(WalkError::Refused(_))));
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_follow_files_and_refuse_dirs_and_broken_links() {
        use std::os::unix::fs::symlink;
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("real.toml"), b"x = 1\n").unwrap();
        symlink("real.toml", d.path().join("link.toml")).unwrap();
        let w = walk(d.path()).unwrap();
        assert_eq!(names(&w), ["link.toml", "real.toml"]);

        std::fs::create_dir(d.path().join("dir")).unwrap();
        std::fs::write(d.path().join("dir/f"), b"f").unwrap();
        symlink("dir", d.path().join("dirlink")).unwrap();
        assert!(matches!(walk(d.path()), Err(WalkError::Refused(_))));
        std::fs::remove_file(d.path().join("dirlink")).unwrap();

        symlink("nowhere", d.path().join("broken")).unwrap();
        assert!(matches!(walk(d.path()), Err(WalkError::Refused(_))));
    }

    #[test]
    fn vanished_root_is_a_change() {
        let d = tempfile::tempdir().unwrap();
        assert!(matches!(
            walk(&d.path().join("gone")),
            Err(WalkError::Changed(_))
        ));
    }
}
