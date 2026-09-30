//! List the files of a directory output exactly as DVC 3 would, refusing
//! every case where DVC would silently drop or reinterpret data.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use super::{Error, Refusal};
use crate::types::PortableRelPath;

/// Files the OS writes into any folder it shows, which push always skips:
/// Finder's `.DS_Store`, AppleDouble `._*` companions (macOS writes them
/// beside files on FAT, exFAT and network volumes), and Explorer's
/// `Thumbs.db` and `desktop.ini`. Each is also a `.dvcignore` line with the
/// same meaning.
pub const DEFAULT_EXCLUDES: [&str; 4] = [".DS_Store", "._*", "Thumbs.db", "desktop.ini"];

/// Entries a directory push skips: [`DEFAULT_EXCLUDES`] plus the caller's
/// patterns. Patterns follow `.gitignore`/`.dvcignore` rules relative to the
/// output: without a `/` a pattern matches a name at any depth; with one
/// (leading or inside) it matches the path from the output root; a trailing
/// `/` matches directories only, and an excluded directory is skipped whole.
/// `*`, `?` and `[…]` never match `/`; `**` matches any number of
/// directories; `\` escapes. Negation (`!`) is not supported.
///
/// Skipped entries are never inspected, so an excluded symlink or special
/// file is not refused. Nested `.git`/`.dvc` and `*.dvc` are refused even if
/// excluded.
#[derive(Debug, Clone)]
pub struct Excludes {
    any: GlobSet,
    dirs: GlobSet,
}

impl Excludes {
    /// The defaults plus `patterns`. A pattern that cannot be compiled is
    /// [`Error::InvalidExclude`].
    pub fn new<I>(patterns: I) -> anyhow::Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut any = GlobSetBuilder::new();
        let mut dirs = GlobSetBuilder::new();
        for pattern in DEFAULT_EXCLUDES {
            add_pattern(pattern, &mut any, &mut dirs)?;
        }
        for pattern in patterns {
            add_pattern(pattern.as_ref(), &mut any, &mut dirs)?;
        }
        Ok(Self {
            any: any.build().context("exclude patterns are too complex")?,
            dirs: dirs.build().context("exclude patterns are too complex")?,
        })
    }

    /// `relpath` is relative to the output, `/`-separated.
    fn excludes(&self, relpath: &str, is_dir: bool) -> bool {
        self.any.is_match(relpath) || (is_dir && self.dirs.is_match(relpath))
    }
}

impl Default for Excludes {
    /// Only [`DEFAULT_EXCLUDES`].
    fn default() -> Self {
        Self::new(std::iter::empty::<&str>()).expect("the default excludes compile")
    }
}

/// Compile one `.gitignore`-style pattern into a glob over the whole
/// relative path, into `dirs` if it ends in `/`.
fn add_pattern(
    pattern: &str,
    any: &mut GlobSetBuilder,
    dirs: &mut GlobSetBuilder,
) -> anyhow::Result<()> {
    let invalid = || Error::InvalidExclude {
        pattern: pattern.to_string(),
    };
    if pattern.starts_with('!') {
        return Err(anyhow::anyhow!("negation is not supported").context(invalid()));
    }
    let (body, dir_only) = match pattern.strip_suffix('/') {
        Some(body) => (body, true),
        None => (pattern, false),
    };
    // A `/` at the start or inside anchors the pattern to the output root.
    let anchored = body.contains('/');
    let body = body.strip_prefix('/').unwrap_or(body);
    if body.is_empty() {
        return Err(anyhow::anyhow!("the pattern names nothing").context(invalid()));
    }
    let full = if anchored {
        body.to_string()
    } else {
        format!("**/{body}")
    };
    let glob = GlobBuilder::new(&full)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .with_context(invalid)?;
    let set = if dir_only { dirs } else { any };
    set.add(glob);
    Ok(())
}

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
    Refused {
        /// Relative to the walked root, `/`-separated.
        relpath: String,
        reason: Refusal,
    },
    Io(anyhow::Error),
}

impl From<WalkError> for anyhow::Error {
    fn from(e: WalkError) -> Self {
        match e {
            WalkError::Changed(m) => anyhow::anyhow!(m),
            WalkError::Refused { relpath, reason } => Error::Refused {
                path: relpath.into(),
                reason,
            }
            .into(),
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
/// - entries matching `excludes` are skipped, and a directory holding only
///   skipped entries counts as empty;
/// - empty directories are counted.
pub fn walk(root: &Path, excludes: &Excludes) -> Result<Walk, WalkError> {
    let refused = |relpath: &str, reason| WalkError::Refused {
        relpath: relpath.to_string(),
        reason,
    };
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
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                let relpath = Path::new(&rel).join(&name);
                return Err(refused(
                    &relpath.to_string_lossy().replace('\\', "/"),
                    Refusal::NotUtf8Name,
                ));
            };
            let relpath = if rel.is_empty() {
                name.to_string()
            } else {
                format!("{rel}/{name}")
            };
            if matches!(name, ".git" | ".hg" | ".dvc" | ".dvcignore") || name.ends_with(".dvc") {
                return Err(refused(&relpath, Refusal::ControlFile));
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
            if excludes.excludes(&relpath, kind.is_dir()) {
                continue;
            }
            any = true;
            if kind.is_dir() {
                stack.push((path, relpath));
                continue;
            }
            let is_file = if kind.is_symlink() {
                match std::fs::metadata(&path) {
                    Ok(m) if m.is_file() => true,
                    Ok(m) if m.is_dir() => {
                        return Err(refused(&relpath, Refusal::SymlinkToDirectory))
                    }
                    Ok(_) => false,
                    Err(_) => return Err(refused(&relpath, Refusal::BrokenSymlink)),
                }
            } else {
                kind.is_file()
            };
            if !is_file {
                return Err(refused(&relpath, Refusal::SpecialFile));
            }
            let relpath = PortableRelPath::new(&relpath).map_err(|e| {
                refused(
                    &relpath,
                    Refusal::NonPortableName {
                        detail: format!("{e:#}"),
                    },
                )
            })?;
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

    fn walk(root: &Path) -> Result<Walk, WalkError> {
        super::walk(root, &Excludes::default())
    }

    #[test]
    fn walks_nested_files_skips_os_junk_and_counts_empty_dirs() {
        let d = tempfile::tempdir().unwrap();
        let r = d.path();
        std::fs::create_dir_all(r.join("site=s1/date=d/src/gt_geometry")).unwrap();
        std::fs::create_dir_all(r.join("empty/inner")).unwrap();
        std::fs::write(r.join("site=s1/date=d/src/labels.jsonl"), b"{}\n").unwrap();
        std::fs::write(r.join("site=s1/date=d/src/gt_geometry/a.parquet"), b"p").unwrap();
        std::fs::write(r.join(".DS_Store"), b"x").unwrap();
        std::fs::write(r.join("site=s1/._labels.jsonl"), b"x").unwrap();
        // A directory holding only junk is as empty as DVC sees it.
        std::fs::write(r.join("empty/inner/Thumbs.db"), b"x").unwrap();
        let w = walk(r).unwrap();
        assert_eq!(
            names(&w),
            [
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
                matches!(
                    walk(d.path()),
                    Err(WalkError::Refused {
                        reason: Refusal::ControlFile,
                        ..
                    })
                ),
                "{bad} accepted"
            );
        }
    }

    #[test]
    fn refuses_non_portable_names() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("café.json"), b"x").unwrap();
        assert!(matches!(
            walk(d.path()),
            Err(WalkError::Refused {
                reason: Refusal::NonPortableName { .. },
                ..
            })
        ));
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
        assert!(matches!(
            walk(d.path()),
            Err(WalkError::Refused {
                reason: Refusal::SymlinkToDirectory,
                ..
            })
        ));
        std::fs::remove_file(d.path().join("dirlink")).unwrap();

        symlink("nowhere", d.path().join("broken")).unwrap();
        assert!(matches!(
            walk(d.path()),
            Err(WalkError::Refused {
                reason: Refusal::BrokenSymlink,
                ..
            })
        ));
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
