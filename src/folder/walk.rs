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
/// `/` matches directories only (a symlink to a directory counts as one, as
/// in DVC), and an excluded directory is skipped whole.
/// `*`, `?` and `[…]` never match `/`; `**` matches any number of
/// directories; `\` escapes. Negation (`!`) is not supported.
///
/// Skipped entries are never read or walked (of a symlink, only its
/// target's type is looked at), so an excluded symlink or special file is
/// not refused. Nested `.git`/`.dvc` and `*.dvc` are refused even if
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

#[cfg(test)]
type Hook = Box<dyn FnMut(&Path) + Send>;

/// Test seam: each hook is called with every directory of a walk of its
/// root right after the walk has listed it, so a test can change the tree at
/// an exact point mid-walk. Keyed by root rather than thread-local: push
/// walks on tokio's blocking pool, and tests run in parallel.
#[cfg(test)]
static AFTER_LISTING: std::sync::Mutex<Vec<(PathBuf, Hook)>> = std::sync::Mutex::new(Vec::new());

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
            // A symlink counts as what it points at, as in DVC: a dir-only
            // pattern matches a symlink to a directory. Only its target's
            // type is read; a symlink is never walked.
            let target = kind.is_symlink().then(|| std::fs::metadata(&path));
            let is_dir = kind.is_dir() || matches!(&target, Some(Ok(m)) if m.is_dir());
            if excludes.excludes(&relpath, is_dir) {
                continue;
            }
            any = true;
            if kind.is_dir() {
                stack.push((path, relpath));
                continue;
            }
            let is_file = match target {
                None => kind.is_file(),
                Some(Ok(m)) if m.is_file() => true,
                Some(Ok(m)) if m.is_dir() => {
                    return Err(refused(&relpath, Refusal::SymlinkToDirectory))
                }
                Some(Ok(_)) => false,
                Some(Err(_)) => return Err(refused(&relpath, Refusal::BrokenSymlink)),
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
        #[cfg(test)]
        for (hooked, hook) in AFTER_LISTING.lock().unwrap().iter_mut() {
            if hooked == root {
                hook(&dir);
            }
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

    /// As in DVC, a dir-only pattern (`scratch/`) matches a symlink to a
    /// directory, which is then skipped, not refused and never walked; it
    /// does not match a symlink to a file.
    #[cfg(unix)]
    #[test]
    fn a_dir_only_exclude_skips_a_symlinked_directory() {
        use std::os::unix::fs::symlink;
        let d = tempfile::tempdir().unwrap();
        let out = d.path().join("out");
        std::fs::create_dir_all(d.path().join("elsewhere")).unwrap();
        std::fs::write(d.path().join("elsewhere/f"), b"f").unwrap();
        std::fs::create_dir_all(out.join("a")).unwrap();
        std::fs::write(out.join("a/real.txt"), b"x").unwrap();
        symlink("../elsewhere", out.join("scratch")).unwrap();
        symlink("../../elsewhere", out.join("a/scratch")).unwrap();
        symlink("real.txt", out.join("a/keep")).unwrap();
        let excludes = Excludes::new(["scratch/", "keep/"]).unwrap();
        let w = super::walk(&out, &excludes).unwrap();
        assert_eq!(names(&w), ["a/keep", "a/real.txt"]);
        assert_eq!(w.empty_dirs, 0);
    }

    #[test]
    fn vanished_root_is_a_change() {
        let d = tempfile::tempdir().unwrap();
        assert!(matches!(
            walk(&d.path().join("gone")),
            Err(WalkError::Changed(_))
        ));
    }

    /// Push `out` (holding `site/labels.jsonl` and `site/gt_geometry/{a,b}`)
    /// while `site/gt_geometry` is renamed to `site/gt_geometry_v2` once,
    /// right after the walk lists the directory ending in `after`. Returns
    /// how many walks push started and the files a pull of the pushed
    /// version restores.
    fn push_renaming_mid_walk(after: &'static str) -> (usize, Vec<(String, Vec<u8>)>) {
        use crate::folder::{
            self, Credentials, HistoryKey, PointerSource, PullOptions, PushOptions, Remote,
            RemoteConfig,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("out");
        let site = out.join("site");
        std::fs::create_dir_all(site.join("gt_geometry")).unwrap();
        std::fs::write(site.join("labels.jsonl"), b"{}\n").unwrap();
        std::fs::write(site.join("gt_geometry/a.parquet"), b"a").unwrap();
        std::fs::write(site.join("gt_geometry/b.parquet"), b"b").unwrap();

        let walks = Arc::new(AtomicUsize::new(0));
        let hook = {
            let (out, walks) = (out.clone(), walks.clone());
            let mut renamed = false;
            move |dir: &Path| {
                if dir == out {
                    walks.fetch_add(1, Ordering::SeqCst);
                }
                if !renamed && dir.ends_with(after) {
                    renamed = true;
                    let site = out.join("site");
                    std::fs::rename(site.join("gt_geometry"), site.join("gt_geometry_v2")).unwrap();
                }
            }
        };
        AFTER_LISTING
            .lock()
            .unwrap()
            .push((out.clone(), Box::new(hook)));
        let remote = Remote::open(&RemoteConfig {
            url: format!("local://{}", tmp.path().join("remote").display()),
            endpoint: None,
            region: None,
            credentials: Credentials::FromEnv,
        })
        .unwrap();
        let pushed = folder::push(
            &remote,
            &out,
            &PushOptions {
                jobs: 2,
                ..PushOptions::new(HistoryKey::new("ds/out").unwrap())
            },
        );
        AFTER_LISTING
            .lock()
            .unwrap()
            .retain(|(root, _)| *root != out);
        let report = pushed.unwrap();
        assert_eq!(report.files, 3);

        let restore = tmp.path().join("restore");
        folder::pull(
            &remote,
            &PointerSource::File(report.pointer_path),
            &PullOptions {
                into: Some(restore.clone()),
                jobs: 2,
                ..PullOptions::default()
            },
        )
        .unwrap();
        let mut restored: Vec<_> = walkdir::WalkDir::new(&restore)
            .into_iter()
            .map(Result::unwrap)
            .filter(|e| e.file_type().is_file())
            .map(|e| {
                let rel = e.path().strip_prefix(&restore).unwrap();
                let rel = rel.to_str().unwrap().replace('\\', "/");
                (rel, std::fs::read(e.path()).unwrap())
            })
            .collect();
        restored.sort();
        (walks.load(Ordering::SeqCst), restored)
    }

    fn renamed_tree() -> Vec<(String, Vec<u8>)> {
        [
            ("site/gt_geometry_v2/a.parquet", &b"a"[..]),
            ("site/gt_geometry_v2/b.parquet", b"b"),
            ("site/labels.jsonl", b"{}\n"),
        ]
        .map(|(p, c)| (p.to_string(), c.to_vec()))
        .to_vec()
    }

    /// Renamed after its parent was listed, before it was: the walk finds
    /// it gone, push walks again and backs up the tree as renamed.
    #[test]
    fn push_retries_when_a_directory_is_renamed_before_the_walk_reaches_it() {
        let (walks, restored) = push_renaming_mid_walk("site");
        assert_eq!(walks, 2);
        assert_eq!(restored, renamed_tree());
    }

    /// Renamed after its files were listed: the walk succeeds with stale
    /// paths, the snapshot finds them gone, and push walks again.
    #[test]
    fn push_retries_when_a_directory_is_renamed_after_its_files_were_listed() {
        let (walks, restored) = push_renaming_mid_walk("gt_geometry");
        assert_eq!(walks, 2);
        assert_eq!(restored, renamed_tree());
    }
}
