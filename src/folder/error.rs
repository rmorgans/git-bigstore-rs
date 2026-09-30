//! Errors a caller can act on by kind.

use std::fmt;
use std::path::{Path, PathBuf};

use super::HistoryRecord;

/// A refusal, or another outcome a caller may want to handle by kind, from
/// any `bigstore::folder` function. Find it with
/// `err.downcast_ref::<folder::Error>()`: it stays reachable whatever
/// context is added above it. Everything else (I/O, network, a corrupt
/// remote) is a plain [`anyhow::Error`]. `Display` is the message the CLI
/// prints.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Files kept changing or vanishing through every retry, so no
    /// consistent snapshot could be taken. Nothing was published; push again
    /// later.
    OutputChanged {
        /// What changed last, e.g. `<path> kept changing while being read`.
        detail: String,
    },
    /// Folder mode will not back up or restore `path`. Nothing was
    /// published (push) or written (pull).
    Refused {
        /// Entries inside a pushed directory, and manifest entries refused by
        /// pull ([`Refusal::CaseCollision`], [`Refusal::UnwritableName`]),
        /// are relative to the output and `/`-separated. Anything else is the
        /// filesystem path as given or derived from the caller's arguments.
        path: PathBuf,
        reason: Refusal,
    },
    /// Local files differ from the version being pulled, under
    /// [`Overwrite::Refuse`](super::Overwrite::Refuse). Nothing was written.
    PullConflict { paths: Vec<PathBuf> },
    /// No version in the history matches the selector (or it is empty).
    NoSuchVersion,
    /// A version id prefix matches more than one version.
    AmbiguousId {
        /// The prefix, lowercased.
        prefix: String,
        /// Every matching version, oldest first.
        candidates: Vec<HistoryRecord>,
    },
    /// A [`Selector::Id`](super::Selector::Id) that is not at least 8 hex
    /// characters.
    InvalidVersionId { prefix: String },
    /// A [`Selector::AtOrBefore`](super::Selector::AtOrBefore) that is not
    /// an RFC 3339 time. The parse error is the next error in the chain.
    InvalidTime { time: String },
    /// An `s3://` remote without an endpoint: folder mode never defaults to
    /// AWS.
    EndpointRequired,
    /// An exclude pattern that cannot be compiled (see
    /// [`Excludes`](super::Excludes)). The reason is the next error in the
    /// chain.
    InvalidExclude { pattern: String },
    /// A remote URL folder mode does not support (only `s3://`, `local://`
    /// and `rclone://`).
    UnsupportedRemote { url: String },
    /// A [`HistoryKey`](super::HistoryKey) that is not a relative,
    /// `/`-separated path of portable names. The reason is the next error in
    /// the chain.
    InvalidHistoryKey { key: String },
    /// A pull from history without [`PullOptions::into`](super::PullOptions::into):
    /// there is no `.dvc` to restore beside.
    DestinationRequired,
    /// The caller's [`CancelToken`](super::CancelToken) was cancelled. A
    /// push published no `.dvc` and no history record (objects already
    /// uploaded stay: they are content-addressed); a pull left every file
    /// either as it was or fully restored, never partly written.
    Cancelled,
}

/// Why a path was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    // Push: the output itself.
    /// The output path has no final name (`..`, `/`).
    NoFileName,
    /// The output is a symlink or a special file.
    NotFileOrDirectory,
    /// The output is itself a `.dvc` file.
    DvcFile,
    /// The `.dvc` file beside the output is not a plain DVC 3 pointer (e.g.
    /// hand-written, or a pipeline stage). The parse error is the next error
    /// in the chain.
    ForeignPointer,
    /// The `.dvc` file beside the output is a pointer for another output.
    PointerForOtherOutput {
        /// The output it names.
        other: String,
    },

    // Push: entries inside a directory output.
    /// A name that is not portable to Linux, macOS and Windows (non-ASCII,
    /// a character Windows forbids, a reserved device name…).
    NonPortableName {
        /// Which rule it breaks, naming it.
        detail: String,
    },
    /// A name that is not valid UTF-8.
    NotUtf8Name,
    /// `.git`, `.hg`, `.dvc`, `.dvcignore` or a `*.dvc` file: a nested
    /// repository or output.
    ControlFile,
    /// A symlink to a directory (DVC would silently skip it).
    SymlinkToDirectory,
    /// A symlink to nothing.
    BrokenSymlink,
    /// A FIFO, socket or device (or a symlink to one).
    SpecialFile,

    // Pull.
    /// The `.dvc` file is not a DVC 3 pointer to one md5-addressed output
    /// (not YAML, several outputs, `cache: false`, an etag-only cloud
    /// output, a `wdir:`…), so there is nothing to restore. Stage fields
    /// and annotations are fine. The parse error is the next error in the
    /// chain.
    UnrestorablePointer,
    /// The `.dvc` file names an output that is not one file or directory
    /// name on this OS (`..`, an absolute path, `a/b`), so it could restore
    /// outside its own directory.
    PointerPathEscapes {
        /// The output it names.
        output: String,
    },
    /// The directory output to restore into is a symlink.
    SymlinkedOutput,
    /// Something other than a directory (a file, a symlink) sits where the
    /// version needs a directory.
    NotADirectory,
    /// Something other than a regular file (a directory, a symlink) sits
    /// where the version has a file.
    NotRegularFile,
    /// A manifest name this OS would read as a different path (`\` or `:`
    /// on Windows). The next error in the chain says why.
    UnwritableName,
    /// Two manifest names differ only by case (any script, not just ASCII)
    /// or by Unicode normalization (NFC vs NFD), and would be one file on
    /// macOS and Windows. Refused on every OS.
    CaseCollision {
        /// The other name.
        other: String,
    },
    /// A file appeared at a path while pulling; it was left untouched.
    AppearedWhilePulling,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputChanged { detail } => {
                write!(f, "{detail}; the output kept changing, push again later")
            }
            Self::Refused { path, reason } => fmt_refusal(path, reason, f),
            Self::PullConflict { paths } => {
                write!(
                    f,
                    "{} local file(s) differ from the version being pulled (nothing written; \
                     use force to replace):",
                    paths.len()
                )?;
                for p in paths {
                    write!(f, "\n  {}", p.display())?;
                }
                Ok(())
            }
            Self::NoSuchVersion => f.write_str("no matching version in history"),
            Self::AmbiguousId { prefix, candidates } => {
                write!(f, "version id prefix {prefix} is ambiguous; it matches:")?;
                for r in candidates {
                    write!(f, "\n  {}  pushed {}", r.id(), r.time.to_rfc3339())?;
                }
                Ok(())
            }
            Self::InvalidVersionId { .. } => {
                f.write_str("version id prefix must be at least 8 hex characters")
            }
            Self::InvalidTime { time } => write!(f, "not an RFC 3339 time: {time:?}"),
            Self::EndpointRequired => f.write_str(
                "S3 remote needs an endpoint (e.g. https://s3.ap-southeast-2.wasabisys.com); \
                 folder mode never defaults to AWS",
            ),
            Self::InvalidExclude { pattern } => write!(f, "invalid exclude pattern {pattern:?}"),
            Self::UnsupportedRemote { url } => write!(
                f,
                "folder mode supports s3://, local:// and rclone:// remotes, not {url}"
            ),
            Self::InvalidHistoryKey { key } => write!(f, "invalid history key {key:?}"),
            Self::DestinationRequired => {
                f.write_str("pulling from history needs a destination (`into`)")
            }
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

fn fmt_refusal(path: &Path, reason: &Refusal, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let p = path.display();
    match reason {
        Refusal::NoFileName => write!(f, "{p} has no usable file name"),
        Refusal::NotFileOrDirectory => {
            write!(f, "{p} is neither a regular file nor a directory")
        }
        Refusal::DvcFile => write!(
            f,
            "{}: cannot back up a .dvc file",
            path.file_name()
                .unwrap_or(path.as_os_str())
                .to_string_lossy()
        ),
        Refusal::ForeignPointer => write!(
            f,
            "{p} exists and is not a plain DVC 3 pointer; refusing to replace it"
        ),
        Refusal::PointerForOtherOutput { other } => write!(
            f,
            "{p} points at {other:?}, not {:?}; refusing to replace it",
            path.file_stem().unwrap_or_default().to_string_lossy()
        ),
        Refusal::NonPortableName { detail } => f.write_str(detail),
        Refusal::NotUtf8Name => write!(f, "{p}: name is not valid UTF-8"),
        Refusal::ControlFile => write!(
            f,
            "{p}: DVC control files and nested repositories/outputs \
             cannot be inside a backed-up directory"
        ),
        Refusal::SymlinkToDirectory => write!(
            f,
            "{p}: symlink to a directory (DVC would silently skip it)"
        ),
        Refusal::BrokenSymlink => write!(f, "{p}: broken symlink"),
        Refusal::SpecialFile => write!(f, "{p}: not a regular file"),
        Refusal::UnrestorablePointer => write!(
            f,
            "{p} is not a DVC 3 pointer to one md5-addressed output; nothing to restore"
        ),
        Refusal::PointerPathEscapes { output } => write!(
            f,
            "{p} names its output {output:?}, which is not a single file or directory \
             name on this OS; refusing to restore it"
        ),
        Refusal::SymlinkedOutput => write!(f, "{p} is a symlink; refusing to write through it"),
        Refusal::NotADirectory => write!(
            f,
            "{p} is in the way (not a directory); refusing to write through it"
        ),
        Refusal::NotRegularFile => write!(f, "{p} is not a regular file; refusing to replace it"),
        Refusal::UnwritableName => write!(f, "cannot restore {:?}", path.to_string_lossy()),
        Refusal::CaseCollision { other } => write!(
            f,
            "{other:?} and {:?} differ only by case or Unicode normalization and would be \
             one file on macOS and Windows",
            path.to_string_lossy()
        ),
        Refusal::AppearedWhilePulling => write!(f, "{p} appeared while pulling; left untouched"),
    }
}

impl std::error::Error for Error {}
