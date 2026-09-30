//! Round trips against a real, pinned DVC. Ignored by default; both CI jobs
//! install it (`uv tool install dvc==3.67.1`) and run them with
//! `BIGSTORE_TEST_DVC=<path to dvc>`. Locally, the same:
//!
//!   uv tool install dvc==3.67.1
//!   BIGSTORE_TEST_DVC=$(command -v dvc) cargo test --test dvc_compat -- --ignored
//!
//! DVC runs with its global/system/site config redirected into the test's
//! temp dir, so it never reads or writes the user's DVC cache or config.

use bigstore::dvc::DvcPointer;
use bigstore::folder::{
    self, Credentials, Excludes, HistoryKey, PointerSource, PullOptions, PushOptions, Remote,
    RemoteConfig, DEFAULT_EXCLUDES,
};
use std::path::{Path, PathBuf};
use std::process::Command;

const DVC_VERSION: &str = "3.67.1";

struct Dvc {
    bin: PathBuf,
    home: PathBuf,
}

impl Dvc {
    fn new(home: &Path) -> Self {
        let bin = PathBuf::from(
            std::env::var("BIGSTORE_TEST_DVC").expect("set BIGSTORE_TEST_DVC to a dvc binary"),
        );
        let dvc = Self {
            bin,
            home: home.to_path_buf(),
        };
        let v = dvc.run(home, &["--version"]);
        assert_eq!(v.trim(), DVC_VERSION, "pinned DVC version");
        dvc
    }

    fn run(&self, cwd: &Path, args: &[&str]) -> String {
        let out = Command::new(&self.bin)
            .args(args)
            .current_dir(cwd)
            .env("DVC_GLOBAL_CONFIG_DIR", self.home.join(".g"))
            .env("DVC_SYSTEM_CONFIG_DIR", self.home.join(".s"))
            .env("DVC_SITE_CACHE_DIR", self.home.join(".site"))
            .env("DVC_NO_ANALYTICS", "1")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "dvc {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

fn write(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<_> = walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| {
            let rel = e.path().strip_prefix(dir).unwrap().to_str().unwrap();
            (rel.replace('\\', "/"), std::fs::read(e.path()).unwrap())
        })
        .collect();
    v.sort();
    v
}

/// The sort-order and escape traps DVC's `.dir` format has, in ASCII names.
fn populate(out: &Path) {
    for (rel, content) in [
        (
            "site=s1/date=2026-09-01/src_01/labels.jsonl",
            &b"{\"t\":1}\n"[..],
        ),
        (
            "site=s1/date=2026-09-01/src_01/gt_geometry/tracks.parquet",
            b"PAR1",
        ),
        ("a-dash.txt", b"dash"),
        ("a.txt", b"adot"),
        ("a/b/c.txt", b"c"),
        ("a_lower.txt", b"a"),
        ("B_upper.txt", b"B"),
        ("empty.parquet", b""),
        ("crlf.txt", b"a\r\nb\r\n"),
        ("with space.json", b"{}"),
        ("~tilde.txt", b"t"),
    ] {
        write(&out.join(rel), content);
    }
}

fn local_remote(store: &Path) -> Remote {
    Remote::open(&RemoteConfig {
        url: format!("local://{}", store.display()),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap()
}

#[test]
#[ignore = "needs a pinned DVC (dvc-compat CI job)"]
fn dvc_pulls_and_verifies_what_bigstore_pushed() {
    let tmp = tempfile::tempdir().unwrap();
    let dvc = Dvc::new(tmp.path());
    let store = tmp.path().join("remote");
    let src = tmp.path().join("src/host=xenoglossicist");
    populate(&src);
    let report = folder::push(
        &local_remote(&store),
        &src,
        &PushOptions::new(HistoryKey::new("ds/host=xenoglossicist").unwrap()),
    )
    .unwrap();

    let consumer = tmp.path().join("consumer");
    std::fs::create_dir_all(&consumer).unwrap();
    dvc.run(&consumer, &["init", "--no-scm", "-q"]);
    dvc.run(
        &consumer,
        &["remote", "add", "-d", "r", store.to_str().unwrap()],
    );
    std::fs::copy(
        &report.pointer_path,
        consumer.join("host=xenoglossicist.dvc"),
    )
    .unwrap();
    dvc.run(&consumer, &["pull", "-q"]);
    assert_eq!(tree(&consumer.join("host=xenoglossicist")), tree(&src));
    assert!(
        dvc.run(&consumer, &["status"]).contains("up to date"),
        "dvc status must be clean: a byte-inexact manifest shows as modified"
    );
    assert!(dvc.run(&consumer, &["status", "-c"]).contains("in sync"));
}

#[test]
#[ignore = "needs a pinned DVC (dvc-compat CI job)"]
fn bigstore_pulls_what_dvc_pushed_and_repushes_identically() {
    let tmp = tempfile::tempdir().unwrap();
    let dvc = Dvc::new(tmp.path());
    let store = tmp.path().join("remote");
    let project = tmp.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    dvc.run(&project, &["init", "--no-scm", "-q"]);
    populate(&project.join("views"));
    write(&project.join("store.toml"), b"x = 1\n");
    dvc.run(&project, &["add", "-q", "views"]);
    dvc.run(&project, &["add", "-q", "store.toml"]);
    dvc.run(
        &project,
        &["remote", "add", "-d", "r", store.to_str().unwrap()],
    );
    dvc.run(&project, &["push", "-q"]);

    let remote = local_remote(&store);
    for (name, pointer) in [("views", "views.dvc"), ("store.toml", "store.toml.dvc")] {
        let restore = tmp.path().join("restore").join(name);
        folder::pull(
            &remote,
            &PointerSource::File(project.join(pointer)),
            &PullOptions {
                into: Some(restore.clone()),
                ..PullOptions::default()
            },
        )
        .unwrap();
        if name == "views" {
            assert_eq!(tree(&restore), tree(&project.join("views")));
        } else {
            assert_eq!(std::fs::read(&restore).unwrap(), b"x = 1\n");
        }
    }

    // Re-pushing the same content from bigstore: same pointer, nothing new.
    let before = tree(&store).len();
    let dvc_yaml = std::fs::read_to_string(project.join("views.dvc")).unwrap();
    let report = folder::push(
        &remote,
        &project.join("views"),
        &PushOptions::new(HistoryKey::new("ds/views").unwrap()),
    )
    .unwrap();
    assert_eq!(report.uploaded, 0);
    assert_eq!(
        std::fs::read_to_string(project.join("views.dvc")).unwrap(),
        dvc_yaml
    );
    // Only the history record is new.
    assert_eq!(tree(&store).len(), before + 1);
}

/// The README's recipe: with [`DEFAULT_EXCLUDES`] (and any custom patterns,
/// anchored ones prefixed with the output's path) in the project's
/// `.dvcignore`, `dvc add` records exactly the manifest bigstore pushes.
#[test]
#[ignore = "needs a pinned DVC (dvc-compat CI job)"]
fn dvc_add_with_the_documented_dvcignore_matches_push() {
    let tmp = tempfile::tempdir().unwrap();
    let dvc = Dvc::new(tmp.path());
    let project = tmp.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    dvc.run(&project, &["init", "--no-scm", "-q"]);
    let views = project.join("views");
    populate(&views);
    for junk in [
        ".DS_Store",
        "a/.DS_Store",
        "._a.txt",
        "a/b/._c.txt",
        "Thumbs.db",
        "site=s1/desktop.ini",
        "x.tmp",
        "a/y.tmp",
        "cache/big.bin",
        "scratch/s.bin",
        "a/scratch/s.bin",
    ] {
        write(&views.join(junk), junk.as_bytes());
    }
    // Not excluded: `/cache` is anchored to the output, `scratch/` matches
    // directories only.
    write(&views.join("a/cache/kept.bin"), b"kept");
    write(&views.join("b/scratch"), b"a file");
    // A symlink to a directory: `scratch/` matches it in DVC and in push,
    // and neither follows it.
    #[cfg(unix)]
    {
        write(&tmp.path().join("elsewhere/f.bin"), b"outside");
        std::os::unix::fs::symlink(tmp.path().join("elsewhere"), views.join("site=s1/scratch"))
            .unwrap();
    }

    let mut ignore = std::fs::read_to_string(project.join(".dvcignore")).unwrap();
    for line in DEFAULT_EXCLUDES
        .iter()
        .chain(&["*.tmp", "/views/cache", "scratch/"])
    {
        ignore.push_str(line);
        ignore.push('\n');
    }
    std::fs::write(project.join(".dvcignore"), ignore).unwrap();
    dvc.run(&project, &["add", "-q", "views"]);
    let by_dvc = DvcPointer::load(&project.join("views.dvc")).unwrap();

    let report = folder::push(
        &local_remote(&tmp.path().join("remote")),
        &views,
        &PushOptions {
            exclude: Excludes::new(["*.tmp", "/cache", "scratch/"]).unwrap(),
            ..PushOptions::new(HistoryKey::new("ds/views").unwrap())
        },
    )
    .unwrap();
    assert_eq!(report.pointer, by_dvc);
    assert_eq!(report.files, 13, "populate's 11 files and the 2 kept");
}
