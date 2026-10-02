//! `bigstore::folder` as a library consumer uses it: plain folders, a
//! `local://` remote, no git.

use bigstore::dvc::{BigstoreMeta, DvcOutput, DvcPointer, Manifest, ManifestEntry, RecordId};
use bigstore::folder::{
    self, layout, CancelToken, Credentials, Error as FolderError, Excludes, HistoryKey,
    HistoryRecord, Link, LogOptions, Overwrite, Phase, PointerSource, Progress, ProgressEvent,
    PullOptions, PushOptions, Pushed, Refusal, Remote, RemoteConfig, Resolve, Selector, SyncState,
};
use bigstore::hash::{hash_file, hash_reader};
use bigstore::types::{HashFunction, Hexdigest, ManifestPath};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The typed refusal in `err`'s chain.
#[track_caller]
fn refused(err: &anyhow::Error) -> (&Path, &Refusal) {
    match err.downcast_ref::<FolderError>() {
        Some(FolderError::Refused { path, reason }) => (path, reason),
        _ => panic!("not a typed refusal: {err:#}"),
    }
}

#[track_caller]
fn folder_error(err: &anyhow::Error) -> &FolderError {
    err.downcast_ref::<FolderError>()
        .unwrap_or_else(|| panic!("not a folder::Error: {err:#}"))
}

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dvc-3.67.1");

struct Env {
    _tmp: tempfile::TempDir,
    data: PathBuf,
    store: PathBuf,
    remote: Remote,
}

fn env() -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("dataset");
    let store = tmp.path().join("remote");
    std::fs::create_dir_all(&data).unwrap();
    let remote = Remote::open(&RemoteConfig {
        url: format!("local://{}", store.display()),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap();
    Env {
        _tmp: tmp,
        data,
        store,
        remote,
    }
}

fn write(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn opts(key: &str) -> PushOptions {
    PushOptions {
        jobs: 4,
        ..PushOptions::new(HistoryKey::new(key).unwrap())
    }
}

fn pull_opts(into: Option<PathBuf>) -> PullOptions {
    PullOptions {
        into,
        jobs: 4,
        ..PullOptions::default()
    }
}

/// `key`'s versions on `e`'s remote.
fn history_log(e: &Env, key: &str) -> anyhow::Result<Vec<HistoryRecord>> {
    folder::log(
        &e.remote,
        &HistoryKey::new(key).unwrap(),
        &LogOptions::default(),
    )
}

/// All regular files under `dir`, relative, with contents.
fn tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<_> = walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| {
            let rel = e.path().strip_prefix(dir).unwrap();
            let rel = rel.to_str().unwrap().replace('\\', "/");
            (rel, std::fs::read(e.path()).unwrap())
        })
        .collect();
    v.sort();
    v
}

/// Object keys on a local remote, relative to its root.
fn remote_keys(store: &Path) -> Vec<String> {
    tree(store).into_iter().map(|(k, _)| k).collect()
}

/// A writer directory in the writer-first layout.
fn writer_dir(e: &Env) -> PathBuf {
    let w = e
        .data
        .join("annotations/reviewer=rick/host=ricks-macbook-pro");
    write(
        &w.join("site=s1/date=2026-09-01/src_01/labels.jsonl"),
        b"{\"t\":1}\n{\"t\":2}\n",
    );
    write(&w.join("site=s1/date=2026-09-01/src_01/regions.jsonl"), b"");
    write(
        &w.join("site=s1/date=2026-09-01/src_01/gt_geometry/tracks.parquet"),
        b"PAR1 geometry",
    );
    write(
        &w.join("site=s1/date=2026-09-02/src_02/labels.jsonl"),
        b"{\"t\":3}\n",
    );
    w
}

const KEY: &str = "ST032_Warrawoona/BeatonsCreek/annotations/reviewer=rick/host=ricks-macbook-pro";

#[test]
fn dataset_fixture_pushes_byte_identical_to_dvc() {
    // The golden `dataset` tree minus the case this mode refuses (non-ASCII
    // name) and the unix-only symlink: its remote objects and manifest must
    // be exactly what `dvc push` writes for the same files.
    let e = env();
    let out = e.data.join("tt");
    for (rel, content) in [
        ("annotations/labels.jsonl", &b"{\"a\":1}\n{\"a\":2}\n"[..]),
        ("annotations/empty.parquet", b""),
        ("views/v1/mask.json", b"{\"m\":[1,2]}"),
        ("config.toml", b"x = 1\n"),
        ("B_upper.txt", b"B"),
        ("a_lower.txt", b"a"),
        ("a-dash.txt", b"dash"),
        ("a/b/c.txt", b"c"),
        ("a.txt", b"adot"),
    ] {
        write(&out.join(rel), content);
    }
    std::fs::create_dir_all(out.join("views/empty_dir")).unwrap();

    let report = folder::push(&e.remote, &out, &opts("x/tt")).unwrap();
    assert_eq!(report.empty_dirs, 1);
    assert_eq!(report.files, 9);

    // Every object key DVC wrote for these contents exists here, with the
    // same bytes.
    let golden_remote = Path::new(GOLDEN).join("dataset/remote_dvc_push");
    for (key, bytes) in tree(&golden_remote) {
        if key.ends_with(".dir") {
            continue; // the golden manifest also lists the two excluded files
        }
        if let Ok(ours) = std::fs::read(e.store.join(&key)) {
            assert_eq!(ours, bytes, "{key}");
        }
    }
    let DvcOutput::Dir {
        manifest,
        size,
        nfiles,
    } = &report.pointer.output
    else {
        panic!("expected a directory pointer");
    };
    assert_eq!((*size, *nfiles), (44, 9));
    let dir_key = format!(
        "files/md5/{}/{}.dir",
        &manifest.to_string()[..2],
        &manifest.to_string()[2..]
    );
    assert!(e.store.join(dir_key).is_file());
}

#[test]
fn round_trip_restores_the_tree_and_repush_is_a_no_op() {
    let e = env();
    let w = writer_dir(&e);
    let first = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(first.files, 4);
    assert_eq!(first.uploaded, 4);
    assert!(
        matches!(first.outcome, Pushed::Published { .. }),
        "{:?}",
        first.outcome
    );
    assert_eq!(
        first.pointer_path,
        w.parent().unwrap().join("host=ricks-macbook-pro.dvc")
    );
    let pointer_bytes = std::fs::read(&first.pointer_path).unwrap();

    let again = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(again.uploaded, 0);
    assert_eq!(
        again.outcome,
        Pushed::AlreadyLatest,
        "no-op push grew history"
    );
    assert_eq!(std::fs::read(&again.pointer_path).unwrap(), pointer_bytes);

    let restore = e.data.parent().unwrap().join("restore");
    let pulled = folder::pull(
        &e.remote,
        &PointerSource::File(first.pointer_path.clone()),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    assert_eq!(pulled.written, 4);
    assert_eq!(tree(&restore), tree(&w));
}

#[test]
fn changing_one_file_uploads_one_object_and_adds_one_version() {
    let e = env();
    let w = writer_dir(&e);
    let first = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let labels = w.join("site=s1/date=2026-09-02/src_02/labels.jsonl");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&labels)
        .unwrap();
    std::io::Write::write_all(&mut f, b"{\"t\":4}\n").unwrap();
    drop(f);

    let second = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(second.uploaded, 1);
    let key = HistoryKey::new(KEY).unwrap();
    let versions = folder::log(&e.remote, &key, &LogOptions::default()).unwrap();
    assert_eq!(versions.len(), 2);
    // Pushed within the same second: order is push order, not id order.
    assert_eq!(versions[0].pointer.output, first.pointer.output);
    assert_eq!(versions[1].pointer.output, second.pointer.output);
    let latest = e.data.parent().unwrap().join("latest");
    folder::pull(
        &e.remote,
        &PointerSource::History {
            key: key.clone(),
            at: Selector::Latest,
        },
        &pull_opts(Some(latest.clone())),
    )
    .unwrap();
    assert_eq!(tree(&latest), tree(&w));

    // The first version is still restorable, by id prefix.
    let old = versions[0].id.to_string();
    let restore = e.data.parent().unwrap().join("v1");
    folder::pull(
        &e.remote,
        &PointerSource::History {
            key,
            at: Selector::Id(old[..8].to_string()),
        },
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    assert_eq!(
        std::fs::read(restore.join("site=s1/date=2026-09-02/src_02/labels.jsonl")).unwrap(),
        b"{\"t\":3}\n"
    );
}

#[test]
fn single_file_outputs_back_up_config_files() {
    let e = env();
    let store_toml = e.data.join("store.toml");
    write(&store_toml, b"x = 1\n");
    let report = folder::push(&e.remote, &store_toml, &opts("ds/store.toml")).unwrap();
    // Byte-identical to `dvc add store.toml` (DVC 3.67.1), then the version
    // it now is, in `meta:`, where DVC writes it.
    assert_eq!(
        std::fs::read_to_string(e.data.join("store.toml.dvc")).unwrap(),
        format!(
            "outs:\n- md5: 3253b41059cac6e987c5a5e9233ea5d0\n  size: 6\n  hash: md5\n  \
             path: store.toml\nmeta:\n  bigstore:\n    base: {}\n",
            report.version
        )
    );
    assert!(e
        .store
        .join("files/md5/32/53b41059cac6e987c5a5e9233ea5d0")
        .is_file());

    std::fs::remove_file(&store_toml).unwrap();
    folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &pull_opts(None),
    )
    .unwrap();
    assert_eq!(std::fs::read(&store_toml).unwrap(), b"x = 1\n");
}

#[test]
fn pull_refuses_differing_files_keeps_extras_and_force_replaces() {
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let labels = w.join("site=s1/date=2026-09-02/src_02/labels.jsonl");
    std::fs::write(&labels, b"local edit\n").unwrap();
    std::fs::write(w.join("unpushed.jsonl"), b"work in progress\n").unwrap();

    let err = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path.clone()),
        &pull_opts(None),
    )
    .unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, std::slice::from_ref(&labels));
    assert_eq!(
        std::fs::read(&labels).unwrap(),
        b"local edit\n",
        "nothing written"
    );

    let forced = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &PullOptions {
            overwrite: Overwrite::Force,
            ..pull_opts(None)
        },
    )
    .unwrap();
    assert_eq!(forced.written, 1);
    assert_eq!(forced.extra_local, 1);
    assert_eq!(std::fs::read(&labels).unwrap(), b"{\"t\":3}\n");
    assert!(w.join("unpushed.jsonl").exists(), "pull must never prune");
}

#[test]
fn corrupted_remote_object_is_rejected_and_nothing_is_written() {
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    // "{"t":3}\n" — overwrite its object on the remote.
    let key = remote_keys(&e.store)
        .into_iter()
        .find(|k| {
            k.starts_with("files/")
                && !k.ends_with(".dir")
                && std::fs::read(e.store.join(k)).unwrap() == b"{\"t\":3}\n"
        })
        .unwrap();
    std::fs::write(e.store.join(&key), b"tampered").unwrap();

    let restore = e.data.parent().unwrap().join("r");
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("integrity check failed"),
        "{err:#}"
    );
    assert!(!restore
        .join("site=s1/date=2026-09-02/src_02/labels.jsonl")
        .exists());
}

#[test]
fn a_failed_upload_publishes_no_manifest_and_no_pointer() {
    // An rclone remote that does not exist: every upload fails.
    let e = env();
    let w = writer_dir(&e);
    let broken = Remote::open(&RemoteConfig {
        url: "rclone://bigstore-test-no-such-remote:bucket".into(),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap();
    assert!(folder::push(&broken, &w, &opts(KEY)).is_err());
    assert!(!w
        .parent()
        .unwrap()
        .join("host=ricks-macbook-pro.dvc")
        .exists());
}

#[test]
fn s3_without_an_endpoint_is_refused_before_any_request() {
    let err = Remote::open(&RemoteConfig {
        url: "s3://artifacts.supersensory.com.au/dvc".into(),
        endpoint: None,
        region: Some("ap-southeast-2".into()),
        credentials: Credentials::Static {
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
        },
    })
    .err()
    .expect("must refuse");
    assert!(
        matches!(folder_error(&err), FolderError::EndpointRequired),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("never defaults to AWS"),
        "{err:#}"
    );
}

#[test]
fn refuses_to_replace_a_foreign_dvc_file() {
    let e = env();
    let w = writer_dir(&e);
    let pointer = w.parent().unwrap().join("host=ricks-macbook-pro.dvc");
    std::fs::write(&pointer, "cmd: python train.py\nouts:\n- path: model\n").unwrap();
    let err = folder::push(&e.remote, &w, &opts(KEY)).unwrap_err();
    assert_eq!(refused(&err), (pointer.as_path(), &Refusal::ForeignPointer));
    assert!(std::fs::read_to_string(&pointer)
        .unwrap()
        .starts_with("cmd:"));
}

#[test]
fn unterminated_jsonl_warns_but_pushes() {
    let e = env();
    let w = writer_dir(&e);
    write(&w.join("partial.jsonl"), b"{\"t\":1}\n{\"t\":");
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
    assert!(report.warnings[0].contains("partial.jsonl"));
}

#[test]
fn calling_from_inside_a_tokio_runtime_is_an_error_that_names_the_async_fn() {
    let e = env();
    let w = writer_dir(&e);
    let key = HistoryKey::new(KEY).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let errors = rt.block_on(async {
        [
            ("push", folder::push(&e.remote, &w, &opts(KEY)).map(drop)),
            (
                "status",
                folder::status(&e.remote, &w, &opts(KEY)).map(drop),
            ),
            (
                "pull",
                folder::pull(&e.remote, &history(KEY, Selector::Latest), &pull_opts(None))
                    .map(drop),
            ),
            (
                "log",
                folder::log(&e.remote, &key, &LogOptions::default()).map(drop),
            ),
            ("keys", folder::keys(&e.remote, None).map(drop)),
        ]
    });
    for (name, result) in errors {
        let err = result.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("tokio runtime"), "{name}: {msg}");
        assert!(
            msg.contains(&format!("bigstore::folder::{name}_async")),
            "{name}: {msg}"
        );
    }
    assert!(remote_keys(&e.store).is_empty(), "something was published");
}

#[test]
fn pulls_what_dvc_pushed() {
    // The golden remote DVC 3.67.1 wrote with `dvc push`, including a
    // non-ASCII name push would refuse: bigstore restores every file and
    // each one hashes to the manifest's md5.
    let e = env();
    let golden = Path::new(GOLDEN).join("dataset");
    let remote = Remote::open(&RemoteConfig {
        url: format!("local://{}", golden.join("remote_dvc_push").display()),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap();
    let restore = e.data.join("tt");
    let report = folder::pull(
        &remote,
        &PointerSource::File(golden.join("tt.dvc")),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    let entries = bigstore::dvc::parse_dir_manifest(&golden.join("manifest.dir")).unwrap();
    assert_eq!(entries.len(), 11);
    assert_eq!(report.written + report.unchanged, 11);
    for entry in &entries {
        let path = restore.join(entry.relpath.as_str());
        assert_eq!(hash_file(&path, HashFunction::Md5).unwrap(), entry.md5);
    }
}

/// Lay out `files` on `e`'s remote the way `dvc push` from Linux would
/// (objects, then the `.dir` manifest), and write `out.dvc` for it in
/// `e.data`. Returns the pointer path.
fn dvc_pushed_dir(e: &Env, files: &[(&str, &[u8])]) -> PathBuf {
    let object = |md5: &Hexdigest| {
        let hex = md5.to_string();
        e.store.join("files/md5").join(&hex[..2]).join(&hex[2..])
    };
    let mut entries = Vec::new();
    let mut size = 0;
    for (name, content) in files {
        let md5 = hash_reader(&mut &content[..], HashFunction::Md5).unwrap();
        write(&object(&md5), content);
        size += content.len() as u64;
        entries.push(ManifestEntry {
            relpath: ManifestPath::new(name).unwrap(),
            md5,
        });
    }
    let manifest = Manifest::from_entries(entries).unwrap();
    let id = manifest.id();
    let dir = object(&id).with_extension("dir");
    write(&dir, &manifest.to_bytes());
    let pointer = DvcPointer {
        output: DvcOutput::Dir {
            manifest: id,
            size,
            nfiles: files.len() as u64,
        },
        path: "out".into(),
        meta: None,
    };
    let path = e.data.join("out.dvc");
    write(&path, pointer.to_yaml().as_bytes());
    path
}

/// `back\slash.txt` is valid DVC data and an ordinary name on Unix: pull
/// restores it, while push keeps refusing names Windows could not create.
#[cfg(unix)]
#[test]
fn pulls_a_name_push_refuses_when_this_os_can_create_it() {
    let e = env();
    let pointer = dvc_pushed_dir(&e, &[("back\\slash.txt", b"b"), ("sub/ok.txt", b"o")]);
    let report = folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap();
    assert_eq!(report.written, 2);
    let out = e.data.join("out");
    assert_eq!(std::fs::read(out.join("back\\slash.txt")).unwrap(), b"b");
    assert_eq!(std::fs::read(out.join("sub/ok.txt")).unwrap(), b"o");

    let err = folder::push(&e.remote, &out, &opts("ds/out")).unwrap_err();
    let (path, reason) = refused(&err);
    assert_eq!(path, Path::new("back\\slash.txt"));
    assert!(matches!(reason, Refusal::NonPortableName { .. }), "{err:#}");
    assert!(
        format!("{err:#}").contains(r#""back\\slash.txt""#),
        "{err:#}"
    );
}

/// On Windows `\` is a separator: a hostile manifest name must be refused,
/// by name, before anything is written.
#[cfg(windows)]
#[test]
fn windows_refuses_to_pull_a_name_it_would_misread() {
    let e = env();
    let pointer = dvc_pushed_dir(&e, &[("a\\..\\..\\escaped.txt", b"x"), ("ok.txt", b"o")]);
    let err = folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap_err();
    assert_eq!(
        refused(&err),
        (
            Path::new("a\\..\\..\\escaped.txt"),
            &Refusal::UnwritableName
        )
    );
    assert!(
        format!("{err:#}").contains(r#""a\\..\\..\\escaped.txt""#),
        "{err:#}"
    );
    assert!(!e.data.join("out").exists(), "nothing written");
    assert!(!e.data.parent().unwrap().join("escaped.txt").exists());
}

#[cfg(unix)]
#[test]
fn cli_works_without_git_on_path() {
    let e = env();
    let w = writer_dir(&e);
    // A PATH containing only a directory with no git in it.
    let empty_bin = e.data.parent().unwrap().join("empty-bin");
    std::fs::create_dir_all(&empty_bin).unwrap();
    let bin = env!("CARGO_BIN_EXE_git-bigstore");
    let remote = format!("local://{}", e.store.display());
    let run = |args: &[&str]| {
        let out = std::process::Command::new(bin)
            .args(args)
            .env_clear()
            .env("PATH", &empty_bin)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    };
    run(&[
        "folder",
        "push",
        w.to_str().unwrap(),
        "--history",
        KEY,
        "--remote",
        &remote,
    ]);
    let restore = e.data.parent().unwrap().join("cli-restore");
    run(&[
        "folder",
        "pull",
        "--history",
        KEY,
        "--into",
        restore.to_str().unwrap(),
        "--remote",
        &remote,
    ]);
    assert_eq!(tree(&restore), tree(&w));
    let log = run(&["folder", "log", KEY, "--remote", &remote]);
    assert_eq!(String::from_utf8_lossy(&log.stdout).lines().count(), 1);
}

/// Bare relative arguments, run from the parent directory: `dataset`'s and
/// `store.toml.dvc`'s parent is the empty path, which must mean the current
/// directory (on Windows it once failed after uploading everything).
#[test]
fn cli_accepts_bare_relative_paths_in_the_current_directory() {
    let e = env();
    writer_dir(&e);
    let parent = e.data.parent().unwrap();
    write(&parent.join("store.toml"), b"[store]\nurl = \"x\"\n");
    let remote = format!("local://{}", e.store.display());
    let run = |args: &[&str]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_git-bigstore"))
            .args(args)
            .args(["--remote", &remote])
            .current_dir(parent)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    };
    run(&["folder", "push", "dataset", "--history", "ds/dir"]);
    assert!(DvcPointer::load(&parent.join("dataset.dvc")).is_ok());
    let log = run(&["folder", "log", "ds/dir"]);
    assert_eq!(String::from_utf8_lossy(&log.stdout).lines().count(), 1);

    run(&["folder", "push", "store.toml", "--history", "ds/store"]);
    std::fs::remove_file(parent.join("store.toml")).unwrap();
    run(&["folder", "pull", "store.toml.dvc"]);
    let want = b"[store]\nurl = \"x\"\n";
    assert_eq!(std::fs::read(parent.join("store.toml")).unwrap(), want);

    run(&[
        "folder",
        "pull",
        "--history",
        "ds/store",
        "--into",
        "out.bin",
    ]);
    assert_eq!(std::fs::read(parent.join("out.bin")).unwrap(), want);
}

/// Open files must be bounded by `--jobs`, not by the number of files: a
/// launchd service gets 256 descriptors by default. The limit is lowered
/// for the child process only.
#[cfg(unix)]
#[test]
fn a_push_of_many_files_fits_a_low_open_file_limit() {
    let e = env();
    let w = e.data.join("many");
    for i in 0..400 {
        write(
            &w.join(format!("f{i:03}.jsonl")),
            format!("{{\"i\":{i}}}\n").as_bytes(),
        );
    }
    let out = std::process::Command::new("/bin/sh")
        .args(["-c", r#"ulimit -n 256 && exec "$0" "$@""#])
        .arg(env!("CARGO_BIN_EXE_git-bigstore"))
        .args(["folder", "push"])
        .arg(&w)
        .args(["--history", "ds/many", "--jobs", "8", "--remote"])
        .arg(format!("local://{}", e.store.display()))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let pointer = DvcPointer::load(&e.data.join("many.dvc")).unwrap();
    assert!(
        matches!(pointer.output, DvcOutput::Dir { nfiles: 400, .. }),
        "{pointer:?}"
    );
    // 400 objects, the manifest and the history record.
    assert_eq!(remote_keys(&e.store).len(), 402);
}

#[test]
fn an_equivalent_crlf_pointer_is_left_untouched() {
    // DVC on Windows writes `.dvc` files with CRLF; re-pushing the same
    // content must not rewrite (churn) them.
    let e = env();
    let w = writer_dir(&e);
    let first = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let crlf = std::fs::read_to_string(&first.pointer_path)
        .unwrap()
        .replace('\n', "\r\n");
    std::fs::write(&first.pointer_path, &crlf).unwrap();
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(std::fs::read_to_string(&first.pointer_path).unwrap(), crlf);
}

/// Put a 0.2 history record on `e`'s remote directly, as a 0.2 push would
/// have: a single-file pointer to `md5` at `time` (`<time>-<md5>.dvc`).
fn write_record(e: &Env, key: &str, time: &str, md5: &str) {
    let pointer = DvcPointer {
        output: DvcOutput::File {
            md5: Hexdigest::new(md5, HashFunction::Md5).unwrap(),
            size: 1,
        },
        path: "f".into(),
        meta: None,
    };
    write(
        &e.store
            .join(format!("bigstore-history/{key}/{time}-{md5}.dvc")),
        pointer.to_yaml().as_bytes(),
    );
}

/// A record id: the first half of the SHA-256 of `bytes`.
fn record_id(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    hex::encode(&sha2::Sha256::digest(bytes)[..16])
}

/// A 0.2 record's id: that of its file name. Later records name it as a
/// parent, so it can never change.
fn legacy_id(time: &str, md5: &str) -> String {
    record_id(format!("{time}-{md5}.dvc").as_bytes())
}

#[test]
fn an_ambiguous_version_id_is_refused_and_lists_the_candidates() {
    // Two 0.2 records whose ids share 8 characters but not 9: found by
    // trying times (a birthday search, some 2^16 names).
    let e = env();
    let md5 = "a".repeat(32);
    let time = |s: u32| {
        let t = chrono::DateTime::from_timestamp(1_788_000_000 + i64::from(s), 0).unwrap();
        t.format("%Y%m%dT%H%M%S.000000000Z").to_string()
    };
    let mut seen = std::collections::HashMap::new();
    let (t1, t2) = (0..)
        .find_map(|s| {
            let id = legacy_id(&time(s), &md5);
            match seen.insert(id[..8].to_string(), s) {
                Some(other) if legacy_id(&time(other), &md5)[8..9] != id[8..9] => Some((other, s)),
                _ => None,
            }
        })
        .unwrap();
    let (a, b) = (time(t1), time(t2));
    write_record(&e, "k", &a, &md5);
    write_record(&e, "k", &b, &md5);
    let (id_a, id_b) = (legacy_id(&a, &md5), legacy_id(&b, &md5));
    let pull = |id: &str| {
        folder::pull(
            &e.remote,
            &PointerSource::History {
                key: HistoryKey::new("k").unwrap(),
                at: Selector::Id(id.into()),
            },
            &pull_opts(Some(e.data.join("f"))),
        )
    };
    let err = pull(&id_a[..8].to_uppercase()).unwrap_err();
    let FolderError::AmbiguousId { prefix, candidates } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(*prefix, id_a[..8]);
    let ids: Vec<String> = candidates.iter().map(|r| r.id.to_string()).collect();
    assert_eq!(ids, [id_a.clone(), id_b.clone()]);
    let msg = format!("{err:#}");
    assert!(msg.contains("ambiguous"), "{msg}");
    assert!(msg.contains(&id_a) && msg.contains(&id_b), "{msg}");
    // A longer prefix picks one; the object is absent, so the fetch fails.
    let msg = format!("{:#}", pull(&id_b[..9]).unwrap_err());
    assert!(!msg.contains("ambiguous"), "{msg}");
    assert!(!e.data.join("f").exists());
}

fn history(key: &str, at: Selector) -> PointerSource {
    PointerSource::History {
        key: HistoryKey::new(key).unwrap(),
        at,
    }
}

#[test]
#[ignore = "needs the rclone binary; CI installs it and runs ignored tests"]
fn round_trips_through_an_rclone_remote() {
    // `:local:` is an on-the-fly rclone remote: no config file, no env.
    let e = env();
    let w = writer_dir(&e);
    let url = format!("rclone://:local:{}", e.store.display());
    let remote = Remote::open(&RemoteConfig {
        url,
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap();
    let report = folder::push(&remote, &w, &opts(KEY)).unwrap();
    assert_eq!(report.uploaded, 4);
    let DvcOutput::Dir { manifest, .. } = &report.pointer.output else {
        panic!("expected a directory pointer")
    };
    let manifest_key = format!("files/md5/{}/{}.dir", manifest.prefix(), manifest.rest());
    assert!(remote_keys(&e.store).contains(&manifest_key));

    let restore = e.data.parent().unwrap().join("restore");
    let pulled = folder::pull(
        &remote,
        &history(KEY, Selector::Latest),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    assert_eq!(pulled.written, 4);
    assert_eq!(tree(&restore), tree(&w));
}

#[test]
fn at_or_before_restores_the_version_in_force_at_that_time() {
    let e = env();
    let w = writer_dir(&e);
    let labels = w.join("site=s1/date=2026-09-02/src_02/labels.jsonl");
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    std::fs::write(&labels, b"{\"t\":4}\n").unwrap();
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let log = history_log(&e, KEY).unwrap();
    let [v1, v2] = &log[..] else {
        panic!("{log:?}")
    };
    let rfc =
        |t: chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let restore = |at: String, into: &str| {
        let into = e.data.parent().unwrap().join(into);
        folder::pull(
            &e.remote,
            &history(KEY, Selector::AtOrBefore(at)),
            &pull_opts(Some(into.clone())),
        )
        .map(|r| (r, into))
    };

    let err = restore(rfc(v1.time - chrono::Duration::seconds(1)), "early").unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::NoSuchVersion),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("no matching version"),
        "{err:#}"
    );

    // Just before the second push, the first version was current.
    let (r, into) = restore(rfc(v2.time - chrono::Duration::nanoseconds(1)), "mid").unwrap();
    assert_eq!(r.pointer.output, v1.pointer.output);
    let rel = labels.strip_prefix(&w).unwrap();
    assert_eq!(std::fs::read(into.join(rel)).unwrap(), b"{\"t\":3}\n");

    let (r, _) = restore(rfc(v2.time), "at").unwrap();
    assert_eq!(r.pointer.output, v2.pointer.output);

    let err = restore("yesterday".into(), "bad").unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::InvalidTime { time } if time == "yesterday"),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("RFC 3339"), "{err:#}");
}

#[test]
fn history_holds_only_its_own_outputs_records() {
    // Key `k/sub` is nested under key `k`; neither sees the other's
    // versions, and stray objects under the prefix are not versions.
    let e = env();
    let md5 = |c: char| c.to_string().repeat(32);
    write_record(&e, "k", "20260901T000000.000000000Z", &md5('a'));
    write_record(&e, "k/sub", "20260902T000000.000000000Z", &md5('b'));
    write(&e.store.join("bigstore-history/k/README.txt"), b"notes");
    write(&e.store.join("bigstore-history/k/not-a-time.dvc"), b"x");

    let ids = |key: &str| -> Vec<String> {
        history_log(&e, key)
            .unwrap()
            .iter()
            .map(|r| r.output_id().to_string())
            .collect()
    };
    assert_eq!(ids("k"), [md5('a')]);
    assert_eq!(ids("k/sub"), [md5('b')]);
}

#[cfg(unix)]
#[test]
fn pull_never_writes_through_a_symlinked_directory_or_over_a_non_file() {
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let pull = |into: &Path| {
        folder::pull(
            &e.remote,
            &PointerSource::File(report.pointer_path.clone()),
            &PullOptions {
                overwrite: Overwrite::Force,
                ..pull_opts(Some(into.to_path_buf()))
            },
        )
    };
    let root = e.data.parent().unwrap();

    // `site=s1` is a symlink to a directory outside the restore target.
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let into = root.join("r1");
    std::fs::create_dir(&into).unwrap();
    std::os::unix::fs::symlink(&outside, into.join("site=s1")).unwrap();
    let err = pull(&into).unwrap_err();
    assert_eq!(
        refused(&err),
        (into.join("site=s1").as_path(), &Refusal::NotADirectory)
    );
    assert!(
        format!("{err:#}").contains("refusing to write through"),
        "{err:#}"
    );
    assert!(tree(&outside).is_empty(), "nothing written outside");
    assert!(tree(&into).is_empty(), "nothing written at all");

    // A directory, or a symlink, where a file belongs is never replaced.
    let labels = "site=s1/date=2026-09-02/src_02/labels.jsonl";
    for (name, make) in [
        (
            "r2",
            (|p: &Path| std::fs::create_dir_all(p).unwrap()) as fn(&Path),
        ),
        ("r3", |p: &Path| {
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink("/dev/null", p).unwrap();
        }),
    ] {
        let into = root.join(name);
        make(&into.join(labels));
        let err = pull(&into).unwrap_err();
        assert_eq!(
            refused(&err),
            (into.join(labels).as_path(), &Refusal::NotRegularFile)
        );
        assert!(format!("{err:#}").contains("not a regular file"), "{err:#}");
        assert!(tree(&into).is_empty(), "{name}: nothing written");
    }
}

#[cfg(unix)]
#[test]
fn pull_refuses_a_directory_output_that_is_a_symlink() {
    // A hostile checkout: `out.dvc` beside a committed symlink `out ->
    // elsewhere`. Pulling must not follow it, from the pointer or `into`.
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let root = e.data.parent().unwrap();
    let outside = root.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::remove_dir_all(&w).unwrap();
    std::os::unix::fs::symlink(&outside, &w).unwrap();

    for into in [None, Some(w.clone())] {
        let err = folder::pull(
            &e.remote,
            &PointerSource::File(report.pointer_path.clone()),
            &pull_opts(into),
        )
        .unwrap_err();
        assert_eq!(refused(&err), (w.as_path(), &Refusal::SymlinkedOutput));
        let msg = format!("{err:#}");
        assert!(msg.contains("is a symlink"), "{msg}");
        assert!(msg.contains(&w.display().to_string()), "{msg}");
        assert!(tree(&outside).is_empty(), "nothing written outside");
    }
}

#[test]
fn names_differing_only_by_case_are_refused_before_writing() {
    let e = env();
    let pointer = dvc_pushed_dir(&e, &[("Labels.jsonl", b"A"), ("labels.jsonl", b"a")]);
    let err = folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap_err();
    assert_eq!(
        refused(&err),
        (
            Path::new("labels.jsonl"),
            &Refusal::CaseCollision {
                other: "Labels.jsonl".into()
            }
        )
    );
    let msg = format!("{err:#}");
    assert!(msg.contains("differ only by case"), "{msg}");
    assert!(
        msg.contains("Labels.jsonl") && msg.contains("\"labels.jsonl\""),
        "{msg}"
    );
    assert!(!e.data.join("out").exists());
}

#[test]
fn a_single_jsonl_file_without_a_final_newline_warns() {
    let e = env();
    let file = e.data.join("labels.jsonl");
    write(&file, b"{\"t\":1}\n{\"t\":");
    let report = folder::push(&e.remote, &file, &opts("ds/labels")).unwrap();
    assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
    assert!(report.warnings[0].contains("labels.jsonl"));

    write(&file, b"{\"t\":1}\n");
    let report = folder::push(&e.remote, &file, &opts("ds/labels")).unwrap();
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
}

#[test]
fn a_refused_pull_lists_every_differing_file_and_writes_nothing() {
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    // Pull reports the destination as given, then the file's relative path
    // with the native separator (`\` on Windows).
    let under = |rel: &str| {
        let mut p = w.clone();
        p.extend(rel.split('/'));
        p
    };
    let a = under("site=s1/date=2026-09-01/src_01/labels.jsonl");
    let b = under("site=s1/date=2026-09-02/src_02/labels.jsonl");
    std::fs::write(&a, b"edit a\n").unwrap();
    std::fs::write(&b, b"edit b\n").unwrap();
    std::fs::remove_file(w.join("site=s1/date=2026-09-01/src_01/regions.jsonl")).unwrap();

    let err = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &pull_opts(None),
    )
    .unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, &[a.clone(), b.clone()]);
    let shown = err.to_string();
    assert!(shown.starts_with("2 local file(s) differ"), "{shown}");
    for p in paths {
        assert!(shown.contains(&p.display().to_string()), "{shown}");
    }
    // Refusal is all or nothing: the missing file was not restored either.
    assert!(!w
        .join("site=s1/date=2026-09-01/src_01/regions.jsonl")
        .exists());
    assert_eq!(std::fs::read(&a).unwrap(), b"edit a\n");
}

#[test]
fn remotes_other_than_s3_local_and_rclone_are_refused() {
    let err = Remote::open(&RemoteConfig {
        url: "gs://bucket/dvc".into(),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .err()
    .expect("must refuse");
    assert!(
        matches!(folder_error(&err), FolderError::UnsupportedRemote { url } if url == "gs://bucket/dvc"),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("gs://bucket/dvc"), "{err:#}");
}

#[cfg(unix)]
#[test]
fn push_refuses_an_output_that_is_a_symlink() {
    let e = env();
    let w = writer_dir(&e);
    let link = e.data.join("link");
    std::os::unix::fs::symlink(&w, &link).unwrap();
    let err = folder::push(&e.remote, &link, &opts(KEY)).unwrap_err();
    assert_eq!(
        refused(&err),
        (link.as_path(), &Refusal::NotFileOrDirectory)
    );
    assert!(
        format!("{err:#}").contains("neither a regular file nor a directory"),
        "{err:#}"
    );
    assert!(!e.data.join("link.dvc").exists());
    assert!(remote_keys(&e.store).is_empty());
}

#[test]
fn push_refuses_to_replace_a_pointer_it_cannot_read_or_that_names_another_output() {
    let e = env();
    let file = e.data.join("store.toml");
    write(&file, b"x = 1\n");
    let pointer = e.data.join("store.toml.dvc");
    let foreign = "outs:\n- md5: 3253b41059cac6e987c5a5e9233ea5d0\n  size: 6\n  hash: md5\n  path: other.toml\n";
    write(&pointer, foreign.as_bytes());
    let err = folder::push(&e.remote, &file, &opts("ds/store.toml")).unwrap_err();
    assert_eq!(
        refused(&err),
        (
            pointer.as_path(),
            &Refusal::PointerForOtherOutput {
                other: "other.toml".into()
            }
        )
    );
    assert!(format!("{err:#}").contains("\"other.toml\""), "{err:#}");
    assert_eq!(std::fs::read_to_string(&pointer).unwrap(), foreign);

    // A directory where the pointer goes cannot be read as one.
    std::fs::remove_file(&pointer).unwrap();
    std::fs::create_dir(&pointer).unwrap();
    let err = folder::push(&e.remote, &file, &opts("ds/store.toml")).unwrap_err();
    assert!(format!("{err:#}").contains("failed to read"), "{err:#}");
    assert!(remote_keys(&e.store).is_empty(), "nothing uploaded");
}

/// A target pull cannot stat is an error, never taken for "missing".
#[cfg(unix)]
#[test]
fn pull_stops_at_a_directory_it_cannot_search() {
    use std::os::unix::fs::PermissionsExt;
    let e = env();
    let w = writer_dir(&e);
    let report = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let into = e.data.parent().unwrap().join("r");
    let locked = into.join("site=s1");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &pull_opts(Some(into.clone())),
    )
    .unwrap_err();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(format!("{err:#}").contains("ermission denied"), "{err:#}");
    assert!(tree(&into).is_empty(), "nothing written");
}

#[cfg(unix)]
#[test]
fn an_unreadable_file_fails_the_push_before_anything_is_published() {
    use std::os::unix::fs::PermissionsExt;
    let e = env();
    let w = writer_dir(&e);
    let secret = w.join("secret.jsonl");
    write(&secret, b"{}\n");
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = folder::push(&e.remote, &w, &opts(KEY)).unwrap_err();
    assert!(format!("{err:#}").contains("secret.jsonl"), "{err:#}");
    assert!(!w
        .parent()
        .unwrap()
        .join("host=ricks-macbook-pro.dvc")
        .exists());
    assert!(remote_keys(&e.store).is_empty());
}

#[test]
fn a_corrupt_history_record_fails_log_and_names_the_record() {
    let e = env();
    let dir = e.store.join("bigstore-history/k");
    let not_utf8 = "20260901T000000.000000000Z-a.dvc";
    write(&dir.join(not_utf8), b"\xff\xfe");
    let err = history_log(&e, "k").unwrap_err();
    assert!(format!("{err:#}").contains(not_utf8), "{err:#}");

    std::fs::remove_file(dir.join(not_utf8)).unwrap();
    let not_a_pointer = "20260901T000000.000000000Z-b.dvc";
    write(&dir.join(not_a_pointer), b"outs: []\n");
    let err = history_log(&e, "k").unwrap_err();
    assert!(format!("{err:#}").contains(not_a_pointer), "{err:#}");
}

#[test]
fn records_pushed_in_the_same_nanosecond_have_a_stable_latest() {
    let e = env();
    let time = "20260901T000000.000000000Z";
    let (a, b) = ("a".repeat(32), "b".repeat(32));
    write_record(&e, "k", time, &b);
    write_record(&e, "k", time, &a);
    let log = history_log(&e, "k").unwrap();
    let ids: Vec<String> = log.iter().map(|r| r.output_id().to_string()).collect();
    assert_eq!(ids, [a, b], "ties are ordered by record key");
}

#[test]
fn push_needs_an_existing_output_with_a_name() {
    let e = env();
    let err = folder::push(&e.remote, &e.data.join("missing"), &opts(KEY)).unwrap_err();
    assert!(format!("{err:#}").contains("failed to stat"), "{err:#}");
    let err = folder::push(&e.remote, &e.data.join(".."), &opts(KEY)).unwrap_err();
    assert_eq!(
        refused(&err),
        (e.data.join("..").as_path(), &Refusal::NoFileName)
    );
    assert!(
        format!("{err:#}").contains("no usable file name"),
        "{err:#}"
    );
    assert!(remote_keys(&e.store).is_empty());
}

#[cfg(unix)]
#[test]
fn a_read_only_remote_fails_the_upload_and_publishes_nothing() {
    use std::os::unix::fs::PermissionsExt;
    let e = env();
    let w = writer_dir(&e);
    std::fs::set_permissions(&e.store, std::fs::Permissions::from_mode(0o555)).unwrap();
    let err = folder::push(&e.remote, &w, &opts(KEY)).unwrap_err();
    std::fs::set_permissions(&e.store, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(format!("{err:#}").contains("upload of"), "{err:#}");
    assert!(!w
        .parent()
        .unwrap()
        .join("host=ricks-macbook-pro.dvc")
        .exists());
    assert!(remote_keys(&e.store).is_empty());
}

#[test]
fn pull_of_a_version_whose_manifest_is_missing_names_it() {
    let e = env();
    let pointer = dvc_pushed_dir(&e, &[("a.txt", b"a")]);
    let manifest = remote_keys(&e.store)
        .into_iter()
        .find(|k| k.ends_with(".dir"))
        .unwrap();
    std::fs::remove_file(e.store.join(&manifest)).unwrap();
    let err = folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap_err();
    assert!(
        format!("{err:#}").contains("is not on the remote"),
        "{err:#}"
    );
    assert!(!e.data.join("out").exists());
}

/// A `.dvc` names its output relative to its own directory, and push only
/// ever writes one component. A pointer naming anything else is refused
/// before a byte is written, wherever it points.
#[test]
fn pull_refuses_a_pointer_whose_path_leaves_its_directory() {
    let e = env();
    let file = e.data.join("store.toml");
    write(&file, b"x = 1\n");
    let report = folder::push(&e.remote, &file, &opts("ds/store.toml")).unwrap();
    let DvcOutput::File { md5, .. } = &report.pointer.output else {
        panic!("expected a file pointer")
    };
    let outside = e.data.parent().unwrap().join("escaped");
    let absolute = outside.to_str().unwrap().to_string();
    for bad in ["../escaped", absolute.as_str(), "sub/escaped", "", "."] {
        let dvc = e.data.join("evil.dvc");
        let yaml = format!("outs:\n- md5: {md5}\n  size: 6\n  hash: md5\n  path: '{bad}'\n");
        write(&dvc, yaml.as_bytes());
        let err = folder::pull(
            &e.remote,
            &PointerSource::File(dvc.clone()),
            &pull_opts(None),
        )
        .expect_err(bad);
        assert_eq!(
            refused(&err),
            (
                dvc.as_path(),
                &Refusal::PointerPathEscapes { output: bad.into() }
            )
        );
        let msg = format!("{err:#}");
        assert!(msg.contains(&format!("{bad:?}")), "{bad:?}: {msg}");
        assert!(msg.contains("evil.dvc"), "{bad:?}: {msg}");
        assert!(!outside.exists(), "{bad:?}: wrote outside");
        assert!(!e.data.join("sub").exists(), "{bad:?}: wrote below");
    }
}

/// Every refusal inside a directory output is typed, names the entry
/// relative to the output, keeps its message, and publishes nothing.
#[cfg(unix)]
#[test]
fn every_refusal_inside_a_pushed_directory_is_typed() {
    use std::os::unix::fs::symlink;
    let control =
        ": DVC control files and nested repositories/outputs cannot be inside a backed-up directory";
    let cafe = "path \"sub/café.json\": \"café.json\": only printable ASCII names are portable";
    /// Make the offending entry; the path, reason and message expected.
    type Case = (fn(&Path), &'static str, Refusal, String);
    let cases: [Case; 6] = [
        (
            |o| write(&o.join(".git/HEAD"), b"x"),
            ".git",
            Refusal::ControlFile,
            format!(".git{control}"),
        ),
        (
            |o| write(&o.join("sub/x.dvc"), b"x"),
            "sub/x.dvc",
            Refusal::ControlFile,
            format!("sub/x.dvc{control}"),
        ),
        (
            |o| {
                write(&o.join("d/f"), b"f");
                symlink("d", o.join("l")).unwrap();
            },
            "l",
            Refusal::SymlinkToDirectory,
            "l: symlink to a directory (DVC would silently skip it)".into(),
        ),
        (
            |o| symlink("nowhere", o.join("b")).unwrap(),
            "b",
            Refusal::BrokenSymlink,
            "b: broken symlink".into(),
        ),
        (
            |o| {
                let fifo = std::process::Command::new("mkfifo")
                    .arg(o.join("p"))
                    .status()
                    .unwrap();
                assert!(fifo.success());
            },
            "p",
            Refusal::SpecialFile,
            "p: not a regular file".into(),
        ),
        (
            |o| write(&o.join("sub/café.json"), b"x"),
            "sub/café.json",
            Refusal::NonPortableName {
                detail: cafe.into(),
            },
            cafe.into(),
        ),
    ];
    for (make, path, reason, message) in cases {
        let e = env();
        let out = e.data.join("out");
        write(&out.join("ok.txt"), b"ok");
        make(&out);
        let err = folder::push(&e.remote, &out, &opts("ds/out")).unwrap_err();
        assert_eq!(refused(&err), (Path::new(path), &reason), "{err:#}");
        assert_eq!(err.to_string(), message);
        assert!(!e.data.join("out.dvc").exists(), "{path}: pointer written");
        assert!(remote_keys(&e.store).is_empty(), "{path}: uploaded");
    }
}

#[test]
fn push_refuses_an_output_that_is_a_pointer_or_has_a_non_portable_name() {
    let e = env();
    let dvc = e.data.join("old.dvc");
    write(&dvc, b"outs: []\n");
    let err = folder::push(&e.remote, &dvc, &opts("ds/old")).unwrap_err();
    assert_eq!(refused(&err), (dvc.as_path(), &Refusal::DvcFile));
    assert_eq!(err.to_string(), "old.dvc: cannot back up a .dvc file");

    let cafe = e.data.join("café");
    write(&cafe.join("a.txt"), b"a");
    let err = folder::push(&e.remote, &cafe, &opts("ds/cafe")).unwrap_err();
    let detail = "\"café\": only printable ASCII names are portable";
    assert_eq!(
        refused(&err),
        (
            cafe.as_path(),
            &Refusal::NonPortableName {
                detail: detail.into()
            }
        )
    );
    assert_eq!(err.to_string(), detail);
    assert!(remote_keys(&e.store).is_empty());
}

#[test]
fn selecting_a_version_that_is_absent_or_malformed_is_typed() {
    let e = env();
    let pull = |at: Selector| {
        folder::pull(
            &e.remote,
            &history("k", at),
            &pull_opts(Some(e.data.join("r"))),
        )
        .unwrap_err()
    };
    let err = pull(Selector::Latest);
    assert!(
        matches!(folder_error(&err), FolderError::NoSuchVersion),
        "{err:#}"
    );
    assert_eq!(err.to_string(), "no matching version in history");
    for bad in ["deadbee", "zzzzzzzz"] {
        let err = pull(Selector::Id(bad.into()));
        assert!(
            matches!(folder_error(&err), FolderError::InvalidVersionId { prefix } if prefix == bad),
            "{err:#}"
        );
        assert_eq!(
            err.to_string(),
            "version id prefix must be at least 8 hex characters"
        );
    }
}

/// A file appended to faster than it can be read never yields a snapshot:
/// push gives up with a typed error and publishes nothing.
#[test]
fn an_output_that_never_stops_changing_is_typed() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let e = env();
    let file = e.data.join("growing.bin");
    write(&file, &vec![0u8; 2 << 20]);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (file, stop) = (file.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&file)
                .unwrap();
            while !stop.load(Ordering::Relaxed) {
                std::io::Write::write_all(&mut f, b"x").unwrap();
            }
        })
    };
    let result = folder::push(&e.remote, &file, &opts("ds/growing"));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let err = result.unwrap_err();
    let FolderError::OutputChanged { detail } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(
        *detail,
        format!("{} kept changing while being read", file.display())
    );
    assert_eq!(
        err.to_string(),
        format!("{detail}; the output kept changing, push again later")
    );
    assert!(!e.data.join("growing.bin.dvc").exists());
    assert!(remote_keys(&e.store).is_empty());
}

/// Finder writes `.DS_Store` just by showing a folder, and Explorer and
/// non-Mac volumes add their own files. None of it is data: a push after
/// they appear is a no-op.
#[test]
fn os_junk_appearing_is_not_a_new_version() {
    let e = env();
    let w = writer_dir(&e);
    let first = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let pointer = std::fs::read(&first.pointer_path).unwrap();
    for junk in [
        ".DS_Store",
        "site=s1/.DS_Store",
        "site=s1/date=2026-09-02/src_02/._labels.jsonl",
        "Thumbs.db",
        "site=s1/desktop.ini",
    ] {
        write(&w.join(junk), b"junk");
    }
    let again = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(again.pointer.output, first.pointer.output);
    assert_eq!((again.files, again.uploaded), (first.files, 0));
    assert_eq!(
        again.outcome,
        Pushed::AlreadyLatest,
        "junk made a new version"
    );
    assert_eq!(std::fs::read(&again.pointer_path).unwrap(), pointer);
    let versions = history_log(&e, KEY).unwrap();
    assert_eq!(versions.len(), 1);
}

#[test]
fn custom_excludes_follow_gitignore_rules_relative_to_the_output() {
    let e = env();
    let out = e.data.join("out");
    for rel in [
        "keep.txt",
        "x.tmp",
        "a/y.tmp",
        "cache/big.bin",
        "a/cache/kept.bin",
        "scratch/s.bin",
        "a/scratch/s.bin",
        "a/b/only.log",
        "b/scratch",
    ] {
        write(&out.join(rel), rel.as_bytes());
    }
    let report = folder::push(
        &e.remote,
        &out,
        &PushOptions {
            exclude: Excludes::new(["*.tmp", "/cache", "scratch/", "a/b/*.log"]).unwrap(),
            ..opts("ds/out")
        },
    )
    .unwrap();
    let restore = e.data.parent().unwrap().join("restore");
    folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    let kept: Vec<String> = tree(&restore).into_iter().map(|(k, _)| k).collect();
    // `/cache` is anchored to the output; `scratch/` matches directories
    // only, at any depth.
    assert_eq!(kept, ["a/cache/kept.bin", "b/scratch", "keep.txt"]);
    // `a/b` held only an excluded file, so DVC would see it empty too.
    assert_eq!(report.empty_dirs, 1);
}

#[test]
fn an_invalid_exclude_pattern_is_typed() {
    for bad in ["!keep.txt", "a[", "/", ""] {
        let err = Excludes::new([bad]).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::InvalidExclude { pattern } if pattern == bad),
            "{bad:?}: {err:#}"
        );
    }
}

#[test]
fn a_record_whose_name_and_pointer_disagree_is_refused() {
    // Versions are chosen by record name; one whose pointer holds another
    // version must fail rather than restore the wrong one.
    let e = env();
    let (named, held) = ("a".repeat(32), "b".repeat(32));
    write_record(&e, "k", "20260901T000000.000000000Z", &held);
    let dir = e.store.join("bigstore-history/k");
    std::fs::rename(
        dir.join(format!("20260901T000000.000000000Z-{held}.dvc")),
        dir.join(format!("20260901T000000.000000000Z-{named}.dvc")),
    )
    .unwrap();
    let id = legacy_id("20260901T000000.000000000Z", &named);
    for at in [Selector::Latest, Selector::Id(id[..8].into())] {
        let err = folder::pull(
            &e.remote,
            &history("k", at),
            &pull_opts(Some(e.data.join("f"))),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("bad record") && msg.contains(&held), "{msg}");
    }
    let err = history_log(&e, "k").unwrap_err();
    assert!(format!("{err:#}").contains(&named), "{err:#}");
}

#[test]
fn keys_lists_every_history_key_under_a_prefix() {
    let e = env();
    let md5 = |c: char| c.to_string().repeat(32);
    let time = "20260901T000000.000000000Z";
    for (key, c) in [
        ("s/annotations/reviewer=rick/host=a", 'a'),
        ("s/annotations/reviewer=rick/host=a/sub", 'b'),
        ("s/annotations/reviewer=ann/host=b", 'c'),
        ("s/annotations/reviewer=rickard/host=c", 'd'),
        ("t/store.toml", 'e'),
    ] {
        write_record(&e, key, time, &md5(c));
    }
    // Not records: no key is made of them.
    write(&e.store.join("bigstore-history/s/README.txt"), b"notes");
    write(
        &e.store.join("bigstore-history/s/stray/not-a-time.dvc"),
        b"x",
    );
    write(
        &e.store
            .join(format!("bigstore-history/{time}-{}.dvc", md5('f'))),
        b"x",
    );

    let keys = |prefix: Option<&str>| -> Vec<String> {
        let prefix = prefix.map(|p| HistoryKey::new(p).unwrap());
        folder::keys(&e.remote, prefix.as_ref())
            .unwrap()
            .iter()
            .map(|k| k.as_str().to_string())
            .collect()
    };
    assert_eq!(
        keys(None),
        [
            "s/annotations/reviewer=ann/host=b",
            "s/annotations/reviewer=rick/host=a",
            "s/annotations/reviewer=rick/host=a/sub",
            "s/annotations/reviewer=rickard/host=c",
            "t/store.toml",
        ]
    );
    // A prefix matches whole path components.
    assert_eq!(
        keys(Some("s/annotations/reviewer=rick")),
        [
            "s/annotations/reviewer=rick/host=a",
            "s/annotations/reviewer=rick/host=a/sub",
        ]
    );
    assert_eq!(
        keys(Some("s/annotations/reviewer=rick/host=a")),
        [
            "s/annotations/reviewer=rick/host=a",
            "s/annotations/reviewer=rick/host=a/sub",
        ]
    );
    assert!(keys(Some("nothing/here")).is_empty());
}

#[test]
fn status_says_what_push_would_do_and_writes_nothing() {
    let e = env();
    let w = writer_dir(&e);
    let parent = w.parent().unwrap().to_path_buf();
    let listing = |dir: &Path| -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|d| d.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    };
    let before = (listing(&parent), tree(&w));

    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    assert!(matches!(s.sync, SyncState::NoHistory), "{:?}", s.sync);
    assert_eq!((s.files, s.to_upload, s.already_present), (4, 4, 0));
    let total: u64 = tree(&w).iter().map(|(_, c)| c.len() as u64).sum();
    assert_eq!((s.to_upload_bytes, s.already_present_bytes), (total, 0));
    assert_eq!((listing(&parent), tree(&w)), before, "status wrote locally");
    assert!(
        remote_keys(&e.store).is_empty(),
        "status wrote to the remote"
    );

    let pushed = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(
        s.pointer,
        DvcPointer {
            meta: None,
            ..pushed.pointer.clone()
        },
        "status predicts the pointer, less its base"
    );
    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    assert!(matches!(s.sync, SyncState::InSync), "{:?}", s.sync);
    assert_eq!((s.to_upload, s.already_present), (0, 4));
    assert_eq!(s.already_present_bytes, total);
    // A no-op push counts what is already there, as status does.
    let again = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!((again.uploaded, again.already_present), (0, 4));

    let labels = w.join("site=s1/date=2026-09-02/src_02/labels.jsonl");
    std::fs::write(&labels, b"{\"t\":3}\n{\"t\":4}\n").unwrap();
    let keys_before = remote_keys(&e.store);
    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    assert!(matches!(s.sync, SyncState::LocalAhead), "{:?}", s.sync);
    assert_eq!((s.to_upload, s.to_upload_bytes), (1, 16));
    assert_eq!(remote_keys(&e.store), keys_before);

    // Another host pushes a newer version; this copy is still what its
    // .dvc records.
    std::fs::write(&labels, b"{\"t\":3}\n").unwrap();
    let other = e.data.parent().unwrap().join("other/host");
    folder::pull(
        &e.remote,
        &history(KEY, Selector::Latest),
        &pull_opts(Some(other.clone())),
    )
    .unwrap();
    std::fs::write(other.join("new.jsonl"), b"{}\n").unwrap();
    let newer = folder::push(&e.remote, &other, &opts(KEY)).unwrap();
    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    let SyncState::RemoteAhead { latest } = &s.sync else {
        panic!("{:?}", s.sync)
    };
    assert_eq!(latest.pointer.output, newer.pointer.output);

    // Both changed since the .dvc.
    std::fs::write(&labels, b"{\"t\":5}\n").unwrap();
    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    let SyncState::Stale { base, head } = &s.sync else {
        panic!("{:?}", s.sync)
    };
    assert_eq!(
        (base.as_ref(), &head.id),
        (Some(&pushed.version), &newer.version)
    );
}

#[track_caller]
fn assert_cancelled(err: &anyhow::Error) {
    assert!(
        matches!(folder_error(err), FolderError::Cancelled),
        "{err:#}"
    );
}

#[test]
fn a_cancelled_push_publishes_nothing() {
    let e = env();
    let w = writer_dir(&e);
    let pointer_path =
        w.with_file_name(format!("{}.dvc", w.file_name().unwrap().to_str().unwrap()));
    let cancelled = || {
        let o = opts(KEY);
        o.cancel.cancel();
        o
    };

    let err = folder::push(&e.remote, &w, &cancelled()).unwrap_err();
    assert_cancelled(&err);
    assert!(!pointer_path.exists(), "a .dvc was written");
    assert!(remote_keys(&e.store).is_empty(), "something was published");

    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let pointer = std::fs::read(&pointer_path).unwrap();
    let keys = remote_keys(&e.store);
    write(&w.join("new.jsonl"), b"{}\n");
    let err = folder::push(&e.remote, &w, &cancelled()).unwrap_err();
    assert_cancelled(&err);
    assert_eq!(std::fs::read(&pointer_path).unwrap(), pointer);
    assert_eq!(remote_keys(&e.store), keys, "history or objects changed");
    let err = folder::status(&e.remote, &w, &cancelled()).unwrap_err();
    assert_cancelled(&err);
}

#[test]
fn a_cancelled_pull_writes_nothing() {
    let e = env();
    let w = writer_dir(&e);
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let into = e.data.parent().unwrap().join("restore");
    let o = PullOptions {
        into: Some(into.clone()),
        ..PullOptions::default()
    };
    o.cancel.cancel();
    let err = folder::pull(&e.remote, &history(KEY, Selector::Latest), &o).unwrap_err();
    assert_cancelled(&err);
    assert!(tree(&into).is_empty(), "{:?}", tree(&into));
}

/// A progress callback recording every event, and the events so far.
fn recorder() -> (Progress, Arc<Mutex<Vec<ProgressEvent>>>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    (Progress::new(move |e| sink.lock().unwrap().push(e)), events)
}

/// Files and bytes advanced in `phase`, and the totals it started with.
fn phase_sums(events: &[ProgressEvent], phase: Phase) -> ((u64, Option<u64>), (u64, u64)) {
    let mut started = None;
    let mut done = (0, 0);
    for e in events {
        match *e {
            ProgressEvent::Started {
                phase: p,
                files,
                bytes,
            } if p == phase => {
                started = Some((files, bytes));
                done = (0, 0);
            }
            ProgressEvent::Advanced {
                phase: p,
                files,
                bytes,
            } if p == phase => {
                done = (done.0 + files, done.1 + bytes);
            }
            _ => {}
        }
    }
    (
        started.unwrap_or_else(|| panic!("{phase:?} never started: {events:?}")),
        done,
    )
}

#[test]
fn push_and_pull_report_progress_per_file_and_byte() {
    let e = env();
    let w = writer_dir(&e);
    let total: u64 = tree(&w).iter().map(|(_, c)| c.len() as u64).sum();
    let (progress, events) = recorder();
    let _ = folder::push(
        &e.remote,
        &w,
        &PushOptions {
            progress,
            ..opts(KEY)
        },
    )
    .unwrap();
    let events = std::mem::take(&mut *events.lock().unwrap());
    assert_eq!(
        phase_sums(&events, Phase::Hashing),
        ((4, Some(total)), (4, total))
    );
    assert_eq!(
        phase_sums(&events, Phase::Uploading),
        ((4, Some(total)), (4, total))
    );

    let into = e.data.parent().unwrap().join("restore");
    let (progress, events) = recorder();
    folder::pull(
        &e.remote,
        &history(KEY, Selector::Latest),
        &PullOptions {
            progress,
            ..pull_opts(Some(into))
        },
    )
    .unwrap();
    let events = std::mem::take(&mut *events.lock().unwrap());
    assert_eq!(phase_sums(&events, Phase::Hashing).1 .0, 4, "{events:?}");
    assert_eq!(
        phase_sums(&events, Phase::Downloading),
        ((4, None), (4, total))
    );
}

/// `n` files of distinct content under `dir`.
fn many_files(dir: &Path, n: usize) -> u64 {
    for i in 0..n {
        write(
            &dir.join(format!("f{i:03}.txt")),
            format!("file {i}\n").as_bytes(),
        );
    }
    tree(dir).iter().map(|(_, c)| c.len() as u64).sum()
}

#[test]
fn a_push_cancelled_mid_upload_leaves_history_and_pointer_alone() {
    let e = env();
    let out = e.data.join("out");
    many_files(&out, 20);
    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    let o = PushOptions {
        jobs: 1,
        cancel,
        progress: Progress::new(move |e| {
            if let ProgressEvent::Advanced {
                phase: Phase::Uploading,
                ..
            } = e
            {
                trigger.cancel();
            }
        }),
        ..opts("ds/out")
    };
    let err = folder::push(&e.remote, &out, &o).unwrap_err();
    assert_cancelled(&err);
    let keys = remote_keys(&e.store);
    let objects = keys.iter().filter(|k| k.starts_with("files/")).count();
    assert!((1..20).contains(&objects), "{keys:?}");
    assert!(
        !keys.iter().any(|k| k.ends_with(".dir")),
        "manifest uploaded"
    );
    assert!(!keys.iter().any(|k| k.starts_with("bigstore-history/")));
    assert!(!e.data.join("out.dvc").exists());

    // The next push completes, skipping what is already there.
    let r = folder::push(&e.remote, &out, &opts("ds/out")).unwrap();
    assert_eq!((r.uploaded, r.already_present), (20 - objects, objects));
}

#[test]
fn a_pull_cancelled_mid_download_leaves_only_whole_files() {
    let e = env();
    let out = e.data.join("out");
    many_files(&out, 20);
    let _ = folder::push(&e.remote, &out, &opts("ds/out")).unwrap();
    let into = e.data.parent().unwrap().join("restore");
    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    let o = PullOptions {
        jobs: 1,
        cancel,
        progress: Progress::new(move |e| {
            if let ProgressEvent::Advanced {
                phase: Phase::Downloading,
                ..
            } = e
            {
                trigger.cancel();
            }
        }),
        ..pull_opts(Some(into.clone()))
    };
    let err = folder::pull(&e.remote, &history("ds/out", Selector::Latest), &o).unwrap_err();
    assert_cancelled(&err);
    let restored = tree(&into);
    assert!((1..20).contains(&restored.len()), "{restored:?}");
    let original: std::collections::BTreeMap<_, _> = tree(&out).into_iter().collect();
    for (name, content) in &restored {
        assert_eq!(
            original.get(name),
            Some(content),
            "{name} is not a whole file"
        );
    }
}

#[test]
fn names_differing_only_by_normalization_or_unicode_case_are_refused() {
    // APFS and HFS+ treat each pair as one name (NFC vs NFD `é`; `Ä` vs
    // `ä`), so restoring both would leave one file holding either content.
    let nfd = "cafe\u{301}.txt";
    let nfc = "caf\u{e9}.txt";
    for (first, second) in [(nfd, nfc), ("\u{c4}rger.txt", "\u{e4}rger.txt")] {
        let e = env();
        let pointer = dvc_pushed_dir(&e, &[(first, b"1"), (second, b"2")]);
        let err =
            folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap_err();
        let (path, reason) = refused(&err);
        let names = [first, second];
        let Refusal::CaseCollision { other } = reason else {
            panic!("{err:#}")
        };
        assert!(
            names.contains(&path.to_str().unwrap()) && names.contains(&other.as_str()),
            "{err:#}"
        );
        assert_ne!(path.to_str().unwrap(), other, "{err:#}");
        assert!(!e.data.join("out").exists());
    }

    // Different letters are different names.
    let e = env();
    let pointer = dvc_pushed_dir(&e, &[(nfc, b"1"), ("cafe.txt", b"2")]);
    folder::pull(&e.remote, &PointerSource::File(pointer), &pull_opts(None)).unwrap();
}

#[test]
fn pull_restores_from_a_dvc_file_with_stage_fields_and_types_unreadable_ones() {
    // `dvc import-url` writes deps, frozen and a stage md5; `dvc add --desc`
    // writes annotations. Pull only reads the output, so both restore.
    let e = env();
    let plain = dvc_pushed_dir(&e, &[("a.txt", b"a"), ("b/c.txt", b"c")]);
    let DvcOutput::Dir { manifest, .. } = DvcPointer::load(&plain).unwrap().output else {
        panic!("a directory pointer")
    };
    std::fs::remove_file(&plain).unwrap();
    for (fixture, dir) in [
        ("imported_dir.dvc", "5b94ef7ba4840901cc23311660411a1d"),
        ("annotated.dvc", "c1aa8378201c5b38b6b109d77fbf79bc"),
    ] {
        let text = std::fs::read_to_string(format!("{GOLDEN}/stage_fields/{fixture}")).unwrap();
        let name = fixture.trim_end_matches(".dvc");
        let text = text.replace(dir, &manifest.to_string());
        let dvc = e.data.join(fixture);
        write(&dvc, text.as_bytes());
        let r = folder::pull(
            &e.remote,
            &PointerSource::File(dvc.clone()),
            &pull_opts(None),
        )
        .unwrap_or_else(|err| panic!("{fixture}: {err:#}"));
        assert_eq!(r.written, 2, "{fixture}");
        assert_eq!(
            std::fs::read(e.data.join(name).join("b/c.txt")).unwrap(),
            b"c"
        );
        assert_eq!(
            std::fs::read_to_string(&dvc).unwrap(),
            text,
            "pull rewrote it"
        );
    }

    // What names no md5-addressed output is refused, typed.
    for fixture in ["uncached.bin.dvc", "etag_only.bin.dvc"] {
        let dvc = e.data.join(fixture);
        std::fs::copy(format!("{GOLDEN}/stage_fields/{fixture}"), &dvc).unwrap();
        let err = folder::pull(
            &e.remote,
            &PointerSource::File(dvc.clone()),
            &pull_opts(None),
        )
        .unwrap_err();
        assert_eq!(
            refused(&err),
            (dvc.as_path(), &Refusal::UnrestorablePointer)
        );
    }
}

#[test]
fn an_invalid_history_key_and_a_history_pull_without_a_destination_are_typed() {
    for key in ["", "/abs", "a/../b", "a/nul", "a:b"] {
        let err = HistoryKey::new(key).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::InvalidHistoryKey { key: k } if k == key),
            "{key:?}: {err:#}"
        );
    }

    let e = env();
    let w = writer_dir(&e);
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let err =
        folder::pull(&e.remote, &history(KEY, Selector::Latest), &pull_opts(None)).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::DestinationRequired),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("into"), "{err:#}");
}

// ── Edges: long paths ───────────────────────────────

/// `n` directory levels of 62 characters each, `/`-joined.
fn deep(tag: &str, n: usize) -> String {
    (0..n)
        .map(|i| format!("{tag}{i}_{}", "x".repeat(58)))
        .collect::<Vec<_>>()
        .join("/")
}

/// Everything past Windows' 260-character `MAX_PATH`: the output's own
/// path (so its `.dvc` too), each file's path inside it (~250 characters,
/// like asset-store's deepest), and the pull destination. On Windows this
/// only passes because every path handed to tempfile's raw `MoveFileExW`
/// is verbatim; the paths push and pull report stay as the caller gave them.
#[test]
fn outputs_and_files_beyond_max_path_push_and_pull() {
    let e = env();
    let output = e.data.join(deep("out", 4)).join("host=h");
    let rel = format!(
        "site=s1/date=2026-09-01/{}/src_0001_camera_left/gt_geometry/tracks.parquet",
        deep("src", 3)
    );
    let twin = format!("site=s1/date=2026-09-02/{}/labels.jsonl", deep("src", 3));
    assert!(rel.len() >= 250, "{}", rel.len());
    assert!(output.join(&rel).as_os_str().len() > 300);
    write(&output.join(&rel), b"PAR1 geometry");
    write(&output.join(&twin), b"{\"t\":1}\n");
    // Same content under another long path: one download, one copy.
    write(
        &output.join(format!("{}/copy.jsonl", deep("dup", 4))),
        b"{\"t\":1}\n",
    );

    let report = folder::push(&e.remote, &output, &opts("ds/long")).unwrap();
    assert_eq!(report.files, 3);
    let pointer = output.parent().unwrap().join("host=h.dvc");
    assert_eq!(report.pointer_path.as_os_str(), pointer.as_os_str());
    assert!(pointer.is_file());

    let restore = e.data.parent().unwrap().join(deep("in", 4));
    let pulled = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path.clone()),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap();
    assert_eq!(pulled.written, 3);
    assert_eq!(tree(&restore), tree(&output));

    // A conflict names the file in the caller's form, then force replaces.
    // Caller's root as given, then the relative path with native separators.
    let mut target = restore.clone();
    target.extend(rel.split('/'));
    std::fs::write(&target, b"local edit").unwrap();
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path.clone()),
        &pull_opts(Some(restore.clone())),
    )
    .unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].as_os_str(), target.as_os_str());
    let forced = folder::pull(
        &e.remote,
        &PointerSource::File(report.pointer_path),
        &PullOptions {
            overwrite: Overwrite::Force,
            ..pull_opts(Some(restore.clone()))
        },
    )
    .unwrap();
    assert_eq!(forced.written, 1);
    assert_eq!(std::fs::read(&target).unwrap(), b"PAR1 geometry");
}

// ── Edges: executable outputs ───────────────────────

/// What `dvc add` writes for a 0755 `#!/bin/sh\n` (as in the fixture
/// `stage_fields/run.sh.dvc`), naming `path`.
#[cfg(unix)]
fn isexec_pointer(path: &str) -> String {
    format!(
        "outs:\n- md5: 3e2b31c72181b87149ff995e7202c0e3\n  size: 10\n  isexec: true\n  \
         hash: md5\n  path: {path}\n"
    )
}

/// A `.dvc` DVC wrote for an executable file (`isexec: true`) restores it
/// executable, and makes an identical local copy executable without
/// rewriting it. History records come from push, which records no mode.
#[cfg(unix)]
#[test]
fn pull_of_an_isexec_pointer_restores_an_executable_file() {
    use std::os::unix::fs::PermissionsExt;
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode();
    let e = env();
    let tool = e.data.join("tool.sh");
    write(&tool, b"#!/bin/sh\n");
    let report = folder::push(&e.remote, &tool, &opts("ds/tool")).unwrap();
    std::fs::write(&report.pointer_path, isexec_pointer("tool.sh")).unwrap();
    let from_dvc = PointerSource::File(report.pointer_path.clone());

    std::fs::remove_file(&tool).unwrap();
    let pulled = folder::pull(&e.remote, &from_dvc, &pull_opts(None)).unwrap();
    assert_eq!(pulled.written, 1);
    assert_eq!(mode(&tool) & 0o100, 0o100, "{:o}", mode(&tool));

    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o644)).unwrap();
    let again = folder::pull(&e.remote, &from_dvc, &pull_opts(None)).unwrap();
    assert_eq!((again.written, again.unchanged), (0, 1));
    assert_eq!(mode(&tool) & 0o100, 0o100, "{:o}", mode(&tool));

    let restored = e.data.join("from-history");
    folder::pull(
        &e.remote,
        &history("ds/tool", Selector::Latest),
        &pull_opts(Some(restored.clone())),
    )
    .unwrap();
    assert_eq!(mode(&restored) & 0o111, 0, "{:o}", mode(&restored));
}

/// DVC 3 `.dir` manifests carry no modes (`dvc add` of a directory holding
/// a 0755 file records none), but a manifest hashed with per-file metadata
/// can mark a file `isexec`. Pull refuses it by name before writing
/// anything rather than restore the file without its mode; the same for a
/// directory output marked `isexec` in its `.dvc`.
#[test]
fn pull_refuses_an_executable_mark_inside_a_directory() {
    let e = env();
    let tool = e.data.join("tool.sh");
    write(&tool, b"#!/bin/sh\n");
    let _ = folder::push(&e.remote, &tool, &opts("ds/tool")).unwrap();
    let raw = br#"[{"isexec": true, "md5": "3e2b31c72181b87149ff995e7202c0e3", "relpath": "sub/run.sh"}]"#;
    let id = hash_reader(&mut &raw[..], HashFunction::Md5).unwrap();
    let id = id.to_string();
    write(
        &e.store
            .join(format!("files/md5/{}/{}.dir", &id[..2], &id[2..])),
        raw,
    );
    let dvc = e.data.join("out.dvc");
    let dir_pointer =
        format!("outs:\n- md5: {id}.dir\n  size: 10\n  nfiles: 1\n  hash: md5\n  path: out\n");
    std::fs::write(&dvc, &dir_pointer).unwrap();
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(dvc.clone()),
        &pull_opts(None),
    )
    .unwrap_err();
    let (path, reason) = refused(&err);
    assert_eq!(path, Path::new("sub/run.sh"));
    assert!(matches!(reason, Refusal::ExecutableInDirectory), "{err:#}");
    assert!(!e.data.join("out").exists(), "nothing written");

    let marked = dir_pointer.replace("  hash: md5", "  isexec: true\n  hash: md5");
    std::fs::write(&dvc, marked).unwrap();
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(dvc.clone()),
        &pull_opts(None),
    )
    .unwrap_err();
    let (path, reason) = refused(&err);
    assert_eq!(path, dvc);
    assert!(matches!(reason, Refusal::ExecutableInDirectory), "{err:#}");
}

// ──────────────────────────────────────────────────
// Async API
// ──────────────────────────────────────────────────

#[test]
fn every_async_fn_can_be_spawned_with_owned_arguments() {
    // A caller on its own runtime moves owned arguments into `tokio::spawn`,
    // which needs each future to be `Send + 'static`: this test is that it
    // compiles. The futures are never polled.
    fn spawnable<F>(_: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
    }
    let e = env();
    let (remote, path, key) = (Arc::new(e.remote), e.data, HistoryKey::new(KEY).unwrap());

    let (r, p, o) = (Arc::clone(&remote), path.clone(), opts(KEY));
    spawnable(async move { folder::push_async(&r, &p, &o).await });
    let (r, p, o) = (Arc::clone(&remote), path, opts(KEY));
    spawnable(async move { folder::status_async(&r, &p, &o).await });
    let (r, s, o) = (
        Arc::clone(&remote),
        history(KEY, Selector::Latest),
        pull_opts(None),
    );
    spawnable(async move { folder::pull_async(&r, &s, &o).await });
    let (r, k, o) = (Arc::clone(&remote), key.clone(), LogOptions::default());
    spawnable(async move { folder::log_async(&r, &k, &o).await });
    let (r, k) = (remote, key);
    spawnable(async move { folder::keys_async(&r, Some(&k)).await });
}

#[tokio::test(flavor = "current_thread")]
async fn async_fns_push_and_restore_on_a_current_thread_runtime() {
    let e = env();
    let w = writer_dir(&e);
    let key = HistoryKey::new(KEY).unwrap();
    let pushed = folder::push_async(&e.remote, &w, &opts(KEY)).await.unwrap();
    assert_eq!((pushed.files, pushed.uploaded), (4, 4));
    assert!(pushed.pointer_path.exists());
    let written = pushed
        .outcome
        .record()
        .expect("a first version")
        .to_string();

    let status = folder::status_async(&e.remote, &w, &opts(KEY))
        .await
        .unwrap();
    assert!(
        matches!(status.sync, SyncState::InSync),
        "{:?}",
        status.sync
    );
    assert_eq!(status.to_upload, 0);

    let log = folder::log_async(&e.remote, &key, &LogOptions::default())
        .await
        .unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(
        (&log[0].key, &log[0].id, &log[0].pointer.output),
        (&written, &pushed.version, &pushed.pointer.output)
    );
    assert_eq!(folder::keys_async(&e.remote, None).await.unwrap(), [key]);

    let into = e.data.parent().unwrap().join("restore");
    let pulled = folder::pull_async(
        &e.remote,
        &history(KEY, Selector::Latest),
        &pull_opts(Some(into.clone())),
    )
    .await
    .unwrap();
    assert_eq!((pulled.written, pulled.unchanged), (4, 0));
    assert_eq!(tree(&into), tree(&w));
    let again = folder::pull_async(
        &e.remote,
        &PointerSource::File(pushed.pointer_path),
        &pull_opts(Some(into.clone())),
    )
    .await
    .unwrap();
    assert_eq!((again.written, again.unchanged), (0, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_async_pushes_of_different_outputs_on_a_multi_thread_runtime() {
    let e = env();
    let (a, b) = (e.data.join("a"), e.data.join("b"));
    many_files(&a, 20);
    write(&b.join("only.txt"), b"b\n");
    let remote = Arc::new(e.remote);
    let spawn_push = |output: &Path, key: &str| {
        let (remote, output, o) = (Arc::clone(&remote), output.to_path_buf(), opts(key));
        tokio::spawn(async move { folder::push_async(&remote, &output, &o).await })
    };
    let (pushed_a, pushed_b) = tokio::join!(spawn_push(&a, "ds/a"), spawn_push(&b, "ds/b"));
    let (pushed_a, pushed_b) = (pushed_a.unwrap().unwrap(), pushed_b.unwrap().unwrap());
    assert_eq!((pushed_a.files, pushed_a.uploaded), (20, 20));
    assert_eq!((pushed_b.files, pushed_b.uploaded), (1, 1));

    let keys = folder::keys_async(&remote, None).await.unwrap();
    let keys: Vec<&str> = keys.iter().map(HistoryKey::as_str).collect();
    assert_eq!(keys, ["ds/a", "ds/b"]);
    for (output, key) in [(&a, "ds/a"), (&b, "ds/b")] {
        let into = e.data.parent().unwrap().join(key.replace('/', "-"));
        folder::pull_async(
            &remote,
            &history(key, Selector::Latest),
            &pull_opts(Some(into.clone())),
        )
        .await
        .unwrap();
        assert_eq!(tree(&into), tree(output), "{key}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_and_progress_work_through_the_async_api() {
    let e = env();
    let out = e.data.join("out");
    let total = many_files(&out, 20);

    // Cancelled mid-upload: no .dvc and no history, as with `push`.
    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    let o = PushOptions {
        jobs: 1,
        cancel,
        progress: Progress::new(move |e| {
            if let ProgressEvent::Advanced {
                phase: Phase::Uploading,
                ..
            } = e
            {
                trigger.cancel();
            }
        }),
        ..opts("ds/out")
    };
    let err = folder::push_async(&e.remote, &out, &o).await.unwrap_err();
    assert_cancelled(&err);
    assert!(!e.data.join("out.dvc").exists());
    let err = folder::status_async(&e.remote, &out, &o).await.unwrap_err();
    assert_cancelled(&err);

    let (progress, events) = recorder();
    let pushed = folder::push_async(
        &e.remote,
        &out,
        &PushOptions {
            progress,
            ..opts("ds/out")
        },
    )
    .await
    .unwrap();
    let events = std::mem::take(&mut *events.lock().unwrap());
    assert_eq!(
        phase_sums(&events, Phase::Hashing),
        ((20, Some(total)), (20, total))
    );
    let ((to_upload, _), (uploaded, _)) = phase_sums(&events, Phase::Uploading);
    assert_eq!(
        (to_upload, uploaded),
        (pushed.uploaded as u64, pushed.uploaded as u64)
    );
    assert!((1..20).contains(&pushed.already_present), "{pushed:?}");

    let into = e.data.parent().unwrap().join("restore");
    let o = pull_opts(Some(into.clone()));
    o.cancel.cancel();
    let err = folder::pull_async(&e.remote, &history("ds/out", Selector::Latest), &o)
        .await
        .unwrap_err();
    assert_cancelled(&err);
    assert!(tree(&into).is_empty());

    let (progress, events) = recorder();
    folder::pull_async(
        &e.remote,
        &history("ds/out", Selector::Latest),
        &PullOptions {
            progress,
            ..pull_opts(Some(into.clone()))
        },
    )
    .await
    .unwrap();
    let events = std::mem::take(&mut *events.lock().unwrap());
    assert_eq!(
        phase_sums(&events, Phase::Downloading),
        ((20, None), (20, total))
    );
    assert_eq!(tree(&into), tree(&out));

    let o = LogOptions::default();
    o.cancel.cancel();
    let err = folder::log_async(&e.remote, &HistoryKey::new("ds/out").unwrap(), &o)
        .await
        .unwrap_err();
    assert_cancelled(&err);
}

/// A progress callback that, at the first `Advanced` event of `phase`, asks
/// a task on the caller's runtime to answer and waits up to 10 s for it;
/// the flag says whether it did. A callback called from the runtime's own
/// thread (the work beside it running there too) blocks that thread, so on
/// a `current_thread` runtime nothing can answer.
fn runtime_probe(phase: Phase) -> (Progress, tokio::task::JoinHandle<()>, Arc<AtomicBool>) {
    let (ask, asked) = tokio::sync::oneshot::channel::<()>();
    let (answer, answered) = std::sync::mpsc::channel::<()>();
    let flag = Arc::new(AtomicBool::new(false));
    let set = Arc::clone(&flag);
    let channels = Mutex::new(Some((ask, answered)));
    let progress = Progress::new(move |e| {
        let ProgressEvent::Advanced { phase: p, .. } = e else {
            return;
        };
        let first = if p == phase {
            channels.lock().unwrap().take()
        } else {
            None
        };
        if let Some((ask, answered)) = first {
            ask.send(()).unwrap();
            let ok = answered.recv_timeout(Duration::from_secs(10)).is_ok();
            set.store(ok, Ordering::SeqCst);
        }
    });
    let responder = tokio::spawn(async move {
        if asked.await.is_ok() {
            let _ = answer.send(());
        }
    });
    (progress, responder, flag)
}

#[tokio::test(flavor = "current_thread")]
async fn file_work_runs_off_the_callers_runtime() {
    // Walking, snapshotting, hashing, classifying and placing files block;
    // on the caller's runtime thread they would stall every other task on
    // it. On a current_thread runtime another task gets to run only if they
    // happen elsewhere.
    let e = env();
    let w = writer_dir(&e);
    let (progress, responder, answered) = runtime_probe(Phase::Hashing);
    let o = PushOptions {
        progress,
        ..opts(KEY)
    };
    let _ = folder::push_async(&e.remote, &w, &o).await.unwrap();
    responder.await.unwrap();
    assert!(
        answered.load(Ordering::SeqCst),
        "push hashed on the runtime"
    );

    write(&w.join("new.jsonl"), b"{}\n");
    let (progress, responder, answered) = runtime_probe(Phase::Hashing);
    let o = PushOptions {
        progress,
        ..opts(KEY)
    };
    folder::status_async(&e.remote, &w, &o).await.unwrap();
    responder.await.unwrap();
    assert!(
        answered.load(Ordering::SeqCst),
        "status hashed on the runtime"
    );

    for phase in [Phase::Hashing, Phase::Downloading] {
        let into = e.data.parent().unwrap().join(format!("{phase:?}"));
        let (progress, responder, answered) = runtime_probe(phase);
        let o = PullOptions {
            progress,
            ..pull_opts(Some(into))
        };
        folder::pull_async(&e.remote, &history(KEY, Selector::Latest), &o)
            .await
            .unwrap();
        responder.await.unwrap();
        assert!(
            answered.load(Ordering::SeqCst),
            "pull {phase:?} ran on the runtime"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_an_async_push_stops_its_hashing() {
    // Dropping the future (an aborted task, a lost `select!`, a timeout) is
    // how async callers cancel. The hashing it started must stop at the next
    // file, as with a CancelToken, not run on unseen through the rest.
    let e = env();
    let out = e.data.join("out");
    many_files(&out, 20);
    let (started, hashing) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let hashed = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&hashed);
    let gate = Mutex::new(Some((started, released)));
    let o = PushOptions {
        jobs: 1,
        progress: Progress::new(move |e| {
            if let ProgressEvent::Advanced {
                phase: Phase::Hashing,
                ..
            } = e
            {
                count.fetch_add(1, Ordering::SeqCst);
                let first = gate.lock().unwrap().take();
                if let Some((started, released)) = first {
                    started.send(()).unwrap();
                    let _ = released.recv_timeout(Duration::from_secs(10));
                }
            }
        }),
        ..opts("ds/out")
    };
    let remote = Arc::new(e.remote);
    let push = {
        let (remote, out) = (Arc::clone(&remote), out.clone());
        tokio::spawn(async move { folder::push_async(&remote, &out, &o).await })
    };
    hashing.await.unwrap();
    push.abort();
    assert!(push.await.unwrap_err().is_cancelled());
    let _ = release.send(());

    // Whatever still runs holds the callback, and with it `hashed`.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while Arc::strong_count(&hashed) > 1 {
        assert!(std::time::Instant::now() < deadline, "work never stopped");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        hashed.load(Ordering::SeqCst),
        1,
        "hashing went on after the push was dropped"
    );
    assert!(!e.data.join("out.dvc").exists());
    assert!(remote_keys(&e.store).is_empty(), "something was published");
}

// ──────────────────────────────────────────────────
// Parent-linked history
// ──────────────────────────────────────────────────

/// The base the `.dvc` at `path` records.
#[track_caller]
fn base_of(path: &Path) -> Option<RecordId> {
    match DvcPointer::load(path).unwrap().meta {
        Some(BigstoreMeta::Base(id)) => Some(id),
        None => None,
        other => panic!("not a base: {other:?}"),
    }
}

#[track_caller]
fn stale_base(err: &anyhow::Error) -> (Option<&RecordId>, &[RecordId]) {
    match folder_error(err) {
        FolderError::StaleBase { base, heads } => (base.as_ref(), heads),
        _ => panic!("not StaleBase: {err:#}"),
    }
}

#[track_caller]
fn diverged(err: &anyhow::Error) -> &[RecordId] {
    match folder_error(err) {
        FolderError::Diverged { heads } => heads,
        _ => panic!("not Diverged: {err:#}"),
    }
}

fn as_writer(key: &str, writer: &str) -> PushOptions {
    PushOptions {
        writer: writer.into(),
        ..opts(key)
    }
}

fn by_id(id: &RecordId) -> Selector {
    Selector::Id(id.as_str()[..8].into())
}

/// Run the CLI against `e`'s remote; returns (success, stdout, stderr).
fn cli(e: &Env, args: &[&str]) -> (bool, String, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_git-bigstore"))
        .args(args)
        .args(["--remote", &format!("local://{}", e.store.display())])
        .output()
        .unwrap();
    let text = |b: Vec<u8>| String::from_utf8(b).unwrap();
    (out.status.success(), text(out.stdout), text(out.stderr))
}

#[test]
fn a_push_whose_base_is_not_the_latest_version_is_refused_and_publishes_nothing() {
    let e = env();
    let (a, b) = (e.data.join("a/f"), e.data.join("b/f"));
    write(&a, b"v1");
    let v1 = folder::push(&e.remote, &a, &opts("k")).unwrap().version;
    // Another host takes v1; then this one pushes v2.
    folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(b.clone())),
    )
    .unwrap();
    let b_dvc = e.data.join("b/f.dvc");
    assert_eq!(base_of(&b_dvc), Some(v1.clone()));
    write(&a, b"v2");
    let v2 = folder::push(&e.remote, &a, &opts("k")).unwrap().version;

    // Unchanged since its pull, b is behind; changed, it is stale.
    let s = folder::status(&e.remote, &b, &opts("k")).unwrap();
    assert!(
        matches!(&s.sync, SyncState::RemoteAhead { latest } if latest.id == v2),
        "{:?}",
        s.sync
    );
    write(&b, b"b's change");
    let s = folder::status(&e.remote, &b, &opts("k")).unwrap();
    let SyncState::Stale { base, head } = &s.sync else {
        panic!("{:?}", s.sync)
    };
    assert_eq!((base.as_ref(), &head.id), (Some(&v1), &v2));

    let keys = remote_keys(&e.store);
    let dvc = std::fs::read(&b_dvc).unwrap();
    let err = folder::push(&e.remote, &b, &opts("k")).unwrap_err();
    assert_eq!(stale_base(&err), (Some(&v1), &[v2.clone()][..]));
    let msg = format!("{err:#}");
    assert!(
        msg.contains(v1.as_str()) && msg.contains(v2.as_str()),
        "{msg}"
    );
    assert_eq!(remote_keys(&e.store), keys, "a refused push published");
    assert_eq!(std::fs::read(&b_dvc).unwrap(), dvc);

    // No base at all, while history has versions: stale too.
    let c = e.data.join("c/f");
    write(&c, b"c");
    let err = folder::push(&e.remote, &c, &opts("k")).unwrap_err();
    assert_eq!(stale_base(&err), (None, &[v2][..]));
    assert!(!e.data.join("c/f.dvc").exists());
    assert_eq!(remote_keys(&e.store), keys, "a refused push published");
}

#[test]
fn pushes_racing_from_one_base_both_land_as_a_fork_that_a_merge_joins() {
    let e = env();
    let (a, b) = (e.data.join("a/f"), e.data.join("b/f"));
    write(&a, b"v1");
    let v1 = folder::push(&e.remote, &a, &as_writer("k", "host-a"))
        .unwrap()
        .version;
    folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(b.clone())),
    )
    .unwrap();
    write(&a, b"a's change");
    write(&b, b"b's change");

    // B's push checks history (one head: its base), and before it
    // publishes, A's push from the same base lands.
    let raced = Arc::new(Mutex::new(None));
    let progress = {
        let (raced, a) = (Arc::clone(&raced), a.clone());
        let remote = Remote::open(&RemoteConfig {
            url: format!("local://{}", e.store.display()),
            endpoint: None,
            region: None,
            credentials: Credentials::FromEnv,
        })
        .unwrap();
        Progress::new(move |event| {
            let mut raced = raced.lock().unwrap();
            if matches!(
                event,
                ProgressEvent::Started {
                    phase: Phase::Uploading,
                    ..
                }
            ) && raced.is_none()
            {
                let push = || folder::push(&remote, &a, &as_writer("k", "host-a"));
                *raced = Some(
                    std::thread::scope(|s| s.spawn(push).join())
                        .unwrap()
                        .unwrap(),
                );
            }
        })
    };
    let b_push = folder::push(
        &e.remote,
        &b,
        &PushOptions {
            progress,
            ..as_writer("k", "host-b")
        },
    )
    .unwrap();
    let a_push = raced
        .lock()
        .unwrap()
        .take()
        .expect("A pushed during B's push");
    assert!(
        matches!(a_push.outcome, Pushed::Published { .. }),
        "{:?}",
        a_push.outcome
    );
    let Pushed::Forked { with, .. } = &b_push.outcome else {
        panic!("B raced A from the same base: {:?}", b_push.outcome)
    };
    assert_eq!(with, std::slice::from_ref(&a_push.version));
    let log = history_log(&e, "k").unwrap();
    let mut forked: Vec<(RecordId, Vec<RecordId>, Option<String>)> = log[1..]
        .iter()
        .map(|r| (r.id.clone(), r.parents.clone(), r.writer.clone()))
        .collect();
    forked.sort();
    let mut want = vec![
        (
            a_push.version.clone(),
            vec![v1.clone()],
            Some("host-a".into()),
        ),
        (
            b_push.version.clone(),
            vec![v1.clone()],
            Some("host-b".into()),
        ),
    ];
    want.sort();
    assert_eq!((log.len(), forked), (3, want));
    let mut heads = vec![a_push.version.clone(), b_push.version.clone()];
    heads.sort();

    // Latest is ambiguous; a version by id or time is not.
    let c = e.data.join("c/f");
    let err = folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(c.clone())),
    )
    .unwrap_err();
    assert_eq!(diverged(&err), heads);
    assert!(!c.exists() && !e.data.join("c/f.dvc").exists());
    folder::pull(
        &e.remote,
        &history("k", by_id(&b_push.version)),
        &pull_opts(Some(c.clone())),
    )
    .unwrap();
    assert_eq!(std::fs::read(&c).unwrap(), b"b's change");
    assert_eq!(
        base_of(&e.data.join("c/f.dvc")),
        Some(b_push.version.clone())
    );
    let newest = log.last().unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    let d = e.data.join("d/f");
    folder::pull(
        &e.remote,
        &history("k", Selector::AtOrBefore(now)),
        &pull_opts(Some(d.clone())),
    )
    .unwrap();
    assert_eq!(base_of(&e.data.join("d/f.dvc")), Some(newest.id.clone()));

    // Each side's next push is refused until a merge.
    let s = folder::status(&e.remote, &a, &opts("k")).unwrap();
    assert!(
        matches!(&s.sync, SyncState::Diverged { heads: h } if *h == heads),
        "{:?}",
        s.sync
    );
    assert_eq!((&s.heads, s.based), (&heads, false));
    write(&a, b"a again");
    let err = folder::push(&e.remote, &a, &opts("k")).unwrap_err();
    assert_eq!(diverged(&err), heads);
    let (ok, out, err) = cli(&e, &["folder", "log", "k"]);
    assert!(ok, "{err}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3, "{out}");
    assert!(
        lines[0].contains(&format!(
            "{v1}  file  2 bytes  by host-a  <- root  [fork: 2 children]"
        )),
        "{out}"
    );
    for line in &lines[1..] {
        assert!(line.ends_with(&format!("<- {v1}  [head]")), "{out}");
    }
    assert!(err.contains("history has forked: 2 heads"), "{err}");
    let args = ["folder", "status", a.to_str().unwrap(), "--history", "k"];
    let (ok, out, err) = cli(&e, &args);
    assert!(ok, "{err}");
    assert!(
        out.contains(&format!(
            "diverged: history has forked into versions {}, {}",
            heads[0], heads[1]
        )),
        "{out}"
    );

    // A merge needs a base among the heads: none, or an older version, is
    // stale.
    let merge = |p: &Path| {
        folder::push(
            &e.remote,
            p,
            &PushOptions {
                resolve: Resolve::Merge,
                ..opts("k")
            },
        )
    };
    let old = e.data.join("old/f");
    folder::pull(
        &e.remote,
        &history("k", by_id(&v1)),
        &pull_opts(Some(old.clone())),
    )
    .unwrap();
    write(&old, b"from v1");
    let err = merge(&old).unwrap_err();
    assert_eq!(stale_base(&err), (Some(&v1), &heads[..]));

    // A reconciles B's change into its own and merges, from the CLI.
    write(&a, b"a's change + b's change");
    let args = [
        "folder",
        "push",
        a.to_str().unwrap(),
        "--history",
        "k",
        "--resolve",
        "merge",
    ];
    let (ok, _, err) = cli(&e, &args);
    assert!(ok, "{err}");
    assert!(!err.contains("forked"), "{err}");
    let merged = base_of(&e.data.join("a/f.dvc")).unwrap();
    let log = history_log(&e, "k").unwrap();
    assert_eq!(
        (log.len(), &log[3].id, &log[3].parents),
        (4, &merged, &heads)
    );
    let name = format!("bigstore-history/k/{}+{}/{merged}.dvc", heads[0], heads[1]);
    assert!(remote_keys(&e.store).contains(&name), "{name}");
    let latest = e.data.join("g/f");
    folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(latest.clone())),
    )
    .unwrap();
    assert_eq!(std::fs::read(&latest).unwrap(), b"a's change + b's change");
    let (ok, out, err) = cli(&e, &["folder", "log", "k"]);
    assert!(ok && err.is_empty(), "{err}");
    assert!(
        out.lines()
            .last()
            .unwrap()
            .ends_with(&format!("<- {}, {}  [merge]  [head]", heads[0], heads[1])),
        "{out}"
    );
    // B, last synced to its own side, is behind the merge.
    write(&b, b"b again");
    let err = folder::push(&e.remote, &b, &opts("k")).unwrap_err();
    assert_eq!(stale_base(&err), (Some(&b_push.version), &[merged][..]));
}

#[test]
fn a_version_published_without_its_dvc_is_adopted_not_pushed_again() {
    let e = env();
    let f = e.data.join("f");
    let dvc = e.data.join("f.dvc");
    write(&f, b"v1");
    let v1 = folder::push(&e.remote, &f, &opts("k")).unwrap().version;
    let at_v1 = std::fs::read(&dvc).unwrap();
    write(&f, b"v2");
    let v2 = folder::push(&e.remote, &f, &opts("k")).unwrap().version;
    // A crash after publishing v2's record, before writing the .dvc: it
    // still says v1, which is not the head.
    std::fs::write(&dvc, &at_v1).unwrap();
    assert_eq!(base_of(&dvc), Some(v1));
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert!(matches!(s.sync, SyncState::InSync), "{:?}", s.sync);
    // In sync, but not based on the head: what push adopts.
    assert_eq!((&s.heads[..], s.based), (std::slice::from_ref(&v2), false));
    let again = folder::push(&e.remote, &f, &opts("k")).unwrap();
    assert_eq!(
        (again.outcome, &again.version),
        (Pushed::AlreadyLatest, &v2)
    );
    assert_eq!(base_of(&dvc), Some(v2.clone()));
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert_eq!((&s.heads[..], s.based), (std::slice::from_ref(&v2), true));
    // Or a crash before the first .dvc was written.
    std::fs::remove_file(&dvc).unwrap();
    let again = folder::push(&e.remote, &f, &opts("k")).unwrap();
    assert_eq!(
        (again.outcome, &again.version),
        (Pushed::AlreadyLatest, &v2)
    );
    assert_eq!(base_of(&dvc), Some(v2));
    assert_eq!(history_log(&e, "k").unwrap().len(), 2);
}

/// A 0.2 version of `key` holding `content` at `time`: its object, and its
/// record. Returns the record's id.
fn legacy_version(e: &Env, key: &str, time: &str, content: &[u8]) -> RecordId {
    let md5 = hash_reader(&mut &content[..], HashFunction::Md5).unwrap();
    let hex = md5.to_string();
    write(
        &e.store.join("files/md5").join(&hex[..2]).join(&hex[2..]),
        content,
    );
    let pointer = DvcPointer {
        output: DvcOutput::File {
            md5,
            size: content.len() as u64,
        },
        path: "f".into(),
        meta: None,
    };
    write(
        &e.store
            .join(format!("bigstore-history/{key}/{time}-{hex}.dvc")),
        pointer.to_yaml().as_bytes(),
    );
    let log = history_log(e, key).unwrap();
    let id = legacy_id(time, &hex);
    log.into_iter()
        .map(|r| r.id)
        .find(|r| r.as_str() == id)
        .expect("the record is read with its name's id")
}

#[test]
fn a_02_history_reads_as_a_straight_line_that_new_versions_continue() {
    let e = env();
    let l1 = legacy_version(&e, "k", "20260901T000000.000000000Z", b"v1");
    let l2 = legacy_version(&e, "k", "20260902T000000.000000000Z", b"v2");
    type Line = Vec<(RecordId, Vec<RecordId>)>;
    let line = || -> Line {
        let log = history_log(&e, "k").unwrap();
        log.into_iter().map(|r| (r.id, r.parents)).collect()
    };
    assert_eq!(
        line(),
        [(l1.clone(), vec![]), (l2.clone(), vec![l1.clone()])]
    );
    let log = history_log(&e, "k").unwrap();
    assert!(log.iter().all(|r| r.writer.is_none()), "{log:?}");
    assert_eq!(log[0].time.to_rfc3339(), "2026-09-01T00:00:00+00:00");

    // The latest is the 0.2 head; a time picks by the record name's.
    let f = e.data.join("f");
    folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(f.clone())),
    )
    .unwrap();
    assert_eq!(std::fs::read(&f).unwrap(), b"v2");
    assert_eq!(base_of(&e.data.join("f.dvc")), Some(l2.clone()));
    let old = e.data.join("old/f");
    let at = Selector::AtOrBefore("2026-09-01T12:00:00Z".into());
    folder::pull(&e.remote, &history("k", at), &pull_opts(Some(old.clone()))).unwrap();
    assert_eq!(std::fs::read(&old).unwrap(), b"v1");

    // A new version continues the line, named for its parent.
    write(&f, b"v3");
    let v3 = folder::push(&e.remote, &f, &opts("k")).unwrap().version;
    let name = format!("bigstore-history/k/{l2}/{v3}.dvc");
    assert!(remote_keys(&e.store).contains(&name), "{name}");
    assert_eq!(
        line(),
        [
            (l1.clone(), vec![]),
            (l2.clone(), vec![l1]),
            (v3.clone(), vec![l2.clone()])
        ]
    );
    let now = chrono::Utc::now().to_rfc3339();
    let at = e.data.join("at/f");
    folder::pull(
        &e.remote,
        &history("k", Selector::AtOrBefore(now)),
        &pull_opts(Some(at.clone())),
    )
    .unwrap();
    assert_eq!(std::fs::read(&at).unwrap(), b"v3");

    // A 0.2 writer not upgraded with the rest continues its own line from
    // the last 0.2 record: a fork, not a silent new latest.
    let l3 = legacy_version(&e, "k", "20991231T000000.000000000Z", b"v4 from 0.2");
    let err = folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &pull_opts(Some(e.data.join("latest"))),
    )
    .unwrap_err();
    let mut heads = vec![l3, v3];
    heads.sort();
    assert_eq!(diverged(&err), heads);
}

/// The pointer 0.2 wrote beside a single-file output `name` holding
/// `content`: no base.
fn pointer_02(content: &[u8], name: &str) -> DvcPointer {
    DvcPointer {
        output: DvcOutput::File {
            md5: hash_reader(&mut &content[..], HashFunction::Md5).unwrap(),
            size: content.len() as u64,
        },
        path: name.into(),
        meta: None,
    }
}

#[test]
fn an_output_0_2_pushed_as_the_head_follows_it_when_changed() {
    let e = env();
    legacy_version(&e, "k", "20260901T000000.000000000Z", b"v0");
    let head = legacy_version(&e, "k", "20260902T000000.000000000Z", b"v1");
    // 0.2 pushed (or pulled) this output as v1, the head; then it changed.
    let f = e.data.join("up/f");
    write(&f, b"v1 edited");
    let dvc = e.data.join("up/f.dvc");
    write(&dvc, pointer_02(b"v1", "f").to_yaml().as_bytes());
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert!(matches!(s.sync, SyncState::LocalAhead), "{:?}", s.sync);
    let pushed = folder::push(&e.remote, &f, &opts("k")).unwrap();
    assert!(
        matches!(pushed.outcome, Pushed::Published { .. }),
        "{:?}",
        pushed.outcome
    );
    let log = history_log(&e, "k").unwrap();
    let last = log.last().unwrap();
    assert_eq!((&last.id, &last.parents), (&pushed.version, &vec![head]));
    assert_eq!(base_of(&dvc), Some(pushed.version.clone()));

    // One whose 0.2 `.dvc` records an older version is stale.
    let g = e.data.join("old/f");
    write(&g, b"v0 edited");
    write(
        &e.data.join("old/f.dvc"),
        pointer_02(b"v0", "f").to_yaml().as_bytes(),
    );
    let s = folder::status(&e.remote, &g, &opts("k")).unwrap();
    assert!(
        matches!(&s.sync, SyncState::Stale { base: None, head } if head.id == pushed.version),
        "{:?}",
        s.sync
    );
    let keys = remote_keys(&e.store);
    let err = folder::push(&e.remote, &g, &opts("k")).unwrap_err();
    assert_eq!(stale_base(&err), (None, &[pushed.version][..]));
    assert_eq!(remote_keys(&e.store), keys, "a refused push published");
}

#[test]
fn a_02_version_is_still_selected_by_its_content_id() {
    let e = env();
    let md5 = |c: &[u8]| {
        hash_reader(&mut &c[..], HashFunction::Md5)
            .unwrap()
            .to_string()
    };
    let t1 = "20260901T000000.000000000Z";
    let v1 = legacy_version(&e, "k", t1, b"v1");
    legacy_version(&e, "k", "20260902T000000.000000000Z", b"v2");
    let f = e.data.join("f");
    let by_content = Selector::Id(md5(b"v1")[..8].to_uppercase());
    folder::pull(
        &e.remote,
        &history("k", by_content),
        &pull_opts(Some(f.clone())),
    )
    .unwrap();
    assert_eq!(std::fs::read(&f).unwrap(), b"v1");
    assert_eq!(base_of(&e.data.join("f.dvc")), Some(v1.clone()));

    // A prefix that is one version's record id and another's content id
    // matches both.
    let time = "20260903T000000.000000000Z";
    write_record(&e, "k", time, v1.as_str());
    let err = folder::pull(
        &e.remote,
        &history("k", by_id(&v1)),
        &pull_opts(Some(e.data.join("g"))),
    )
    .unwrap_err();
    let FolderError::AmbiguousId { candidates, .. } = folder_error(&err) else {
        panic!("{err:#}")
    };
    let ids: Vec<String> = candidates.iter().map(|r| r.id.to_string()).collect();
    assert_eq!(ids, [v1.to_string(), legacy_id(time, v1.as_str())]);
}

#[test]
fn a_record_whose_name_disagrees_with_its_content_is_refused() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"v1");
    let v1 = folder::push(&e.remote, &f, &as_writer("k", "host-a"))
        .unwrap()
        .version;
    write(&f, b"v2");
    let v2 = folder::push(&e.remote, &f, &as_writer("k", "host-a"))
        .unwrap()
        .version;
    let dir = e.store.join("bigstore-history/k");
    let genuine = dir.join(format!("{v1}/{v2}.dvc"));
    let bytes = std::fs::read(&genuine).unwrap();
    let out = e.data.join("out");
    let pull = |at: Selector| {
        let err =
            folder::pull(&e.remote, &history("k", at), &pull_opts(Some(out.clone()))).unwrap_err();
        format!("{err:#}")
    };

    // Moved to claim other parents: its content says v1.
    let moved = dir.join(format!("root/{v2}.dvc"));
    std::fs::rename(&genuine, &moved).unwrap();
    let msg = pull(by_id(&v2));
    assert!(
        msg.contains("bad record") && msg.contains(&format!("it follows {v1}, not the root")),
        "{msg}"
    );
    let msg = format!("{:#}", history_log(&e, "k").unwrap_err());
    assert!(msg.contains("bad record"), "{msg}");
    std::fs::rename(&moved, &genuine).unwrap();

    // Edited in place: its bytes are no longer the id its name gives.
    let edited = String::from_utf8(bytes.clone()).unwrap();
    std::fs::write(&genuine, edited.replace("host-a", "host-b")).unwrap();
    for at in [Selector::Latest, by_id(&v2)] {
        let msg = pull(at);
        assert!(
            msg.contains("bad record") && msg.contains(&format!("not the {v2} its name says")),
            "{msg}"
        );
    }
    std::fs::write(&genuine, &bytes).unwrap();

    // A pointer that is no record, under a record's name.
    let beside = std::fs::read(e.data.join("f.dvc")).unwrap();
    write(
        &dir.join(format!("{v2}/{}.dvc", record_id(&beside))),
        &beside,
    );
    let msg = pull(Selector::Latest);
    assert!(
        msg.contains("bad record") && msg.contains("its meta is not what its name calls for"),
        "{msg}"
    );
    assert!(!out.exists() && !e.data.join("out.dvc").exists());
}

#[test]
fn a_dvc_with_someone_elses_meta_is_never_replaced() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"x");
    let dvc = e.data.join("f.dvc");
    let theirs = format!(
        "outs:\n- md5: 9dd4e461268c8034f5c8564e155c67a6\n  size: 1\n  hash: md5\n  path: f\n\
         meta:\n  bigstore:\n    base: {}\n  author: rick\n",
        "a".repeat(32)
    );
    write(&dvc, theirs.as_bytes());
    let err = folder::push(&e.remote, &f, &opts("k")).unwrap_err();
    assert_eq!(refused(&err), (dvc.as_path(), &Refusal::ForeignPointer));
    // Nor does a pull from history, before it writes anything.
    let g = e.data.join("g");
    write(&g, b"y");
    let _ = folder::push(&e.remote, &g, &opts("k")).unwrap();
    let o = PullOptions {
        overwrite: Overwrite::Force,
        ..pull_opts(Some(f.clone()))
    };
    let err = folder::pull(&e.remote, &history("k", Selector::Latest), &o).unwrap_err();
    assert_eq!(refused(&err), (dvc.as_path(), &Refusal::ForeignPointer));
    assert_eq!(std::fs::read(&f).unwrap(), b"x");
    assert_eq!(std::fs::read_to_string(&dvc).unwrap(), theirs);
}

#[test]
fn a_pull_writes_its_base_last_so_a_cancelled_one_leaves_the_base() {
    let e = env();
    let out = e.data.join("out");
    many_files(&out, 20);
    let v1 = folder::push(&e.remote, &out, &opts("ds/out"))
        .unwrap()
        .version;
    for (rel, _) in tree(&out) {
        write(&out.join(&rel), format!("changed {rel}\n").as_bytes());
    }
    let v2 = folder::push(&e.remote, &out, &opts("ds/out"))
        .unwrap()
        .version;
    let dvc = e.data.join("out.dvc");
    let at_v2 = std::fs::read(&dvc).unwrap();
    let cancelled_after_one_file = |into: &Path| {
        let cancel = CancelToken::new();
        let trigger = cancel.clone();
        PullOptions {
            jobs: 1,
            overwrite: Overwrite::Force,
            cancel,
            progress: Progress::new(move |e| {
                if let ProgressEvent::Advanced {
                    phase: Phase::Downloading,
                    ..
                } = e
                {
                    trigger.cancel();
                }
            }),
            ..pull_opts(Some(into.to_path_buf()))
        }
    };
    let old = || history("ds/out", by_id(&v1));
    let err = folder::pull(&e.remote, &old(), &cancelled_after_one_file(&out)).unwrap_err();
    assert_cancelled(&err);
    let replaced = tree(&out)
        .iter()
        .filter(|(_, c)| !c.starts_with(b"changed"))
        .count();
    assert!((1..20).contains(&replaced), "{replaced}");
    assert_eq!(
        std::fs::read(&dvc).unwrap(),
        at_v2,
        "a cancelled pull moved the base"
    );
    let fresh = e.data.join("fresh");
    let err = folder::pull(&e.remote, &old(), &cancelled_after_one_file(&fresh)).unwrap_err();
    assert_cancelled(&err);
    assert!(!e.data.join("fresh.dvc").exists());

    // Pulled whole, v1 is the base, so a push of changes to it is stale.
    let o = PullOptions {
        overwrite: Overwrite::Force,
        ..pull_opts(Some(out.clone()))
    };
    folder::pull(&e.remote, &old(), &o).unwrap();
    assert_eq!(base_of(&dvc), Some(v1.clone()));
    write(&out.join("f000.txt"), b"edited on v1\n");
    let err = folder::push(&e.remote, &out, &opts("ds/out")).unwrap_err();
    assert_eq!(stale_base(&err), (Some(&v1), &[v2][..]));
}

/// Restore `key`'s latest version into `into`, beside a `.dvc` naming it.
fn pull_latest(
    e: &Env,
    key: &str,
    into: &Path,
    overwrite: Overwrite,
) -> anyhow::Result<folder::PullReport> {
    folder::pull(
        &e.remote,
        &history(key, Selector::Latest),
        &PullOptions {
            overwrite,
            ..pull_opts(Some(into.to_path_buf()))
        },
    )
}

/// Host A's output at v1, pushed to `key`, and host B's copy of it, pulled.
fn two_hosts(e: &Env, key: &str) -> (PathBuf, PathBuf) {
    let a = e.data.join(format!("a/{key}"));
    write(&a.join("keep.txt"), b"same\n");
    write(&a.join("edit.txt"), b"v1\n");
    write(&a.join("gone.txt"), b"removed in v2\n");
    write(&a.join("sub/deep.txt"), b"v1\n");
    let _ = folder::push(&e.remote, &a, &opts(key)).unwrap();
    let b = e.data.join(format!("b/{key}"));
    pull_latest(e, key, &b, Overwrite::Refuse).unwrap();
    (a, b)
}

/// A's second version: one file edited, one removed, one added, one deep
/// file edited.
fn push_v2(e: &Env, key: &str, a: &Path) -> RecordId {
    write(&a.join("edit.txt"), b"v2\n");
    std::fs::remove_file(a.join("gone.txt")).unwrap();
    write(&a.join("sub/deep.txt"), b"v2\n");
    write(&a.join("new.txt"), b"added in v2\n");
    folder::push(&e.remote, a, &opts(key)).unwrap().version
}

fn dvc_beside(output: &Path) -> PathBuf {
    let mut dvc = output.as_os_str().to_owned();
    dvc.push(".dvc");
    PathBuf::from(dvc)
}

#[test]
fn a_catch_up_pull_brings_an_unchanged_output_to_the_latest_version() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    let v2 = push_v2(&e, "k", &a);
    let s = folder::status(&e.remote, &b, &opts("k")).unwrap();
    assert!(
        matches!(&s.sync, SyncState::RemoteAhead { latest } if latest.id == v2),
        "{:?}",
        s.sync
    );

    // Without catching up, every changed file is a conflict.
    let err = pull_latest(&e, "k", &b, Overwrite::Refuse).unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, &[b.join("edit.txt"), b.join("sub/deep.txt")]);

    write(&b.join("notes.txt"), b"B's own, never pushed\n");
    let r = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap();
    assert_eq!(
        (r.written, r.unchanged, r.removed, r.extra_local),
        (3, 1, 1, 1)
    );
    let mut expected = tree(&a);
    expected.push(("notes.txt".into(), b"B's own, never pushed\n".to_vec()));
    expected.sort();
    assert_eq!(tree(&b), expected);
    assert_eq!(base_of(&dvc_beside(&b)), Some(v2));

    std::fs::remove_file(b.join("notes.txt")).unwrap();
    let s = folder::status(&e.remote, &b, &opts("k")).unwrap();
    assert!(matches!(s.sync, SyncState::InSync), "{:?}", s.sync);
}

#[test]
fn a_catch_up_pull_refuses_files_changed_since_the_base_and_writes_nothing() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    let v1 = base_of(&dvc_beside(&b)).unwrap();
    // B edits a file A left alone, and one A removes.
    write(&b.join("keep.txt"), b"B's edit\n");
    write(&b.join("gone.txt"), b"B's edit\n");
    push_v2(&e, "k", &a);

    let before = tree(&b);
    let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, &[b.join("keep.txt"), b.join("gone.txt")]);
    assert_eq!(tree(&b), before, "nothing written or removed");
    assert_eq!(base_of(&dvc_beside(&b)), Some(v1));

    // With no .dvc beside it, nothing is known to be unchanged.
    write(&b.join("keep.txt"), b"same\n");
    write(&b.join("gone.txt"), b"removed in v2\n");
    std::fs::remove_file(dvc_beside(&b)).unwrap();
    let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, &[b.join("edit.txt"), b.join("sub/deep.txt")]);
}

#[test]
fn a_catch_up_pull_leaves_a_file_written_while_it_runs() {
    let e = env();
    // Written between being checked and being replaced.
    let (a, b) = two_hosts(&e, "replaced");
    push_v2(&e, "replaced", &a);
    let target = b.join("edit.txt");
    let meanwhile = |path: PathBuf| {
        Progress::new(move |event| {
            if let ProgressEvent::Started {
                phase: Phase::Downloading,
                ..
            } = event
            {
                std::fs::write(&path, b"written meanwhile\n").unwrap();
            }
        })
    };
    let pull = |key: &str, into: &Path, progress| {
        folder::pull(
            &e.remote,
            &history(key, Selector::Latest),
            &PullOptions {
                overwrite: Overwrite::IfUnchanged,
                progress,
                ..pull_opts(Some(into.to_path_buf()))
            },
        )
    };
    let err = pull("replaced", &b, meanwhile(target.clone())).unwrap_err();
    assert_eq!(
        refused(&err),
        (target.as_path(), &Refusal::ChangedWhilePulling)
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"written meanwhile\n");

    // Written between being checked and being removed.
    let (a, b) = two_hosts(&e, "removed");
    std::fs::remove_file(a.join("gone.txt")).unwrap();
    let _ = folder::push(&e.remote, &a, &opts("removed")).unwrap();
    let gone = b.join("gone.txt");
    let v1 = base_of(&dvc_beside(&b));
    let err = pull("removed", &b, meanwhile(gone.clone())).unwrap_err();
    assert_eq!(
        refused(&err),
        (gone.as_path(), &Refusal::ChangedWhilePulling)
    );
    assert_eq!(std::fs::read(&gone).unwrap(), b"written meanwhile\n");
    assert_eq!(base_of(&dvc_beside(&b)), v1, "the base moved");
}

#[cfg(unix)]
#[test]
fn a_root_confines_push_and_pull_against_symlinks_at_every_level() {
    use std::os::unix::fs::symlink;
    let e = env();
    let root = e.data.join("dataset");
    let outside = e.data.join("outside");
    let secret = outside.join("private/secret.txt");
    write(&secret, b"not the dataset's\n");
    write(&root.join("real/out/f.txt"), b"x\n");
    symlink(&outside, root.join("linked")).unwrap();
    symlink(root.join("real/out"), root.join("aliased")).unwrap();
    let push_in = |root: &Path, rel: &str| {
        folder::push(
            &e.remote,
            Path::new(rel),
            &PushOptions {
                root: Some(root.to_path_buf()),
                ..opts("k")
            },
        )
    };
    let status_in = |rel: &str| {
        folder::status(
            &e.remote,
            Path::new(rel),
            &PushOptions {
                root: Some(root.clone()),
                ..opts("k")
            },
        )
    };
    let refusal = |err: anyhow::Error| {
        let (path, reason) = refused(&err);
        (path.to_path_buf(), reason.clone())
    };
    let redirected = |at: PathBuf| (at, Refusal::SymlinkedComponent);

    // A directory on the way, and the output itself.
    for (rel, at) in [
        ("linked/private", root.join("linked")),
        ("aliased", root.join("aliased")),
    ] {
        assert_eq!(
            refusal(push_in(&root, rel).unwrap_err()),
            redirected(at.clone())
        );
        assert_eq!(refusal(status_in(rel).unwrap_err()), redirected(at));
    }
    // Its .dvc.
    symlink(&secret, root.join("real/out.dvc")).unwrap();
    assert_eq!(
        refusal(push_in(&root, "real/out").unwrap_err()),
        redirected(root.join("real/out.dvc"))
    );
    std::fs::remove_file(root.join("real/out.dvc")).unwrap();
    // Paths that are not plain names below the root.
    for rel in [
        "../outside/private",
        "./real/out",
        "",
        outside.join("private").to_str().unwrap(),
    ] {
        assert_eq!(
            refusal(push_in(&root, rel).unwrap_err()),
            (PathBuf::from(rel), Refusal::OutsideRoot)
        );
    }
    assert_eq!(
        refusal(push_in(&root, "real/out/f.txt/below").unwrap_err()),
        (root.join("real/out/f.txt"), Refusal::NotADirectory)
    );
    assert!(remote_keys(&e.store).is_empty(), "something was published");
    assert!(!outside.join("private.dvc").exists());

    // The root itself may be a symlink.
    symlink(&root, e.data.join("root-link")).unwrap();
    let pushed = push_in(&e.data.join("root-link"), "real/out").unwrap();
    assert!(
        matches!(pushed.outcome, Pushed::Published { .. }),
        "{:?}",
        pushed.outcome
    );

    let pull_in = |source: PointerSource, into: Option<&str>| {
        folder::pull(
            &e.remote,
            &source,
            &PullOptions {
                root: Some(root.clone()),
                ..pull_opts(into.map(PathBuf::from))
            },
        )
    };
    let latest = || history("k", Selector::Latest);
    assert_eq!(
        refusal(pull_in(latest(), Some("linked/restored")).unwrap_err()),
        redirected(root.join("linked"))
    );
    assert!(!outside.join("restored").exists());
    assert_eq!(
        refusal(pull_in(latest(), Some("aliased")).unwrap_err()),
        redirected(root.join("aliased"))
    );
    symlink(&secret, root.join("restored.dvc")).unwrap();
    assert_eq!(
        refusal(pull_in(latest(), Some("restored")).unwrap_err()),
        redirected(root.join("restored.dvc"))
    );
    assert_eq!(std::fs::read(&secret).unwrap(), b"not the dataset's\n");
    std::fs::remove_file(root.join("restored.dvc")).unwrap();
    // A .dvc source reached through a symlinked directory.
    std::fs::copy(root.join("real/out.dvc"), outside.join("out.dvc")).unwrap();
    assert_eq!(
        refusal(pull_in(PointerSource::File("linked/out.dvc".into()), None).unwrap_err()),
        redirected(root.join("linked"))
    );
    assert!(!outside.join("out").exists());

    pull_in(latest(), Some("restored")).unwrap();
    assert_eq!(tree(&root.join("restored")), tree(&root.join("real/out")));
    std::fs::remove_dir_all(root.join("real/out")).unwrap();
    pull_in(PointerSource::File("real/out.dvc".into()), None).unwrap();
    assert_eq!(tree(&root.join("real/out")), tree(&root.join("restored")));
}

#[cfg(windows)]
#[test]
fn a_root_refuses_a_junction_on_the_way_to_the_output() {
    let e = env();
    let root = e.data.join("dataset");
    let outside = e.data.join("outside");
    write(&outside.join("private/secret.txt"), b"not the dataset's\n");
    std::fs::create_dir_all(&root).unwrap();
    let made = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(root.join("linked"))
        .arg(&outside)
        .status()
        .unwrap();
    assert!(made.success());
    let err = folder::push(
        &e.remote,
        Path::new("linked/private"),
        &PushOptions {
            root: Some(root.clone()),
            ..opts("k")
        },
    )
    .unwrap_err();
    assert_eq!(
        refused(&err),
        (root.join("linked").as_path(), &Refusal::SymlinkedComponent)
    );
    write(&e.data.join("src/f.txt"), b"x\n");
    let _ = folder::push(&e.remote, &e.data.join("src"), &opts("k")).unwrap();
    let err = folder::pull(
        &e.remote,
        &history("k", Selector::Latest),
        &PullOptions {
            root: Some(root.clone()),
            ..pull_opts(Some("linked/restored".into()))
        },
    )
    .unwrap_err();
    assert_eq!(
        refused(&err),
        (root.join("linked").as_path(), &Refusal::SymlinkedComponent)
    );
    assert!(!outside.join("restored").exists());
    assert!(!outside.join("restored.dvc").exists());
}

#[test]
fn a_remote_that_cannot_be_opened_is_typed() {
    let err = Remote::open(&RemoteConfig {
        url: "s3://bucket/prefix".into(),
        endpoint: Some("https://s3.example.invalid".into()),
        region: None,
        credentials: Credentials::Static {
            access_key_id: String::new(),
            secret_access_key: "s".into(),
        },
    })
    .err()
    .expect("must refuse");
    assert!(
        matches!(folder_error(&err), FolderError::CredentialsMissing),
        "{err:#}"
    );

    let e = env();
    let file = e.data.join("a-file");
    write(&file, b"x");
    let url = format!("local://{}", file.join("remote").display());
    let err = Remote::open(&RemoteConfig {
        url: url.clone(),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .err()
    .expect("must refuse");
    assert!(
        matches!(folder_error(&err), FolderError::RemoteUnusable { url: u } if *u == url),
        "{err:#}"
    );
}

/// Put a version on `e`'s remote directly: a first version of `key`
/// holding `content`, as a push would have written it.
fn first_version(e: &Env, key: &str, content: &[u8]) {
    let pointer = DvcPointer {
        output: DvcOutput::File {
            md5: hash_reader(&mut &content[..], HashFunction::Md5).unwrap(),
            size: content.len() as u64,
        },
        path: "f".into(),
        meta: Some(BigstoreMeta::Record {
            parents: Vec::new(),
            writer: "test".into(),
            time: chrono::Utc::now(),
        }),
    };
    let bytes = pointer.to_yaml();
    let id = record_id(bytes.as_bytes());
    write(
        &e.store
            .join(format!("bigstore-history/{key}/root/{id}.dvc")),
        bytes.as_bytes(),
    );
}

#[test]
fn a_history_with_no_head_or_too_many_to_merge_is_typed() {
    let e = env();
    let key = HistoryKey::new("k").unwrap();
    // Two records each naming the other as its parent: no head.
    let (x, y) = (record_id(b"x"), record_id(b"y"));
    write(
        &e.store.join(format!("bigstore-history/k/{y}/{x}.dvc")),
        b"x",
    );
    write(
        &e.store.join(format!("bigstore-history/k/{x}/{y}.dvc")),
        b"y",
    );
    let f = e.data.join("f");
    write(&f, b"local\n");
    let err = folder::status(&e.remote, &f, &opts("k")).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::NoHead { key: k } if *k == key),
        "{err:#}"
    );
    let err = pull_latest(&e, "k", &e.data.join("r"), Overwrite::Refuse).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::NoHead { key: k } if *k == key),
        "{err:#}"
    );

    // Nine first versions: one more head than a version can follow.
    let key = HistoryKey::new("nine").unwrap();
    for i in 0..9 {
        first_version(&e, "nine", format!("version {i}\n").as_bytes());
    }
    let heads = history_log(&e, "nine").unwrap();
    let base = DvcPointer {
        meta: Some(BigstoreMeta::Base(heads[0].id.clone())),
        ..heads[0].pointer.clone()
    };
    std::fs::write(e.data.join("f.dvc"), base.to_yaml()).unwrap();
    let before = remote_keys(&e.store);
    let err = folder::push(
        &e.remote,
        &f,
        &PushOptions {
            resolve: Resolve::Merge,
            ..opts("nine")
        },
    )
    .unwrap_err();
    let FolderError::TooManyHeads {
        key: k,
        heads: h,
        max,
    } = folder_error(&err)
    else {
        panic!("{err:#}")
    };
    assert_eq!((k, h.len(), *max), (&key, 9, 8));
    assert_eq!(remote_keys(&e.store), before, "something was published");
}

#[test]
fn a_catch_up_pull_never_discards_content_the_remote_lacks() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    push_v2(&e, "k", &a);
    // v1's manifest stays, but the object of a file v2 removed is gone.
    let md5 = hash_reader(&mut &b"removed in v2\n"[..], HashFunction::Md5)
        .unwrap()
        .to_string();
    let object = e
        .store
        .join(format!("files/md5/{}/{}", &md5[..2], &md5[2..]));
    std::fs::remove_file(&object).unwrap();

    let before = tree(&b);
    let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
    let gone = b.join("gone.txt");
    assert_eq!(refused(&err), (gone.as_path(), &Refusal::BaseNotOnRemote));
    assert_eq!(tree(&b), before, "nothing written or removed");
}

#[test]
fn a_catch_up_refuses_a_file_deleted_here_but_a_restore_writes_it_again() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    std::fs::remove_file(b.join("keep.txt")).unwrap();
    // Pulling the version B is based on restores what B deleted.
    let r = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap();
    assert_eq!(r.written, 1);
    assert!(b.join("keep.txt").is_file());

    // Catching up never undoes a deletion made here.
    std::fs::remove_file(b.join("keep.txt")).unwrap();
    let v1 = base_of(&dvc_beside(&b));
    push_v2(&e, "k", &a);
    let before = tree(&b);
    let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
    let FolderError::PullConflict { paths } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(paths, &[b.join("keep.txt")]);
    assert_eq!(tree(&b), before, "nothing written or removed");
    assert_eq!(base_of(&dvc_beside(&b)), v1);
}

#[test]
fn a_catch_up_refuses_a_rename_that_changes_only_case() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    // Case-only, in two steps so it works on a case-insensitive disk.
    std::fs::rename(a.join("keep.txt"), a.join("tmp")).unwrap();
    std::fs::rename(a.join("tmp"), a.join("Keep.txt")).unwrap();
    let _ = folder::push(&e.remote, &a, &opts("k")).unwrap();

    let before = tree(&b);
    let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
    assert_eq!(
        refused(&err),
        (
            Path::new("keep.txt"),
            &Refusal::CaseCollision {
                other: "Keep.txt".into()
            }
        )
    );
    assert_eq!(tree(&b), before, "nothing written or removed");
}

#[test]
fn a_catch_up_never_replaces_content_the_remote_lacks_or_has_at_another_size() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    push_v2(&e, "k", &a);
    // "v1\n": the base content of edit.txt and sub/deep.txt, both replaced.
    let md5 = hash_reader(&mut &b"v1\n"[..], HashFunction::Md5)
        .unwrap()
        .to_string();
    let object = e
        .store
        .join(format!("files/md5/{}/{}", &md5[..2], &md5[2..]));
    let replaced = [b.join("edit.txt"), b.join("sub/deep.txt")];
    let before = tree(&b);
    for damage in [Some(&b"truncated"[..]), None] {
        match damage {
            Some(bytes) => std::fs::write(&object, bytes).unwrap(),
            None => std::fs::remove_file(&object).unwrap(),
        }
        let err = pull_latest(&e, "k", &b, Overwrite::IfUnchanged).unwrap_err();
        let (path, reason) = refused(&err);
        assert_eq!(reason, &Refusal::BaseNotOnRemote, "{err:#}");
        assert!(replaced.iter().any(|p| p == path), "{}", path.display());
        assert_eq!(tree(&b), before, "nothing written or removed");
    }
}

#[test]
fn a_catch_up_from_a_dvc_file_refuses_like_refuse() {
    let e = env();
    let (a, b) = two_hosts(&e, "k");
    push_v2(&e, "k", &a);
    // A copy of A's .dvc (at v2) beside B's output, still at v1 content.
    std::fs::copy(dvc_beside(&a), dvc_beside(&b)).unwrap();
    let err = folder::pull(
        &e.remote,
        &PointerSource::File(dvc_beside(&b)),
        &PullOptions {
            overwrite: Overwrite::IfUnchanged,
            ..pull_opts(None)
        },
    )
    .unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::PullConflict { .. }),
        "{err:#}"
    );
}

/// The object for `content` on a local remote, relative to its root.
fn object_key(content: &[u8]) -> String {
    let md5 = hash_reader(&mut &content[..], HashFunction::Md5).unwrap();
    format!("files/md5/{}/{}", md5.prefix(), md5.rest())
}

fn verified(e: &Env, key: &str, version: &RecordId) -> folder::Completeness {
    folder::verify(&e.remote, &HistoryKey::new(key).unwrap(), version).unwrap()
}

fn repair(key: &str) -> PushOptions {
    PushOptions {
        repair: true,
        ..opts(key)
    }
}

#[test]
fn status_without_history_names_no_heads_and_is_not_based() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"v1");
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert!(matches!(s.sync, SyncState::NoHistory), "{:?}", s.sync);
    assert_eq!((s.heads.len(), s.based), (0, false));
    let v1 = folder::push(&e.remote, &f, &opts("k")).unwrap().version;
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert_eq!((&s.heads[..], s.based), (std::slice::from_ref(&v1), true));
    // Changed since: the head is the base, but the output is not it.
    write(&f, b"v2");
    let s = folder::status(&e.remote, &f, &opts("k")).unwrap();
    assert!(matches!(s.sync, SyncState::LocalAhead), "{:?}", s.sync);
    assert_eq!((&s.heads[..], s.based), (std::slice::from_ref(&v1), false));
}

#[test]
fn a_lost_file_object_is_found_by_verify_and_restored_by_repair() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"precious");
    let v1 = folder::push(&e.remote, &f, &opts("k")).unwrap().version;
    let complete = verified(&e, "k", &v1);
    assert_eq!((complete.objects, complete.is_complete()), (1, true));
    let object = object_key(b"precious");
    std::fs::remove_file(e.store.join(&object)).unwrap();

    let lost = verified(&e, "k", &v1);
    assert_eq!(
        (lost.objects, &lost.missing[..]),
        (1, std::slice::from_ref(&object))
    );
    let s = folder::status(&e.remote, &f, &repair("k")).unwrap();
    assert!(
        matches!(s.sync, SyncState::InSync) && s.based,
        "{:?}",
        s.sync
    );
    assert_eq!(s.to_upload, 1);
    let repaired = folder::push(&e.remote, &f, &repair("k")).unwrap();
    assert_eq!(
        (repaired.outcome, &repaired.version, repaired.uploaded),
        (Pushed::AlreadyLatest, &v1, 1)
    );
    assert!(verified(&e, "k", &v1).is_complete());
    assert_eq!(history_log(&e, "k").unwrap().len(), 1);
}

#[test]
fn objects_lost_behind_a_manifest_are_found_by_verify_and_restored_by_repair() {
    let e = env();
    let w = writer_dir(&e);
    let pushed = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let v1 = pushed.version;
    let DvcOutput::Dir { manifest, .. } = &pushed.pointer.output else {
        panic!("a directory")
    };
    let manifest = format!("files/md5/{}/{}.dir", manifest.prefix(), manifest.rest());
    let complete = verified(&e, KEY, &v1);
    assert_eq!((complete.objects, complete.is_complete()), (4, true));
    // The keys a version needs: here every store file, the record included.
    let store_files: Vec<String> = walkdir::WalkDir::new(&e.store)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|f| f.file_type().is_file())
        .map(|f| {
            let rel = f.path().strip_prefix(&e.store).unwrap();
            rel.to_str().unwrap().replace('\\', "/")
        })
        .filter(|k| bigstore::folder::layout::kind(k) != bigstore::folder::layout::Kind::Other)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(store_files.len(), 6);
    assert_eq!(complete.keys, store_files);
    let object = object_key(b"PAR1 geometry");
    std::fs::remove_file(e.store.join(&object)).unwrap();

    // The manifest is still there, so a plain push trusts that its objects
    // are, and would upload nothing.
    let s = folder::status(&e.remote, &w, &opts(KEY)).unwrap();
    assert!(
        matches!(s.sync, SyncState::InSync) && s.based,
        "{:?}",
        s.sync
    );
    assert_eq!(s.to_upload, 0);
    let lost = verified(&e, KEY, &v1);
    assert_eq!(
        (lost.objects, &lost.missing[..]),
        (4, std::slice::from_ref(&object))
    );
    assert_eq!(
        folder::status(&e.remote, &w, &repair(KEY))
            .unwrap()
            .to_upload,
        1
    );
    let repaired = folder::push(&e.remote, &w, &repair(KEY)).unwrap();
    assert_eq!(
        (repaired.outcome, &repaired.version, repaired.uploaded),
        (Pushed::AlreadyLatest, &v1, 1)
    );
    assert!(verified(&e, KEY, &v1).is_complete());

    // A lost manifest: the objects behind it are unknown until it is back.
    std::fs::remove_file(e.store.join(&manifest)).unwrap();
    let lost = verified(&e, KEY, &v1);
    assert_eq!(
        (lost.objects, &lost.missing[..]),
        (0, std::slice::from_ref(&manifest))
    );
    let record = store_files
        .iter()
        .find(|k| k.starts_with("bigstore-history/"))
        .unwrap();
    assert_eq!(lost.keys, [record.clone(), manifest.clone()]);
    let repaired = folder::push(&e.remote, &w, &repair(KEY)).unwrap();
    assert_eq!(
        (repaired.outcome, repaired.uploaded),
        (Pushed::AlreadyLatest, 0)
    );
    assert!(verified(&e, KEY, &v1).is_complete());
    // One whose bytes are not the manifest named is as good as lost.
    std::fs::write(e.store.join(&manifest), b"[]").unwrap();
    assert_eq!(verified(&e, KEY, &v1).missing, [manifest]);
    assert_eq!(history_log(&e, KEY).unwrap().len(), 1);
}

#[test]
fn verify_of_a_version_not_in_the_history_is_no_such_version() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"v1");
    let other = folder::push(&e.remote, &f, &opts("other")).unwrap().version;
    let err = folder::verify(&e.remote, &HistoryKey::new("k").unwrap(), &other).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::NoSuchVersion),
        "{err:#}"
    );
}

#[test]
fn every_file_push_writes_is_a_store_file_that_verifies() {
    let e = env();
    let w = writer_dir(&e);
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    write(
        &w.join("site=s1/date=2026-09-01/src_01/labels.jsonl"),
        b"{}\n",
    );
    let _ = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let mut kinds = Vec::new();
    for (key, bytes) in tree(&e.store) {
        let kind = layout::kind(&key);
        assert_ne!(kind, layout::Kind::Other, "{key}");
        layout::verify(&key, &bytes).unwrap_or_else(|err| panic!("{key}: {err:#}"));
        kinds.push(kind);
        // One byte more is not that file.
        let mut damaged = bytes.clone();
        damaged.push(b'\n');
        let err = layout::verify(&key, &damaged).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::Integrity { key: k } if *k == key),
            "{err:#}"
        );
    }
    kinds.sort();
    kinds.dedup();
    assert_eq!(
        kinds,
        [
            layout::Kind::Object,
            layout::Kind::Manifest,
            layout::Kind::Record
        ]
    );
}

#[test]
fn a_record_under_parents_its_bytes_do_not_name_does_not_verify() {
    let e = env();
    let f = e.data.join("f");
    write(&f, b"v1");
    let _ = folder::push(&e.remote, &f, &opts("k")).unwrap();
    let (key, bytes) = tree(&e.store)
        .into_iter()
        .find(|(k, _)| k.starts_with("bigstore-history/"))
        .unwrap();
    layout::verify(&key, &bytes).unwrap();
    let moved = key.replace("/root/", &format!("/{}/", "ab".repeat(16)));
    let err = layout::verify(&moved, &bytes).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Integrity { .. }),
        "{err:#}"
    );
    let err = layout::verify("files/md5/ab/cd", b"").unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::InvalidStoreKey { .. }),
        "{err:#}"
    );
}

// ──────────────────────────────────────────────────
// Single storage: Link::Place
// ──────────────────────────────────────────────────

const MIB: usize = 1 << 20;

fn place_opts(key: &str) -> PushOptions {
    PushOptions {
        link: Link::Place {
            min_bytes: MIB as u64,
        },
        ..opts(key)
    }
}

/// Where the object holding `bytes` lives in a local store.
#[cfg(unix)]
fn object_file(store: &Path, bytes: &[u8]) -> PathBuf {
    let md5 = hash_reader(&mut &bytes[..], HashFunction::Md5).unwrap();
    let md5 = md5.to_string();
    store.join("files/md5").join(&md5[..2]).join(&md5[2..])
}

/// Whether `a` and `b` are one file (a hard link), by inode.
#[cfg(unix)]
fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let (a, b) = (std::fs::metadata(a).unwrap(), std::fs::metadata(b).unwrap());
    (a.dev(), a.ino()) == (b.dev(), b.ino())
}

/// A run directory: one table of 2 MiB (linked) and a small product file
/// (copied).
fn run_dir(e: &Env) -> (PathBuf, Vec<u8>, Vec<u8>) {
    let run = e.data.join("pipeline_run=1");
    let table: Vec<u8> = (0..2 * MIB).map(|i| (i % 251) as u8).collect();
    let product = b"{\"tables\":[\"tracks\"]}\n".to_vec();
    write(&run.join("tracks/part-0.parquet"), &table);
    write(&run.join("product.json"), &product);
    (run, table, product)
}

#[cfg(unix)]
#[test]
fn place_links_large_files_into_the_store_and_back_and_copies_small_ones() {
    let e = env();
    let (run, table, product) = run_dir(&e);
    let report = folder::push(&e.remote, &run, &place_opts("runs/1")).unwrap();
    assert!(matches!(report.outcome, Pushed::Published { .. }));
    assert_eq!((report.uploaded, report.linked), (2, 1));
    let (table_obj, product_obj) = (
        object_file(&e.store, &table),
        object_file(&e.store, &product),
    );
    assert_eq!(std::fs::read(&table_obj).unwrap(), table);
    assert_eq!(std::fs::read(&product_obj).unwrap(), product);
    let working = run.join("tracks/part-0.parquet");
    // A reflink is a file of its own; a hard link is the working file.
    assert_eq!(same_file(&working, &table_obj), report.cloned == 0);
    assert!(!same_file(&run.join("product.json"), &product_obj));
    // No temp file is left beside the objects.
    assert!(remote_keys(&e.store).iter().all(|k| !k.contains('#')));

    let status = folder::status(&e.remote, &run, &place_opts("runs/1")).unwrap();
    assert!(
        matches!(status.sync, SyncState::InSync),
        "{:?}",
        status.sync
    );
    assert!(status.based);
    let again = folder::push(&e.remote, &run, &place_opts("runs/1")).unwrap();
    assert_eq!(again.outcome, Pushed::AlreadyLatest);
    assert_eq!((again.uploaded, again.linked), (0, 0));

    // Pull it elsewhere the same way.
    let into = e.data.join("restored");
    let pulled = folder::pull(
        &e.remote,
        &history("runs/1", Selector::Latest),
        &PullOptions {
            link: Link::Place {
                min_bytes: MIB as u64,
            },
            ..pull_opts(Some(into.clone()))
        },
    )
    .unwrap();
    assert_eq!((pulled.written, pulled.linked), (2, 1));
    assert_eq!(tree(&into), tree(&run));
    let restored = into.join("tracks/part-0.parquet");
    assert_eq!(same_file(&restored, &table_obj), pulled.cloned == 0);
    assert!(!same_file(&into.join("product.json"), &product_obj));
    let leftovers: Vec<_> = tree(&into)
        .into_iter()
        .filter(|(k, _)| k.contains(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    // Re-place a differing file, as a heal does: Force, Place. The bad file
    // is replaced by temp + rename (never written through, which would
    // write a hard-linked store object too).
    let staged = e.data.join("bad.tmp");
    std::fs::write(&staged, vec![9u8; table.len()]).unwrap();
    std::fs::rename(&staged, &restored).unwrap();
    let replaced = folder::pull(
        &e.remote,
        &history("runs/1", Selector::Latest),
        &PullOptions {
            overwrite: Overwrite::Force,
            link: Link::Place {
                min_bytes: MIB as u64,
            },
            ..pull_opts(Some(into.clone()))
        },
    )
    .unwrap();
    assert_eq!((replaced.written, replaced.linked), (1, 1));
    assert_eq!(std::fs::read(&restored).unwrap(), table);
    assert_eq!(std::fs::read(&table_obj).unwrap(), table);
    assert_eq!(same_file(&restored, &table_obj), replaced.cloned == 0);
}

#[cfg(unix)]
#[test]
fn a_symlink_in_a_placed_output_is_copied_never_linked() {
    // A link to a file outside the run (say a cache rewritten in place):
    // its bytes are backed up, but the store never shares its blocks.
    let e = env();
    let outside = e.data.join("cache/x.parquet");
    let table: Vec<u8> = (0..2 * MIB).map(|i| (i % 241) as u8).collect();
    write(&outside, &table);
    let run = e.data.join("pipeline_run=2");
    std::fs::create_dir_all(&run).unwrap();
    std::os::unix::fs::symlink(&outside, run.join("link.parquet")).unwrap();
    let report = folder::push(&e.remote, &run, &place_opts("runs/2")).unwrap();
    assert_eq!((report.uploaded, report.linked, report.cloned), (1, 0, 0));
    let object = object_file(&e.store, &table);
    assert_eq!(std::fs::read(&object).unwrap(), table);
    assert!(!same_file(&object, &outside));
}

#[test]
fn a_placed_push_fails_when_a_file_keeps_changing_while_hashed() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let e = env();
    let file = e.data.join("growing.bin");
    write(&file, &vec![0u8; 2 * MIB]);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (file, stop) = (file.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&file)
                .unwrap();
            while !stop.load(Ordering::Relaxed) {
                std::io::Write::write_all(&mut f, b"x").unwrap();
            }
        })
    };
    let result = folder::push(&e.remote, &file, &place_opts("ds/growing"));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let err = result.unwrap_err();
    let FolderError::OutputChanged { detail } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(
        *detail,
        format!("{} changed while being hashed", file.display())
    );
    assert!(!e.data.join("growing.bin.dvc").exists());
    assert!(remote_keys(&e.store).is_empty());
}

#[test]
fn place_needs_a_local_remote() {
    let e = env();
    let (run, _, _) = run_dir(&e);
    // A `.dvc` to pull from, pushed to the local remote.
    let _ = folder::push(&e.remote, &run, &opts("runs/1")).unwrap();
    let s3 = Remote::open(&RemoteConfig {
        url: "s3://bucket/dvc".into(),
        endpoint: Some("http://127.0.0.1:9".into()),
        region: Some("ap-southeast-2".into()),
        credentials: Credentials::Static {
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
        },
    })
    .unwrap();
    let errs = [
        folder::push(&s3, &run, &place_opts("runs/1")).map(drop),
        folder::status(&s3, &run, &place_opts("runs/1")).map(drop),
        folder::pull(
            &s3,
            &PointerSource::File(e.data.join("pipeline_run=1.dvc")),
            &PullOptions {
                link: Link::Place { min_bytes: 0 },
                ..pull_opts(Some(run.clone()))
            },
        )
        .map(drop),
    ];
    for result in errs {
        let err = result.unwrap_err();
        let (path, reason) = refused(&err);
        assert_eq!(*reason, Refusal::PlaceNeedsLocalRemote, "{err:#}");
        assert_eq!(path, run);
    }
}
