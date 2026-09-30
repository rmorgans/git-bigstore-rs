//! `bigstore::folder` as a library consumer uses it: plain folders, a
//! `local://` remote, no git.

use bigstore::dvc::{DvcOutput, DvcPointer, Manifest, ManifestEntry};
use bigstore::folder::{
    self, CancelToken, Credentials, Error as FolderError, Excludes, HistoryKey, Overwrite, Phase,
    PointerSource, Progress, ProgressEvent, PullOptions, PushOptions, Refusal, Remote,
    RemoteConfig, Selector, SyncState,
};
use bigstore::hash::{hash_file, hash_reader};
use bigstore::types::{HashFunction, Hexdigest, ManifestPath};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    assert!(first.history_record.is_some());
    assert_eq!(
        first.pointer_path,
        w.parent().unwrap().join("host=ricks-macbook-pro.dvc")
    );
    let pointer_bytes = std::fs::read(&first.pointer_path).unwrap();

    let again = folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(again.uploaded, 0);
    assert!(again.history_record.is_none(), "no-op push grew history");
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
    let versions = folder::log(&e.remote, &key, 4).unwrap();
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
    let old = versions[0].id().to_string();
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
    // Byte-identical to `dvc add store.toml` (DVC 3.67.1).
    assert_eq!(
        std::fs::read_to_string(e.data.join("store.toml.dvc")).unwrap(),
        "outs:\n- md5: 3253b41059cac6e987c5a5e9233ea5d0\n  size: 6\n  hash: md5\n  path: store.toml\n"
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
fn calling_from_inside_a_tokio_runtime_is_an_error_not_a_panic() {
    let e = env();
    let w = writer_dir(&e);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let err = rt
        .block_on(async { folder::push(&e.remote, &w, &opts(KEY)) })
        .unwrap_err();
    assert!(format!("{err}").contains("tokio runtime"), "{err}");
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
    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    assert_eq!(std::fs::read_to_string(&first.pointer_path).unwrap(), crlf);
}

/// Put a history record on `e`'s remote directly, as another host's push
/// would have: a single-file pointer to `md5` at `time` (record format).
fn write_record(e: &Env, key: &str, time: &str, md5: &str) {
    let pointer = DvcPointer {
        output: DvcOutput::File {
            md5: Hexdigest::new(md5, HashFunction::Md5).unwrap(),
            size: 1,
        },
        path: "f".into(),
    };
    write(
        &e.store
            .join(format!("bigstore-history/{key}/{time}-{md5}.dvc")),
        pointer.to_yaml().as_bytes(),
    );
}

#[test]
fn an_ambiguous_version_id_is_refused_and_lists_the_candidates() {
    let e = env();
    let a = format!("deadbeef{}", "0".repeat(24));
    let b = format!("deadbeef{}", "1".repeat(24));
    write_record(&e, "k", "20260901T000000.000000000Z", &a);
    write_record(&e, "k", "20260902T000000.000000000Z", &b);
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
    let err = pull("DEADBEEF").unwrap_err();
    let FolderError::AmbiguousId { prefix, candidates } = folder_error(&err) else {
        panic!("{err:#}")
    };
    assert_eq!(prefix, "deadbeef");
    let ids: Vec<String> = candidates.iter().map(|r| r.id().to_string()).collect();
    assert_eq!(ids, [a.clone(), b.clone()]);
    let msg = format!("{err:#}");
    assert!(msg.contains("ambiguous"), "{msg}");
    assert!(msg.contains(&a) && msg.contains(&b), "{msg}");
    assert!(
        msg.contains("2026-09-01") && msg.contains("2026-09-02"),
        "{msg}"
    );
    // A longer prefix picks one; the object is absent, so the fetch fails.
    let msg = format!("{:#}", pull(&b[..9]).unwrap_err());
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
    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    std::fs::write(&labels, b"{\"t\":4}\n").unwrap();
    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let log = folder::log(&e.remote, &HistoryKey::new(KEY).unwrap(), 4).unwrap();
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
        folder::log(&e.remote, &HistoryKey::new(key).unwrap(), 4)
            .unwrap()
            .iter()
            .map(|r| r.id().to_string())
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
    let a = w.join("site=s1/date=2026-09-01/src_01/labels.jsonl");
    let b = w.join("site=s1/date=2026-09-02/src_02/labels.jsonl");
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
    assert!(shown.contains(&a.display().to_string()) && shown.contains(&b.display().to_string()));
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
    let err = folder::log(&e.remote, &HistoryKey::new("k").unwrap(), 4).unwrap_err();
    assert!(format!("{err:#}").contains(not_utf8), "{err:#}");

    std::fs::remove_file(dir.join(not_utf8)).unwrap();
    let not_a_pointer = "20260901T000000.000000000Z-b.dvc";
    write(&dir.join(not_a_pointer), b"outs: []\n");
    let err = folder::log(&e.remote, &HistoryKey::new("k").unwrap(), 4).unwrap_err();
    assert!(format!("{err:#}").contains(not_a_pointer), "{err:#}");
}

#[test]
fn records_pushed_in_the_same_nanosecond_have_a_stable_latest() {
    let e = env();
    let time = "20260901T000000.000000000Z";
    let (a, b) = ("a".repeat(32), "b".repeat(32));
    write_record(&e, "k", time, &b);
    write_record(&e, "k", time, &a);
    let log = folder::log(&e.remote, &HistoryKey::new("k").unwrap(), 4).unwrap();
    let ids: Vec<String> = log.iter().map(|r| r.id().to_string()).collect();
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
    assert!(again.history_record.is_none(), "junk made a new version");
    assert_eq!(std::fs::read(&again.pointer_path).unwrap(), pointer);
    let versions = folder::log(&e.remote, &HistoryKey::new(KEY).unwrap(), 4).unwrap();
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
    for at in [Selector::Latest, Selector::Id(named[..8].into())] {
        let err = folder::pull(
            &e.remote,
            &history("k", at),
            &pull_opts(Some(e.data.join("f"))),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("bad record") && msg.contains(&held), "{msg}");
    }
    let err = folder::log(&e.remote, &HistoryKey::new("k").unwrap(), 4).unwrap_err();
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
    assert_eq!(s.pointer, pushed.pointer, "status predicts the pointer");
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
    assert!(matches!(s.sync, SyncState::Diverged { .. }), "{:?}", s.sync);
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

    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
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
    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
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
    folder::push(
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
    folder::push(&e.remote, &out, &opts("ds/out")).unwrap();
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
    folder::push(&e.remote, &w, &opts(KEY)).unwrap();
    let err =
        folder::pull(&e.remote, &history(KEY, Selector::Latest), &pull_opts(None)).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::DestinationRequired),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("into"), "{err:#}");
}
