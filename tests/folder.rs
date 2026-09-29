//! `bigstore::folder` as a library consumer uses it: plain folders, a
//! `local://` remote, no git.

use bigstore::dvc::{DvcOutput, DvcPointer, Manifest, ManifestEntry};
use bigstore::folder::{
    self, Credentials, HistoryKey, Overwrite, PointerSource, PullConflict, PullOptions,
    PushOptions, Remote, RemoteConfig, Selector,
};
use bigstore::hash::{hash_file, hash_reader};
use bigstore::types::{HashFunction, Hexdigest, ManifestPath};
use std::path::{Path, PathBuf};

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
        history: HistoryKey::new(key).unwrap(),
        jobs: 4,
    }
}

fn pull_opts(into: Option<PathBuf>) -> PullOptions {
    PullOptions {
        into,
        overwrite: Overwrite::Refuse,
        jobs: 4,
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
    let versions = folder::log(&e.remote, &key).unwrap();
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
    let conflict = err.downcast_ref::<PullConflict>().expect("typed conflict");
    assert_eq!(conflict.paths, std::slice::from_ref(&labels));
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
    assert!(folder::push(&e.remote, &w, &opts(KEY)).is_err());
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
    let msg = format!("{:#}", pull("DEADBEEF").unwrap_err());
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
