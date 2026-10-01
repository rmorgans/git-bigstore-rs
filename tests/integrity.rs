//! `bigstore::folder::integrity`: scrub, replace and quarantine on a local
//! store (`LocalFileSystem`, as `local://` builds it) and on an in-memory
//! store whose ETags are what a test says.

use bigstore::backend::store::build_local_store;
use bigstore::folder::integrity::{self, Quarantined, Replaced, ScrubOptions, ScrubReport, Source};
use bigstore::folder::layout::{self, Kind};
use bigstore::folder::Error as FolderError;
use futures::stream::BoxStream;
use md5::Digest as _;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SCOPE: &str = "ST032_Warrawoona/BeatonsCreek_dataset1_September2026";

fn md5_hex(bytes: &[u8]) -> String {
    hex::encode(md5::Md5::digest(bytes))
}

/// An object holding `bytes`: its key.
fn object_key(bytes: &[u8]) -> String {
    let md5 = md5_hex(bytes);
    format!("files/md5/{}/{}", &md5[..2], &md5[2..])
}

/// A history record of `SCOPE/<output>` with no parents, valid for its
/// name: its key and bytes.
fn record(output: &str) -> (String, Vec<u8>) {
    let bytes = b"outs:\n- md5: d41d8cd98f00b204e9800998ecf8427e\n  size: 1\n  hash: md5\n  \
        path: f\nmeta:\n  bigstore:\n    parents: []\n    writer: test\n    \
        time: 2026-10-01T00:00:00.000000000Z\n"
        .to_vec();
    let id = {
        use sha2::Digest as _;
        hex::encode(&sha2::Sha256::digest(&bytes)[..16])
    };
    let key = format!("bigstore-history/{SCOPE}/{output}/root/{id}.dvc");
    layout::verify(&key, &bytes).expect("a valid record");
    (key, bytes)
}

fn path_of(root: &Path, key: &str) -> std::path::PathBuf {
    let mut path = root.to_path_buf();
    path.extend(key.split('/'));
    path
}

fn write(root: &Path, key: &str, bytes: &[u8]) {
    let path = path_of(root, key);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn read(root: &Path, key: &str) -> Vec<u8> {
    std::fs::read(path_of(root, key)).unwrap()
}

/// Every file under `root`, `/`-separated and relative, with its bytes.
fn files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| {
            let rel = e.path().strip_prefix(root).unwrap();
            let key = rel.to_str().unwrap().replace('\\', "/");
            (key, std::fs::read(e.path()).unwrap())
        })
        .collect()
}

fn quarantined(root: &Path) -> BTreeMap<String, Vec<u8>> {
    files(root)
        .into_iter()
        .filter(|(k, _)| k.starts_with("quarantine/"))
        .collect()
}

fn local(root: &Path) -> Box<dyn ObjectStore> {
    build_local_store(root.to_str().unwrap()).unwrap()
}

fn scrub(store: &dyn ObjectStore, deep: bool) -> ScrubReport {
    let opts = ScrubOptions {
        deep,
        ..ScrubOptions::default()
    };
    integrity::scrub(store, &opts).unwrap()
}

#[track_caller]
fn folder_error(err: &anyhow::Error) -> &FolderError {
    err.downcast_ref::<FolderError>()
        .unwrap_or_else(|| panic!("not a folder::Error: {err:#}"))
}

/// A store with two good objects and a good record; returns their keys
/// and bytes.
fn seeded(root: &Path) -> Vec<(String, Vec<u8>)> {
    let files = vec![
        (object_key(b"labels\n"), b"labels\n".to_vec()),
        (object_key(&[7u8; 4096]), vec![7u8; 4096]),
        record("annotations"),
    ];
    for (key, bytes) in &files {
        write(root, key, bytes);
    }
    files
}

#[test]
fn a_truncated_object_and_a_corrupt_record_are_found_and_healed_by_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let store = local(&root);
    let report = scrub(&*store, false);
    assert_eq!(report.checked, 3);
    assert!(report.is_clean());

    let (object, good_object) = seeded[1].clone();
    let (rec, good_record) = seeded[2].clone();
    write(&root, &object, &good_object[..100]);
    let mut corrupt = good_record.clone();
    corrupt[0] = b'X';
    write(&root, &rec, &corrupt);

    let report = scrub(&*store, false);
    assert_eq!(report.checked, 3);
    assert_eq!(report.damaged, {
        let mut both = vec![object.clone(), rec.clone()];
        both.sort();
        both
    });
    assert!(report.unreadable.is_empty());

    // The object from a file (a working copy), the record from bytes.
    let source = tmp.path().join("working-copy");
    std::fs::write(&source, &good_object).unwrap();
    let healed = integrity::replace(&*store, &object, Source::File(source)).unwrap();
    let Replaced::Replaced {
        quarantined: q_object,
    } = healed
    else {
        panic!("{healed:?}")
    };
    let healed = integrity::replace(&*store, &rec, Source::Bytes(good_record.clone())).unwrap();
    let Replaced::Replaced {
        quarantined: q_record,
    } = healed
    else {
        panic!("{healed:?}")
    };

    assert_eq!(read(&root, &object), good_object);
    assert_eq!(read(&root, &rec), good_record);
    assert!(q_object.starts_with(&format!("quarantine/{object}.")));
    assert!(q_record.starts_with(&format!("quarantine/{rec}.")));
    assert_eq!(read(&root, &q_object), &good_object[..100]);
    assert_eq!(read(&root, &q_record), corrupt);
    assert_eq!(layout::kind(&q_record), Kind::Other);
    // Quarantine is not scrubbed: the store is clean, three files checked.
    assert_eq!(scrub(&*store, true).checked, 3);
    assert!(scrub(&*store, true).is_clean());
    assert!(files(&root).keys().all(|k| !k.contains('#')));
}

#[test]
fn bytes_that_are_not_what_the_key_names_are_refused_before_anything_is_written() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let (object, good) = seeded[0].clone();
    write(&root, &object, b"damaged");
    let before = files(&root);
    let store = local(&root);
    for source in [
        Source::Bytes(b"wrong".to_vec()),
        Source::File(tmp.path().join("missing-file")),
    ] {
        let err = integrity::replace(&*store, &object, source).unwrap_err();
        assert!(!matches!(
            err.downcast_ref::<FolderError>(),
            Some(FolderError::WriteUnverified { .. })
        ));
    }
    let err = integrity::replace(&*store, &object, Source::Bytes(b"wrong".to_vec())).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::Integrity { .. }));
    let err = integrity::replace(&*store, ".lock", Source::Bytes(good)).unwrap_err();
    assert!(matches!(
        folder_error(&err),
        FolderError::InvalidStoreKey { .. }
    ));
    assert_eq!(files(&root), before);
}

#[test]
fn a_large_object_is_replaced_by_a_checked_multipart_upload() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let big: Vec<u8> = (0..(11u32 << 20)).map(|i| (i % 251) as u8).collect();
    let key = object_key(&big);
    write(&root, &key, &big[..big.len() - 1]);
    let store = local(&root);
    assert_eq!(scrub(&*store, false).damaged, vec![key.clone()]);

    // A file that is not the object is refused, the damaged copy kept.
    let wrong = tmp.path().join("wrong");
    let mut other = big.clone();
    other[5 << 20] ^= 1;
    std::fs::write(&wrong, &other).unwrap();
    let err = integrity::replace(&*store, &key, Source::File(wrong)).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::Integrity { .. }));
    assert_eq!(read(&root, &key).len(), big.len() - 1);

    let source = tmp.path().join("good");
    std::fs::write(&source, &big).unwrap();
    let healed = integrity::replace(&*store, &key, Source::File(source)).unwrap();
    assert!(matches!(healed, Replaced::Replaced { .. }), "{healed:?}");
    assert!(read(&root, &key) == big);
    assert!(scrub(&*store, true).is_clean());
    assert!(files(&root).keys().all(|k| !k.contains('#')));
}

#[test]
fn quarantine_in_place_moves_a_damaged_object_aside_and_refuses_a_record() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let store = local(&root);
    let (object, good) = seeded[0].clone();
    let manifest = format!("{}.dir", object_key(b"[]"));

    // Good, absent: nothing moves.
    assert_eq!(
        integrity::quarantine(&*store, &object).unwrap(),
        Quarantined::Good
    );
    assert_eq!(
        integrity::quarantine(&*store, &manifest).unwrap(),
        Quarantined::Absent
    );
    assert!(quarantined(&root).is_empty());

    write(&root, &object, b"damaged");
    let moved = integrity::quarantine(&*store, &object).unwrap();
    let Quarantined::Moved { quarantined: to } = moved else {
        panic!("{moved:?}")
    };
    assert!(!path_of(&root, &object).exists());
    assert_eq!(read(&root, &to), b"damaged");

    // A record, damaged or not, is never made absent.
    let (rec, good_record) = seeded[2].clone();
    write(&root, &rec, b"damaged record");
    let err = integrity::quarantine(&*store, &rec).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::RecordKept { key } if *key == rec),
        "{err:#}"
    );
    assert_eq!(read(&root, &rec), b"damaged record");
    // Only replace heals it.
    integrity::replace(&*store, &rec, Source::Bytes(good_record)).unwrap();
    // The object's name is absent: replace places it.
    assert_eq!(
        integrity::replace(&*store, &object, Source::Bytes(good)).unwrap(),
        Replaced::Placed
    );
    assert!(scrub(&*store, true).is_clean());
}

#[cfg(unix)]
#[test]
fn an_unreadable_file_is_reported_and_never_mutated() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let (object, good) = seeded[0].clone();
    let (rec, good_record) = seeded[2].clone();
    write(&root, &object, b"damaged");
    write(&root, &rec, b"damaged record");
    for key in [&object, &rec] {
        std::fs::set_permissions(path_of(&root, key), std::fs::Permissions::from_mode(0o000))
            .unwrap();
    }
    if std::fs::read(path_of(&root, &object)).is_ok() {
        eprintln!("skipped: permissions do not stop this user (root?)");
        return;
    }
    let store = local(&root);
    let report = scrub(&*store, false);
    assert_eq!(report.checked, 3);
    assert!(report.damaged.is_empty());
    let keys: Vec<&str> = report.unreadable.iter().map(|u| u.key.as_str()).collect();
    let mut want = vec![object.as_str(), rec.as_str()];
    want.sort();
    assert_eq!(keys, want);
    assert!(report
        .unreadable
        .iter()
        .all(|u| u.reason.to_lowercase().contains("permission denied")));

    let err = integrity::replace(&*store, &object, Source::Bytes(good)).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Unreadable { key, .. } if *key == object),
        "{err:#}"
    );
    let err = integrity::replace(&*store, &rec, Source::Bytes(good_record)).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::Unreadable { .. }));
    let err = integrity::quarantine(&*store, &object).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::Unreadable { .. }));

    for key in [&object, &rec] {
        std::fs::set_permissions(path_of(&root, key), std::fs::Permissions::from_mode(0o644))
            .unwrap();
    }
    assert_eq!(read(&root, &object), b"damaged");
    assert_eq!(read(&root, &rec), b"damaged record");
    assert!(quarantined(&root).is_empty());
}

#[test]
fn a_copy_that_became_good_between_scrub_and_heal_is_healed_by_other() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let (rec, good_record) = seeded[2].clone();
    write(&root, &rec, b"damaged record");
    let store = local(&root);
    assert_eq!(scrub(&*store, false).damaged, vec![rec.clone()]);
    // Someone else heals it.
    write(&root, &rec, &good_record);
    let before = files(&root);
    assert_eq!(
        integrity::replace(&*store, &rec, Source::Bytes(good_record)).unwrap(),
        Replaced::HealedByOther
    );
    assert_eq!(files(&root), before);
}

#[test]
fn concurrent_replaces_of_one_key_converge_on_good_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let (object, good) = seeded[1].clone();
    let (rec, good_record) = seeded[2].clone();
    write(&root, &object, b"damaged");
    write(&root, &rec, b"damaged record");
    let store: Arc<dyn ObjectStore> = local(&root).into();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let results = rt.block_on(async {
        let mut tasks = Vec::new();
        for i in 0..16 {
            let store = Arc::clone(&store);
            let (key, bytes) = match i % 2 {
                0 => (object.clone(), good.clone()),
                _ => (rec.clone(), good_record.clone()),
            };
            tasks.push(tokio::spawn(async move {
                integrity::replace_async(&*store, &key, Source::Bytes(bytes)).await
            }));
        }
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        results
    });
    for result in results {
        let replaced = result.unwrap();
        assert!(
            matches!(
                replaced,
                Replaced::Replaced { .. } | Replaced::HealedByOther
            ),
            "{replaced:?}"
        );
    }
    assert_eq!(read(&root, &object), good);
    assert_eq!(read(&root, &rec), good_record);
    // Whatever was quarantined is the damaged bytes or good ones, never
    // anything else.
    for (key, bytes) in quarantined(&root) {
        assert!(
            [&b"damaged"[..], b"damaged record", &good, &good_record].contains(&&bytes[..]),
            "{key}"
        );
    }
    let temps: Vec<String> = files(&root)
        .into_keys()
        .filter(|k| k.contains('#'))
        .collect();
    assert!(temps.is_empty(), "{temps:?}");
    assert!(scrub(&*store, true).is_clean());
}

#[test]
fn quarantine_temp_and_other_entries_are_never_scrubbed() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let seeded = seeded(&root);
    let (object, _) = seeded[0].clone();
    for other in [
        format!("quarantine/{object}.20261001T000000.000000000Z"),
        format!("{object}#a1b2c3d4"),
        format!("{object}#7"),
        format!("{object}.partial"),
        ".lock".to_string(),
        "files/md5/.DS_Store".to_string(),
    ] {
        assert_eq!(layout::kind(&other), Kind::Other, "{other}");
        write(&root, &other, b"not what any name says");
    }
    let store = local(&root);
    for deep in [false, true] {
        let report = scrub(&*store, deep);
        assert_eq!(report.checked, 3);
        assert!(report.is_clean(), "{report:?}");
    }
}

// ──────────────────────────────────────────────────
// A store whose ETags a test chooses
// ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Etags {
    /// The md5 of what it stores, as S3 gives for a single-part PUT.
    Md5,
    /// None at all.
    Missing,
    /// One that matches nothing.
    Mismatch,
}

/// `InMemory` with ETags per [`Etags`], counting reads; with `corrupt`, it
/// stores every PUT with its first byte flipped; with `short`, every read
/// ends half way through, the size reported unchanged.
#[derive(Debug)]
struct EtagStore {
    inner: InMemory,
    etags: Etags,
    corrupt: bool,
    short: bool,
    md5s: Mutex<HashMap<StorePath, String>>,
    gets: AtomicUsize,
}

impl EtagStore {
    fn new(etags: Etags) -> Self {
        Self {
            inner: InMemory::new(),
            etags,
            corrupt: false,
            short: false,
            md5s: Mutex::default(),
            gets: AtomicUsize::new(0),
        }
    }

    fn etag(&self, location: &StorePath) -> Option<String> {
        match self.etags {
            Etags::Md5 => Some(format!(
                "\"{}\"",
                self.md5s.lock().unwrap().get(location).cloned()?
            )),
            Etags::Missing => None,
            Etags::Mismatch => Some("\"0123456789abcdef0123456789abcdef\"".into()),
        }
    }

    fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for EtagStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EtagStore({:?})", self.etags)
    }
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = object_store::Result<T>> + Send + 'a>>;

impl ObjectStore for EtagStore {
    fn put_opts<'s, 'l, 'a>(
        &'s self,
        location: &'l StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> BoxFut<'a, PutResult>
    where
        's: 'a,
        'l: 'a,
        Self: 'a,
    {
        Box::pin(async move {
            let mut bytes: Vec<u8> = payload.iter().flat_map(|b| b.to_vec()).collect();
            if self.corrupt && !bytes.is_empty() {
                bytes[0] ^= 0xff;
            }
            self.md5s
                .lock()
                .unwrap()
                .insert(location.clone(), md5_hex(&bytes));
            let mut put = self
                .inner
                .put_opts(location, PutPayload::from(bytes), opts)
                .await?;
            put.e_tag = self.etag(location);
            Ok(put)
        })
    }

    fn put_multipart_opts<'s, 'l, 'a>(
        &'s self,
        location: &'l StorePath,
        opts: PutMultipartOptions,
    ) -> BoxFut<'a, Box<dyn MultipartUpload>>
    where
        's: 'a,
        'l: 'a,
        Self: 'a,
    {
        self.inner.put_multipart_opts(location, opts)
    }

    fn get_opts<'s, 'l, 'a>(
        &'s self,
        location: &'l StorePath,
        options: GetOptions,
    ) -> BoxFut<'a, GetResult>
    where
        's: 'a,
        'l: 'a,
        Self: 'a,
    {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let got = self.inner.get_opts(location, options).await?;
            if !self.short {
                return Ok(got);
            }
            use futures::StreamExt;
            let (meta, range, attributes, extensions) = (
                got.meta.clone(),
                got.range.clone(),
                got.attributes.clone(),
                got.extensions.clone(),
            );
            let bytes = got.bytes().await?;
            let half = bytes.slice(..bytes.len() / 2);
            Ok(GetResult {
                payload: object_store::GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(half) }).boxed(),
                ),
                meta,
                range,
                attributes,
                extensions,
            })
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::StreamExt;
        // Copied first: `etag` takes the lock itself.
        let keys: Vec<StorePath> = self.md5s.lock().unwrap().keys().cloned().collect();
        let etags: HashMap<StorePath, Option<String>> = keys
            .into_iter()
            .map(|k| (k.clone(), self.etag(&k)))
            .collect();
        self.inner
            .list(prefix)
            .map(move |meta| {
                let mut meta = meta?;
                meta.e_tag = etags.get(&meta.location).cloned().flatten();
                Ok(meta)
            })
            .boxed()
    }

    fn list_with_delimiter<'s, 'l, 'a>(
        &'s self,
        prefix: Option<&'l StorePath>,
    ) -> BoxFut<'a, ListResult>
    where
        's: 'a,
        'l: 'a,
        Self: 'a,
    {
        self.inner.list_with_delimiter(prefix)
    }

    fn copy_opts<'s, 'f, 't, 'a>(
        &'s self,
        from: &'f StorePath,
        to: &'t StorePath,
        options: CopyOptions,
    ) -> BoxFut<'a, ()>
    where
        's: 'a,
        'f: 'a,
        't: 'a,
        Self: 'a,
    {
        Box::pin(async move {
            self.inner.copy_opts(from, to, options).await?;
            let md5 = self.md5s.lock().unwrap().get(from).cloned();
            if let Some(md5) = md5 {
                self.md5s.lock().unwrap().insert(to.clone(), md5);
            }
            Ok(())
        })
    }
}

fn put_raw(store: &EtagStore, key: &str, bytes: &[u8]) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(store.put(
        &StorePath::parse(key).unwrap(),
        PutPayload::from(bytes.to_vec()),
    ))
    .unwrap();
}

fn get_raw(store: &EtagStore, key: &str) -> Vec<u8> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let got = store.get(&StorePath::parse(key).unwrap()).await.unwrap();
        got.bytes().await.unwrap().to_vec()
    })
}

#[test]
fn an_md5_etag_proves_an_object_unread_and_anything_else_is_read() {
    let object_bytes = b"labels\n".to_vec();
    let object = object_key(&object_bytes);
    let (rec, rec_bytes) = record("annotations");
    for etags in [Etags::Md5, Etags::Missing, Etags::Mismatch] {
        let store = EtagStore::new(etags);
        put_raw(&store, &object, &object_bytes);
        put_raw(&store, &rec, &rec_bytes);
        let before = store.gets();
        assert!(scrub(&store, false).is_clean());
        // The record is always read; the object only without an md5 ETag.
        let reads = match etags {
            Etags::Md5 => 1,
            Etags::Missing | Etags::Mismatch => 2,
        };
        assert_eq!(store.gets() - before, reads, "{etags:?}");
        let before = store.gets();
        assert!(scrub(&store, true).is_clean());
        assert_eq!(store.gets() - before, 2, "deep, {etags:?}");
    }

    // An ETag that matches the name, over bytes that do not: cheap mode
    // trusts the provider, deep mode reads and finds the damage.
    let store = EtagStore::new(Etags::Md5);
    put_raw(&store, &object, b"other bytes");
    store
        .md5s
        .lock()
        .unwrap()
        .insert(StorePath::parse(&object).unwrap(), md5_hex(&object_bytes));
    assert!(scrub(&store, false).is_clean());
    assert_eq!(scrub(&store, true).damaged, vec![object]);
}

#[test]
fn after_a_put_an_md5_etag_is_trusted_and_anything_else_is_read_back() {
    let good = vec![7u8; 4096];
    let object = object_key(&good);
    let (rec, rec_bytes) = record("annotations");
    for etags in [Etags::Md5, Etags::Missing, Etags::Mismatch] {
        for (key, bytes) in [(&object, &good), (&rec, &rec_bytes)] {
            let store = EtagStore::new(etags);
            put_raw(&store, key, b"damaged");
            let before = store.gets();
            let replaced = integrity::replace(&store, key, Source::Bytes(bytes.clone())).unwrap();
            assert!(
                matches!(replaced, Replaced::Replaced { .. }),
                "{replaced:?}"
            );
            // One read to re-verify the damaged copy; one more to check
            // the write unless its ETag is the bytes' md5.
            let reads = match etags {
                Etags::Md5 => 1,
                Etags::Missing | Etags::Mismatch => 2,
            };
            assert_eq!(store.gets() - before, reads, "{etags:?} {key}");
            assert_eq!(&get_raw(&store, key), bytes);
        }
    }

    // A store that keeps other bytes than it was sent, without an md5
    // ETag: the read back fails, and says so.
    for etags in [Etags::Missing, Etags::Mismatch] {
        let mut store = EtagStore::new(etags);
        put_raw(&store, &object, b"damaged");
        store.corrupt = true;
        let err = integrity::replace(&store, &object, Source::Bytes(good.clone())).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::WriteUnverified { key } if *key == object),
            "{err:#}"
        );
    }
}

#[test]
fn a_short_read_is_unreadable_never_damaged() {
    let good = vec![7u8; 4096];
    let object = object_key(&good);
    let (rec, rec_bytes) = record("annotations");
    let mut store = EtagStore::new(Etags::Missing);
    put_raw(&store, &object, &good);
    put_raw(&store, &rec, b"damaged record");
    store.short = true;
    let report = scrub(&store, true);
    assert_eq!(report.checked, 2);
    assert!(report.damaged.is_empty(), "{report:?}");
    let keys: Vec<&str> = report.unreadable.iter().map(|u| u.key.as_str()).collect();
    let mut want = vec![object.as_str(), rec.as_str()];
    want.sort();
    assert_eq!(keys, want);
    assert!(report.unreadable[0].reason.contains("bytes of the"));

    let err = integrity::replace(&store, &rec, Source::Bytes(rec_bytes)).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Unreadable { .. }),
        "{err:#}"
    );
    let err = integrity::quarantine(&store, &object).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Unreadable { .. }),
        "{err:#}"
    );
    store.short = false;
    assert_eq!(get_raw(&store, &rec), b"damaged record");
    assert_eq!(get_raw(&store, &object), good);
}

#[test]
fn an_escaped_key_is_scrubbed_and_replaced_under_its_own_name() {
    // `~` in a history key is `%7E` on disk, as object_store writes it.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("store");
    let (rec, good) = record("reviewer%7Erick");
    write(&root, &rec, b"damaged record");
    let store = local(&root);
    assert_eq!(scrub(&*store, false).damaged, vec![rec.clone()]);
    let healed = integrity::replace(&*store, &rec, Source::Bytes(good.clone())).unwrap();
    assert!(matches!(healed, Replaced::Replaced { .. }), "{healed:?}");
    assert_eq!(read(&root, &rec), good);
    let records: Vec<String> = files(&root)
        .into_keys()
        .filter(|k| k.starts_with("bigstore-history/"))
        .collect();
    assert_eq!(records, vec![rec]);
}
