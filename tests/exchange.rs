//! `bigstore::folder::exchange` between store directories, over in-process
//! pipes and over a subprocess of this test binary serving on its stdin
//! and stdout. The harness is this file's own (`harness = false`): libtest
//! writes to stdout, which a subprocess server needs for the protocol.

use bigstore::folder::exchange::{self, Client, Code, ServeOptions};
use bigstore::folder::integrity::Replaced;
use bigstore::folder::layout::{self, Kind};
use bigstore::folder::{
    self, Credentials, Error as FolderError, HistoryKey, PushOptions, Remote, RemoteConfig,
};
use bigstore::pktline::{self, Packet, PktReader};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Set in a subprocess of this binary: what it does instead of testing.
const HELPER: &str = "BIGSTORE_EXCHANGE_TEST_HELPER";
/// The dataset every test store's histories are under.
const SCOPE: &str = "ST032_Warrawoona/BeatonsCreek_dataset1_September2026";

type Test = (&'static str, fn());

const TESTS: &[Test] = &[
    (
        "an_empty_store_and_a_full_one_end_identical",
        an_empty_store_and_a_full_one_end_identical,
    ),
    (
        "partial_stores_exchange_both_ways_then_move_nothing",
        partial_stores_exchange_both_ways_then_move_nothing,
    ),
    (
        "a_file_sent_that_is_not_what_its_name_says_is_refused",
        a_file_sent_that_is_not_what_its_name_says_is_refused,
    ),
    (
        "a_file_fetched_that_is_not_what_its_name_says_is_refused",
        a_file_fetched_that_is_not_what_its_name_says_is_refused,
    ),
    (
        "traversing_and_odd_keys_are_refused_on_both_sides",
        traversing_and_odd_keys_are_refused_on_both_sides,
    ),
    (
        "a_name_already_present_is_left_untouched",
        a_name_already_present_is_left_untouched,
    ),
    (
        "concurrent_sends_of_one_key_both_succeed_with_one_file",
        concurrent_sends_of_one_key_both_succeed_with_one_file,
    ),
    (
        "a_stream_cut_mid_file_places_nothing_and_a_rerun_completes",
        a_stream_cut_mid_file_places_nothing_and_a_rerun_completes,
    ),
    (
        "no_record_or_manifest_is_placed_without_its_objects",
        no_record_or_manifest_is_placed_without_its_objects,
    ),
    (
        "an_unknown_version_fails_on_both_sides",
        an_unknown_version_fails_on_both_sides,
    ),
    (
        "a_program_that_is_not_a_server_is_refused",
        a_program_that_is_not_a_server_is_refused,
    ),
    (
        "the_server_exits_promptly_when_its_input_ends_mid_file",
        the_server_exits_promptly_when_its_input_ends_mid_file,
    ),
    (
        "records_outside_the_opened_history_are_refused",
        records_outside_the_opened_history_are_refused,
    ),
    (
        "a_spawned_server_exchanges_a_deep_merge_record_and_closes",
        a_spawned_server_exchanges_a_deep_merge_record_and_closes,
    ),
    (
        "a_store_opened_without_create_is_empty_and_refuses_files",
        a_store_opened_without_create_is_empty_and_refuses_files,
    ),
    (
        "a_cancelled_client_stops_a_call_blocked_on_the_far_program",
        a_cancelled_client_stops_a_call_blocked_on_the_far_program,
    ),
    (
        "a_request_out_of_order_ends_the_session_on_both_sides",
        a_request_out_of_order_ends_the_session_on_both_sides,
    ),
    (
        "a_record_over_its_size_limit_is_refused_and_the_session_goes_on",
        a_record_over_its_size_limit_is_refused_and_the_session_goes_on,
    ),
    (
        "versions_negotiate_to_the_highest_both_sides_speak",
        versions_negotiate_to_the_highest_both_sides_speak,
    ),
    (
        "a_far_scrub_finds_damage_and_heal_replaces_it",
        a_far_scrub_finds_damage_and_heal_replaces_it,
    ),
    (
        "an_unreadable_far_copy_is_reported_and_never_healed",
        an_unreadable_far_copy_is_reported_and_never_healed,
    ),
    (
        "an_open_guard_is_held_for_the_session_and_a_refusal_leaves_the_store_untouched",
        an_open_guard_is_held_for_the_session_and_a_refusal_leaves_the_store_untouched,
    ),
];

fn main() {
    if let Ok(mode) = std::env::var(HELPER) {
        std::process::exit(helper(&mode));
    }
    let mut args = std::env::args().skip(1);
    let mut filters = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list" => {
                for (name, _) in TESTS {
                    println!("{name}: test");
                }
                return;
            }
            // libtest flags that take a value: skip it.
            "--test-threads" | "--skip" | "--color" | "--format" | "--logfile" | "-Z" => {
                args.next();
            }
            flag if flag.starts_with('-') => {}
            filter => filters.push(filter.to_string()),
        }
    }
    let chosen: Vec<&Test> = TESTS
        .iter()
        .filter(|(name, _)| filters.is_empty() || filters.iter().any(|f| name.contains(f.as_str())))
        .collect();
    println!("\nrunning {} tests", chosen.len());
    let mut failed = Vec::new();
    for (name, test) in chosen.iter().copied() {
        let ok = std::panic::catch_unwind(*test).is_ok();
        println!("test {name} ... {}", if ok { "ok" } else { "FAILED" });
        if !ok {
            failed.push(*name);
        }
    }
    println!(
        "\ntest result: {}. {} passed; {} failed\n",
        if failed.is_empty() { "ok" } else { "FAILED" },
        chosen.len() - failed.len(),
        failed.len()
    );
    if !failed.is_empty() {
        std::process::exit(101);
    }
}

/// What a subprocess of this binary does. Nothing but the protocol may go
/// to stdout.
fn helper(mode: &str) -> i32 {
    match mode {
        "serve" => {
            let served = exchange::serve(
                std::io::stdin(),
                std::io::stdout().lock(),
                &ServeOptions::new("test-helper"),
            );
            match served {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("serve: {e:#}");
                    1
                }
            }
        }
        // A login banner before the server.
        "banner" => {
            println!("Welcome to the far host");
            0
        }
        // The far shell found no such program.
        "missing" => 127,
        // A server that answers the handshake, then nothing.
        "stall" => {
            let mut raw = Raw::new(std::io::stdin().lock(), std::io::stdout().lock());
            assert!(raw.recv_line().starts_with("bigstore-exchange-client "));
            raw.line("bigstore-exchange-server stall");
            raw.json(serde_json::json!({"version": {"version": 1}}));
            std::thread::sleep(Duration::from_secs(60));
            0
        }
        other => panic!("unknown helper {other}"),
    }
}

// ──────────────────────────────────────────────────
// Stores
// ──────────────────────────────────────────────────

fn scope() -> HistoryKey {
    HistoryKey::new(SCOPE).unwrap()
}

fn write(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Push output `name` (a directory) once per entry of `versions`, each its
/// content, into the store in `store`: objects, manifests and records, as
/// folder mode writes them.
fn push_versions(store: &Path, work: &Path, name: &str, versions: &[&str]) {
    let remote = Remote::open(&RemoteConfig {
        url: format!("local://{}", store.display()),
        endpoint: None,
        region: None,
        credentials: Credentials::FromEnv,
    })
    .unwrap();
    let out = work.join(name);
    for content in versions {
        write(&out.join("labels.jsonl"), content.as_bytes());
        write(&out.join("sub/regions.jsonl"), b"{\"r\":1}\n");
        let opts = PushOptions {
            jobs: 2,
            ..PushOptions::new(HistoryKey::new(&format!("{SCOPE}/{name}")).unwrap())
        };
        let _ = folder::push(&remote, &out, &opts).unwrap();
    }
}

/// Every file under `dir`, `/`-separated and relative, with its bytes.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .map(|e| {
            let rel = e.path().strip_prefix(dir).unwrap();
            let key = rel.to_str().unwrap().replace('\\', "/");
            (key, std::fs::read(e.path()).unwrap())
        })
        .collect()
}

/// The store files under `dir`.
fn listing(dir: &Path) -> BTreeSet<String> {
    files(dir)
        .into_keys()
        .filter(|k| layout::kind(k) != Kind::Other)
        .collect()
}

/// Temp files left under `dir`.
fn temps(dir: &Path) -> Vec<String> {
    files(dir).into_keys().filter(|k| k.contains('#')).collect()
}

fn keys_of(dir: &Path, kind: Kind) -> Vec<String> {
    listing(dir)
        .into_iter()
        .filter(|k| layout::kind(k) == kind)
        .collect()
}

/// Bring the client's far store and `local` to the union of both: what
/// each lacks, sent or fetched. Returns how many files went each way.
fn sync(client: &mut Client, local: &Path) -> (usize, usize) {
    let theirs = client.list().unwrap();
    let mine = listing(local);
    let to: Vec<&String> = mine.difference(&theirs).collect();
    let from: Vec<&String> = theirs.difference(&mine).collect();
    let sent = client.send(&to, local).unwrap();
    let fetched = client.fetch(&from, local).unwrap();
    assert_eq!(sent.stored, to.len());
    assert_eq!(fetched.stored, from.len());
    (to.len(), from.len())
}

// ──────────────────────────────────────────────────
// Sessions
// ──────────────────────────────────────────────────

type Served = std::thread::JoinHandle<anyhow::Result<()>>;

/// A server on a thread, and the two pipe ends that talk to it.
fn serve_in_process() -> (std::io::PipeReader, std::io::PipeWriter, Served) {
    serve_in_process_with(ServeOptions::new("in-process"))
}

fn serve_in_process_with(opts: ServeOptions) -> (std::io::PipeReader, std::io::PipeWriter, Served) {
    let (to_server, client_writes) = std::io::pipe().unwrap();
    let (client_reads, from_server) = std::io::pipe().unwrap();
    let served = std::thread::spawn(move || exchange::serve(to_server, from_server, &opts));
    (client_reads, client_writes, served)
}

/// A client on a server on a thread, opened on `far`.
fn session(far: &Path) -> (Client, Served) {
    let (reader, writer, served) = serve_in_process();
    let mut client = Client::connect(reader, writer).unwrap();
    assert_eq!(client.far_build(), "in-process");
    client.open(far.to_str().unwrap(), &scope(), true).unwrap();
    (client, served)
}

fn close(client: Client, served: Served) {
    client.close().unwrap();
    served.join().unwrap().unwrap();
}

/// This test binary as a subprocess in helper `mode`.
fn helper_command(mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.env(HELPER, mode).stderr(Stdio::null());
    command
}

/// The protocol by hand, for what a well-behaved client or server never
/// sends. Packets are written straight to `w`, so a body can stop part way.
struct Raw<R: Read, W: Write> {
    r: PktReader<R>,
    w: W,
}

impl<R: Read, W: Write> Raw<R, W> {
    fn new(r: R, w: W) -> Self {
        Self {
            r: PktReader::new(r),
            w,
        }
    }

    fn packet(&mut self, payload: &[u8]) {
        write!(self.w, "{:04x}", payload.len() + 4).unwrap();
        self.w.write_all(payload).unwrap();
    }

    fn flush_pkt(&mut self) {
        self.w.write_all(b"0000").unwrap();
        self.w.flush().unwrap();
    }

    fn line(&mut self, line: &str) {
        self.packet(format!("{line}\n").as_bytes());
        self.w.flush().unwrap();
    }

    fn json(&mut self, value: serde_json::Value) {
        self.line(&value.to_string());
    }

    /// Data packets of `bytes`, with no flush ending them.
    fn partial(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(pktline::MAX_DATA_LEN) {
            self.packet(chunk);
        }
        self.w.flush().unwrap();
    }

    fn body(&mut self, bytes: &[u8]) {
        self.partial(bytes);
        self.flush_pkt();
    }

    fn keys(&mut self, keys: &[&str]) {
        for key in keys {
            self.packet(format!("{key}\n").as_bytes());
        }
        self.flush_pkt();
    }

    fn recv_line(&mut self) -> String {
        match self.r.packet().unwrap() {
            Some(Packet::Data(bytes)) => String::from_utf8(bytes.to_vec())
                .unwrap()
                .trim_end_matches('\n')
                .to_string(),
            other => panic!("not a line: {other:?}"),
        }
    }

    fn recv(&mut self) -> serde_json::Value {
        serde_json::from_str(&self.recv_line()).unwrap()
    }

    /// Handshake as a version 1 client and open `far` on `history`.
    fn open(&mut self, far: &Path, history: &str) {
        self.line("bigstore-exchange-client 1");
        assert!(self.recv_line().starts_with("bigstore-exchange-server "));
        assert_eq!(self.recv(), serde_json::json!({"version": {"version": 1}}));
        self.json(serde_json::json!({"open": {
            "store": far.to_str().unwrap(), "history": history, "create": true
        }}));
        assert!(self.recv().get("opened").is_some());
    }
}

/// The far side's refusal in `err`.
#[track_caller]
fn far_refusal(err: &anyhow::Error) -> (Code, Option<&str>) {
    match err.downcast_ref::<exchange::Error>() {
        Some(exchange::Error::Refused { code, key }) => (*code, key.as_deref()),
        _ => panic!("not a far refusal: {err:#}"),
    }
}

#[track_caller]
fn folder_error(err: &anyhow::Error) -> &FolderError {
    err.downcast_ref::<FolderError>()
        .unwrap_or_else(|| panic!("not a folder::Error: {err:#}"))
}

/// A history record of `SCOPE/<output>` following `parents`, valid for its
/// name: its key and bytes.
fn record(output: &str, parents: &[String]) -> (String, Vec<u8>) {
    // Ids and md5s with a letter: all digits, YAML would read a number.
    let mut yaml = format!(
        "outs:\n- md5: {}\n  size: 1\n  hash: md5\n  path: f\nmeta:\n  bigstore:\n    parents:",
        "d41d8cd98f00b204e9800998ecf8427e"
    );
    if parents.is_empty() {
        yaml.push_str(" []");
    }
    for p in parents {
        yaml.push_str(&format!("\n    - {p}"));
    }
    yaml.push_str("\n    writer: test\n    time: 2026-10-01T00:00:00.000000000Z\n");
    let bytes = yaml.into_bytes();
    let id = {
        use sha2::Digest as _;
        hex::encode(&sha2::Sha256::digest(&bytes)[..16])
    };
    let dir = match parents {
        [] => "root".to_string(),
        ids => ids.join("+"),
    };
    let key = format!("bigstore-history/{SCOPE}/{output}/{dir}/{id}.dvc");
    layout::verify(&key, &bytes).expect("a valid record");
    (key, bytes)
}

// ──────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────

fn an_empty_store_and_a_full_one_end_identical() {
    for full_side_is_far in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
        let full = if full_side_is_far { &far } else { &local };
        push_versions(
            full,
            &tmp.path().join("work"),
            "annotations",
            &["a\n", "b\n"],
        );
        std::fs::create_dir_all(&local).unwrap();
        let (mut client, served) = session(&far);
        let (sent, fetched) = sync(&mut client, &local);
        let moved = listing(full).len();
        assert_eq!(
            (sent, fetched),
            if full_side_is_far {
                (0, moved)
            } else {
                (moved, 0)
            }
        );
        assert_eq!(files(&far), files(&local));
        assert_eq!(sync(&mut client, &local), (0, 0));
        close(client, served);
    }
}

fn partial_stores_exchange_both_ways_then_move_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    let work = tmp.path().join("work");
    push_versions(&far, &work.join("far"), "views", &["far 1\n", "far 2\n"]);
    push_versions(&local, &work.join("local"), "annotations", &["mine\n"]);
    // Both hold the same content of a third output: its objects and
    // manifest are the same names on both sides.
    push_versions(&far, &work.join("far"), "store", &["shared\n"]);
    push_versions(&local, &work.join("local"), "store", &["shared\n"]);
    let (mut client, served) = session(&far);
    let (sent, fetched) = sync(&mut client, &local);
    assert!(sent > 0 && fetched > 0, "{sent} {fetched}");
    assert_eq!(listing(&far), listing(&local));
    assert_eq!(files(&far), files(&local));
    assert_eq!(sync(&mut client, &local), (0, 0));
    close(client, served);
}

fn a_file_sent_that_is_not_what_its_name_says_is_refused() {
    for kind in [Kind::Object, Kind::Manifest, Kind::Record] {
        let tmp = tempfile::tempdir().unwrap();
        let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
        push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
        let bad = keys_of(&local, kind)[0].clone();
        let path = local.join(&bad);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        let (mut client, served) = session(&far);
        let err = client.send(listing(&local), &local).unwrap_err();
        assert_eq!(far_refusal(&err), (Code::Integrity, Some(bad.as_str())));
        assert!(!far.join(&bad).exists());
        assert_eq!(temps(&far), Vec::<String>::new());
        // A refusal ends the request, not the session.
        assert!(!client.list().unwrap().contains(&bad));
        close(client, served);
    }
}

fn a_file_fetched_that_is_not_what_its_name_says_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&far, &tmp.path().join("work"), "annotations", &["a\n"]);
    let objects = keys_of(&far, Kind::Object);
    let bad = objects.last().unwrap().clone();
    std::fs::write(far.join(&bad), b"not this").unwrap();
    std::fs::create_dir_all(&local).unwrap();
    let (mut client, served) = session(&far);
    let err = client.fetch(listing(&far), &local).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Integrity { key } if *key == bad),
        "{err:#}"
    );
    // The objects before it are placed; it, and everything after, is not.
    let placed = listing(&local);
    assert_eq!(
        placed,
        objects[..objects.len() - 1].iter().cloned().collect()
    );
    assert_eq!(temps(&local), Vec::<String>::new());
    close(client, served);
}

fn traversing_and_odd_keys_are_refused_on_both_sides() {
    let tmp = tempfile::tempdir().unwrap();
    let far = tmp.path().join("store/far");
    let local = tmp.path().join("local");
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let object = keys_of(&local, Kind::Object)[0].clone();
    let odd = [
        "../evil".to_string(),
        "files/md5/../../evil".to_string(),
        format!("/{object}"),
        format!("C:/{object}"),
        object.replace('/', "\\"),
        format!("{object}#tmp"),
        format!("{object}.partial"),
        "files/md5/./ab".to_string(),
        format!("files/md5/{}", object["files/md5/".len()..].to_uppercase()),
        format!("bigstore-history/{SCOPE}/../root/{}.dvc", "0".repeat(32)),
        "bigstore-history/a~b/root/00000000000000000000000000000000.dvc".to_string(),
        ".DS_Store".to_string(),
        String::new(),
    ];
    let (mut client, served) = session(&far);
    for key in &odd {
        assert_eq!(layout::kind(key), Kind::Other, "{key}");
        let err = client.send([key], &local).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::InvalidStoreKey { key: k } if k == key),
            "{key}: {err:#}"
        );
        let err = client.fetch([key], &local).unwrap_err();
        assert!(
            matches!(folder_error(&err), FolderError::InvalidStoreKey { .. }),
            "{err:#}"
        );
    }
    close(client, served);

    // A client that sends them anyway: the server refuses each, writing
    // nothing anywhere.
    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.open(&far, SCOPE);
    let before = files(tmp.path());
    for key in &odd {
        raw.json(serde_json::json!({"put": {}}));
        raw.json(serde_json::json!({"file": {"key": key, "size": 4}}));
        raw.body(b"evil");
        raw.json(serde_json::json!({"end": {}}));
        assert_eq!(
            raw.recv(),
            serde_json::json!({"error": {"code": "key", "key": key}}),
            "{key}"
        );
        raw.json(serde_json::json!({"get": {}}));
        raw.keys(&[key]);
        assert_eq!(
            raw.recv(),
            serde_json::json!({"error": {"code": "key", "key": key}})
        );
    }
    assert_eq!(files(tmp.path()), before);
    raw.json(serde_json::json!({"close": {}}));
    served.join().unwrap().unwrap();
}

fn a_name_already_present_is_left_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let keys = listing(&local);
    let (mut client, served) = session(&far);
    client.send(&keys, &local).unwrap();
    let stamp = |key: &String| {
        let meta = std::fs::metadata(far.join(key)).unwrap();
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(&meta);
        #[cfg(not(unix))]
        let inode = 0;
        (inode, meta.modified().unwrap())
    };
    let before: Vec<_> = keys.iter().map(stamp).collect();
    std::thread::sleep(Duration::from_millis(20));
    let again = client.send(&keys, &local).unwrap();
    assert_eq!((again.stored, again.present), (0, keys.len()));
    assert_eq!(keys.iter().map(stamp).collect::<Vec<_>>(), before);
    close(client, served);
}

fn concurrent_sends_of_one_key_both_succeed_with_one_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(
        &local,
        &tmp.path().join("work"),
        "annotations",
        &["a\n", "b\n"],
    );
    let keys = listing(&local);
    let barrier = std::sync::Barrier::new(2);
    let results: Vec<_> = std::thread::scope(|s| {
        let racers: Vec<_> = (0..2)
            .map(|_| {
                s.spawn(|| {
                    let (mut client, served) = session(&far);
                    barrier.wait();
                    let sent = client.send(&keys, &local).unwrap();
                    close(client, served);
                    sent
                })
            })
            .collect();
        racers.into_iter().map(|r| r.join().unwrap()).collect()
    });
    let stored: usize = results.iter().map(|r| r.stored).sum();
    let present: usize = results.iter().map(|r| r.present).sum();
    assert_eq!((stored, stored + present), (keys.len(), 2 * keys.len()));
    assert_eq!(files(&far), files(&local));
}

fn a_stream_cut_mid_file_places_nothing_and_a_rerun_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let object = keys_of(&local, Kind::Object)[0].clone();
    let bytes = std::fs::read(local.join(&object)).unwrap();

    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.open(&far, SCOPE);
    raw.json(serde_json::json!({"put": {}}));
    raw.json(serde_json::json!({"file": {"key": object, "size": bytes.len() + 1000}}));
    raw.partial(&bytes);
    drop(raw);
    let err = served.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<exchange::Error>(),
            Some(exchange::Error::SessionBroken)
        ),
        "{err:#}"
    );
    assert!(!far.join(&object).exists());
    assert_eq!(temps(&far), Vec::<String>::new());

    // A temp file a killed process left behind is no store file: no
    // listing shows it, and it is never sent.
    let stray = format!("{object}#Ab12Cd34");
    write(&far.join(&stray), b"half");
    let (mut client, served) = session(&far);
    assert!(client.list().unwrap().is_empty());
    sync(&mut client, &local);
    assert_eq!(listing(&far), listing(&local));
    close(client, served);
}

fn no_record_or_manifest_is_placed_without_its_objects() {
    // Whatever order the keys are given in, objects go first, then
    // manifests, then records, and a refusal stops what follows: so a
    // transfer cut by the first object it cannot place leaves no manifest
    // and no record behind it, in either direction.
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let objects = keys_of(&local, Kind::Object);
    let mut keys: Vec<String> = listing(&local).into_iter().rev().collect();
    keys.sort_by_key(|k| std::cmp::Reverse(layout::kind(k)));
    assert_eq!(layout::kind(&keys[0]), Kind::Record);
    let last = objects.last().unwrap();
    std::fs::write(local.join(last), b"damaged").unwrap();
    let (mut client, served) = session(&far);
    let err = client.send(&keys, &local).unwrap_err();
    assert_eq!(far_refusal(&err), (Code::Integrity, Some(last.as_str())));
    let placed = listing(&far);
    assert!(
        placed.iter().all(|k| layout::kind(k) == Kind::Object),
        "{placed:?}"
    );
    assert_eq!(placed.len(), objects.len() - 1);
    close(client, served);

    // Fetching from a far store holding all of it, that object damaged.
    let far = tmp.path().join("far2");
    for key in listing(&local) {
        write(&far.join(&key), &std::fs::read(local.join(&key)).unwrap());
    }
    let back = tmp.path().join("back");
    let (mut client, served) = session(&far);
    let err = client.fetch(&keys, &back).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Integrity { key } if key == last),
        "{err:#}"
    );
    let placed = listing(&back);
    assert!(
        placed.iter().all(|k| layout::kind(k) == Kind::Object),
        "{placed:?}"
    );
    assert_eq!(placed.len(), objects.len() - 1);
    close(client, served);
}

fn an_unknown_version_fails_on_both_sides() {
    // A client offering only version 99: the server answers, refuses, and
    // returns a typed error, before anything is opened.
    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.line("bigstore-exchange-client 99");
    assert_eq!(raw.recv_line(), "bigstore-exchange-server in-process");
    assert_eq!(
        raw.recv(),
        serde_json::json!({"error": {"code": "version", "versions": [2, 1]}})
    );
    let err = served.join().unwrap().unwrap_err();
    match err.downcast_ref::<exchange::Error>() {
        Some(exchange::Error::Version { ours, theirs, .. }) => {
            assert_eq!(
                (ours.as_slice(), theirs.as_slice()),
                (&[2, 1][..], &[99][..])
            );
        }
        _ => panic!("{err:#}"),
    }

    // A server speaking only version 3: the client fails naming its build.
    let (client_reads, from_server) = std::io::pipe().unwrap();
    let (to_server, client_writes) = std::io::pipe().unwrap();
    let fake = std::thread::spawn(move || {
        let mut raw = Raw::new(to_server, from_server);
        assert_eq!(raw.recv_line(), "bigstore-exchange-client 2 1");
        raw.line("bigstore-exchange-server future-build");
        raw.json(serde_json::json!({"error": {"code": "version", "versions": [3]}}));
    });
    let err = Client::connect(client_reads, client_writes)
        .err()
        .expect("refused");
    fake.join().unwrap();
    match err.downcast_ref::<exchange::Error>() {
        Some(exchange::Error::Version {
            ours,
            theirs,
            far_build,
        }) => {
            assert_eq!(
                (ours.as_slice(), theirs.as_slice()),
                (&[2, 1][..], &[3][..])
            );
            assert_eq!(far_build.as_deref(), Some("future-build"));
        }
        _ => panic!("{err:#}"),
    }
}

fn a_program_that_is_not_a_server_is_refused() {
    for mode in ["banner", "missing"] {
        let err = Client::spawn(helper_command(mode))
            .err()
            .expect("not a server");
        assert!(
            matches!(
                err.downcast_ref::<exchange::Error>(),
                Some(exchange::Error::NotAServer)
            ),
            "{mode}: {err:#}"
        );
    }
}

fn the_server_exits_promptly_when_its_input_ends_mid_file() {
    // A client killed mid-transfer: the server's input ends while it is
    // writing a file. It must remove its temp file and exit at once, not
    // linger on the far machine.
    let tmp = tempfile::tempdir().unwrap();
    let far = tmp.path().join("far");
    let mut child = helper_command("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut raw = Raw::new(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    raw.open(&far, SCOPE);
    let object = format!("files/md5/ab/{}", "c".repeat(30));
    raw.json(serde_json::json!({"put": {}}));
    raw.json(serde_json::json!({"file": {"key": object, "size": 50u64 << 20}}));
    raw.partial(&vec![7u8; 1 << 20]);
    // The server is now part way through the file, temp file and all.
    let temp_seen = Instant::now();
    while temps(&far).is_empty() {
        assert!(
            temp_seen.elapsed() < Duration::from_secs(10),
            "no temp file"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let Raw { r, w } = raw;
    drop(w);
    let cut = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(
            cut.elapsed() < Duration::from_secs(10),
            "server still running"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    let took = cut.elapsed();
    drop(r);
    assert!(took < Duration::from_secs(1), "exited after {took:?}");
    assert!(!status.success());
    assert_eq!(temps(&far), Vec::<String>::new());
    assert!(!far.join(&object).exists());
}

fn records_outside_the_opened_history_are_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let record = keys_of(&local, Kind::Record)[0].clone();
    // A history that is a prefix of the store's, but not a whole component.
    let other = &SCOPE[..SCOPE.len() - 4];
    let (reader, writer, served) = serve_in_process();
    let mut client = Client::connect(reader, writer).unwrap();
    client
        .open(
            far.to_str().unwrap(),
            &HistoryKey::new(other).unwrap(),
            true,
        )
        .unwrap();
    let err = client.send([&record], &local).unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::OutOfScope { key, .. } if *key == record),
        "{err:#}"
    );
    let err = client.fetch([&record], &local).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::OutOfScope { .. }));
    close(client, served);

    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.open(&far, other);
    let bytes = std::fs::read(local.join(&record)).unwrap();
    raw.json(serde_json::json!({"put": {}}));
    raw.json(serde_json::json!({"file": {"key": record, "size": bytes.len()}}));
    raw.body(&bytes);
    raw.json(serde_json::json!({"end": {}}));
    assert_eq!(
        raw.recv(),
        serde_json::json!({"error": {"code": "scope", "key": record}})
    );
    assert!(!far.join(&record).exists());
    raw.json(serde_json::json!({"close": {}}));
    served.join().unwrap().unwrap();
}

fn a_spawned_server_exchanges_a_deep_merge_record_and_closes() {
    // A merge of 7 heads under a long history key: a path well past
    // Windows' 260-character MAX_PATH.
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    // Seven parents: eight make a 263-byte name, past the 255 bytes a name
    // may have on every filesystem a store lives on.
    let parents: Vec<String> = (0..7).map(|i| format!("a{i}{}", "e".repeat(30))).collect();
    let output = "annotations/reviewer=rick/host=xenoglossicist";
    let (merge, bytes) = record(output, &parents);
    assert!(merge.len() > 300, "{}", merge.len());
    let mut long = local.clone();
    long.extend(merge.split('/'));
    std::fs::create_dir_all(long.parent().unwrap()).unwrap();
    std::fs::write(&long, &bytes).unwrap();

    let mut client = Client::spawn(helper_command("serve")).unwrap();
    assert_eq!(client.far_build(), "test-helper");
    assert!(!client.open(far.to_str().unwrap(), &scope(), true).unwrap());
    let all = listing(&local);
    assert!(all.contains(&merge));
    let sent = client.send(&all, &local).unwrap();
    assert_eq!(sent.stored, all.len());
    assert_eq!(client.list().unwrap(), all);
    let back = tmp.path().join("back");
    let fetched = client.fetch(&all, &back).unwrap();
    assert_eq!(fetched.stored, all.len());
    client.close().unwrap();
    assert_eq!(files(&back), files(&local));
}

fn a_store_opened_without_create_is_empty_and_refuses_files() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("absent"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let (reader, writer, served) = serve_in_process();
    let mut client = Client::connect(reader, writer).unwrap();
    assert!(!client.open(far.to_str().unwrap(), &scope(), false).unwrap());
    assert!(client.list().unwrap().is_empty());
    let err = client.send(listing(&local), &local).unwrap_err();
    assert_eq!(far_refusal(&err).0, Code::Open);
    let object = keys_of(&local, Kind::Object)[0].clone();
    let err = client.fetch([&object], &local).unwrap_err();
    assert_eq!(far_refusal(&err), (Code::Missing, Some(object.as_str())));
    close(client, served);
    assert!(!far.exists());
}

fn a_cancelled_client_stops_a_call_blocked_on_the_far_program() {
    let mut client = Client::spawn(helper_command("stall")).unwrap();
    assert_eq!(client.far_build(), "stall");
    let canceller = client.canceller();
    let cancelling = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        canceller.cancel();
    });
    let started = Instant::now();
    // The far program never answers: only the cancel ends the call.
    let err = client.open("/nowhere", &scope(), false).unwrap_err();
    cancelling.join().unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        matches!(folder_error(&err), FolderError::Cancelled),
        "{err:#}"
    );
    let err = client.list().unwrap_err();
    assert!(
        matches!(folder_error(&err), FolderError::Cancelled),
        "{err:#}"
    );
}

fn a_request_out_of_order_ends_the_session_on_both_sides() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut client, served) = session(&tmp.path().join("far"));
    // A store is opened once per session.
    let err = client
        .open(tmp.path().to_str().unwrap(), &scope(), true)
        .unwrap_err();
    assert_eq!(far_refusal(&err), (Code::Protocol, None));
    let err = client.list().unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<exchange::Error>(),
            Some(exchange::Error::SessionBroken)
        ),
        "{err:#}"
    );
    let err = served.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<exchange::Error>(),
            Some(exchange::Error::SessionBroken)
        ),
        "{err:#}"
    );
}

fn a_record_over_its_size_limit_is_refused_and_the_session_goes_on() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
    let record = keys_of(&local, Kind::Record)[0].clone();
    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.open(&far, SCOPE);
    let oversized = vec![b'x'; (64 << 10) + 1];
    raw.json(serde_json::json!({"put": {}}));
    raw.json(serde_json::json!({"file": {"key": record, "size": oversized.len()}}));
    raw.body(&oversized);
    // A valid file after it: nothing is placed after a refusal.
    let object = keys_of(&local, Kind::Object)[0].clone();
    let bytes = std::fs::read(local.join(&object)).unwrap();
    raw.json(serde_json::json!({"file": {"key": object, "size": bytes.len()}}));
    raw.body(&bytes);
    raw.json(serde_json::json!({"end": {}}));
    assert_eq!(
        raw.recv(),
        serde_json::json!({"error": {"code": "integrity", "key": record}})
    );
    assert!(!far.join(&record).exists() && !far.join(&object).exists());
    assert_eq!(temps(&far), Vec::<String>::new());
    raw.json(serde_json::json!({"list": {}}));
    assert_eq!(raw.recv(), serde_json::json!({"listing": {}}));
    assert!(matches!(raw.r.packet().unwrap(), Some(Packet::Flush)));
    raw.json(serde_json::json!({"close": {}}));
    served.join().unwrap().unwrap();
}

fn versions_negotiate_to_the_highest_both_sides_speak() {
    let tmp = tempfile::tempdir().unwrap();
    let far = tmp.path().join("far");

    // This build on both sides: version 2.
    let (client, served) = session(&far);
    assert_eq!(client.version(), 2);
    close(client, served);

    // A client offering versions in any order gets the highest.
    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.line("bigstore-exchange-client 1 2");
    raw.recv_line();
    assert_eq!(raw.recv(), serde_json::json!({"version": {"version": 2}}));
    raw.json(serde_json::json!({"close": {}}));
    served.join().unwrap().unwrap();

    // A 0.5 client (version 1 only) gets version 1, which has no scrub.
    let (reader, writer, served) = serve_in_process();
    let mut raw = Raw::new(reader, writer);
    raw.open(&far, SCOPE);
    raw.json(serde_json::json!({"scrub": {"deep": false}}));
    assert_eq!(
        raw.recv(),
        serde_json::json!({"error": {"code": "protocol"}})
    );
    assert!(served.join().unwrap().is_err());

    // A 0.5 server (version 1 only): the session works as it did, and
    // scrub and heal are refused here, before anything is sent.
    let (client_reads, from_server) = std::io::pipe().unwrap();
    let (to_server, client_writes) = std::io::pipe().unwrap();
    let far_str = far.to_str().unwrap().to_string();
    let old = std::thread::spawn(move || {
        let mut raw = Raw::new(to_server, from_server);
        assert_eq!(raw.recv_line(), "bigstore-exchange-client 2 1");
        raw.line("bigstore-exchange-server 0.5.0");
        raw.json(serde_json::json!({"version": {"version": 1}}));
        assert_eq!(
            raw.recv(),
            serde_json::json!({"open": {"store": far_str, "history": SCOPE, "create": true}})
        );
        raw.json(serde_json::json!({"opened": {"existed": true}}));
        assert_eq!(raw.recv(), serde_json::json!({"close": {}}));
    });
    let mut client = Client::connect(client_reads, client_writes).unwrap();
    assert_eq!(client.version(), 1);
    assert_eq!(client.far_build(), "0.5.0");
    client.open(far.to_str().unwrap(), &scope(), true).unwrap();
    let unsupported = |err: anyhow::Error| {
        assert!(
            matches!(
                err.downcast_ref::<exchange::Error>(),
                Some(exchange::Error::Unsupported { version: 1 })
            ),
            "{err:#}"
        )
    };
    unsupported(client.scrub(false).unwrap_err());
    let object = format!("files/md5/ab/{}", "c".repeat(30));
    unsupported(client.heal(&object, tmp.path()).unwrap_err());
    client.close().unwrap();
    old.join().unwrap();
}

fn a_far_scrub_finds_damage_and_heal_replaces_it() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(
        &local,
        &tmp.path().join("work"),
        "annotations",
        &["a\n", "b\n"],
    );
    let (mut client, served) = session(&far);
    let all = listing(&local);
    client.send(&all, &local).unwrap();
    let report = client.scrub(false).unwrap();
    assert_eq!(report.checked, all.len());
    assert!(report.is_clean());

    // Damage an object (truncated) and a record (one byte) on the far side,
    // and leave entries there that are no store files.
    let object = keys_of(&local, Kind::Object)[0].clone();
    let record = keys_of(&local, Kind::Record)[0].clone();
    let at = |root: &Path, key: &str| {
        let mut path = root.to_path_buf();
        path.extend(key.split('/'));
        path
    };
    let good_object = std::fs::read(at(&local, &object)).unwrap();
    let good_record = std::fs::read(at(&local, &record)).unwrap();
    std::fs::write(at(&far, &object), &good_object[..good_object.len() - 1]).unwrap();
    let mut bad_record = good_record.clone();
    bad_record[0] ^= 1;
    std::fs::write(at(&far, &record), &bad_record).unwrap();
    for other in [
        format!("quarantine/{object}.20261001T000000.000000000Z"),
        format!("{object}#a1b2c3d4"),
        format!("{object}.partial"),
        ".lock".to_string(),
    ] {
        write(&at(&far, &other), b"not what any name says");
    }

    assert_eq!(client.list().unwrap(), all);
    let report = client.scrub(false).unwrap();
    assert_eq!(report.checked, all.len());
    let mut damaged = vec![object.clone(), record.clone()];
    damaged.sort();
    assert_eq!(report.damaged, damaged);
    assert!(report.unreadable.is_empty());

    // Bytes that are not what the key names: refused, nothing changed.
    let wrong = tmp.path().join("wrong");
    std::fs::write(&wrong, b"wrong").unwrap();
    let before = files(&far);
    let err = client.heal(&object, &wrong).unwrap_err();
    assert_eq!(far_refusal(&err), (Code::Integrity, Some(object.as_str())));
    assert_eq!(files(&far), before);
    // A record outside the session's history: refused before sending.
    let (outside, _) = record_outside();
    let err = client.heal(&outside, &wrong).unwrap_err();
    assert!(matches!(folder_error(&err), FolderError::OutOfScope { .. }));

    for (key, damaged_bytes) in [
        (&object, &good_object[..good_object.len() - 1]),
        (&record, &bad_record[..]),
    ] {
        let healed = client.heal(key, &at(&local, key)).unwrap();
        let Replaced::Replaced { quarantined } = healed else {
            panic!("{healed:?}")
        };
        assert!(
            quarantined.starts_with(&format!("quarantine/{key}.")),
            "{quarantined}"
        );
        assert_eq!(
            std::fs::read(at(&far, &quarantined)).unwrap(),
            damaged_bytes
        );
        assert_eq!(
            std::fs::read(at(&far, key)).unwrap(),
            std::fs::read(at(&local, key)).unwrap()
        );
    }
    assert!(client.scrub(true).unwrap().is_clean());

    // The far copy is good now: a heal is refused as healed by another,
    // and writes nothing.
    let before = files(&far);
    assert_eq!(
        client.heal(&record, &at(&local, &record)).unwrap(),
        Replaced::HealedByOther
    );
    assert_eq!(files(&far), before);
    assert_eq!(client.list().unwrap(), all);
    close(client, served);
    assert_eq!(temps(&far), vec![format!("{object}#a1b2c3d4")]);
}

/// A record of another dataset than `SCOPE`.
fn record_outside() -> (String, Vec<u8>) {
    let (key, bytes) = record("annotations", &[]);
    (key.replace(SCOPE, "ST999_Elsewhere/dataset"), bytes)
}

fn an_unreadable_far_copy_is_reported_and_never_healed() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
        push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);
        let (mut client, served) = session(&far);
        client.send(listing(&local), &local).unwrap();
        let object = keys_of(&local, Kind::Object)[0].clone();
        let mut path = far.clone();
        path.extend(object.split('/'));
        std::fs::write(&path, b"damaged").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&path).is_ok() {
            eprintln!("skipped: permissions do not stop this user (root?)");
            return;
        }
        let report = client.scrub(true).unwrap();
        assert!(report.damaged.is_empty());
        assert_eq!(report.unreadable.len(), 1);
        assert_eq!(report.unreadable[0].key, object);
        assert!(report.unreadable[0]
            .reason
            .to_lowercase()
            .contains("permission denied"));

        let mut good = local.clone();
        good.extend(object.split('/'));
        let err = client.heal(&object, &good).unwrap_err();
        assert_eq!(far_refusal(&err), (Code::Unreadable, Some(object.as_str())));
        close(client, served);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"damaged");
        assert!(!far.join("quarantine").exists());
        assert!(temps(&far).is_empty());
    }
}

/// Set when dropped.
struct Held(Arc<AtomicBool>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn an_open_guard_is_held_for_the_session_and_a_refusal_leaves_the_store_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let (far, local) = (tmp.path().join("far"), tmp.path().join("local"));
    push_versions(&local, &tmp.path().join("work"), "annotations", &["a\n"]);

    let released = Arc::new(AtomicBool::new(false));
    let opened: Arc<Mutex<Vec<PathBuf>>> = Arc::default();
    let opts = {
        let (released, opened) = (Arc::clone(&released), Arc::clone(&opened));
        ServeOptions::new("guarded").with_open_guard(move |dir| {
            opened.lock().unwrap().push(dir.to_path_buf());
            Ok(Box::new(Held(Arc::clone(&released))) as Box<dyn Send>)
        })
    };
    let (reader, writer, served) = serve_in_process_with(opts);
    let mut client = Client::connect(reader, writer).unwrap();
    client.open(far.to_str().unwrap(), &scope(), true).unwrap();
    assert_eq!(*opened.lock().unwrap(), vec![far.clone()]);
    client.send(listing(&local), &local).unwrap();
    assert!(client.scrub(false).unwrap().is_clean());
    assert!(!released.load(Ordering::SeqCst), "released mid-session");
    client.close().unwrap();
    served.join().unwrap().unwrap();
    assert!(released.load(Ordering::SeqCst), "never released");

    // A guard that cannot be had: busy, and the store is not created.
    let busy = tmp.path().join("busy");
    let refusing = || {
        ServeOptions::new("guarded").with_open_guard(|_| anyhow::bail!("locked by another backup"))
    };
    let (reader, writer, served) = serve_in_process_with(refusing());
    let mut client = Client::connect(reader, writer).unwrap();
    let err = client
        .open(busy.to_str().unwrap(), &scope(), true)
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<exchange::Error>(),
            Some(exchange::Error::Busy)
        ),
        "{err:#}"
    );
    assert!(client.list().is_err());
    close(client, served);
    assert!(!busy.exists());

    // A 0.5 client, which has no `busy`, is told `open`.
    let (reader, writer, served) = serve_in_process_with(refusing());
    let mut raw = Raw::new(reader, writer);
    raw.line("bigstore-exchange-client 1");
    raw.recv_line();
    assert_eq!(raw.recv(), serde_json::json!({"version": {"version": 1}}));
    raw.json(serde_json::json!({"open": {
        "store": busy.to_str().unwrap(), "history": SCOPE, "create": true
    }}));
    assert_eq!(raw.recv(), serde_json::json!({"error": {"code": "open"}}));
    raw.json(serde_json::json!({"close": {}}));
    served.join().unwrap().unwrap();
    assert!(!busy.exists());
}
