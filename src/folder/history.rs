//! The version log of each output, kept on the remote at
//! `bigstore-history/<key>/<time>-<id>.dvc`: one DVC 3 pointer per pushed
//! version. A record's name carries its time and id, so choosing a version
//! needs only a listing; a record is fetched only for its pointer.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt, TryStreamExt};

use super::{block_on, Error, Remote};
use crate::backend;
use crate::dvc::{DvcOutput, DvcPointer};
use crate::types::{Hexdigest, PortableRelPath};

/// Largest history record fetched.
const MAX_RECORD_BYTES: u64 = 64 << 10;
/// Record name time format: fixed width, so name order is time order.
const RECORD_TIME: &str = "%Y%m%dT%H%M%S%.9fZ";

/// Identifies an output across hosts and time, e.g.
/// `ST032_Warrawoona/BeatonsCreek_dataset1_September2026/annotations/reviewer=rick/host=xenoglossicist`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HistoryKey(PortableRelPath);

impl HistoryKey {
    pub fn new(key: &str) -> Result<Self> {
        Ok(Self(PortableRelPath::new(key).with_context(|| {
            Error::InvalidHistoryKey {
                key: key.to_string(),
            }
        })?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// One pushed version of an output.
#[derive(Debug, Clone)]
pub struct HistoryRecord {
    /// Remote key of the record.
    pub key: String,
    /// When it was pushed (UTC), as recorded in the key.
    pub time: DateTime<Utc>,
    pub pointer: DvcPointer,
}

impl HistoryRecord {
    /// The id this version is addressed by: manifest id or file md5.
    pub fn id(&self) -> &Hexdigest {
        output_id(&self.pointer.output)
    }
}

fn output_id(output: &DvcOutput) -> &Hexdigest {
    match output {
        DvcOutput::Dir { manifest, .. } => manifest,
        DvcOutput::File { md5, .. } => md5,
    }
}

/// Which version to restore.
#[derive(Debug, Clone)]
pub enum Selector {
    Latest,
    /// An id prefix of at least 8 hex characters; must match one version.
    Id(String),
    /// The newest version pushed at or before this time (RFC 3339).
    AtOrBefore(String),
}

/// A record as listed: what its name says, before its pointer is fetched.
struct Listed {
    key: String,
    time: DateTime<Utc>,
    /// The version id its name gives, lowercased.
    id: String,
}

/// The time and id in a record's file name; `None` for anything else.
fn parse_record_name(name: &str) -> Option<(DateTime<Utc>, &str)> {
    let (time, rest) = name.split_once('-')?;
    let id = rest.strip_suffix(".dvc")?;
    let time = chrono::NaiveDateTime::parse_from_str(time, RECORD_TIME).ok()?;
    Some((time.and_utc(), id))
}

fn history_prefix(remote: &Remote, key: &HistoryKey) -> String {
    remote.key(&format!("bigstore-history/{}/", key.as_str()))
}

/// The records of `key`, oldest first (ties in time by record key). One
/// listing; nothing is fetched.
async fn list(remote: &Remote, key: &HistoryKey) -> Result<Vec<Listed>> {
    let prefix = history_prefix(remote, key);
    let mut records: Vec<Listed> = backend::list(&remote.backend, &prefix)
        .await?
        .into_iter()
        .filter_map(|object| {
            let name = object.strip_prefix(&prefix)?;
            // Records sit directly under the prefix; deeper keys are other
            // outputs.
            if name.contains('/') {
                return None;
            }
            let (time, id) = parse_record_name(name)?;
            Some(Listed {
                time,
                id: id.to_ascii_lowercase(),
                key: object,
            })
        })
        .collect();
    records.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.key.cmp(&b.key)));
    Ok(records)
}

/// Fetch a listed record. Its pointer must be the version its name says:
/// versions are selected by name.
async fn fetch(remote: &Remote, listed: &Listed) -> Result<HistoryRecord> {
    let object = &listed.key;
    let bytes = backend::get_bytes(&remote.backend, object, MAX_RECORD_BYTES)
        .await?
        .with_context(|| format!("history record {object} vanished"))?;
    let text = String::from_utf8(bytes).with_context(|| format!("{object} is not UTF-8"))?;
    let pointer = DvcPointer::parse(&text).with_context(|| format!("bad record {object}"))?;
    let id = output_id(&pointer.output).to_string();
    anyhow::ensure!(
        id == listed.id,
        "bad record {object}: it holds version {id}, not the {} its name says",
        listed.id
    );
    Ok(HistoryRecord {
        key: object.clone(),
        time: listed.time,
        pointer,
    })
}

/// Fetch `listed`, `jobs` at a time, in order.
async fn fetch_all<'a>(
    remote: &Remote,
    listed: impl IntoIterator<Item = &'a Listed>,
    jobs: usize,
) -> Result<Vec<HistoryRecord>> {
    stream::iter(listed)
        .map(|l| fetch(remote, l))
        .buffered(jobs.max(1))
        .try_collect()
        .await
}

/// Every pushed version of `key`, oldest first: one listing, then every
/// record fetched, `jobs` at a time.
pub fn log(remote: &Remote, key: &HistoryKey, jobs: usize) -> Result<Vec<HistoryRecord>> {
    block_on(async { fetch_all(remote, &list(remote, key).await?, jobs).await })?
}

/// Every history key holding at least one version, sorted: all of them, or
/// `under` and the keys below it (`a/b` matches `a/b` and `a/b/c`, not
/// `a/bc`). One listing; no record is fetched. Objects that are not records
/// are ignored.
pub fn keys(remote: &Remote, under: Option<&HistoryKey>) -> Result<Vec<HistoryKey>> {
    let root = remote.key("bigstore-history/");
    let prefix = match under {
        Some(key) => history_prefix(remote, key),
        None => root.clone(),
    };
    let mut keys: Vec<HistoryKey> = block_on(backend::list(&remote.backend, &prefix))??
        .iter()
        .filter_map(|object| {
            let (key, name) = object.strip_prefix(&root)?.rsplit_once('/')?;
            parse_record_name(name)?;
            HistoryKey::new(key).ok()
        })
        .collect();
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// A [`Selector`] checked before anything is listed.
enum Pick {
    Latest,
    /// Lowercase hex, 8 or more characters.
    Id(String),
    AtOrBefore(DateTime<Utc>),
}

impl Pick {
    fn new(at: &Selector) -> Result<Self> {
        Ok(match at {
            Selector::Latest => Self::Latest,
            Selector::Id(prefix) => {
                if prefix.len() < 8 || !prefix.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(Error::InvalidVersionId {
                        prefix: prefix.clone(),
                    }
                    .into());
                }
                Self::Id(prefix.to_ascii_lowercase())
            }
            Selector::AtOrBefore(when) => Self::AtOrBefore(
                DateTime::parse_from_rfc3339(when)
                    .with_context(|| Error::InvalidTime { time: when.clone() })?
                    .with_timezone(&Utc),
            ),
        })
    }
}

/// The version `at` picks from `key`'s history. Only that record is fetched
/// (for an ambiguous id, the candidates, `jobs` at a time, to report them).
pub(super) async fn select(
    remote: &Remote,
    key: &HistoryKey,
    at: &Selector,
    jobs: usize,
) -> Result<HistoryRecord> {
    let pick = Pick::new(at)?;
    let listed = list(remote, key).await?;
    let found = match pick {
        Pick::Latest => listed.last(),
        Pick::Id(prefix) => {
            let matches: Vec<&Listed> = listed
                .iter()
                .filter(|l| l.id.starts_with(&prefix))
                .collect();
            match matches.as_slice() {
                [] => None,
                [one] => Some(*one),
                many => {
                    let candidates = fetch_all(remote, many.iter().copied(), jobs).await?;
                    return Err(Error::AmbiguousId { prefix, candidates }.into());
                }
            }
        }
        Pick::AtOrBefore(when) => listed.iter().rev().find(|l| l.time <= when),
    };
    match found {
        Some(l) => fetch(remote, l).await,
        None => Err(Error::NoSuchVersion.into()),
    }
}

/// Append `pointer` to `key`'s history unless the latest record is the same
/// version; only that record is fetched. Record names are `<time>-<id>.dvc`
/// with nanosecond time, so two hosts pushing at once never overwrite each
/// other's record. The time is never earlier than the latest record's, so
/// the newest push is always `Latest` even if this host's clock lags.
pub(super) async fn append(
    remote: &Remote,
    key: &HistoryKey,
    pointer: &DvcPointer,
) -> Result<Option<String>> {
    let latest = latest(remote, key).await?;
    if latest
        .as_ref()
        .is_some_and(|r| r.pointer.output == pointer.output)
    {
        return Ok(None);
    }
    let mut time = Utc::now();
    if let Some(r) = &latest {
        time = time.max(r.time + chrono::Duration::nanoseconds(1));
    }
    let record = format!(
        "{}{}-{}.dvc",
        history_prefix(remote, key),
        time.format(RECORD_TIME),
        output_id(&pointer.output)
    );
    backend::put_bytes(&remote.backend, &record, pointer.to_yaml().into_bytes()).await?;
    Ok(Some(record))
}

/// The newest version of `key`, if any; only that record is fetched.
pub(super) async fn latest(remote: &Remote, key: &HistoryKey) -> Result<Option<HistoryRecord>> {
    match list(remote, key).await?.last() {
        Some(l) => Ok(Some(fetch(remote, l).await?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::backend::{self, Backend};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    type BoxFut<'a, T> = Pin<Box<dyn Future<Output = object_store::Result<T>> + Send + 'a>>;

    /// `InMemory` recording every GET (not HEAD) of a history record.
    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        record_gets: Mutex<Vec<String>>,
    }

    impl CountingStore {
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut self.record_gets.lock().unwrap())
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingStore")
        }
    }

    impl ObjectStore for CountingStore {
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
            self.inner.put_opts(location, payload, opts)
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
            if !options.head && location.as_ref().starts_with("bigstore-history/") {
                self.record_gets.lock().unwrap().push(location.to_string());
            }
            self.inner.get_opts(location, options)
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
            self.inner.list(prefix)
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
            self.inner.copy_opts(from, to, options)
        }
    }

    /// Version `i`'s id: its first 8 characters are `i` in hex.
    fn id(i: usize) -> String {
        format!("{i:08x}{}", "0".repeat(24))
    }

    /// A remote on a [`CountingStore`] whose history key `k` already holds
    /// `n` single-file versions, one second apart from 2026-09-01T00:00:01Z,
    /// with ids [`id`]`(1..=n)`.
    fn remote_with_history(n: usize) -> (Remote, Arc<CountingStore>) {
        let store = Arc::new(CountingStore::default());
        let remote = Remote {
            backend: Backend::ObjectStore(store.clone()),
            prefix: String::new(),
        };
        block_on(async {
            for i in 1..=n {
                let md5 = id(i);
                let pointer = DvcPointer {
                    output: DvcOutput::File {
                        md5: Hexdigest::new(&md5, crate::types::HashFunction::Md5).unwrap(),
                        size: 1,
                    },
                    path: "f".into(),
                };
                let key = format!("bigstore-history/k/20260901T0000{i:02}.000000000Z-{md5}.dvc");
                backend::put_bytes(&remote.backend, &key, pointer.to_yaml().into_bytes())
                    .await
                    .unwrap();
            }
        })
        .unwrap();
        (remote, store)
    }

    fn push_opts() -> PushOptions {
        PushOptions::new(HistoryKey::new("k").unwrap())
    }

    fn pull_from(remote: &Remote, at: Selector, into: PathBuf) -> Result<PullReport> {
        pull(
            remote,
            &PointerSource::History {
                key: HistoryKey::new("k").unwrap(),
                at,
            },
            &PullOptions {
                into: Some(into),
                ..PullOptions::default()
            },
        )
    }

    #[test]
    fn push_and_pull_fetch_only_the_record_they_need() {
        let (remote, store) = remote_with_history(50);
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("f");
        std::fs::write(&f, b"new").unwrap();

        let pushed = push(&remote, &f, &push_opts()).unwrap();
        let written = pushed.history_record.expect("a new version");
        let latest_old = "bigstore-history/k/20260901T000050.000000000Z-";
        let gets = store.take();
        assert!(
            matches!(&gets[..], [one] if one.starts_with(latest_old)),
            "push read {} records, not just the latest",
            gets.len()
        );

        pull_from(&remote, Selector::Latest, dir.path().join("latest")).unwrap();
        assert_eq!(store.take(), [written]);

        // Absent objects: the pulls fail after selecting, which is enough.
        let seventh = id(7);
        pull_from(
            &remote,
            Selector::Id(seventh[..8].into()),
            dir.path().join("id"),
        )
        .unwrap_err();
        let record = format!("bigstore-history/k/20260901T000007.000000000Z-{seventh}.dvc");
        assert_eq!(store.take(), [record]);

        let at = Selector::AtOrBefore("2026-09-01T00:00:20.5Z".into());
        pull_from(&remote, at, dir.path().join("at")).unwrap_err();
        let record = format!(
            "bigstore-history/k/20260901T000020.000000000Z-{}.dvc",
            id(20)
        );
        assert_eq!(store.take(), [record]);

        let key = HistoryKey::new("k").unwrap();
        assert_eq!(log(&remote, &key, 4).unwrap().len(), 51);
        assert_eq!(store.take().len(), 51);
    }
}
