//! The version history of each output, kept on the remote under
//! `bigstore-history/<key>/`. A version is a record: a DVC 3 `.dvc`
//! pointer carrying `meta: {bigstore: {parents, writer, time}}`, named
//! `<parents>/<id>.dvc`. `<id>` identifies the record's bytes;
//! `<parents>` is `root` for a first version, else the ids it follows,
//! sorted and `+`-joined (several for a merge, at most 8). So one listing
//! gives the whole graph, and a record is fetched only for its pointer,
//! writer and time. A head is a record no other names as a parent: one
//! head is the latest version, several are a fork.
//!
//! Records bigstore 0.2 wrote, `<time>-<content id>.dvc` directly under the
//! key, are read as a straight line in time order, the oldest first; none
//! is written any more.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::HashSet;

use super::layout::MAX_RECORD_BYTES;
use super::{archived, block_on, each_in_order, CancelToken, Error, Remote, Resolve};
use crate::backend;
use crate::dvc::{BigstoreMeta, DvcOutput, DvcPointer, RecordId};
use crate::types::{Hexdigest, PortableRelPath};

/// The time in a 0.2 record's name.
const LEGACY_TIME: &str = "%Y%m%dT%H%M%S%.9fZ";
/// Most parents a record has, which bounds its name.
const MAX_PARENTS: usize = 8;
/// The parents part of a first version's name.
const ROOT: &str = "root";

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

/// One pushed version of an output: a node of its history.
#[derive(Debug, Clone)]
pub struct HistoryRecord {
    /// Remote key of the record.
    pub key: String,
    pub id: RecordId,
    /// The versions it follows, sorted: none for the first, several for a
    /// merge. A 0.2 record follows the one before it in time.
    pub parents: Vec<RecordId>,
    /// Who pushed it ([`PushOptions::writer`](super::PushOptions::writer));
    /// `None` for a 0.2 record.
    pub writer: Option<String>,
    /// When it was pushed (UTC), as the record says (a 0.2 record's name).
    pub time: DateTime<Utc>,
    /// The version's output and the name it was pushed from; `meta` is
    /// `None`.
    pub pointer: DvcPointer,
}

impl HistoryRecord {
    /// The content this version holds: its manifest id or file md5.
    pub fn output_id(&self) -> &Hexdigest {
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
    /// The head. A forked history has several: [`Error::Diverged`].
    Latest,
    /// An id prefix of at least 8 hex characters: of a record id, or of a
    /// 0.2 record's content id (the md5 in its name, which 0.2 listed as
    /// its id). Must match one version.
    Id(String),
    /// The newest version pushed at or before this time (RFC 3339), by the
    /// time each record holds. Every record is fetched.
    AtOrBefore(String),
}

/// A record as listed: what its name says, before it is fetched.
struct Listed {
    key: String,
    id: RecordId,
    parents: Vec<RecordId>,
    /// A 0.2 record's name: its time and content id (lowercased).
    legacy: Option<(DateTime<Utc>, String)>,
}

/// What a record's name says.
pub(super) enum Name<'a> {
    Linked {
        parents: Vec<RecordId>,
        id: RecordId,
    },
    /// A 0.2 record, and its file name.
    Legacy {
        time: DateTime<Utc>,
        content: &'a str,
        file: &'a str,
    },
}

/// The history key and record name of `rel`, a key below
/// `bigstore-history/`; `None` for anything that is not a record. The two
/// forms cannot be confused: a record id is hex, a 0.2 name has a `-`.
pub(super) fn parse_record_path(rel: &str) -> Option<(&str, Name<'_>)> {
    let (dir, file) = rel.rsplit_once('/')?;
    if let Some((key, parents)) = dir.rsplit_once('/') {
        if let Some(name) = parse_linked(parents, file) {
            return Some((key, name));
        }
    }
    let (time, rest) = file.split_once('-')?;
    let content = rest.strip_suffix(".dvc")?;
    let time = chrono::NaiveDateTime::parse_from_str(time, LEGACY_TIME).ok()?;
    Some((
        dir,
        Name::Legacy {
            time: time.and_utc(),
            content,
            file,
        },
    ))
}

fn parse_linked<'a>(parents: &str, file: &str) -> Option<Name<'a>> {
    let id = RecordId::parse(file.strip_suffix(".dvc")?)?;
    let parents = match parents {
        ROOT => Vec::new(),
        ids => ids
            .split('+')
            .map(RecordId::parse)
            .collect::<Option<Vec<_>>>()
            .filter(|ids| ids.len() <= MAX_PARENTS)?,
    };
    Some(Name::Linked { parents, id })
}

/// The parents part of a record name: `root`, or the ids `+`-joined.
fn parents_name(parents: &[RecordId]) -> String {
    match parents {
        [] => ROOT.to_string(),
        ids => ids
            .iter()
            .map(RecordId::as_str)
            .collect::<Vec<_>>()
            .join("+"),
    }
}

fn history_root(remote: &Remote) -> String {
    remote.key("bigstore-history/")
}

fn history_prefix(remote: &Remote, key: &HistoryKey) -> String {
    remote.key(&format!("bigstore-history/{}/", key.as_str()))
}

/// The records of `key`, from one listing; nothing is fetched. A 0.2
/// record's id is that of its file name, and it follows the 0.2 record
/// before it in time (ties by record key).
async fn list(remote: &Remote, key: &HistoryKey) -> Result<Vec<Listed>> {
    let root = history_root(remote);
    let mut records = Vec::new();
    let mut legacy = Vec::new();
    for backend::Listed { key: object, .. } in
        remote.store.list(&history_prefix(remote, key)).await?
    {
        let Some((of, name)) = object.strip_prefix(&root).and_then(parse_record_path) else {
            continue;
        };
        // Deeper keys are other outputs.
        if of != key.as_str() {
            continue;
        }
        match name {
            Name::Linked { parents, id } => records.push(Listed {
                key: object.clone(),
                id,
                parents,
                legacy: None,
            }),
            Name::Legacy {
                time,
                content,
                file,
            } => legacy.push(Listed {
                id: RecordId::digest(file.as_bytes()),
                parents: Vec::new(),
                legacy: Some((time, content.to_ascii_lowercase())),
                key: object.clone(),
            }),
        }
    }
    let time = |l: &Listed| l.legacy.as_ref().map(|(time, _)| *time);
    legacy.sort_by(|a, b| time(a).cmp(&time(b)).then_with(|| a.key.cmp(&b.key)));
    let mut previous: Option<RecordId> = None;
    for mut l in legacy {
        l.parents.extend(previous.replace(l.id.clone()));
        records.push(l);
    }
    Ok(records)
}

/// The records no other record names as a parent, by id.
fn heads(listed: &[Listed]) -> Vec<&Listed> {
    let named: HashSet<&RecordId> = listed.iter().flat_map(|l| &l.parents).collect();
    let mut heads: Vec<&Listed> = listed.iter().filter(|l| !named.contains(&l.id)).collect();
    heads.sort_by(|a, b| a.id.cmp(&b.id));
    heads.dedup_by(|a, b| a.id == b.id);
    heads
}

fn ids(listed: &[&Listed]) -> Vec<RecordId> {
    listed.iter().map(|l| l.id.clone()).collect()
}

/// Fetch a listed record. It must be what its name says, since versions
/// are chosen by name: a record's bytes must have its id, and its parents
/// must be those its name gives; a 0.2 record must hold the content its
/// name gives.
async fn fetch(remote: &Remote, listed: &Listed) -> Result<HistoryRecord> {
    let object = &listed.key;
    let bytes = remote
        .store
        .get(object, MAX_RECORD_BYTES)
        .await
        .map_err(archived)?
        .with_context(|| format!("history record {object} vanished"))?;
    let digest = RecordId::digest(&bytes);
    let text = String::from_utf8(bytes).with_context(|| format!("{object} is not UTF-8"))?;
    let pointer = DvcPointer::parse(&text).with_context(|| format!("bad record {object}"))?;
    let (writer, time) = match (&listed.legacy, pointer.meta.clone()) {
        (Some((time, content)), None) => {
            let id = output_id(&pointer.output).to_string();
            anyhow::ensure!(
                id == *content,
                "bad record {object}: it holds version {id}, not the {content} its name says"
            );
            (None, *time)
        }
        (
            None,
            Some(BigstoreMeta::Record {
                parents,
                writer,
                time,
            }),
        ) => {
            anyhow::ensure!(
                digest == listed.id,
                "bad record {object}: its content is record {digest}, not the {} its name says",
                listed.id
            );
            anyhow::ensure!(
                parents == listed.parents,
                "bad record {object}: it follows {}, not the {} its name says",
                parents_name(&parents),
                parents_name(&listed.parents)
            );
            (Some(writer), time)
        }
        _ => anyhow::bail!("bad record {object}: its meta is not what its name calls for"),
    };
    Ok(HistoryRecord {
        key: object.clone(),
        id: listed.id.clone(),
        parents: listed.parents.clone(),
        writer,
        time,
        pointer: DvcPointer {
            meta: None,
            ..pointer
        },
    })
}

/// Fetch `listed`, `jobs` at a time, in order. Once `cancel` is cancelled
/// no fetch starts, and the call is [`Error::Cancelled`].
async fn fetch_all<'a, I>(
    remote: &Remote,
    listed: I,
    jobs: usize,
    cancel: &CancelToken,
) -> Result<Vec<HistoryRecord>>
where
    I: IntoIterator<Item = &'a Listed>,
    I::IntoIter: Send,
{
    each_in_order(listed, jobs, |l| async move {
        cancel.check()?;
        fetch(remote, l).await
    })
    .await
}

/// How to read a log. `LogOptions::default()` fetches 8 records at a time
/// and cannot be cancelled.
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Records fetched at once (at least 1).
    pub jobs: usize,
    /// Stops the log between record fetches: no fetch starts once it is
    /// cancelled, and the call returns [`Error::Cancelled`].
    pub cancel: CancelToken,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            jobs: crate::transfer::DEFAULT_CONCURRENCY,
            cancel: CancelToken::default(),
        }
    }
}

/// Every pushed version of `key`, oldest first (ties by record key), each
/// with its parents: one listing, then every record fetched, `opts.jobs`
/// at a time.
pub fn log(remote: &Remote, key: &HistoryKey, opts: &LogOptions) -> Result<Vec<HistoryRecord>> {
    block_on("log", log_async(remote, key, opts))?
}

/// [`log`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn log_async(
    remote: &Remote,
    key: &HistoryKey,
    opts: &LogOptions,
) -> Result<Vec<HistoryRecord>> {
    let listed = list(remote, key).await?;
    let mut records = fetch_all(remote, &listed, opts.jobs, &opts.cancel).await?;
    records.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.key.cmp(&b.key)));
    Ok(records)
}

/// Every history key holding at least one version, sorted: all of them, or
/// `under` and the keys below it (`a/b` matches `a/b` and `a/b/c`, not
/// `a/bc`). One listing; no record is fetched. Objects that are not records
/// are ignored, and so are records under a key [`HistoryKey::new`] rejects
/// (written by another tool, or by hand, with a non-portable name): such a
/// key is left out silently, not reported.
pub fn keys(remote: &Remote, under: Option<&HistoryKey>) -> Result<Vec<HistoryKey>> {
    block_on("keys", keys_async(remote, under))?
}

/// [`keys`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn keys_async(remote: &Remote, under: Option<&HistoryKey>) -> Result<Vec<HistoryKey>> {
    let root = history_root(remote);
    let prefix = match under {
        Some(key) => history_prefix(remote, key),
        None => root.clone(),
    };
    let mut keys: Vec<HistoryKey> = remote
        .store
        .list(&prefix)
        .await?
        .iter()
        .filter_map(|object| {
            let (key, _) = parse_record_path(object.key.strip_prefix(&root)?)?;
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

/// The version `at` picks from `key`'s history. `Latest` and `Id` fetch
/// only that record (for an ambiguous id, the candidates, to report them);
/// `AtOrBefore` fetches every record, `jobs` at a time, stopping once
/// `cancel` is cancelled.
pub(super) async fn select(
    remote: &Remote,
    key: &HistoryKey,
    at: &Selector,
    jobs: usize,
    cancel: &CancelToken,
) -> Result<HistoryRecord> {
    let pick = Pick::new(at)?;
    let listed = list(remote, key).await?;
    let found = match pick {
        Pick::Latest => match heads(&listed)[..] {
            [] if listed.is_empty() => None,
            [] => return Err(Error::NoHead { key: key.clone() }.into()),
            [one] => Some(one),
            ref many => return Err(Error::Diverged { heads: ids(many) }.into()),
        },
        Pick::Id(prefix) => {
            let matches: Vec<&Listed> = listed
                .iter()
                .filter(|l| {
                    l.id.as_str().starts_with(&prefix)
                        || l.legacy
                            .as_ref()
                            .is_some_and(|(_, content)| content.starts_with(&prefix))
                })
                .collect();
            match matches[..] {
                [] => None,
                [one] => Some(one),
                ref many => {
                    let candidates = fetch_all(remote, many.iter().copied(), jobs, cancel).await?;
                    return Err(Error::AmbiguousId { prefix, candidates }.into());
                }
            }
        }
        Pick::AtOrBefore(when) => {
            return fetch_all(remote, &listed, jobs, cancel)
                .await?
                .into_iter()
                .filter(|r| r.time <= when)
                .max_by(|a, b| a.time.cmp(&b.time).then_with(|| a.key.cmp(&b.key)))
                .ok_or_else(|| Error::NoSuchVersion.into());
        }
    };
    match found {
        Some(l) => fetch(remote, l).await,
        None => Err(Error::NoSuchVersion.into()),
    }
}

/// The version `id` of `key`'s history: one listing, then that record.
pub(super) async fn find(
    remote: &Remote,
    key: &HistoryKey,
    id: &RecordId,
) -> Result<HistoryRecord> {
    let listed = list(remote, key).await?;
    match listed.iter().find(|l| l.id == *id) {
        Some(l) => fetch(remote, l).await,
        None => Err(Error::NoSuchVersion.into()),
    }
}

/// The heads of a history.
pub(super) enum Heads {
    /// No version yet.
    None,
    /// The latest version.
    One(Box<HistoryRecord>),
    /// A fork, sorted by id.
    Many(Vec<HistoryRecord>),
}

/// The heads of `key`'s history, fetched `jobs` at a time: one listing,
/// then only the heads.
pub(super) async fn heads_of(remote: &Remote, key: &HistoryKey, jobs: usize) -> Result<Heads> {
    let listed = list(remote, key).await?;
    Ok(match heads(&listed)[..] {
        [] if listed.is_empty() => Heads::None,
        [] => return Err(Error::NoHead { key: key.clone() }.into()),
        [one] => Heads::One(Box::new(fetch(remote, one).await?)),
        ref many => Heads::Many(
            fetch_all(remote, many.iter().copied(), jobs, &CancelToken::default()).await?,
        ),
    })
}

/// What a push does to history, decided before anything is uploaded.
pub(super) enum Next {
    /// The output already is the only head: publish nothing; the head
    /// becomes its base.
    Adopt(RecordId),
    /// Publish a record following `parents`, timed after each of them.
    Publish {
        parents: Vec<RecordId>,
        after: Option<DateTime<Utc>>,
    },
}

/// The base a `.dvc` records, if it is one bigstore wrote beside an output.
pub(super) fn base_of(local: Option<&DvcPointer>) -> Option<&RecordId> {
    match local?.meta.as_ref()? {
        BigstoreMeta::Base(id) => Some(id),
        BigstoreMeta::Record { .. } => None,
    }
}

/// Whether an output whose `.dvc` is `local` was last synced to `head`:
/// its base is the head or, for a `.dvc` without a base (0.2 wrote it), it
/// records the head's content.
pub(super) fn follows(head: &HistoryRecord, local: Option<&DvcPointer>) -> bool {
    match base_of(local) {
        Some(base) => *base == head.id,
        None => local.is_some_and(|p| p.output == head.pointer.output),
    }
}

/// Decide what pushing `output`, whose `.dvc` is `local`, does to `key`'s
/// history, in order: an output equal to the only head adopts it (so a
/// missing or stale base, as after a crash between the record and the
/// `.dvc`, is no refusal); an output that does not [`follow`](follows) the
/// only head, having no `.dvc` or another base, is [`Error::StaleBase`];
/// with several heads it is [`Error::Diverged`] unless `resolve` merges
/// them, which needs a base among them.
pub(super) async fn next(
    remote: &Remote,
    key: &HistoryKey,
    output: &DvcOutput,
    local: Option<&DvcPointer>,
    resolve: Resolve,
    jobs: usize,
) -> Result<Next> {
    let base = base_of(local);
    let stale = |heads| Error::StaleBase {
        base: base.cloned(),
        heads,
    };
    Ok(match heads_of(remote, key, jobs).await? {
        Heads::None => Next::Publish {
            parents: Vec::new(),
            after: None,
        },
        Heads::One(head) if head.pointer.output == *output => Next::Adopt(head.id),
        Heads::One(head) if follows(&head, local) => Next::Publish {
            parents: vec![head.id],
            after: Some(head.time),
        },
        Heads::One(head) => return Err(stale(vec![head.id]).into()),
        Heads::Many(heads) => {
            let ids: Vec<RecordId> = heads.iter().map(|h| h.id.clone()).collect();
            match base {
                None => return Err(stale(ids).into()),
                Some(_) if resolve != Resolve::Merge => {
                    return Err(Error::Diverged { heads: ids }.into())
                }
                Some(base) if !ids.contains(base) => return Err(stale(ids).into()),
                Some(_) if ids.len() > MAX_PARENTS => {
                    return Err(Error::TooManyHeads {
                        key: key.clone(),
                        heads: ids,
                        max: MAX_PARENTS,
                    }
                    .into())
                }
                Some(_) => Next::Publish {
                    after: heads.iter().map(|h| h.time).max(),
                    parents: ids,
                },
            }
        }
    })
}

/// Publish `pointer` as a version of `key` following `parents` (sorted),
/// pushed by `writer`, under a create-once name. Its time is now, but
/// never before `after`, so a version is never older than one it follows
/// even if this host's clock lags. Returns its id and remote key.
pub(super) async fn append(
    remote: &Remote,
    key: &HistoryKey,
    pointer: &DvcPointer,
    parents: Vec<RecordId>,
    after: Option<DateTime<Utc>>,
    writer: &str,
) -> Result<(RecordId, String)> {
    let mut time = Utc::now();
    if let Some(after) = after {
        time = time.max(after + chrono::Duration::nanoseconds(1));
    }
    let dir = parents_name(&parents);
    let record = DvcPointer {
        meta: Some(BigstoreMeta::Record {
            parents,
            writer: writer.to_string(),
            time,
        }),
        ..pointer.clone()
    };
    let bytes = record.to_yaml().into_bytes();
    let id = RecordId::digest(&bytes);
    let object = format!("{}{dir}/{id}.dvc", history_prefix(remote, key));
    remote.store.put(&object, bytes).await?;
    Ok((id, object))
}

/// The records other than `id` that follow one of its `parents` (or, for
/// a first version, the other first versions): pushes that raced it from
/// the same base, sorted. One listing.
pub(super) async fn forked_with(
    remote: &Remote,
    key: &HistoryKey,
    id: &RecordId,
    parents: &[RecordId],
) -> Result<Vec<RecordId>> {
    let mut ids: Vec<RecordId> = list(remote, key)
        .await?
        .into_iter()
        .filter(|l| {
            l.id != *id
                && if parents.is_empty() {
                    l.parents.is_empty()
                } else {
                    l.parents.iter().any(|p| parents.contains(p))
                }
        })
        .map(|l| l.id)
        .collect();
    ids.sort();
    ids.dedup();
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::backend::Store;
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

    /// `InMemory` recording every GET (not HEAD) of a history record, and
    /// cancelling `cancel_on_get` at each.
    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        record_gets: Mutex<Vec<String>>,
        cancel_on_get: CancelToken,
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
                self.cancel_on_get.cancel();
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

    /// Version `i`'s content id: its first 8 characters are `i` in hex.
    fn id(i: usize) -> String {
        format!("{i:08x}{}", "0".repeat(24))
    }

    /// Version `i`'s 0.2 record name.
    fn legacy_name(i: usize) -> String {
        format!("20260901T0000{i:02}.000000000Z-{}.dvc", id(i))
    }

    /// A remote on a [`CountingStore`] whose history key `k` already holds
    /// `n` single-file 0.2 versions, one second apart from
    /// 2026-09-01T00:00:01Z, named [`legacy_name`]`(1..=n)`.
    fn remote_with_history(n: usize) -> (Remote, Arc<CountingStore>) {
        let store = Arc::new(CountingStore::default());
        let remote = Remote {
            store: Store::from_object_store(store.clone()),
            prefix: String::new(),
            local: None,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            for i in 1..=n {
                let pointer = DvcPointer {
                    output: DvcOutput::File {
                        md5: Hexdigest::new(&id(i), crate::types::HashFunction::Md5).unwrap(),
                        size: 1,
                    },
                    path: "f".into(),
                    meta: None,
                };
                let key = format!("bigstore-history/k/{}", legacy_name(i));
                remote
                    .store
                    .put(&key, pointer.to_yaml().into_bytes())
                    .await
                    .unwrap();
            }
        });
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
    fn push_pull_and_log_fetch_only_the_records_they_need() {
        let (remote, store) = remote_with_history(50);
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("f");
        std::fs::write(&f, b"new").unwrap();
        // Last synced to the 50th version.
        let synced = DvcPointer {
            output: DvcOutput::File {
                md5: Hexdigest::new(&id(50), crate::types::HashFunction::Md5).unwrap(),
                size: 1,
            },
            path: "f".into(),
            meta: Some(BigstoreMeta::Base(RecordId::digest(
                legacy_name(50).as_bytes(),
            ))),
        };
        std::fs::write(dir.path().join("f.dvc"), synced.to_yaml()).unwrap();

        let pushed = push(&remote, &f, &push_opts()).unwrap();
        let written = pushed.outcome.record().expect("a new version").to_string();
        let head = format!("bigstore-history/k/{}", legacy_name(50));
        assert_eq!(store.take(), [head], "push reads only the head");

        pull_from(&remote, Selector::Latest, dir.path().join("latest")).unwrap();
        assert_eq!(store.take(), [written]);

        // Absent objects: the pull fails after selecting, which is enough.
        let seventh = RecordId::digest(legacy_name(7).as_bytes());
        pull_from(
            &remote,
            Selector::Id(seventh.as_str()[..8].into()),
            dir.path().join("id"),
        )
        .unwrap_err();
        let record = format!("bigstore-history/k/{}", legacy_name(7));
        assert_eq!(store.take(), [record]);

        // A time needs every record's.
        let at = Selector::AtOrBefore("2026-09-01T00:00:20.5Z".into());
        let err = pull_from(&remote, at, dir.path().join("at")).unwrap_err();
        let object = format!("files/md5/00/{}", &id(20)[2..]);
        assert!(format!("{err:#}").contains(&object), "{err:#}");
        assert_eq!(store.take().len(), 51);

        let key = HistoryKey::new("k").unwrap();
        assert_eq!(
            log(&remote, &key, &LogOptions::default()).unwrap().len(),
            51
        );
        assert_eq!(store.take().len(), 51);
    }

    #[test]
    fn log_stops_between_record_fetches_when_cancelled() {
        let (remote, store) = remote_with_history(10);
        let opts = LogOptions {
            jobs: 1,
            cancel: store.cancel_on_get.clone(),
        };
        let err = log(&remote, &HistoryKey::new("k").unwrap(), &opts).unwrap_err();
        assert!(
            matches!(err.downcast_ref::<Error>(), Some(Error::Cancelled)),
            "{err:#}"
        );
        assert_eq!(store.take().len(), 1, "no fetch starts once cancelled");
    }

    #[test]
    fn reading_an_archived_object_or_record_is_an_archived_error() {
        use crate::backend::testing::{Fault, FaultStore};
        let archived = |under| Remote {
            store: Store::from_object_store(Arc::new(FaultStore::new(
                under,
                Fault::Forbidden("InvalidObjectState"),
            ))),
            prefix: String::new(),
            local: None,
        };
        let archived_key = |err: anyhow::Error| match err.downcast_ref::<Error>() {
            Some(Error::Archived { key }) => key.clone(),
            _ => panic!("{err:#}"),
        };
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"x").unwrap();
        let tree = dir.path().join("d");
        std::fs::create_dir(&tree).unwrap();
        std::fs::write(tree.join("a"), b"a").unwrap();
        let d_opts = PushOptions::new(HistoryKey::new("d").unwrap());

        // Objects: a file's content, and a directory's `.dir` manifest.
        let remote = archived("files/");
        let pushed = push(&remote, &file, &push_opts()).unwrap();
        let key =
            archived_key(pull_from(&remote, Selector::Latest, dir.path().join("o")).unwrap_err());
        assert_eq!(
            key,
            remote.object_key(super::output_id(&pushed.pointer.output))
        );
        let pushed = push(&remote, &tree, &d_opts).unwrap();
        let err = pull(
            &remote,
            &PointerSource::File(pushed.pointer_path),
            &PullOptions {
                into: Some(dir.path().join("o2")),
                ..PullOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            archived_key(err),
            remote.manifest_key(super::output_id(&pushed.pointer.output))
        );

        // History records.
        let remote = archived("bigstore-history/");
        let pushed = push(&remote, &file, &push_opts()).unwrap();
        let err = log(
            &remote,
            &HistoryKey::new("k").unwrap(),
            &LogOptions::default(),
        )
        .unwrap_err();
        assert_eq!(Some(archived_key(err).as_str()), pushed.outcome.record());
    }
}
