//! Remote storage: [`Store`], one bucket or remote behind object_store or
//! the rclone binary, with the operations bigstore needs of it.

pub(crate) mod rclone;
pub mod store;
#[cfg(test)]
pub(crate) mod testing;

use anyhow::Result;
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt};
use std::path::Path;
use std::sync::Arc;

use crate::config::{BackendConfig, BigstoreConfig};
use crate::types::Hexdigest;

/// A remote object store. Keys are `/`-separated object names, used as
/// given: callers add any prefix of their own.
pub struct Store {
    transport: Transport,
}

enum Transport {
    ObjectStore(Arc<dyn ObjectStore>),
    Rclone(rclone::RcloneBackend),
}

/// What [`Store::head`] knows of an object.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ObjectMeta {
    pub size: u64,
    pub modified: DateTime<Utc>,
}

/// An object as [`Store::list`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Listed {
    pub key: String,
    pub size: u64,
    pub modified: DateTime<Utc>,
}

/// A failure a caller may want to handle by kind, found with
/// `err.downcast_ref::<backend::Error>()`. Anything else a [`Store`] fails
/// with is a plain [`anyhow::Error`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// `key` is in an archive storage class (S3 GLACIER or DEEP_ARCHIVE)
    /// and was not restored, so it cannot be read: the store answered
    /// `InvalidObjectState`. Restore it, then read again.
    Archived { key: String },
    /// [`store::build_strict_s3`] was asked for credentials from the
    /// environment, and `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` is
    /// unset or empty.
    CredentialsMissing,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archived { key } => write!(
                f,
                "{key} is archived (InvalidObjectState): restore it before reading it"
            ),
            Self::CredentialsMissing => f.write_str(
                "S3 credentials missing: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must both be set",
            ),
        }
    }
}

impl std::error::Error for Error {}

impl Store {
    /// The store `cfg` names.
    pub fn open(cfg: &BigstoreConfig) -> Result<Self> {
        let transport = match &cfg.backend {
            BackendConfig::S3 { .. } | BackendConfig::Gcs { .. } | BackendConfig::Azure { .. } => {
                Transport::ObjectStore(store::build_object_store(&cfg.backend)?.into())
            }
            BackendConfig::Rclone { remote } => {
                Transport::Rclone(rclone::RcloneBackend::new(remote.clone()))
            }
            BackendConfig::Local { path } => {
                Transport::ObjectStore(store::build_local_store(path)?.into())
            }
        };
        Ok(Self { transport })
    }

    /// A store on an object_store client the caller built.
    pub fn from_object_store(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            transport: Transport::ObjectStore(store),
        }
    }

    /// The size and modification time of the object at `key`; `Ok(None)` if
    /// there is none. A failure to reach the store is an error, never `None`.
    pub async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match &self.transport {
            Transport::ObjectStore(store) => {
                match store.head(&object_store::path::Path::from(key)).await {
                    Ok(meta) => Ok(Some(ObjectMeta {
                        size: meta.size,
                        modified: meta.last_modified,
                    })),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Transport::Rclone(r) => r.stat(key).await,
        }
    }

    /// Read a whole object into memory, refusing anything over `limit` bytes.
    /// `Ok(None)` if the object does not exist.
    pub async fn get(&self, key: &str, limit: u64) -> Result<Option<Vec<u8>>> {
        match &self.transport {
            Transport::ObjectStore(store) => {
                let result = match store.get(&object_store::path::Path::from(key)).await {
                    Ok(r) => r,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(e) => return Err(read_error(key, e)),
                };
                check_limit(key, result.meta.size, limit)?;
                let length = result.range.end - result.range.start;
                let bytes = result.bytes().await?;
                check_complete(key, bytes.len() as u64, length)?;
                Ok(Some(bytes.to_vec()))
            }
            Transport::Rclone(r) => {
                let Some(meta) = r.stat(key).await? else {
                    return Ok(None);
                };
                check_limit(key, meta.size, limit)?;
                // No handle open while rclone writes: see `rclone_into`.
                let path = tempfile::NamedTempFile::new()?.into_temp_path();
                r.download(key, &path).await?;
                check_limit(key, tokio::fs::metadata(&path).await?.len(), limit)?;
                Ok(Some(tokio::fs::read(&path).await?))
            }
        }
    }

    /// Store `bytes` at `key` (small objects: manifests, pointers).
    pub async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        match &self.transport {
            Transport::ObjectStore(store) => {
                store
                    .put(&object_store::path::Path::from(key), bytes.into())
                    .await?;
                Ok(())
            }
            Transport::Rclone(r) => {
                let tmp = tempfile::NamedTempFile::new()?;
                tokio::fs::write(tmp.path(), &bytes).await?;
                r.upload(tmp.path(), key).await
            }
        }
    }

    /// Store the file at `path` at `key`, streaming it: large files go up
    /// in parts, and a failed read aborts the upload (see `put_streaming`).
    pub async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        match &self.transport {
            Transport::ObjectStore(store) => {
                let file = tokio::fs::File::open(path).await?;
                put_streaming(Arc::clone(store), object_store::path::Path::from(key), file).await
            }
            Transport::Rclone(r) => r.upload(path, key).await,
        }
    }

    /// Every object whose key starts with `prefix` (a key prefix ending at a
    /// `/`), sorted by key.
    pub async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        let mut listed = match &self.transport {
            Transport::ObjectStore(store) => {
                use futures::TryStreamExt;
                let prefix = object_store::path::Path::from(prefix);
                store
                    .list(Some(&prefix))
                    .map_ok(|meta| Listed {
                        key: meta.location.to_string(),
                        size: meta.size,
                        modified: meta.last_modified,
                    })
                    .try_collect::<Vec<_>>()
                    .await?
            }
            Transport::Rclone(r) => r.list(prefix).await?,
        };
        listed.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(listed)
    }

    /// Download `key` into a new temp file in `dir`, hashing while it
    /// streams, and return the file only if its content is `expected`. The
    /// file is created, written and hashed on the blocking pool.
    pub async fn download_verified(
        &self,
        key: &str,
        expected: &Hexdigest,
        dir: &Path,
    ) -> Result<tempfile::NamedTempFile> {
        use crate::hash::Hasher;

        let dir = dir.to_path_buf();
        let tmp = blocking(move || Ok(tempfile::NamedTempFile::new_in(dir)?)).await?;
        let hash_fn = expected.hash_fn();
        let (tmp, actual) = match &self.transport {
            Transport::ObjectStore(store) => {
                use futures::StreamExt;
                use std::io::Write;
                let result = store
                    .get(&object_store::path::Path::from(key))
                    .await
                    .map_err(|e| read_error(key, e))?;
                let length = result.range.end - result.range.start;
                let mut stream = result.into_stream();
                // The file and hasher go to the blocking pool with each chunk.
                let mut state = (tmp, Hasher::new(hash_fn));
                let mut got = 0;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    got += chunk.len() as u64;
                    state = blocking(move || {
                        let (mut tmp, mut hasher) = state;
                        hasher.update(&chunk);
                        tmp.write_all(&chunk)?;
                        Ok((tmp, hasher))
                    })
                    .await?;
                }
                check_complete(key, got, length)?;
                let (tmp, hasher) = state;
                (tmp, hasher.finalize())
            }
            Transport::Rclone(r) => {
                let tmp = rclone_into(r, key, tmp).await?;
                blocking(move || {
                    let actual = crate::hash::hash_file(tmp.path(), hash_fn)?;
                    Ok((tmp, actual))
                })
                .await?
            }
        };
        anyhow::ensure!(
            actual == *expected,
            "integrity check failed for {key}: expected {expected}, got {actual}"
        );
        Ok(tmp)
    }
}

fn check_limit(key: &str, size: u64, limit: u64) -> Result<()> {
    anyhow::ensure!(
        size <= limit,
        "{key} is {size} bytes, over the {limit}-byte limit"
    );
    Ok(())
}

/// A body that ended before the object's length: cut short in transit, or
/// a store that sends less than it says it holds. Not [`Error::Archived`]:
/// only the store's own answer says that.
fn check_complete(key: &str, got: u64, length: u64) -> Result<()> {
    anyhow::ensure!(
        got >= length,
        "incomplete download of {key} ({got}/{length} bytes)"
    );
    Ok(())
}

/// `err`, from a GET of `key`, as [`Error::Archived`] if the store answered
/// with S3's `InvalidObjectState` error code, otherwise unchanged. A 403
/// alone is not enough: it is usually a permission failure. object_store
/// keeps S3's error code only in the error's message (the response body),
/// so that is where it is read from.
fn read_error(key: &str, err: object_store::Error) -> anyhow::Error {
    if s3_error_code(&err).as_deref() == Some("InvalidObjectState") {
        return Error::Archived { key: key.into() }.into();
    }
    err.into()
}

/// The `<Code>` of the first S3 error response in `err`'s chain.
fn s3_error_code(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut next = Some(err);
    while let Some(e) = next {
        let text = e.to_string();
        let code = text
            .split_once("<Code>")
            .and_then(|(_, rest)| rest.split_once("</Code>"));
        if let Some((code, _)) = code {
            return Some(code.to_owned());
        }
        next = e.source();
    }
    None
}

/// Run blocking filesystem or CPU work on tokio's blocking pool, so it never
/// stalls a runtime worker thread (the caller's, for the library's async
/// API). A panic in `work` resumes in the caller.
pub(crate) async fn blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(e) => match e.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(e) => Err(e.into()),
        },
    }
}

/// Have rclone write `key` to `tmp`'s path. rclone replaces the file by
/// renaming its own partial download over it, which Windows refuses while any
/// handle to the file is open ("Access is denied"), so ours is closed for the
/// download and the file reopened afterwards (on the blocking pool).
async fn rclone_into(
    r: &rclone::RcloneBackend,
    key: &str,
    tmp: tempfile::NamedTempFile,
) -> Result<tempfile::NamedTempFile> {
    let path = tmp.into_temp_path();
    r.download(key, &path).await?;
    blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(tempfile::NamedTempFile::from_parts(file, path))
    })
    .await
}

/// Stream `reader` into `path`. A failed read aborts any multipart upload
/// already started, so no in-progress upload (billed storage on S3) or partial
/// object is left behind.
async fn put_streaming(
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    mut reader: impl tokio::io::AsyncRead + Unpin,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    // BufWriter issues a single PUT for objects under its capacity and
    // switches to multipart (10 MiB parts, safely above S3's 5 MiB
    // minimum) above it — sizing parts correctly regardless of how the
    // file reads back. A raw put_part loop would forward short reads
    // verbatim and risk an EntityTooSmall rejection on complete().
    let mut writer = object_store::buffered::BufWriter::new(store, path);
    if let Err(e) = tokio::io::copy(&mut reader, &mut writer).await {
        // Safe only before shutdown: BufWriter::abort panics once shutdown has
        // been polled. A failing shutdown needs no abort — WriteMultipart::finish
        // aborts the upload itself when a part fails.
        //
        // Known gap (object_store 0.14): if the read fails while
        // CreateMultipartUpload is still in flight, BufWriter is in its
        // Prepare state, abort() is a no-op and an upload the server already
        // created is orphaned. One round trip wide; a bucket lifecycle rule
        // for incomplete multipart uploads is the backstop.
        let err = anyhow::Error::from(e);
        return Err(match writer.abort().await {
            Ok(()) => err,
            Err(abort_err) => err.context(format!(
                "aborting the incomplete multipart upload also failed: {abort_err}"
            )),
        });
    }
    writer.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    type BoxFut<'a, T> = Pin<Box<dyn Future<Output = object_store::Result<T>> + Send + 'a>>;

    /// Multipart bookkeeping as a real service (S3, GCS) sees it: an upload
    /// that is neither completed nor aborted stays in progress — and billed —
    /// after the client drops it.
    #[derive(Debug, Default)]
    struct Uploads {
        started: AtomicUsize,
        in_progress: AtomicUsize,
    }

    /// `InMemory` wrapper recording multipart lifecycles. `ObjectStore` and
    /// `MultipartUpload` are `#[async_trait]` traits; the methods below are
    /// written in the desugared form to avoid a dev-dependency on the macro.
    #[derive(Debug)]
    struct TrackingStore {
        inner: InMemory,
        uploads: Arc<Uploads>,
    }

    impl std::fmt::Display for TrackingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("TrackingStore")
        }
    }

    #[derive(Debug)]
    struct TrackedUpload {
        inner: Box<dyn MultipartUpload>,
        uploads: Arc<Uploads>,
    }

    impl MultipartUpload for TrackedUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            self.inner.put_part(data)
        }

        fn complete<'s, 'a>(&'s mut self) -> BoxFut<'a, PutResult>
        where
            's: 'a,
            Self: 'a,
        {
            Box::pin(async move {
                let result = self.inner.complete().await?;
                self.uploads.in_progress.fetch_sub(1, Ordering::SeqCst);
                Ok(result)
            })
        }

        fn abort<'s, 'a>(&'s mut self) -> BoxFut<'a, ()>
        where
            's: 'a,
            Self: 'a,
        {
            Box::pin(async move {
                self.inner.abort().await?;
                self.uploads.in_progress.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    impl ObjectStore for TrackingStore {
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
            Box::pin(async move {
                let inner = self.inner.put_multipart_opts(location, opts).await?;
                self.uploads.started.fetch_add(1, Ordering::SeqCst);
                self.uploads.in_progress.fetch_add(1, Ordering::SeqCst);
                let upload = TrackedUpload {
                    inner,
                    uploads: Arc::clone(&self.uploads),
                };
                Ok(Box::new(upload) as Box<dyn MultipartUpload>)
            })
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

    /// Yields `remaining` zero bytes, then fails like a disk read error.
    struct FailingReader {
        remaining: usize,
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.remaining == 0 {
                return Poll::Ready(Err(std::io::Error::other("simulated read failure")));
            }
            let n = self.remaining.min(buf.remaining());
            buf.initialize_unfilled_to(n).fill(0);
            buf.advance(n);
            self.remaining -= n;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn failed_read_aborts_multipart_upload() {
        let uploads = Arc::new(Uploads::default());
        let store = Arc::new(TrackingStore {
            inner: InMemory::new(),
            uploads: Arc::clone(&uploads),
        });
        let path = StorePath::from("files/sha256/ab/cdef");

        // 15 MiB is past BufWriter's 10 MiB single-PUT capacity, so the
        // multipart upload has started when the read fails.
        let reader = FailingReader {
            remaining: 15 * 1024 * 1024,
        };
        let err = put_streaming(store.clone(), path.clone(), reader)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("simulated read failure"),
            "{err:#}"
        );

        assert_eq!(
            uploads.started.load(Ordering::SeqCst),
            1,
            "multipart path not taken"
        );
        assert_eq!(
            uploads.in_progress.load(Ordering::SeqCst),
            0,
            "failed upload left a multipart upload in progress"
        );
        assert!(matches!(
            store.inner.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    const KEY: &str = "files/md5/ab/cdef";
    const BODY: &[u8] = &[7; 64];

    /// A store holding [`BODY`] at [`KEY`] whose reads of it fail with
    /// `fault`, and the digest `download_verified` expects of it.
    async fn faulty(fault: testing::Fault) -> (Store, Hexdigest) {
        let store = Store::from_object_store(Arc::new(testing::FaultStore::new("files/", fault)));
        store.put(KEY, BODY.to_vec()).await.unwrap();
        let mut hasher = crate::hash::Hasher::new(crate::types::HashFunction::Md5);
        hasher.update(BODY);
        (store, hasher.finalize())
    }

    /// `get` and `download_verified` of [`KEY`], both failing.
    async fn read_errors(store: &Store, md5: &Hexdigest) -> [anyhow::Error; 2] {
        let dir = tempfile::tempdir().unwrap();
        [
            store.get(KEY, 1 << 20).await.unwrap_err(),
            store
                .download_verified(KEY, md5, dir.path())
                .await
                .unwrap_err(),
        ]
    }

    #[tokio::test]
    async fn a_read_the_store_refuses_as_invalid_object_state_is_archived() {
        let (store, md5) = faulty(testing::Fault::Forbidden("InvalidObjectState")).await;
        for err in read_errors(&store, &md5).await {
            assert!(
                matches!(err.downcast_ref::<Error>(), Some(Error::Archived { key }) if key == KEY),
                "{err:#}"
            );
        }
    }

    #[tokio::test]
    async fn any_other_403_stays_a_permission_error() {
        let (store, md5) = faulty(testing::Fault::Forbidden("AccessDenied")).await;
        for err in read_errors(&store, &md5).await {
            assert!(err.downcast_ref::<Error>().is_none(), "{err:#}");
            assert!(
                matches!(
                    err.downcast_ref::<object_store::Error>(),
                    Some(object_store::Error::PermissionDenied { .. })
                ),
                "{err:#}"
            );
        }
    }

    #[tokio::test]
    async fn a_body_shorter_than_the_object_is_an_incomplete_download() {
        let (store, md5) = faulty(testing::Fault::Truncate).await;
        for err in read_errors(&store, &md5).await {
            assert!(err.downcast_ref::<Error>().is_none(), "{err:#}");
            assert_eq!(
                err.to_string(),
                format!("incomplete download of {KEY} (32/64 bytes)")
            );
        }
    }
}
