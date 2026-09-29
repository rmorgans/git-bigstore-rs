pub(crate) mod rclone;
pub mod store;

use anyhow::Result;
use object_store::{ObjectStore, ObjectStoreExt};
use std::path::Path;
use std::sync::Arc;

use crate::config::{BackendConfig, BigstoreConfig};

pub enum Backend {
    ObjectStore(Arc<dyn ObjectStore>),
    Rclone(rclone::RcloneBackend),
}

pub fn from_config(cfg: &BigstoreConfig) -> Result<Backend> {
    match &cfg.backend {
        BackendConfig::S3 { .. } | BackendConfig::Gcs { .. } | BackendConfig::Azure { .. } => {
            let s = store::build_object_store(&cfg.backend)?;
            Ok(Backend::ObjectStore(Arc::from(s)))
        }
        BackendConfig::Rclone { remote } => {
            Ok(Backend::Rclone(rclone::RcloneBackend::new(remote.clone())))
        }
        BackendConfig::Local { path } => {
            let s = store::build_local_store(path)?;
            Ok(Backend::ObjectStore(Arc::from(s)))
        }
    }
}

pub async fn exists(backend: &Backend, key: &str) -> Result<bool> {
    match backend {
        Backend::ObjectStore(store) => {
            let path = object_store::path::Path::from(key);
            match store.head(&path).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(e.into()),
            }
        }
        Backend::Rclone(r) => r.exists(key).await,
    }
}

/// Upload a local file to the remote. Streams — does not buffer the entire file.
pub async fn upload(backend: &Backend, local_path: &Path, key: &str) -> Result<()> {
    match backend {
        Backend::ObjectStore(store) => {
            let file = tokio::fs::File::open(local_path).await?;
            put_streaming(Arc::clone(store), object_store::path::Path::from(key), file).await
        }
        Backend::Rclone(r) => r.upload(local_path, key).await,
    }
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

/// Download a remote object to a local file. Streams — does not buffer entire file.
pub async fn download(backend: &Backend, key: &str, local_path: &Path) -> Result<()> {
    match backend {
        Backend::ObjectStore(store) => {
            use futures::StreamExt;
            use tokio::io::AsyncWriteExt;

            let path = object_store::path::Path::from(key);
            let result = store.get(&path).await?;

            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file = tokio::fs::File::create(local_path).await?;
            let mut stream = result.into_stream();

            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                file.write_all(&bytes).await?;
            }
            file.flush().await?;

            Ok(())
        }
        Backend::Rclone(r) => r.download(key, local_path).await,
    }
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
}
