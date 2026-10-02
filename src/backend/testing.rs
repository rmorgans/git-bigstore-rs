//! Object stores that fail the way remote stores do: reads, and writes.

use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = object_store::Result<T>> + Send + 'a>>;

/// How a GET fails.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Fault {
    /// The body ends cleanly halfway through the object: a truncated read.
    Truncate,
    /// The request fails 403 with this S3 error code in the body, as
    /// object_store reports it.
    Forbidden(&'static str),
}

/// `InMemory` whose GETs (not HEADs) of keys under `under` fail with
/// `fault`. Everything else, writes included, goes straight through; a
/// PUT answers with the md5 of its bytes as its ETag, as S3 does.
/// `ObjectStore` is an `#[async_trait]` trait; the methods are written in
/// the desugared form to avoid a dev-dependency on the macro.
#[derive(Debug)]
pub(crate) struct FaultStore {
    inner: InMemory,
    under: &'static str,
    fault: Fault,
}

impl FaultStore {
    pub(crate) fn new(under: &'static str, fault: Fault) -> Self {
        Self {
            inner: InMemory::new(),
            under,
            fault,
        }
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultStore")
    }
}

/// An S3 error response, displayed as object_store displays one (its own
/// error type is private).
#[derive(Debug)]
struct S3Error {
    code: &'static str,
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error performing GET https://s3.example.invalid/bucket/key in 20ms - \
             Server returned non-2xx status code: 403 Forbidden: \
             <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{}</Code><Message>denied</Message></Error>",
            self.code
        )
    }
}

impl std::error::Error for S3Error {}

impl ObjectStore for FaultStore {
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
            let bytes: Vec<u8> = payload.iter().flat_map(|b| b.to_vec()).collect();
            let md5 = md5_hex(&bytes);
            let mut put = self.inner.put_opts(location, bytes.into(), opts).await?;
            put.e_tag = Some(format!("\"{md5}\""));
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
        if options.head || !location.as_ref().starts_with(self.under) {
            return self.inner.get_opts(location, options);
        }
        match self.fault {
            Fault::Forbidden(code) => Box::pin(async move {
                Err(object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source: Box::new(S3Error { code }),
                })
            }),
            Fault::Truncate => Box::pin(async move {
                let mut result = self.inner.get_opts(location, options).await?;
                let empty = GetResultPayload::Stream(futures::stream::empty().boxed());
                let GetResultPayload::Stream(body) = std::mem::replace(&mut result.payload, empty)
                else {
                    unreachable!("InMemory returns streams");
                };
                let half = body.take(1).map_ok(|chunk| chunk.slice(..chunk.len() / 2));
                result.payload = GetResultPayload::Stream(half.boxed());
                Ok(result)
            }),
        }
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

/// `InMemory` whose writes go wrong as a test says, counting GETs and
/// multipart uploads. Its own ETags are a counter, never an md5, unless
/// `md5_etags`.
#[derive(Debug, Default)]
pub(crate) struct WriteFaults {
    pub(crate) inner: InMemory,
    /// Every write is stored with its first byte flipped: a provider that
    /// damages what it keeps.
    pub(crate) corrupt: bool,
    /// A PUT answers with the md5 of what it stored as its ETag, as S3
    /// does for a single PUT.
    pub(crate) md5_etags: bool,
    /// This multipart part (counting from 0) fails; every part takes
    /// 100 ms, so several are in flight.
    pub(crate) fail_part: Option<usize>,
    pub(crate) gets: AtomicUsize,
    pub(crate) multiparts: AtomicUsize,
    pub(crate) aborted: Arc<AtomicUsize>,
}

impl std::fmt::Display for WriteFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteFaults")
    }
}

fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = crate::hash::Hasher::new(crate::types::HashFunction::Md5);
    hasher.update(bytes);
    hasher.finalize().to_string()
}

/// `bytes` with the first byte flipped.
fn flipped(mut bytes: Vec<u8>) -> Vec<u8> {
    if let Some(b) = bytes.first_mut() {
        *b ^= 0xff;
    }
    bytes
}

/// A multipart upload into `store` at `location` that fails part `fail`
/// and, with `corrupt`, damages the object once complete.
#[derive(Debug)]
struct FaultyUpload {
    inner: Box<dyn MultipartUpload>,
    store: InMemory,
    location: StorePath,
    corrupt: bool,
    part: usize,
    fail: Option<usize>,
    aborted: Arc<AtomicUsize>,
}

impl MultipartUpload for FaultyUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let fails = self.fail == Some(self.part);
        self.part += 1;
        let inner = self.inner.put_part(data);
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if fails {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "part failed".into(),
                });
            }
            inner.await
        })
    }

    fn complete<'s, 'a>(&'s mut self) -> BoxFut<'a, PutResult>
    where
        's: 'a,
        Self: 'a,
    {
        Box::pin(async move {
            let done = self.inner.complete().await?;
            if !self.corrupt {
                return Ok(done);
            }
            let stored = self.store.get(&self.location).await?.bytes().await?;
            self.store
                .put(&self.location, flipped(stored.to_vec()).into())
                .await
        })
    }

    fn abort<'s, 'a>(&'s mut self) -> BoxFut<'a, ()>
    where
        's: 'a,
        Self: 'a,
    {
        self.aborted.fetch_add(1, Ordering::SeqCst);
        self.inner.abort()
    }
}

impl ObjectStore for WriteFaults {
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
            if self.corrupt {
                bytes = flipped(bytes);
            }
            let md5 = md5_hex(&bytes);
            let mut put = self.inner.put_opts(location, bytes.into(), opts).await?;
            if self.md5_etags {
                put.e_tag = Some(format!("\"{md5}\""));
            }
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
        Box::pin(async move {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            self.multiparts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FaultyUpload {
                inner,
                store: self.inner.clone(),
                location: location.clone(),
                corrupt: self.corrupt,
                part: 0,
                fail: self.fail_part,
                aborted: Arc::clone(&self.aborted),
            }) as Box<dyn MultipartUpload>)
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
        if !options.head {
            self.gets.fetch_add(1, Ordering::SeqCst);
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
