//! An object store that fails reads the way remote stores do.

use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::future::Future;
use std::pin::Pin;

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
/// `fault`. Everything else, writes included, goes straight through.
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
