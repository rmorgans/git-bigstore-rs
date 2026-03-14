mod rclone;
pub mod store;

use anyhow::Result;
use object_store::ObjectStore;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::config::{BackendConfig, BigstoreConfig};

const UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB per part

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
        BackendConfig::Rclone { remote } => Ok(Backend::Rclone(rclone::RcloneBackend::new(
            remote.clone(),
        ))),
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
        Backend::Rclone(r) => r.exists(key),
    }
}

/// Upload a local file to the remote. Streams in chunks — does not buffer entire file.
pub async fn upload(backend: &Backend, local_path: &Path, key: &str) -> Result<()> {
    match backend {
        Backend::ObjectStore(store) => {
            let path = object_store::path::Path::from(key);
            let mut upload = store.put_multipart(&path).await?;
            let mut file = tokio::fs::File::open(local_path).await?;

            loop {
                let mut buf = vec![0u8; UPLOAD_CHUNK_SIZE];
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                buf.truncate(n);
                upload.put_part(buf.into()).await?;
            }

            upload.complete().await?;
            Ok(())
        }
        Backend::Rclone(r) => r.upload(local_path, key),
    }
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
        Backend::Rclone(r) => r.download(key, local_path),
    }
}
