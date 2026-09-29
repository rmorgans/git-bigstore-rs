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
        Backend::Rclone(r) => r.exists(key),
    }
}

/// Upload a local file to the remote. Streams — does not buffer the entire file.
pub async fn upload(backend: &Backend, local_path: &Path, key: &str) -> Result<()> {
    match backend {
        Backend::ObjectStore(store) => {
            use tokio::io::AsyncWriteExt;

            let path = object_store::path::Path::from(key);
            // BufWriter issues a single PUT for objects under its capacity and
            // switches to multipart (10 MiB parts, safely above S3's 5 MiB
            // minimum) above it — sizing parts correctly regardless of how the
            // file reads back. A raw put_part loop would forward short reads
            // verbatim and risk an EntityTooSmall rejection on complete().
            let mut writer = object_store::buffered::BufWriter::new(Arc::clone(store), path);
            let mut file = tokio::fs::File::open(local_path).await?;
            tokio::io::copy(&mut file, &mut writer).await?;
            writer.shutdown().await?;
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
