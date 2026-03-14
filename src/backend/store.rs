use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use crate::config::BackendConfig;

/// Build an ObjectStore client from backend config. Used by both bigstore
/// and the LFS transfer adapter.
pub fn build_object_store(backend: &BackendConfig) -> Result<Box<dyn ObjectStore>> {
    match backend {
        BackendConfig::S3 {
            bucket,
            endpoint,
            region,
            ..
        } => {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

            if let Some(ep) = endpoint {
                builder = builder
                    .with_endpoint(ep)
                    .with_virtual_hosted_style_request(false);
            }
            if let Some(r) = region {
                builder = builder.with_region(r);
            }

            let store = builder.build().context("failed to build S3 client")?;
            Ok(Box::new(store))
        }

        BackendConfig::Gcs { bucket, .. } => {
            let store = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .context("failed to build GCS client")?;
            Ok(Box::new(store))
        }

        BackendConfig::Azure { container, .. } => {
            let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(container)
                .build()
                .context("failed to build Azure client")?;
            Ok(Box::new(store))
        }

        _ => anyhow::bail!("backend type not supported by object_store"),
    }
}

pub fn build_local_store(path: &str) -> Result<Box<dyn ObjectStore>> {
    let store = LocalFileSystem::new_with_prefix(path)
        .context("failed to create local filesystem backend")?;
    Ok(Box::new(store))
}
