use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use crate::config::BackendConfig;

/// Make sure the crypto for TLS and request signing is there before a cloud
/// client is built.
///
/// With `ring` (and not `aws-lc-rs`) reqwest has no built-in rustls provider
/// and would panic, so ring is installed as the process's default provider —
/// unless the application installed one already, which then wins for TLS.
fn cloud_crypto() -> Result<()> {
    #[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
    {
        // Err means a provider is installed already, which is all we need.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    #[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
    anyhow::bail!(
        "s3://, gs:// and az:// need the `aws-lc-rs` or `ring` feature of bigstore \
         (TLS and request signing), which this build leaves out"
    );
    #[cfg(any(feature = "aws-lc-rs", feature = "ring"))]
    Ok(())
}

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
            cloud_crypto()?;
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

        #[cfg(feature = "gcp")]
        BackendConfig::Gcs { bucket, .. } => {
            cloud_crypto()?;
            let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .context("failed to build GCS client")?;
            Ok(Box::new(store))
        }
        #[cfg(not(feature = "gcp"))]
        BackendConfig::Gcs { .. } => {
            anyhow::bail!("gs:// needs the `gcp` feature of bigstore, which this build leaves out")
        }

        #[cfg(feature = "azure")]
        BackendConfig::Azure { container, .. } => {
            cloud_crypto()?;
            let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(container)
                .build()
                .context("failed to build Azure client")?;
            Ok(Box::new(store))
        }
        #[cfg(not(feature = "azure"))]
        BackendConfig::Azure { .. } => {
            anyhow::bail!(
                "az:// needs the `azure` feature of bigstore, which this build leaves out"
            )
        }

        _ => anyhow::bail!("backend type not supported by object_store"),
    }
}

/// Build a `LocalFileSystem` store rooted at `path`, creating the directory if
/// it does not exist yet so a fresh `local:///new/dir` works on first push.
pub fn build_local_store(path: &str) -> Result<Box<dyn ObjectStore>> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create local storage directory {path}"))?;
    let store = LocalFileSystem::new_with_prefix(path)
        .context("failed to create local filesystem backend")?;
    Ok(Box::new(store))
}

/// Credentials for an S3-compatible remote.
#[derive(Clone)]
pub enum Credentials {
    /// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the environment.
    FromEnv,
    Static {
        access_key_id: String,
        secret_access_key: String,
    },
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FromEnv => f.write_str("FromEnv"),
            Self::Static { access_key_id, .. } => f
                .debug_struct("Static")
                .field("access_key_id", access_key_id)
                .field("secret_access_key", &"<redacted>")
                .finish(),
        }
    }
}

/// An S3 client that talks only to `endpoint`, with explicit credentials.
/// Unlike [`build_object_store`] it never defaults to AWS and never falls
/// back to instance metadata (which stalls for seconds off-cloud): missing
/// credentials fail here, before any request.
pub fn build_strict_s3(
    bucket: &str,
    endpoint: &str,
    region: Option<&str>,
    credentials: &Credentials,
) -> Result<Box<dyn ObjectStore>> {
    cloud_crypto()?;
    use object_store::aws::AmazonS3ConfigKey as Key;
    let (key_id, secret) = match credentials {
        Credentials::Static {
            access_key_id,
            secret_access_key,
        } => (access_key_id.clone(), secret_access_key.clone()),
        Credentials::FromEnv => {
            let env = AmazonS3Builder::from_env();
            match (
                env.get_config_value(&Key::AccessKeyId),
                env.get_config_value(&Key::SecretAccessKey),
            ) {
                (Some(k), Some(s)) if !k.is_empty() && !s.is_empty() => (k, s),
                _ => anyhow::bail!(
                    "S3 credentials missing: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
                ),
            }
        }
    };
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(endpoint.starts_with("http://"))
        .with_access_key_id(key_id)
        .with_secret_access_key(secret);
    if let Some(r) = region {
        builder = builder.with_region(r);
    }
    Ok(Box::new(
        builder.build().context("failed to build S3 client")?,
    ))
}

/// The endpoint the environment specifies for S3 (`AWS_ENDPOINT_URL_S3`,
/// `AWS_ENDPOINT_URL`, `AWS_ENDPOINT`), resolved the way object_store does.
pub fn env_s3_endpoint() -> Option<String> {
    use object_store::aws::AmazonS3ConfigKey as Key;
    let env = AmazonS3Builder::from_env();
    env.get_config_value(&Key::S3Endpoint)
        .or_else(|| env.get_config_value(&Key::Endpoint))
        .filter(|e| !e.is_empty())
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "gcp"))]
    #[test]
    fn gs_urls_name_the_missing_gcp_feature() {
        let err = super::build_object_store(&super::BackendConfig::Gcs {
            bucket: "bucket".into(),
            prefix: String::new(),
        })
        .expect_err("gs:// must be refused without the gcp feature");
        assert!(format!("{err:#}").contains("`gcp` feature"), "{err:#}");
    }

    #[cfg(not(feature = "azure"))]
    #[test]
    fn az_urls_name_the_missing_azure_feature() {
        let err = super::build_object_store(&super::BackendConfig::Azure {
            container: "container".into(),
            prefix: String::new(),
        })
        .expect_err("az:// must be refused without the azure feature");
        assert!(format!("{err:#}").contains("`azure` feature"), "{err:#}");
    }

    /// An HTTPS S3 client builds with whichever crypto is compiled in (with
    /// ring alone, reqwest panics unless a rustls provider is installed), and
    /// is refused with an error naming the features when there is none.
    #[test]
    fn s3_clients_need_a_crypto_feature() {
        let strict = super::build_strict_s3(
            "bucket",
            "https://s3.example.invalid",
            None,
            &super::Credentials::Static {
                access_key_id: "id".into(),
                secret_access_key: "secret".into(),
            },
        );
        let from_config = super::build_object_store(&super::BackendConfig::S3 {
            bucket: "bucket".into(),
            prefix: String::new(),
            endpoint: Some("https://s3.example.invalid".into()),
            region: Some("us-east-1".into()),
        });
        if cfg!(any(feature = "aws-lc-rs", feature = "ring")) {
            strict.expect("strict S3 client");
            from_config.expect("S3 client from config");
        } else {
            for err in [strict.unwrap_err(), from_config.unwrap_err()] {
                assert!(
                    format!("{err:#}").contains("`aws-lc-rs` or `ring` feature"),
                    "{err:#}"
                );
            }
        }
    }
}
