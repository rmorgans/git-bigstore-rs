use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::types::{HashFunction, Hexdigest, Layout};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigstoreConfig {
    pub backend: BackendConfig,

    #[serde(default)]
    pub layout: Layout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BackendConfig {
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        region: Option<String>,
    },

    #[serde(rename = "gs")]
    Gcs {
        bucket: String,
        #[serde(default)]
        prefix: String,
    },

    #[serde(rename = "az")]
    Azure {
        container: String,
        #[serde(default)]
        prefix: String,
    },

    #[serde(rename = "rclone")]
    Rclone {
        remote: String,
    },

    #[serde(rename = "local")]
    Local {
        path: String,
    },
}

impl BigstoreConfig {
    /// Parse a storage URL like s3://bucket/prefix or gs://bucket/prefix
    pub fn from_url(url: &str, endpoint: Option<&str>) -> Result<Self> {
        let (scheme, rest) = url
            .split_once("://")
            .context("URL must be scheme://bucket/prefix (e.g. s3://my-bucket/assets)")?;

        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));

        let backend = match scheme {
            "s3" => BackendConfig::S3 {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                endpoint: endpoint.map(String::from),
                region: None,
            },
            "tigris" | "t3" => BackendConfig::S3 {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                endpoint: Some(
                    endpoint
                        .unwrap_or("https://fly.storage.tigris.dev")
                        .to_string(),
                ),
                region: Some("auto".to_string()),
            },
            "r2" => {
                let ep = endpoint.context(
                    "R2 requires --endpoint: https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
                )?;
                BackendConfig::S3 {
                    bucket: bucket.to_string(),
                    prefix: prefix.to_string(),
                    endpoint: Some(ep.to_string()),
                    region: Some("auto".to_string()),
                }
            }
            "gs" => BackendConfig::Gcs {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
            },
            "az" | "azure" => BackendConfig::Azure {
                container: bucket.to_string(),
                prefix: prefix.to_string(),
            },
            "rclone" => BackendConfig::Rclone {
                remote: rest.to_string(),
            },
            "file" | "local" => BackendConfig::Local {
                path: rest.to_string(),
            },
            _ => anyhow::bail!(
                "unsupported scheme: {scheme}\n\
                 supported: s3, r2, tigris (t3), gs, az, rclone, local"
            ),
        };

        Ok(Self {
            backend,
            layout: Layout::default(),
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self).context("failed to serialize config")?;
        std::fs::write(path, content).context("failed to write .bigstore.toml")?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        // Layout is validated during deserialization — invalid templates fail here
        toml::from_str(&content)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Find and load config from a repo root.
    /// Tries .bigstore.toml first, then .bigstore for backward compatibility.
    pub fn find_and_load(repo_root: &Path) -> Result<Self> {
        let toml_path = repo_root.join(".bigstore.toml");
        if toml_path.exists() {
            return Self::load(&toml_path);
        }
        let legacy_path = repo_root.join(".bigstore");
        if legacy_path.exists() {
            eprintln!("note: using legacy .bigstore config; run `git bigstore migrate-config` to upgrade");
            return Self::load(&legacy_path);
        }
        anyhow::bail!(
            "no bigstore config found (looked for .bigstore.toml and .bigstore)"
        )
    }

    pub fn bucket_prefix(&self) -> &str {
        match &self.backend {
            BackendConfig::S3 { prefix, .. }
            | BackendConfig::Gcs { prefix, .. }
            | BackendConfig::Azure { prefix, .. } => prefix.as_str(),
            _ => "",
        }
    }

    pub fn backend_type(&self) -> &str {
        match &self.backend {
            BackendConfig::S3 { .. } => "s3",
            BackendConfig::Gcs { .. } => "gs",
            BackendConfig::Azure { .. } => "az",
            BackendConfig::Rclone { .. } => "rclone",
            BackendConfig::Local { .. } => "local",
        }
    }

    /// Build the remote object key using the configured layout.
    /// Safe: Layout is validated, Hexdigest is validated.
    /// Returns Err if the layout doesn't support the given hash function.
    pub fn remote_object_key(&self, hexdigest: &Hexdigest, hash_fn: HashFunction) -> Result<String> {
        let key = self.layout.object_key(hexdigest, hash_fn)?;

        let bucket_prefix = match &self.backend {
            BackendConfig::S3 { prefix, .. }
            | BackendConfig::Gcs { prefix, .. }
            | BackendConfig::Azure { prefix, .. } => prefix.as_str(),
            _ => "",
        };

        if bucket_prefix.is_empty() {
            Ok(key)
        } else {
            Ok(format!("{bucket_prefix}/{key}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HashFunction;

    fn test_digest() -> Hexdigest {
        Hexdigest::new(&"ab".repeat(32), HashFunction::Sha256).unwrap()
    }

    #[test]
    fn parse_s3_url() {
        let cfg = BigstoreConfig::from_url("s3://my-bucket/assets", None).unwrap();
        assert_eq!(cfg.backend_type(), "s3");
        match &cfg.backend {
            BackendConfig::S3 { bucket, prefix, .. } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "assets");
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn parse_s3_url_no_prefix() {
        let cfg = BigstoreConfig::from_url("s3://my-bucket", None).unwrap();
        match &cfg.backend {
            BackendConfig::S3 { bucket, prefix, .. } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "");
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn parse_tigris_url() {
        let cfg = BigstoreConfig::from_url("t3://my-bucket", None).unwrap();
        match &cfg.backend {
            BackendConfig::S3 { endpoint, .. } => {
                assert_eq!(
                    endpoint.as_deref(),
                    Some("https://fly.storage.tigris.dev")
                );
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn remote_object_key_dvc_layout() {
        let cfg = BigstoreConfig::from_url("s3://bucket/data", None).unwrap();
        let d = test_digest();
        let key = cfg.remote_object_key(&d, HashFunction::Sha256).unwrap();
        assert_eq!(key, format!("data/files/sha256/{}/{}", d.prefix(), d.rest()));
    }

    #[test]
    fn remote_object_key_no_prefix() {
        let cfg = BigstoreConfig::from_url("s3://bucket", None).unwrap();
        let d = test_digest();
        let key = cfg.remote_object_key(&d, HashFunction::Sha256).unwrap();
        assert_eq!(key, format!("files/sha256/{}/{}", d.prefix(), d.rest()));
    }

    #[test]
    fn load_rejects_invalid_layout() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            tmp.path(),
            "layout = \"broken\"\n\n[backend]\ntype = \"local\"\npath = \"/tmp\"\n",
        )
        .unwrap();
        assert!(BigstoreConfig::load(tmp.path()).is_err());
    }

    #[test]
    fn find_and_load_prefers_toml() {
        let tmp = tempfile::TempDir::new().unwrap();
        let toml_cfg = "[backend]\ntype = \"local\"\npath = \"/toml\"\n";
        let legacy_cfg = "[backend]\ntype = \"local\"\npath = \"/legacy\"\n";
        std::fs::write(tmp.path().join(".bigstore.toml"), toml_cfg).unwrap();
        std::fs::write(tmp.path().join(".bigstore"), legacy_cfg).unwrap();

        let cfg = BigstoreConfig::find_and_load(tmp.path()).unwrap();
        match &cfg.backend {
            BackendConfig::Local { path } => assert_eq!(path, "/toml"),
            _ => panic!("expected Local backend"),
        }
    }

    #[test]
    fn find_and_load_falls_back_to_legacy() {
        let tmp = tempfile::TempDir::new().unwrap();
        let legacy_cfg = "[backend]\ntype = \"local\"\npath = \"/legacy\"\n";
        std::fs::write(tmp.path().join(".bigstore"), legacy_cfg).unwrap();

        let cfg = BigstoreConfig::find_and_load(tmp.path()).unwrap();
        match &cfg.backend {
            BackendConfig::Local { path } => assert_eq!(path, "/legacy"),
            _ => panic!("expected Local backend"),
        }
    }

    #[test]
    fn find_and_load_errors_when_neither_exists() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(BigstoreConfig::find_and_load(tmp.path()).is_err());
    }
}
