use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

/// Backend that delegates to the rclone binary for storage operations.
/// Supports any of rclone's 70+ backends.
pub struct RcloneBackend {
    remote: String,
}

impl RcloneBackend {
    pub fn new(remote: String) -> Self {
        Self { remote }
    }

    fn remote_path(&self, key: &str) -> String {
        format!("{}/{}", self.remote, key)
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let output = Command::new("rclone")
            .args(["lsf", &self.remote_path(key)])
            .output()
            .context("failed to run rclone — is it installed?")?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    pub fn upload(&self, local_path: &Path, key: &str) -> Result<()> {
        let local = local_path
            .to_str()
            .context("local path is not valid UTF-8")?;

        let status = Command::new("rclone")
            .args(["copyto", local, &self.remote_path(key)])
            .status()
            .context("failed to run rclone — is it installed?")?;

        anyhow::ensure!(status.success(), "rclone upload failed");
        Ok(())
    }

    pub fn download(&self, key: &str, local_path: &Path) -> Result<()> {
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let local = local_path
            .to_str()
            .context("local path is not valid UTF-8")?;

        let status = Command::new("rclone")
            .args(["copyto", &self.remote_path(key), local])
            .status()
            .context("failed to run rclone — is it installed?")?;

        anyhow::ensure!(status.success(), "rclone download failed");
        Ok(())
    }
}
