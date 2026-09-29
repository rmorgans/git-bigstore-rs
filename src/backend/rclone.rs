use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::path::Path;
use std::process::{Output, Stdio};
use tokio::process::Command;

/// rclone exit codes that mean "the path does not exist" rather than a failure
/// to reach or authenticate against the remote. See `rclone help flags`
/// ("Exit Code" in the rclone docs): 3 = directory not found, 4 = file not
/// found. `lsjson --stat` on a missing object reports 3 on some backends and 4
/// on others.
const EXIT_DIR_NOT_FOUND: i32 = 3;
const EXIT_FILE_NOT_FOUND: i32 = 4;

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

    /// Whether an object exists at `key`. Only rclone's "not found" exit codes
    /// map to `false`; any other failure (missing binary, unknown remote,
    /// auth, network) is an error, so it cannot masquerade as an absent object.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let remote = self.remote_path(key);
        let output = run(["lsjson", "--stat", "--", remote.as_str()]).await?;

        match output.status.code() {
            Some(0) => {}
            Some(EXIT_DIR_NOT_FOUND | EXIT_FILE_NOT_FOUND) => return Ok(false),
            _ => anyhow::bail!("rclone lsjson {remote} failed: {}", describe(&output)),
        }

        let stat: serde_json::Value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("rclone lsjson {remote} returned invalid JSON"))?;
        anyhow::ensure!(
            stat["IsDir"] != serde_json::Value::Bool(true),
            "{remote} is a directory, not an object"
        );
        Ok(true)
    }

    pub async fn upload(&self, local_path: &Path, key: &str) -> Result<()> {
        let remote = self.remote_path(key);
        let args: [&OsStr; 4] = [
            "copyto".as_ref(),
            "--".as_ref(),
            local_path.as_os_str(),
            remote.as_ref(),
        ];
        let output = run(args).await?;
        anyhow::ensure!(
            output.status.success(),
            "rclone upload to {remote} failed: {}",
            describe(&output)
        );
        Ok(())
    }

    pub async fn download(&self, key: &str, local_path: &Path) -> Result<()> {
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let remote = self.remote_path(key);
        let args: [&OsStr; 4] = [
            "copyto".as_ref(),
            "--".as_ref(),
            remote.as_ref(),
            local_path.as_os_str(),
        ];
        let output = run(args).await?;
        anyhow::ensure!(
            output.status.success(),
            "rclone download of {remote} failed: {}",
            describe(&output)
        );
        Ok(())
    }

    /// Keys of all objects under `prefix`. A missing prefix lists as empty.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = prefix.trim_end_matches('/');
        let remote = self.remote_path(base);
        let output = run(["lsf", "-R", "--files-only", "--", remote.as_str()]).await?;
        match output.status.code() {
            Some(0) => {}
            Some(EXIT_DIR_NOT_FOUND | EXIT_FILE_NOT_FOUND) => return Ok(Vec::new()),
            _ => anyhow::bail!("rclone lsf {remote} failed: {}", describe(&output)),
        }
        Ok(String::from_utf8(output.stdout)
            .context("rclone lsf returned non-UTF-8 names")?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| format!("{base}/{l}"))
            .collect())
    }
}

/// Run rclone with `args`, capturing its output. stdout is never inherited:
/// the LFS adapter speaks its protocol on stdout. The child is killed if the
/// future is dropped.
async fn run<I, S>(args: I) -> Result<Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new("rclone")
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .context("failed to run rclone — is it installed?")
}

fn describe(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("{} ({})", stderr.trim(), output.status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "needs the rclone binary; CI installs it and runs ignored tests"]
    async fn exists_distinguishes_absent_objects_from_remote_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("aa")).unwrap();
        std::fs::write(dir.path().join("aa/present"), b"x").unwrap();
        let backend = RcloneBackend::new(dir.path().to_str().unwrap().to_owned());

        assert!(backend.exists("aa/present").await.unwrap());
        // Missing object in an existing directory, and under a missing directory.
        assert!(!backend.exists("aa/absent").await.unwrap());
        assert!(!backend.exists("bb/absent").await.unwrap());

        // An unconfigured remote is a failure to reach storage, not an absent
        // object: reporting `false` would tell callers the object is missing
        // from the remote when the remote was never consulted.
        let unreachable = RcloneBackend::new("bigstore-test-no-such-remote:bucket".to_owned());
        assert!(unreachable.exists("aa/present").await.is_err());
    }
}
