use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::Path;
use std::process::{Output, Stdio};
use tokio::process::Command;

use super::{Listed, ObjectMeta};

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

    /// The object at `key`, from `lsjson --stat`; `None` if there is none.
    /// Only rclone's "not found" exit codes map to `None`; any other failure
    /// (missing binary, unknown remote, auth, network) is an error, so it
    /// cannot masquerade as an absent object.
    pub async fn stat(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let remote = self.remote_path(key);
        let output = run(["lsjson", "--stat", "--no-mimetype", "--", remote.as_str()]).await?;

        match output.status.code() {
            Some(0) => {}
            Some(EXIT_DIR_NOT_FOUND | EXIT_FILE_NOT_FOUND) => return Ok(None),
            _ => anyhow::bail!("rclone lsjson {remote} failed: {}", describe(&output)),
        }

        let entry: Entry = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("rclone lsjson {remote} returned invalid JSON"))?;
        anyhow::ensure!(!entry.is_dir, "{remote} is a directory, not an object");
        let (size, modified) = entry.meta(&remote)?;
        Ok(Some(ObjectMeta { size, modified }))
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

    /// Every object under `prefix`, keyed like [`Self::stat`]'s `key`. A
    /// missing prefix lists as empty.
    pub async fn list(&self, prefix: &str) -> Result<Vec<Listed>> {
        let base = prefix.trim_end_matches('/');
        let remote = self.remote_path(base);
        let output = run([
            "lsjson",
            "-R",
            "--files-only",
            "--no-mimetype",
            "--",
            remote.as_str(),
        ])
        .await?;
        match output.status.code() {
            Some(0) => {}
            Some(EXIT_DIR_NOT_FOUND | EXIT_FILE_NOT_FOUND) => return Ok(Vec::new()),
            _ => anyhow::bail!("rclone lsjson {remote} failed: {}", describe(&output)),
        }
        let entries: Vec<Entry> = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("rclone lsjson {remote} returned invalid JSON"))?;
        entries
            .into_iter()
            .map(|entry| {
                let key = format!("{base}/{}", entry.path);
                let (size, modified) = entry.meta(&key)?;
                Ok(Listed {
                    key,
                    size,
                    modified,
                })
            })
            .collect()
    }
}

/// One object in `rclone lsjson` output.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Entry {
    path: String,
    size: i64,
    mod_time: String,
    #[serde(default)]
    is_dir: bool,
}

impl Entry {
    /// Size and modification time. rclone reports -1 for a size it does not
    /// know, which no object bigstore stores has.
    fn meta(&self, what: &str) -> Result<(u64, DateTime<Utc>)> {
        let size = u64::try_from(self.size)
            .with_context(|| format!("rclone reports no size for {what}"))?;
        let modified = DateTime::parse_from_rfc3339(&self.mod_time)
            .with_context(|| format!("rclone reports a bad time for {what}: {}", self.mod_time))?
            .with_timezone(&Utc);
        Ok((size, modified))
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
    async fn stat_distinguishes_absent_objects_from_remote_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("aa")).unwrap();
        std::fs::write(dir.path().join("aa/present"), b"x").unwrap();
        let backend = RcloneBackend::new(dir.path().to_str().unwrap().to_owned());

        assert_eq!(backend.stat("aa/present").await.unwrap().unwrap().size, 1);
        // Missing object in an existing directory, and under a missing directory.
        assert!(backend.stat("aa/absent").await.unwrap().is_none());
        assert!(backend.stat("bb/absent").await.unwrap().is_none());

        // An unconfigured remote is a failure to reach storage, not an absent
        // object: reporting `false` would tell callers the object is missing
        // from the remote when the remote was never consulted.
        let unreachable = RcloneBackend::new("bigstore-test-no-such-remote:bucket".to_owned());
        assert!(unreachable.stat("aa/present").await.is_err());
    }
}
