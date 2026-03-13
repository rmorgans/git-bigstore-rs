use anyhow::Result;
use std::path::PathBuf;

pub fn git_dir() -> Result<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()?;
    anyhow::ensure!(output.status.success(), "not a git repository");
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(PathBuf::from(path))
}

pub fn repo_root() -> Result<PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()?;
    anyhow::ensure!(output.status.success(), "not a git repository");
    let path = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(PathBuf::from(path))
}

pub fn config_set(key: &str, value: &str) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(["config", key, value])
        .status()?;
    anyhow::ensure!(status.success(), "failed to set git config {key}");
    Ok(())
}
