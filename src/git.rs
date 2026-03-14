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

fn config_get(key: &str) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["config", "--get", key])
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

fn config_set(key: &str, value: &str) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(["config", key, value])
        .status()?;
    anyhow::ensure!(status.success(), "failed to set git config {key}");
    Ok(())
}

// ──────────────────────────────────────────────────
// Filter configuration
// ──────────────────────────────────────────────────
//
// Git's clean/smudge filter has three config keys: clean, smudge, and
// required. This type models them as a unit and enforces:
//
//   1. Presence-consistency: all three must be set, or none.
//   2. Command-shape: clean must end with "filter-clean", smudge with
//      "filter-smudge", and both must share the same binary prefix.
//   3. Required must be "true".
//
// Partial or malformed config is rejected on load() with repair guidance.

/// A valid, complete filter configuration.
///
/// Invariants (enforced by load/new):
/// - `binary` is the shared command prefix (e.g. "git-bigstore" or "/full/path/to/git-bigstore")
/// - clean = "{binary} filter-clean", smudge = "{binary} filter-smudge"
/// - required is always true (set on save, checked on load)
pub struct FilterConfig {
    binary: String,
}

impl FilterConfig {
    /// Default config using bare binary name (requires git-bigstore in PATH).
    pub fn default_commands() -> Self {
        Self {
            binary: "git-bigstore".to_string(),
        }
    }

    /// Read the current filter config from git.
    ///
    /// Returns:
    /// - `Ok(None)` — not configured (all three keys absent)
    /// - `Ok(Some(config))` — valid, complete config
    /// - `Err` — partial, malformed, or inconsistent config
    pub fn load() -> Result<Option<Self>> {
        let clean = config_get("filter.bigstore.clean");
        let smudge = config_get("filter.bigstore.smudge");
        let required = config_get("filter.bigstore.required");

        // All absent = unconfigured
        if clean.is_none() && smudge.is_none() && required.is_none() {
            return Ok(None);
        }

        // Partial presence
        let clean = clean.ok_or_else(|| anyhow::anyhow!(
            "filter.bigstore.smudge is set but filter.bigstore.clean is missing.\n\
             Fix: git config filter.bigstore.clean \"git-bigstore filter-clean\""
        ))?;
        let smudge = smudge.ok_or_else(|| anyhow::anyhow!(
            "filter.bigstore.clean is set but filter.bigstore.smudge is missing.\n\
             Fix: git config filter.bigstore.smudge \"git-bigstore filter-smudge\""
        ))?;

        // Required must be "true"
        match required.as_deref() {
            Some("true") => {}
            Some(other) => anyhow::bail!(
                "filter.bigstore.required is {other:?}, expected \"true\".\n\
                 Fix: git config filter.bigstore.required true"
            ),
            None => anyhow::bail!(
                "filter.bigstore.required is not set.\n\
                 Fix: git config filter.bigstore.required true"
            ),
        }

        // Command shape: must end with "filter-clean" / "filter-smudge"
        let clean_bin = clean.strip_suffix(" filter-clean").ok_or_else(|| anyhow::anyhow!(
            "filter.bigstore.clean has unexpected format: {clean:?}\n\
             Expected: \"<binary> filter-clean\"\n\
             Fix: git config filter.bigstore.clean \"git-bigstore filter-clean\""
        ))?;
        let smudge_bin = smudge.strip_suffix(" filter-smudge").ok_or_else(|| anyhow::anyhow!(
            "filter.bigstore.smudge has unexpected format: {smudge:?}\n\
             Expected: \"<binary> filter-smudge\"\n\
             Fix: git config filter.bigstore.smudge \"git-bigstore filter-smudge\""
        ))?;

        // Same binary prefix
        anyhow::ensure!(
            clean_bin == smudge_bin,
            "filter.bigstore.clean and smudge point at different binaries:\n\
             clean:  {clean:?}\n\
             smudge: {smudge:?}\n\
             Both must use the same binary prefix."
        );

        Ok(Some(Self {
            binary: clean_bin.to_string(),
        }))
    }

    /// Write this filter config to git.
    pub fn save(&self) -> Result<()> {
        config_set(
            "filter.bigstore.clean",
            &format!("{} filter-clean", self.binary),
        )?;
        config_set(
            "filter.bigstore.smudge",
            &format!("{} filter-smudge", self.binary),
        )?;
        config_set("filter.bigstore.required", "true")?;
        Ok(())
    }

    /// The binary path/name used by this config.
    #[cfg(test)]
    pub fn binary(&self) -> &str {
        &self.binary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_commands_roundtrip() {
        let cfg = FilterConfig::default_commands();
        assert_eq!(cfg.binary(), "git-bigstore");
    }
}
