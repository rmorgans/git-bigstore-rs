//! Whether a remote still holds every file a version needs.

use anyhow::Result;
use std::collections::BTreeSet;

use super::{
    archived, block_on, each_in_order, history, layout, HistoryKey, Remote, MAX_MANIFEST_BYTES,
};
use crate::backend;
use crate::dvc::{DvcOutput, Manifest, RecordId};
use crate::types::{Hexdigest, Layout};

/// What [`verify`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Completeness {
    /// Distinct objects the version names: 1 for a file; for a directory,
    /// those its manifest names, or 0 if the manifest is missing (then they
    /// are unknown).
    pub objects: usize,
    /// What the version needs and the remote lacks, sorted: keys relative
    /// to the remote, as [`layout::kind`] takes them
    /// (`files/md5/xx/<30 hex>`, with `.dir` for the manifest). Empty when
    /// the version is complete.
    pub missing: Vec<String>,
}

impl Completeness {
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }
}

/// Whether the remote holds every file version `version` of `key` needs: a
/// file version's object; a directory version's `.dir` manifest, then
/// every object that manifest names. The manifest is read, never trusted
/// for being there: one whose bytes do not hash to its name is reported
/// missing, as is one that is gone (then the objects behind it are not
/// asked for). Objects are asked for, not read: a damaged one shows only
/// when it is restored. One listing, the record, the manifest, then the
/// objects 8 at a time. An id that is not a version of `key` is
/// [`Error::NoSuchVersion`](super::Error::NoSuchVersion).
pub fn verify(remote: &Remote, key: &HistoryKey, version: &RecordId) -> Result<Completeness> {
    block_on("verify", verify_async(remote, key, version))?
}

/// [`verify`] on the caller's tokio runtime (see [the module docs](super)).
pub async fn verify_async(
    remote: &Remote,
    key: &HistoryKey,
    version: &RecordId,
) -> Result<Completeness> {
    let record = history::find(remote, key, version).await?;
    let md5s: BTreeSet<Hexdigest> = match &record.pointer.output {
        DvcOutput::File { md5, .. } => BTreeSet::from([md5.clone()]),
        DvcOutput::Dir { manifest, .. } => {
            let rel = format!("{}.dir", object_rel(manifest));
            let raw = remote
                .store
                .get(&remote.key(&rel), MAX_MANIFEST_BYTES)
                .await
                .map_err(archived)?;
            let gone = Completeness {
                objects: 0,
                missing: vec![rel.clone()],
            };
            let Some(raw) = raw else {
                return Ok(gone);
            };
            let id = manifest.clone();
            // Bytes that are not the manifest named are as good as gone; a
            // manifest that is, but cannot be read, is an error.
            let parsed = backend::blocking(move || {
                if layout::verify(&rel, &raw).is_err() {
                    return Ok(None);
                }
                Ok(Some(Manifest::parse(&raw, &id)?))
            })
            .await?;
            let Some(parsed) = parsed else {
                return Ok(gone);
            };
            parsed.entries().iter().map(|e| e.md5.clone()).collect()
        }
    };
    let jobs = crate::transfer::DEFAULT_CONCURRENCY;
    let missing = each_in_order(&md5s, jobs, |md5| async move {
        let rel = object_rel(md5);
        let there = remote.store.head(&remote.key(&rel)).await?.is_some();
        Ok((!there).then_some(rel))
    })
    .await?;
    Ok(Completeness {
        objects: md5s.len(),
        missing: missing.into_iter().flatten().collect(),
    })
}

/// An object's key, relative to the remote.
fn object_rel(md5: &Hexdigest) -> String {
    Layout::default()
        .object_key(md5)
        .expect("the default layout addresses md5")
}
