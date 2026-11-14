//! Local input identities used at destination removal, without an input-tree inventory.

use crate::safedir::overlap::{DirectoryOverlap, Identity};
use crate::safedir::{Dir, FileMeta};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Default)]
struct Roots {
    slots: std::collections::HashMap<Identity, std::collections::HashMap<OsString, Arc<PathBuf>>>,
}

/// Directory participation and explicit leaf input names, shared by one local invocation.
/// Directory history has one owner: the overlap registry and its transactional reservations.
#[derive(Debug)]
pub(crate) struct InputProtection {
    registry: Arc<DirectoryOverlap>,
    roots: std::sync::Mutex<Roots>,
}

impl InputProtection {
    pub(crate) fn new(registry: Arc<DirectoryOverlap>) -> Self {
        Self {
            registry,
            roots: Default::default(),
        }
    }

    pub(crate) fn record_leaf(&self, parent: Identity, name: &OsStr, path: &Path) {
        let path = Arc::new(path.to_owned());
        let mut roots = self
            .roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        roots
            .slots
            .entry(parent)
            .or_default()
            .entry(name.to_owned())
            .or_insert(path);
    }

    fn check_identity(&self, identity: Identity, path: &Path) -> std::io::Result<()> {
        if let Some(owner) = self.registry.input_owner(identity) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cannot remove {path:?}: overlaps {owner}"),
            ));
        }
        Ok(())
    }

    /// Check the held directory before changing its mode or using it for descent.
    pub(crate) fn check_directory(&self, meta: &FileMeta, path: &Path) -> std::io::Result<()> {
        self.check_directory_identity((meta.dev(), meta.ino()), path)
    }

    /// Check an observed directory identity before a contained, by-name empty-directory removal.
    pub(crate) fn check_directory_identity(
        &self,
        identity: Identity,
        path: &Path,
    ) -> std::io::Result<()> {
        self.check_identity(identity, path)?;
        let root = self
            .roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .slots
            .get(&identity)
            .and_then(|slots| slots.values().next())
            .cloned();
        if let Some(root) = root {
            return Err(protected(path, root.as_path()));
        }
        Ok(())
    }

    /// Check a final name through the same parent identity used by the mutation.
    /// A different hardlink name is not itself a protected input slot.
    pub(crate) fn check_slot(
        &self,
        parent: Identity,
        name: &OsStr,
        path: &Path,
    ) -> std::io::Result<()> {
        self.check_identity(parent, path)?;
        let root = self
            .roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .slots
            .get(&parent)
            .and_then(|slots| slots.get(name))
            .cloned();
        if let Some(root) = root {
            return Err(protected(path, root.as_path()));
        }
        Ok(())
    }

    pub(crate) async fn check_child(
        &self,
        parent: &Dir,
        name: &OsStr,
        path: &Path,
    ) -> std::io::Result<()> {
        self.check_slot(parent.identity().await?, name, path)
    }

    /// Root leaves bypass output-directory admission; check their parent chain before replacement.
    /// The existing ancestry snapshot has the same mount, search, and concurrent-rename limits.
    pub(crate) async fn check_root_parent(&self, parent: &Dir, path: &Path) -> std::io::Result<()> {
        for identity in self.registry.ancestors(parent).await?.iter() {
            self.check_identity(*identity, path)?;
        }
        Ok(())
    }
}

fn protected(destination: &Path, input: &Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("cannot remove {destination:?}: would remove input {input:?}"),
    )
}
