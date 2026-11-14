//! Physical destination authority shared by local copy and link directory admission.

use super::{DirResolution, Error, Settings, Summary};
use crate::input_protection::InputProtection;
use crate::safedir::overlap::{DirectoryOverlap, Evidence, InputRole, Operand};
use crate::safedir::{self, Dir, FileMeta, PreviewEntry};
use anyhow::Context as _;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct DestinationContext {
    origin: Origin,
    registry: Arc<DirectoryOverlap>,
    protection: Arc<InputProtection>,
    operand: Arc<Operand>,
    // a delegated copy keeps the selected input's role through descendants and dereference
    pub(super) input_role: InputRole,
    // explicit root participation stays tentative until ordinary admission/mutation commits it
    _root_claim: Option<safedir::overlap::Reservation>,
}

#[derive(Clone, Debug)]
enum Origin {
    Root {
        named_root: std::path::PathBuf,
        real_parent: Option<Arc<Dir>>,
        preview_parent: Option<PreviewParent>,
    },
    Prepared(PreparedParent),
}

#[derive(Clone, Debug)]
enum PreparedParent {
    Real(Arc<Dir>),
    Preview(PreviewEntry),
}

/// Root parent authority retained by eager strict validation, including unavailable evidence.
#[derive(Clone, Debug)]
pub(crate) enum PreviewParent {
    Held(Arc<Dir>),
    Missing,
    Unavailable,
    NotDirectory,
}

impl PreviewParent {
    pub(crate) async fn open(path: &Path) -> std::io::Result<Self> {
        match safedir::open_preview_operand_parent(path, congestion::Side::Destination).await {
            Ok(parent) => Ok(Self::Held(Arc::new(parent))),
            Err(error) => match error.kind() {
                std::io::ErrorKind::NotFound => Ok(Self::Missing),
                std::io::ErrorKind::PermissionDenied => Ok(Self::Unavailable),
                std::io::ErrorKind::NotADirectory => Ok(Self::NotDirectory),
                _ => Err(error),
            },
        }
    }
}

/// Only successful admission produces the capability used to derive children.
#[derive(Clone, Debug)]
pub(crate) struct PreparedDestination {
    parent: PreparedParent,
    registry: Arc<DirectoryOverlap>,
    protection: Arc<InputProtection>,
    operand: Arc<Operand>,
    input_role: InputRole,
}

impl PreparedDestination {
    pub(crate) fn protection(&self) -> &Arc<InputProtection> {
        &self.protection
    }

    pub(crate) fn child(&self) -> DestinationContext {
        DestinationContext {
            origin: Origin::Prepared(self.parent.clone()),
            registry: Arc::clone(&self.registry),
            protection: Arc::clone(&self.protection),
            operand: Arc::clone(&self.operand),
            input_role: self.input_role,
            _root_claim: None,
        }
    }
}

// keep the per-directory return allocation-free; callers immediately unpack entered state
#[allow(clippy::large_enum_variant)]
pub(crate) enum Admission {
    Skip(Summary),
    Entered {
        destination: PreparedDestination,
        directory: EnteredDirectory,
        snapshot: SourceSnapshot,
    },
}

/// Directory state retained from admission through traversal and finalization.
pub(crate) struct EnteredDirectory {
    pub(crate) dir: Option<Arc<Dir>>,
    /// The same held parent used to create or reuse `dir`, retained for empty-directory cleanup.
    pub(crate) parent: Option<Arc<Dir>>,
    pub(crate) preview: Option<Arc<Dir>>,
    pub(crate) summary: Summary,
    pub(crate) is_fresh: bool,
    pub(crate) we_created: bool,
    pub(crate) reused_lock: Option<safedir::ReusedDirLock>,
}

/// Mutable preservation data sampled before traversal from the chosen held input.
/// ACL preservation refreshes metadata immediately before the ACL reads, after ancestry checks.
/// These separately gated reads do not form an atomic filesystem snapshot.
pub(crate) struct SourceSnapshot {
    pub(crate) meta: FileMeta,
    pub(crate) acls: Option<safedir::Acls>,
}

pub(crate) struct PreservationSource<'a> {
    pub(crate) dir: &'a Dir,
    pub(crate) meta: &'a FileMeta,
    pub(crate) path: &'a Path,
    pub(crate) acl: bool,
}

impl SourceSnapshot {
    pub(crate) async fn capture(dir: &Dir, meta: FileMeta, acl: bool) -> std::io::Result<Self> {
        let (meta, acls) = if acl {
            // the opening snapshot may predate a slow ancestry walk. Sample preservation metadata
            // again here so that unrelated work cannot separate its mode from the ACL reads
            (dir.meta().await?, Some(dir.read_acls().await?))
        } else {
            (meta, None)
        };
        Ok(Self { meta, acls })
    }
}

impl DestinationContext {
    #[cfg(test)]
    pub(crate) fn new(named_root: &Path) -> Self {
        Self::for_operand(named_root, None, named_root)
    }

    pub(crate) fn for_operand(source: &Path, update: Option<&Path>, destination: &Path) -> Self {
        let registry = Arc::new(DirectoryOverlap::default());
        let protection = Arc::new(InputProtection::new(Arc::clone(&registry)));
        let mut context = Self::with_registry(source, destination, registry, protection);
        Arc::get_mut(&mut context.operand)
            .expect("a new operand is unshared")
            .update = update.map(Path::to_owned);
        context
    }

    pub(super) fn with_registry(
        source: &Path,
        named_root: &Path,
        registry: Arc<DirectoryOverlap>,
        protection: Arc<InputProtection>,
    ) -> Self {
        Self {
            origin: Origin::Root {
                named_root: named_root.to_owned(),
                real_parent: None,
                preview_parent: None,
            },
            registry,
            protection,
            operand: Arc::new(Operand {
                source: source.to_owned(),
                update: None,
                destination: named_root.to_owned(),
                destination_ancestors: Default::default(),
            }),
            input_role: InputRole::Source,
            _root_claim: None,
        }
    }

    pub(crate) fn protection(&self) -> &Arc<InputProtection> {
        &self.protection
    }

    pub(crate) fn with_input_role(mut self, role: InputRole) -> Self {
        self.input_role = role;
        self
    }

    pub(crate) async fn record_root(
        &self,
        parent: &Dir,
        name: &OsStr,
        entry: &safedir::Handle,
        path: &Path,
    ) -> std::io::Result<()> {
        if entry.kind() != crate::walk::EntryKind::Dir {
            self.protection
                .record_leaf(parent.identity().await?, name, path);
        }
        Ok(())
    }

    pub(crate) fn reserve_roots(
        &mut self,
        inputs: &[(InputRole, &FileMeta)],
    ) -> std::io::Result<()> {
        if !inputs.is_empty() {
            self._root_claim = Some(self.registry.reserve(
                inputs,
                &Evidence::new(),
                Arc::clone(&self.operand),
            )?);
        }
        Ok(())
    }

    pub(crate) async fn check_leaf_replacement_parent(
        &self,
        parent: &Dir,
        path: &Path,
    ) -> std::io::Result<()> {
        if matches!(self.origin, Origin::Root { .. }) {
            self.protection.check_root_parent(parent, path).await?;
        }
        Ok(())
    }

    pub(crate) fn with_root_parent(mut self, parent: Option<&Arc<Dir>>) -> Self {
        if let Origin::Root { real_parent, .. } = &mut self.origin {
            *real_parent = parent.cloned();
        }
        self
    }

    pub(crate) fn with_preview_parent(mut self, parent: Option<PreviewParent>) -> Self {
        if let Some(parent) = parent {
            let Origin::Root { preview_parent, .. } = &mut self.origin else {
                unreachable!("only operand roots perform eager preview validation");
            };
            *preview_parent = Some(parent);
        }
        self
    }

    async fn preview_root(
        &self,
        named_root: &Path,
        readable: bool,
    ) -> anyhow::Result<(PreviewEntry, Arc<Evidence>)> {
        let (parent_path, name) = safedir::split_parent_and_name(named_root)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let prepared = match &self.origin {
            Origin::Root {
                preview_parent: Some(parent),
                ..
            } => parent.clone(),
            Origin::Root {
                preview_parent: None,
                ..
            } => PreviewParent::open(parent_path).await?,
            Origin::Prepared(_) => unreachable!("only roots resolve operand parents"),
        };
        let parent = match prepared {
            PreviewParent::Held(parent) => parent,
            PreviewParent::Missing => return Ok((PreviewEntry::Missing, Arc::default())),
            PreviewParent::Unavailable if !readable => {
                return Ok((PreviewEntry::Unavailable, Arc::default()));
            }
            PreviewParent::Unavailable => {
                return Err(std::io::Error::from_raw_os_error(libc::EACCES).into());
            }
            PreviewParent::NotDirectory => {
                return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR).into());
            }
        };
        let evidence = self
            .registry
            .ancestors(&parent)
            .await
            .context("while checking destination ancestry")?;
        let entry = safedir::open_preview_entry(&parent, name, readable).await?;
        Ok((entry, evidence))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enter(
        &self,
        prog_track: &'static crate::progress::Progress,
        inputs: &[(InputRole, &FileMeta)],
        preservation: PreservationSource<'_>,
        name: &OsStr,
        path: &Path,
        settings: &Settings,
        dry_run: Option<crate::config::DryRunMode>,
        is_fresh: bool,
    ) -> Result<Admission, Error> {
        let readable = settings.delete.is_some();
        let preview = dry_run.is_some();
        let (parent, entry, evidence) = match (&self.origin, preview) {
            (Origin::Root { named_root, .. }, true) => {
                let (entry, evidence) = self
                    .preview_root(named_root, readable)
                    .await
                    .with_context(|| format!("cannot open destination {path:?} for preview"))
                    .map_err(|error| Error::new(error, Summary::default()))?;
                (None, Some(entry), evidence)
            }
            (Origin::Root { real_parent, .. }, false) => {
                let parent = real_parent
                    .as_ref()
                    .expect("a real root has a held destination parent");
                let evidence = self
                    .registry
                    .ancestors(parent)
                    .await
                    .context("while checking destination ancestry")
                    .map_err(|error| Error::new(error, Summary::default()))?;
                (Some(Arc::clone(parent)), None, evidence)
            }
            (Origin::Prepared(PreparedParent::Real(parent)), false) => {
                (Some(Arc::clone(parent)), None, Arc::default())
            }
            (Origin::Prepared(PreparedParent::Preview(parent)), true) => {
                let entry = match parent {
                    PreviewEntry::Directory(dir) => dir
                        .child(name, readable)
                        .await
                        .with_context(|| format!("cannot open destination {path:?} for preview"))
                        .map_err(|error| Error::new(error, Summary::default()))?,
                    other => other.clone(),
                };
                (None, Some(entry), Arc::default())
            }
            _ => unreachable!("preview and real destination authorities cannot be interchanged"),
        };
        if matches!(&entry, Some(PreviewEntry::Unavailable)) && settings.ignore_existing {
            return Err(Error::new(
                anyhow::Error::new(std::io::Error::from_raw_os_error(libc::EACCES)).context(
                    format!("cannot probe destination {path:?} for ignore-existing"),
                ),
                Summary::default(),
            ));
        }
        if matches!(&entry, Some(PreviewEntry::NonDirectory)) && settings.ignore_existing {
            return Ok(Admission::Skip(skip_summary(prog_track, path, dry_run)));
        }
        if matches!(&self.origin, Origin::Root { .. }) {
            self.operand
                .destination_ancestors
                .get_or_init(|| Arc::clone(&evidence));
        }
        let reservation = match self
            .registry
            .reserve(inputs, &evidence, Arc::clone(&self.operand))
        {
            Ok(reservation) => reservation,
            Err(error) => {
                // only a conflicting ancestry needs an early non-directory skip probe; ordinary
                // ignore-existing directories reuse resolution's authoritative classification
                if settings.ignore_existing
                    && let Some(parent) = &parent
                    && let Some(summary) =
                        skip_non_directory(prog_track, parent, name, path).await?
                {
                    return Ok(Admission::Skip(summary));
                }
                return Err(Error::new(error.into(), Summary::default()));
            }
        };
        let snapshot = match SourceSnapshot::capture(
            preservation.dir,
            preservation.meta.clone(),
            preservation.acl && !preview,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if settings.ignore_existing
                    && let Some(parent) = &parent
                    && let Some(summary) =
                        skip_non_directory(prog_track, parent, name, path).await?
                {
                    return Ok(Admission::Skip(summary));
                }
                return Err(Error::new(
                    anyhow::Error::new(error).context(format!(
                        "cannot capture metadata and ACLs from directory {:?}",
                        preservation.path
                    )),
                    Summary::default(),
                ));
            }
        };
        let (parent, directory) = if let Some(entry) = entry {
            let preview_dir = if let PreviewEntry::Directory(dir) = &entry {
                reservation
                    .output(&dir.meta)
                    .map_err(|error| Error::new(error.into(), Summary::default()))?;
                dir.readable()
            } else {
                None
            };
            (
                PreparedParent::Preview(entry),
                EnteredDirectory {
                    dir: None,
                    parent: None,
                    preview: preview_dir,
                    summary: Summary {
                        directories_created: 1,
                        ..Default::default()
                    },
                    is_fresh,
                    we_created: true,
                    reused_lock: None,
                },
            )
        } else {
            let parent = parent.expect("real admission retains its parent");
            match super::resolve_dst_dir(
                prog_track,
                &parent,
                name,
                path,
                settings,
                &reservation,
                &self.protection,
            )
            .await?
            {
                DirResolution::Skip(summary) => return Ok(Admission::Skip(summary)),
                DirResolution::Proceed(slot) => (
                    PreparedParent::Real(Arc::clone(&slot.dir)),
                    EnteredDirectory {
                        dir: Some(slot.dir),
                        parent: Some(parent),
                        preview: None,
                        summary: slot.summary,
                        is_fresh: slot.is_fresh,
                        we_created: slot.we_created,
                        reused_lock: slot.reused_lock,
                    },
                ),
            }
        };
        reservation.commit();
        Ok(Admission::Entered {
            destination: PreparedDestination {
                parent,
                registry: Arc::clone(&self.registry),
                protection: Arc::clone(&self.protection),
                operand: Arc::clone(&self.operand),
                input_role: self.input_role,
            },
            directory,
            snapshot,
        })
    }

    #[cfg(test)]
    pub(crate) fn parent(dir: Option<Arc<Dir>>) -> Self {
        // explicit test setup starts from a held physical parent or known hypothetical absence
        let parent = dir.map_or(
            PreparedParent::Preview(PreviewEntry::Missing),
            PreparedParent::Real,
        );
        let registry = Arc::new(DirectoryOverlap::default());
        PreparedDestination {
            parent,
            protection: Arc::new(InputProtection::new(Arc::clone(&registry))),
            registry,
            operand: Arc::new(Operand {
                source: "test source".into(),
                update: None,
                destination: "test destination".into(),
                destination_ancestors: Default::default(),
            }),
            input_role: InputRole::Source,
        }
        .child()
    }
}

async fn skip_non_directory(
    prog_track: &'static crate::progress::Progress,
    parent: &Dir,
    name: &OsStr,
    path: &Path,
) -> Result<Option<Summary>, Error> {
    match parent.child_kind(name).await {
        Ok(crate::walk::EntryKind::Dir) => Ok(None),
        Ok(_) => Ok(Some(skip_summary(prog_track, path, None))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::new(
            anyhow::Error::new(error).context(format!("cannot probe destination {path:?}")),
            Summary::default(),
        )),
    }
}

fn skip_summary(
    prog_track: &'static crate::progress::Progress,
    path: &Path,
    dry_run: Option<crate::config::DryRunMode>,
) -> Summary {
    match dry_run {
        None => {
            prog_track.directories_unchanged.inc();
        }
        Some(crate::config::DryRunMode::Brief) => {}
        Some(crate::config::DryRunMode::All) => println!("skip dir {path:?}"),
        Some(crate::config::DryRunMode::Explain) => {
            println!("skip dir {path:?} (destination exists, not a directory)")
        }
    }
    Summary {
        directories_unchanged: 1,
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) async fn test_prepared(
    context: DestinationContext,
    real: Option<&Arc<Dir>>,
    preview: Option<&Arc<Dir>>,
) -> anyhow::Result<PreparedDestination> {
    let parent = if let Some(real) = real {
        PreparedParent::Real(Arc::clone(real))
    } else if let Some(preview) = preview {
        PreparedParent::Preview(safedir::preview_from_readable(Arc::clone(preview)).await?)
    } else {
        PreparedParent::Preview(PreviewEntry::Missing)
    };
    Ok(PreparedDestination {
        parent,
        registry: context.registry,
        protection: context.protection,
        operand: context.operand,
        input_role: context.input_role,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn preview_children_reject_symlinks_and_non_component_names() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let named = root.path().join("named");
        let outside = root.path().join("outside");
        std::fs::create_dir(&named)?;
        std::fs::create_dir_all(outside.join("child"))?;
        std::os::unix::fs::symlink(&outside, named.join("escape"))?;
        let context = DestinationContext::new(&named);
        let PreviewEntry::Directory(dir) = context.preview_root(&named, false).await?.0 else {
            panic!("the named preview root must open as a directory");
        };
        assert!(matches!(
            dir.child(OsStr::new("escape"), false).await?,
            PreviewEntry::NonDirectory
        ));
        for invalid in [
            "",
            "/absolute",
            ".",
            "..",
            "one/./two",
            "one/../two",
            "one/two",
        ] {
            let error = dir.child(OsStr::new(invalid), false).await.unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        }
        Ok(())
    }
}
