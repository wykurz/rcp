//! Shared static timing scopes for async and blocking metadata operations.

pub(crate) enum Phase {
    Rate,
    Admission,
    Queue,
    Execute,
}

pub(crate) fn scope(
    side: congestion::Side,
    op: congestion::MetadataOp,
    phase: Phase,
) -> crate::timing::Scope {
    // static callsites keep aggregate names bounded without allocating per-operation strings.
    // root spans retain only their own dispatcher when they move across tasks or worker threads
    macro_rules! scope {
        ($side:literal, $op:literal, $phase:literal) => {
            crate::timing::Scope::new(tracing::trace_span!(
                target: "rcp::timing",
                parent: None,
                concat!($side, ".metadata.", $op, ".", $phase),
                timing_finished = tracing::field::Empty
            ))
        };
    }
    macro_rules! phases {
        ($side:literal, $op:literal) => {
            match phase {
                Phase::Rate => scope!($side, $op, "wait_rate"),
                Phase::Admission => scope!($side, $op, "wait_admission"),
                Phase::Queue => scope!($side, $op, "wait_worker"),
                Phase::Execute => scope!($side, $op, "execute"),
            }
        };
    }
    macro_rules! operations {
        ($side:literal) => {
            match op {
                congestion::MetadataOp::Stat => phases!($side, "stat"),
                congestion::MetadataOp::ReadLink => phases!($side, "read-link"),
                congestion::MetadataOp::MkDir => phases!($side, "mkdir"),
                congestion::MetadataOp::RmDir => phases!($side, "rmdir"),
                congestion::MetadataOp::Unlink => phases!($side, "unlink"),
                congestion::MetadataOp::HardLink => phases!($side, "hard-link"),
                congestion::MetadataOp::Symlink => phases!($side, "symlink"),
                congestion::MetadataOp::Chmod => phases!($side, "chmod"),
                congestion::MetadataOp::OpenCreate => phases!($side, "open-create"),
            }
        };
    }
    match side {
        congestion::Side::Source => operations!("source"),
        congestion::Side::Destination => operations!("destination"),
    }
}
