//! POSIX ACL preservation, end to end.
//!
//! Two independent ways a copy can end up MORE PERMISSIVE than its source, both covered here:
//! dropping a source ACL that acted as a deny, and letting the destination tree's default ACL be
//! inherited by entries rcp creates. The second is why "do nothing when the source has no ACL" is
//! not correct — preserving means clearing as well as setting.
//!
//! `--preserve-settings=all` deliberately does NOT preserve ACLs: detecting one costs a syscall per
//! entry that `stat` cannot fold in. `all+acl` opts in. That opt-in is only worth anything if the
//! default really pays nothing, so it is asserted on syscall count rather than on outcome.

use std::os::unix::fs::PermissionsExt;

#[path = "support/acl.rs"]
mod acl;
#[path = "support/fixtures.rs"]
mod fixtures;

use acl::{ACL_ACCESS, ACL_DEFAULT, denying_acl, describe_acl, get_acl, granting_acl, set_acl};
use fixtures::{create_test_file, get_file_content, get_file_mode, setup_test_env};

fn rcp(args: &[&str]) -> std::process::Output {
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rcp {args:?} failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn preserves_a_source_access_acl_on_a_file() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("secret.txt");
    let dst_file = dst_dir.path().join("secret.txt");
    create_test_file(&src_file, "secret", 0o700);
    let blob = denying_acl();
    set_acl(&src_file, ACL_ACCESS, &blob);
    rcp(&[
        "--preserve-settings=all+acl",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ]);
    let got = get_acl(&dst_file, ACL_ACCESS);
    assert_eq!(
        got.as_deref(),
        Some(blob.as_slice()),
        "destination ACL is {} (source {}); without the named deny, uid 65534 gains the read and \
         execute that `other` grants and the source withheld",
        describe_acl(got.as_ref()),
        describe_acl(Some(&blob))
    );
    assert_eq!(get_file_mode(&dst_file), get_file_mode(&src_file));
    assert_eq!(get_file_content(&dst_file), "secret");
}

#[test]
fn preserves_both_acls_on_a_directory() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    create_test_file(&src_sub.join("child.txt"), "payload", 0o644);
    let access = denying_acl();
    let default = granting_acl();
    set_acl(&src_sub, ACL_ACCESS, &access);
    set_acl(&src_sub, ACL_DEFAULT, &default);
    rcp(&[
        "--preserve-settings=all+acl",
        src_sub.to_str().unwrap(),
        dst_sub.to_str().unwrap(),
    ]);
    assert_eq!(
        get_acl(&dst_sub, ACL_ACCESS).as_deref(),
        Some(access.as_slice())
    );
    let got_default = get_acl(&dst_sub, ACL_DEFAULT);
    assert_eq!(
        got_default.as_deref(),
        Some(default.as_slice()),
        "the default ACL decides what CHILDREN inherit, so dropping it silently changes the \
         destination tree's inheritance policy; got {}",
        describe_acl(got_default.as_ref())
    );
    assert_eq!(get_file_mode(&dst_sub), get_file_mode(&src_sub));
}

#[test]
fn clears_an_acl_the_destination_tree_would_have_imposed() {
    // §1.2: nothing unusual on the source — just a destination tree with a default ACL, which is
    // what default ACLs are for. Every entry rcp creates beneath it inherits, INCLUDING the
    // directories rcp creates itself, so a subtree is what makes the test meaningful.
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::create_dir(src_sub.join("nested")).unwrap();
    create_test_file(&src_sub.join("private.txt"), "private", 0o640);
    create_test_file(&src_sub.join("nested/deep.txt"), "deeper", 0o640);
    set_acl(dst_dir.path(), ACL_DEFAULT, &granting_acl());
    let dst_sub = dst_dir.path().join("tree");
    rcp(&[
        "--preserve-settings=all+acl",
        src_sub.to_str().unwrap(),
        dst_sub.to_str().unwrap(),
    ]);
    for rel in ["private.txt", "nested", "nested/deep.txt", ""] {
        let path = dst_sub.join(rel);
        let got = get_acl(&path, ACL_ACCESS);
        assert_eq!(
            got,
            None,
            "{path:?} kept an inherited access ACL ({}); its source had none, so uid 65534 was \
             granted access the source never gave",
            describe_acl(got.as_ref())
        );
        let got_default = get_acl(&path, ACL_DEFAULT);
        assert_eq!(
            got_default,
            None,
            "{path:?} kept an inherited default ACL ({}), which would go on to widen anything \
             created under it later",
            describe_acl(got_default.as_ref())
        );
    }
    assert_eq!(get_file_mode(&dst_sub.join("private.txt")), 0o640);
}

#[test]
fn all_without_acl_drops_the_source_acl() {
    // The documented state of the default path, pinned so a change to it is deliberate: `all`
    // copies the mode and nothing else. See `all_does_not_pay_the_acl_probe` for why.
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("secret.txt");
    let dst_file = dst_dir.path().join("secret.txt");
    create_test_file(&src_file, "secret", 0o700);
    set_acl(&src_file, ACL_ACCESS, &denying_acl());
    rcp(&[
        "--preserve-settings=all",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ]);
    assert_eq!(get_acl(&dst_file, ACL_ACCESS), None);
    assert_eq!(get_file_mode(&dst_file), get_file_mode(&src_file));
}

/// Count the ACL-probe syscalls one `rcp` run issues, by tracing it.
///
/// Returns the matching strace lines so a failure can show what was actually called. `-f` follows
/// the thread pool the metadata syscalls run on, without which this would count zero whatever the
/// code did.
fn count_xattr_syscalls(args: &[&str]) -> Vec<String> {
    let output = std::process::Command::new("strace")
        .args([
            "-f",
            "-e",
            "trace=getxattr,fgetxattr,lgetxattr,listxattr,flistxattr,llistxattr",
        ])
        .arg(assert_cmd::cargo::cargo_bin("rcp"))
        .args(args)
        .output()
        .unwrap_or_else(|err| {
            panic!(
                "cannot run strace: {err}. This test asserts on syscall COUNT rather than outcome, \
                 because the whole point of making ACLs opt-in is that the default path does not \
                 pay for them — an outcome-only check cannot see that regress. Install strace."
            )
        });
    assert!(
        output.status.success(),
        "traced rcp {args:?} failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter(|line| line.contains("getxattr(") || line.contains("listxattr("))
        .map(str::to_string)
        .collect()
}

#[test]
fn all_does_not_pay_the_acl_probe() {
    // Detecting an ACL costs a syscall per entry — there is no bit in `stat` for it — which is the
    // entire reason `acl` is opt-in rather than part of `all`. Asserting on outcome alone would let
    // that cost creep back into the default path unnoticed, so this asserts on the syscall count.
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    for i in 0..8 {
        create_test_file(&src_sub.join(format!("f{i}.txt")), "payload", 0o644);
    }
    set_acl(&src_sub.join("f0.txt"), ACL_ACCESS, &denying_acl());
    let plain_dst = dst_dir.path().join("plain");
    let traced = count_xattr_syscalls(&[
        "--preserve-settings=all",
        src_sub.to_str().unwrap(),
        plain_dst.to_str().unwrap(),
    ]);
    // `all` pays exactly ONE ACL syscall for the whole run: the constant source-root probe behind
    // the "this copy drops the root's ACL" warning. The bound is on the CONSTANT rather than on
    // zero, because what must never come back is a probe that scales with the tree.
    assert!(
        traced.len() <= 1,
        "`all` issued {} ACL probe syscall(s) — more than the one constant source-root probe, so \
         every copy that does not want ACLs now pays per entry:\n{}",
        traced.len(),
        traced.join("\n")
    );
    assert!(
        traced.iter().all(|line| line.contains("/proc/self/fd/")),
        "the ACL syscall `all` issued is not the constant source-root probe (which goes through \
         the root handle's /proc/self/fd magic symlink):\n{}",
        traced.join("\n")
    );
    // and prove the counter is not vacuous: the same copy WITH `acl` must show the per-entry probe.
    // Without this the assertion above would also pass if strace traced nothing at all.
    let acl_dst = dst_dir.path().join("with_acl");
    let traced = count_xattr_syscalls(&[
        "--preserve-settings=all+acl",
        src_sub.to_str().unwrap(),
        acl_dst.to_str().unwrap(),
    ]);
    assert!(
        traced.len() > 1,
        "`all+acl` issued {} ACL syscall(s) — no more than the constant root probe `all` pays, so \
         the counter proves nothing about `all`",
        traced.len()
    );
    assert_eq!(
        get_acl(&acl_dst.join("f0.txt"), ACL_ACCESS).as_deref(),
        Some(denying_acl().as_slice())
    );
    assert_eq!(get_acl(&plain_dst.join("f0.txt"), ACL_ACCESS), None);
}

#[test]
fn preserves_an_acl_alongside_a_setuid_mode() {
    // The ordering case: the special bits come from the chmod, the rwx bits from the ACL, and the
    // ACL is applied last so nothing re-derives its mask afterwards.
    let (src_dir, dst_dir) = setup_test_env();
    let src_file = src_dir.path().join("tool.bin");
    let dst_file = dst_dir.path().join("tool.bin");
    create_test_file(&src_file, "payload", 0o4700);
    let blob = denying_acl();
    set_acl(&src_file, ACL_ACCESS, &blob);
    // writing the ACL moved the source's own rwx bits to match it; the destination must reproduce
    // both that and the setuid bit.
    assert_eq!(get_file_mode(&src_file), 0o4755, "fixture source mode");
    rcp(&[
        "--preserve-settings=all+acl",
        src_file.to_str().unwrap(),
        dst_file.to_str().unwrap(),
    ]);
    assert_eq!(get_file_mode(&dst_file), 0o4755);
    assert_eq!(
        get_acl(&dst_file, ACL_ACCESS).as_deref(),
        Some(blob.as_slice())
    );
}

#[test]
fn per_type_acl_applies_only_to_the_type_that_asked_for_it() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_sub = src_dir.path().join("tree");
    let dst_sub = dst_dir.path().join("tree");
    std::fs::create_dir(&src_sub).unwrap();
    std::fs::set_permissions(&src_sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    let src_file = src_sub.join("child.txt");
    create_test_file(&src_file, "payload", 0o700);
    let blob = denying_acl();
    set_acl(&src_sub, ACL_ACCESS, &granting_acl());
    set_acl(&src_file, ACL_ACCESS, &blob);
    rcp(&[
        "--preserve-settings=f:uid,gid,time,acl,7777 d:uid,gid,time,7777 l:uid,gid,time",
        src_sub.to_str().unwrap(),
        dst_sub.to_str().unwrap(),
    ]);
    assert_eq!(
        get_acl(&dst_sub.join("child.txt"), ACL_ACCESS).as_deref(),
        Some(blob.as_slice()),
        "`f:acl` was requested"
    );
    assert_eq!(
        get_acl(&dst_sub, ACL_ACCESS),
        None,
        "`d:acl` was NOT requested, so the directory keeps mode fidelity only"
    );
}

// ── Containment under `--require-toctou-safe` (§10) ─────────────────────────────────────────────
//
// The `acl` attribute preserves the SOURCE's ACLs. `--require-toctou-safe` contains the
// DESTINATION's, which is a different bug: a destination tree's default ACL is inherited by every
// entry rcp creates beneath it, including the directories rcp creates itself. The flag's invariant
// is that no destination entry rcp CREATES carries an ACL entry that did not come from its source.
// A directory rcp creates has BOTH its ACLs stripped; a REUSED directory keeps its own access ACL
// and has its default ACL removed for the copy's duration and restored at the end — it was already
// there, and the flag is about what the copy writes, not about scrubbing the destination tree.
// Either way nothing created beneath one can inherit.
//
// The two flags are orthogonal and deliberately do not imply each other; the tests below pin both
// halves of that asymmetry, the last of them on syscall count.

/// `--require-toctou-safe` refuses to run without `openat2(2)`, so on a kernel without it there is
/// nothing to assert (same shape as `cli_parsing_tests.rs`).
fn strict_mode_unusable() -> bool {
    if common::safedir::openat2_available() {
        return false;
    }
    eprintln!("skipping: this kernel lacks openat2(2), --require-toctou-safe refuses");
    true
}

/// A three-level source tree with no ACLs anywhere, so every ACL observed on the destination came
/// from the destination tree's own inheritance rather than from the copy.
fn build_aclless_source(src_root: &std::path::Path) -> std::path::PathBuf {
    let tree = src_root.join("tree");
    std::fs::create_dir(&tree).unwrap();
    std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir_all(tree.join("one/two")).unwrap();
    create_test_file(&tree.join("a.txt"), "a", 0o644);
    create_test_file(&tree.join("one/b.txt"), "b", 0o644);
    create_test_file(&tree.join("one/two/c.txt"), "c", 0o644);
    tree
}

/// Assert every entry of `build_aclless_source`'s copy, at every depth, carries neither ACL.
fn assert_no_inherited_acls(dst_tree: &std::path::Path) {
    for rel in ["", "a.txt", "one", "one/b.txt", "one/two", "one/two/c.txt"] {
        let path = dst_tree.join(rel);
        let access = get_acl(&path, ACL_ACCESS);
        assert_eq!(
            access,
            None,
            "{path:?} carries an inherited access ACL ({}); its source had none, so uid 65534 was \
             granted access the source never gave",
            describe_acl(access.as_ref())
        );
        let default = get_acl(&path, ACL_DEFAULT);
        assert_eq!(
            default,
            None,
            "{path:?} carries an inherited default ACL ({}), which would go on to widen anything \
             created under it later",
            describe_acl(default.as_ref())
        );
    }
}

/// §13.11 — a REUSED destination directory carrying both ACLs. Nothing created inside it during the
/// copy inherits its default ACL, and both of its own ACLs are put back afterwards.
///
/// The two halves need each other: stripping without restoring would destroy an ACL the destination
/// had before the copy, and restoring without stripping would leave every child widened.
///
/// "Put back" is asserted against a TWIN copy of the same fixture run WITHOUT the flag, because that
/// is the lockdown's actual contract: a successful copy leaves the directory byte-identical to a
/// copy that never locked it down.
///
/// The source directory's mode (`0o700`) is deliberately DIFFERENT from the mode the destination's
/// own access ACL implies (`0o755`). Lockdown leaves the access ACL untouched, so the fixtures
/// differ only in what the finalize chmod does to its mask — but keep them unequal: equal modes make
/// the twin comparison pass for the wrong reason, since a destination whose ACL already agrees with
/// the source's mode survives almost any mishandling unchanged.
#[test]
fn strict_mode_contains_and_restores_a_reused_directorys_acls() {
    if strict_mode_unusable() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    // canonicalize: TMPDIR itself may contain symlinked components (e.g. under nix-shell), which
    // strict operand resolution would — correctly — refuse
    let tmp = tmp.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&tmp);
    std::fs::set_permissions(&src_tree, std::fs::Permissions::from_mode(0o700)).unwrap();
    // two identical pre-existing destination directories, each carrying both ACLs, copied into with
    // and without the flag
    let default = granting_acl();
    let make_dst = |name: &str| {
        let dst = tmp.join(name);
        std::fs::create_dir(&dst).unwrap();
        set_acl(&dst, ACL_ACCESS, &denying_acl());
        set_acl(&dst, ACL_DEFAULT, &default);
        assert_eq!(
            get_file_mode(&dst),
            0o755,
            "fixture: writing the access ACL sets the directory's own rwx bits from it, to \
             something other than the source's 0o700"
        );
        dst
    };
    let strict_dst = make_dst("strict_dst");
    let plain_dst = make_dst("plain_dst");
    rcp(&[
        "--require-toctou-safe",
        "--overwrite",
        src_tree.to_str().unwrap(),
        strict_dst.to_str().unwrap(),
    ]);
    rcp(&[
        "--overwrite",
        src_tree.to_str().unwrap(),
        plain_dst.to_str().unwrap(),
    ]);
    // containment: `a.txt` is the load-bearing one — a FILE gets no strip of its own, so it comes
    // out clean only because its parent's default ACL was gone before it was created.
    for rel in ["a.txt", "one", "one/b.txt", "one/two", "one/two/c.txt"] {
        let path = strict_dst.join(rel);
        let got = get_acl(&path, ACL_ACCESS);
        assert_eq!(
            got,
            None,
            "{path:?} inherited the reused directory's default ACL ({}) — the lockdown restricts \
             the MODE, but chmod does not touch a default ACL, so it has to be stripped outright",
            describe_acl(got.as_ref())
        );
        let got_default = get_acl(&path, ACL_DEFAULT);
        assert_eq!(got_default, None, "{path:?} inherited a default ACL");
    }
    // ...and the unflagged twin shows the inheritance was genuinely in play, so the assertions
    // above are about the flag rather than about a fixture that never inherited anything
    assert!(
        get_acl(&plain_dst.join("a.txt"), ACL_ACCESS).is_some(),
        "without the flag the same fixture must still inherit, or nothing above was contained"
    );
    // restore: the reused directory itself is left exactly as the unflagged copy left its twin
    let strict_default = get_acl(&strict_dst, ACL_DEFAULT);
    assert_eq!(
        strict_default.as_deref(),
        Some(default.as_slice()),
        "the reused directory permanently lost the default ACL it had before the copy; got {}",
        describe_acl(strict_default.as_ref())
    );
    let plain_default = get_acl(&plain_dst, ACL_DEFAULT);
    assert_eq!(
        strict_default,
        plain_default,
        "the lockdown changed the reused directory's default ACL relative to an unflagged copy: \
         {} vs {}",
        describe_acl(strict_default.as_ref()),
        describe_acl(plain_default.as_ref())
    );
    let strict_access = get_acl(&strict_dst, ACL_ACCESS);
    assert!(
        strict_access.is_some(),
        "the reused directory's own access ACL was not put back at all"
    );
    let plain_access = get_acl(&plain_dst, ACL_ACCESS);
    // byte-exact, not merely equivalent: this IS the lockdown's documented contract
    assert_eq!(
        strict_access,
        plain_access,
        "the lockdown changed the reused directory's access ACL relative to an unflagged copy: \
         {} vs {}",
        describe_acl(strict_access.as_ref()),
        describe_acl(plain_access.as_ref())
    );
    assert_eq!(
        get_file_mode(&strict_dst),
        0o700,
        "the reused directory must end at the SOURCE mode, not at the 0o755 its own access ACL \
         implies, and not at the interim lockdown 0o700"
    );
    assert_eq!(
        get_file_mode(&strict_dst),
        get_file_mode(&plain_dst),
        "the lockdown changed the reused directory's final mode relative to an unflagged copy"
    );
    assert_eq!(get_file_content(&strict_dst.join("one/two/c.txt")), "c");
}

/// An ABORTED strict copy must not destroy the reused directory's ACL.
///
/// The lockdown removes the destination's default ACL and holds the only copy of those bytes in
/// memory. Every path that locks a directory and then never reaches finalize therefore had the
/// power to destroy it permanently and silently: `--fail-early` returns from the walk driver
/// without calling `dir_post` and aborts in-flight siblings by dropping their `JoinSet`, so one
/// unreadable leaf could take out many directories' ACLs at once. `link_dir_contents` needs no flag
/// at all — a failed `read_entries` returns early. The remote destination can fail between locking
/// a directory and registering it. That is why the restore lives in `ReusedDirLock`'s `Drop` rather
/// than at each of those sites.
///
/// This is a data-destruction regression test, so it asserts the copy actually FAILED first: a run
/// with enough privilege to read the unreadable source directory would copy it successfully and
/// prove nothing.
#[test]
fn an_aborted_strict_copy_does_not_destroy_the_reused_directorys_acl() {
    if strict_mode_unusable() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let tmp = tmp.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&tmp);
    // a source directory the copier cannot enumerate: its leaf fails, and --fail-early turns that
    // into an abort of the whole walk before any directory reaches finalize. It sits at the BOTTOM
    // of the tree on purpose — the walk locks every destination directory on the way down, so by
    // the time this fails there are three live lockdowns to lose, which is the real blast radius:
    // --fail-early drops the sibling `JoinSet` and cancels every in-flight directory task at once.
    let unreadable = src_tree.join("one/two/unreadable");
    std::fs::create_dir(&unreadable).unwrap();
    create_test_file(&unreadable.join("b.txt"), "x", 0o644);
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    // the matching pre-existing destination directories, each carrying both ACLs. NB: build these
    // by joining real components — `Path::join("")` appends a trailing separator, and a trailing
    // slash on rcp's destination means "copy INTO", which would create a fresh subdirectory and
    // reuse nothing at all, quietly making this test vacuous.
    let dst_tree = tmp.join("dst");
    let reused = [
        dst_tree.clone(),
        dst_tree.join("one"),
        dst_tree.join("one/two"),
    ];
    let access = denying_acl();
    let default = granting_acl();
    for dir in &reused {
        std::fs::create_dir(dir).unwrap();
        set_acl(dir, ACL_ACCESS, &access);
        set_acl(dir, ACL_DEFAULT, &default);
    }
    let output = std::process::Command::new(assert_cmd::cargo::cargo_bin("rcp"))
        .args([
            "--require-toctou-safe",
            "--overwrite",
            "--fail-early",
            // -vv so the guard's own `debug!` is emitted: the assertion below observes that `Drop`
            // ran, rather than inferring it from the filesystem. Filesystem state alone cannot tell
            // "the guard restored it" from "it was never removed" — and cannot tell either from
            // "the binary under test was not the one you think", which is a real way to be misled.
            // NB: this crate's tracing writer is STDOUT, not stderr.
            "-vv",
            src_tree.to_str().unwrap(),
            dst_tree.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    // restore the fixture's permissions before any assertion can panic, or the TempDir cleanup
    // fails and buries the real failure under a Drop error
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        !output.status.success(),
        "the copy was expected to ABORT — it must not be possible to read the 0o000 source \
         directory, or this test asserts nothing about the aborted path"
    );
    for dir in &reused {
        assert_eq!(
            get_file_mode(dir),
            0o700,
            "{dir:?} is not at the interim lockdown mode, so it was never locked down and says \
             nothing about what an aborted lockdown leaves behind"
        );
    }
    let logged = String::from_utf8_lossy(&output.stdout);
    let restores = logged
        .matches("restored the default ACL of reused destination directory")
        .count();
    assert_eq!(
        restores,
        reused.len(),
        "expected one guard restore per locked directory; the ACLs below may look intact simply \
         because nothing ever removed them"
    );
    for dir in &reused {
        let got_default = get_acl(dir, ACL_DEFAULT);
        assert_eq!(
            got_default.as_deref(),
            Some(default.as_slice()),
            "the aborted copy destroyed {dir:?}'s default ACL; it held {} and those bytes existed \
             nowhere else, so nothing can recover them and nothing reported the loss",
            describe_acl(Some(&default))
        );
        // the access ACL is never stripped by the lockdown, so its named entries are still there.
        // Its base entries reflect the 0o700 an aborted lockdown leaves behind — the documented,
        // VISIBLE outcome, and fully undone by an operator's `chmod` back to 0o755.
        let got_access = get_acl(dir, ACL_ACCESS);
        assert!(
            got_access.is_some(),
            "the aborted copy destroyed {dir:?}'s access ACL as well"
        );
        assert!(
            describe_acl(got_access.as_ref()).contains("u:65534:---"),
            "the named entry that made {dir:?}'s ACL worth keeping is gone: {}",
            describe_acl(got_access.as_ref())
        );
    }
}

/// §13.12 — the §1.2 regression test for directories rcp CREATES, which is the case a reused-
/// directory lockdown cannot reach.
///
/// The administrator's default ACL sits on the destination PARENT — the one directory rcp neither
/// creates nor reuses — so every directory below it is one rcp made itself with `mkdirat`. A
/// directory created under a parent with a default ACL inherits BOTH an access and a default ACL,
/// and its children then inherit in turn, so this walks the whole subtree rather than one level:
/// without the post-`mkdirat` strip every entry listed carries an entry its source never had.
#[test]
fn strict_mode_prevents_inheritance_in_every_directory_it_creates() {
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&src_base);
    set_acl(&dst_base, ACL_DEFAULT, &granting_acl());
    let dst_tree = dst_base.join("tree");
    rcp(&[
        "--require-toctou-safe",
        src_tree.to_str().unwrap(),
        dst_tree.to_str().unwrap(),
    ]);
    assert_no_inherited_acls(&dst_tree);
    assert_eq!(get_file_content(&dst_tree.join("one/two/c.txt")), "c");
}

// ── The three containment levels of §1.2.1 ─────────────────────────────────────────────────────
//
// | mode                                  | §1.2 contained?                  | cost               |
// | ------------------------------------- | -------------------------------- | ------------------ |
// | default (neither `acl` nor strict)    | no — documented                  | none               |
// | `acl` requested                       | yes, cleared at finalize         | per-ENTRY probe    |
// | `--require-toctou-safe`               | yes, prevented at creation       | per-DIRECTORY strip|
//
// One test per row, over one shared fixture, so a change that silently moves the cost from one row
// to another fails exactly one of them instead of passing everywhere.

/// Row 1: the default path leaves the destination tree's inherited entry in place. This is a
/// deliberate hole, not an oversight — containing it costs either the per-entry probe or the
/// per-directory strip, and neither is worth imposing on ordinary data movement for what is a
/// privileged-copy concern. Pinned so that changing it has to be a decision.
#[test]
fn default_leaves_the_destination_trees_inherited_acl() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(dst_dir.path(), ACL_DEFAULT, &granting_acl());
    let dst_tree = dst_dir.path().join("tree");
    rcp(&[src_tree.to_str().unwrap(), dst_tree.to_str().unwrap()]);
    assert!(
        get_acl(&dst_tree.join("a.txt"), ACL_ACCESS).is_some(),
        "the default path is documented as NOT containing an inherited destination ACL; if this \
         now passes, the containment moved into the default path and its cost came with it"
    );
    assert!(
        get_acl(&dst_tree, ACL_DEFAULT).is_some(),
        "a directory rcp created under a default ACL inherits one of its own"
    );
}

/// Row 2: `acl` clears the inherited entry, at finalize — the applier's clear step runs on every
/// entry, which is what the per-entry probe buys.
#[test]
fn acl_clears_the_destination_trees_inherited_acl() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(dst_dir.path(), ACL_DEFAULT, &granting_acl());
    let dst_tree = dst_dir.path().join("tree");
    rcp(&[
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        dst_tree.to_str().unwrap(),
    ]);
    assert_no_inherited_acls(&dst_tree);
}

/// Row 3: `--require-toctou-safe` prevents the inherited entry at creation, without `acl` and
/// without its per-entry cost. Same fixture and same end state as row 2, reached a different way —
/// which `strict_mode_does_not_enable_acl_preservation` below distinguishes on syscall count.
#[test]
fn strict_mode_prevents_the_destination_trees_inherited_acl() {
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&src_base);
    set_acl(&dst_base, ACL_DEFAULT, &granting_acl());
    let dst_tree = dst_base.join("tree");
    rcp(&[
        "--require-toctou-safe",
        src_tree.to_str().unwrap(),
        dst_tree.to_str().unwrap(),
    ]);
    assert_no_inherited_acls(&dst_tree);
}

/// §10.2 — `--require-toctou-safe` must NOT silently enable `acl`.
///
/// The two flags close different bugs: strict mode contains the DESTINATION's inherited ACLs, and
/// says nothing about preserving the SOURCE's. Auto-enabling `acl` would impose the per-entry probe
/// (§1.3) on a flag people reach for a different reason, and would silently override an explicit
/// `--preserve-settings`. So this asserts on syscall count as well as outcome: an outcome-only
/// check cannot see the cost creep in.
#[test]
fn strict_mode_does_not_enable_acl_preservation() {
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&src_base);
    // a source ACL strict mode is NOT expected to carry: §1.1 stays open under this flag alone
    set_acl(&src_tree.join("a.txt"), ACL_ACCESS, &denying_acl());
    let strict_dst = dst_base.join("strict");
    let traced = count_xattr_syscalls(&[
        "--require-toctou-safe",
        src_tree.to_str().unwrap(),
        strict_dst.to_str().unwrap(),
    ]);
    // one constant source-root probe is expected (the "this copy drops the root's ACL" warning);
    // anything beyond it is the per-entry cost creeping in
    assert!(
        traced.len() <= 1,
        "--require-toctou-safe issued {} ACL probe syscall(s) — more than the one constant \
         source-root probe, so it has started paying the per-entry cost that makes `acl` \
         opt-in:\n{}",
        traced.len(),
        traced.join("\n")
    );
    assert_eq!(
        get_acl(&strict_dst.join("a.txt"), ACL_ACCESS),
        None,
        "strict mode does not preserve the SOURCE's ACLs — that is what `all+acl` is for"
    );
    // and prove the counter is not vacuous: the SAME flag WITH `acl` must show the probe
    let both_dst = dst_base.join("both");
    let traced = count_xattr_syscalls(&[
        "--require-toctou-safe",
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        both_dst.to_str().unwrap(),
    ]);
    assert!(
        traced.len() > 1,
        "`--require-toctou-safe --preserve-settings=all+acl` issued {} ACL syscall(s) — no more \
         than the constant root probe the run without `acl` pays, so the counter proves nothing \
         about it",
        traced.len()
    );
    assert_eq!(
        get_acl(&both_dst.join("a.txt"), ACL_ACCESS).as_deref(),
        Some(denying_acl().as_slice()),
        "pairing the flag with `all+acl` is what closes both bugs at once"
    );
}

/// Count the ACL-removal syscalls one `rcp` run issues. Companion to `count_xattr_syscalls`, which
/// counts the read side.
fn count_removexattr_syscalls(args: &[&str]) -> Vec<String> {
    let output = std::process::Command::new("strace")
        .args(["-f", "-e", "trace=removexattr,fremovexattr,lremovexattr"])
        .arg(assert_cmd::cargo::cargo_bin("rcp"))
        .args(args)
        .output()
        .unwrap_or_else(|err| {
            panic!(
                "cannot run strace: {err}. This test asserts on syscall COUNT rather than outcome, \
                 because strict-mode containment is only affordable if it is paid per DIRECTORY — \
                 an outcome-only check cannot see that regress into a per-entry cost. Install \
                 strace."
            )
        });
    assert!(
        output.status.success(),
        "traced rcp {args:?} failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter(|line| line.contains("removexattr("))
        .map(str::to_string)
        .collect()
}

/// The §1.2.1 cost claim, both halves: strict-mode containment is 2 syscalls per DIRECTORY and none
/// per file, and the DEFAULT path pays nothing at all.
///
/// The per-directory half is only true because stripping a directory's ACLs stops the inheritance
/// chain for its whole subtree — a child created inside a stripped directory has no ACL to begin
/// with, so there is nothing to clear on it. The cheap way to state that as a test is to hold the
/// directory count fixed and vary the file count: the strip count must not move.
///
/// The default-path half is what makes the whole trade acceptable — containment is a privileged-copy
/// concern and ordinary data movement must not fund it — and it rests on a single `if` in
/// `Dir::make_dir`. Removing that gate is caught here on COST, which is the thing being defended;
/// `default_leaves_the_destination_trees_inherited_acl` catches the same mutation on BEHAVIOUR,
/// because containment leaking into the default path also silently moves that row of the §1.2.1
/// table. Two tests, two different reasons the gate matters.
#[test]
fn strict_mode_strips_once_per_directory_not_per_file() {
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    // two source trees with the SAME number of directories (the tree itself plus `sub`) and very
    // different numbers of files
    let mut counts = Vec::new();
    let mut trees = Vec::new();
    for (name, files) in [("few", 2usize), ("many", 40usize)] {
        let tree = src_base.join(name);
        std::fs::create_dir(&tree).unwrap();
        std::fs::create_dir(tree.join("sub")).unwrap();
        for i in 0..files {
            create_test_file(&tree.join(format!("f{i}.txt")), "x", 0o644);
        }
        let dst_tree = dst_base.join(name);
        counts.push(count_removexattr_syscalls(&[
            "--require-toctou-safe",
            tree.to_str().unwrap(),
            dst_tree.to_str().unwrap(),
        ]));
        trees.push(tree);
    }
    let (few, many) = (&counts[0], &counts[1]);
    assert_eq!(
        few.len(),
        4,
        "expected 2 strips for each of the 2 directories rcp created; got:\n{}",
        few.join("\n")
    );
    assert_eq!(
        many.len(),
        few.len(),
        "the strip count grew from {} to {} when only the FILE count changed, so containment is \
         now being paid per entry instead of per directory:\n{}",
        few.len(),
        many.len(),
        many.join("\n")
    );
    // the same copy without the flag must strip nothing: the gate, not the strip, is what keeps the
    // default path free
    let plain_dst = dst_base.join("plain");
    let plain =
        count_removexattr_syscalls(&[trees[1].to_str().unwrap(), plain_dst.to_str().unwrap()]);
    assert!(
        plain.is_empty(),
        "a copy WITHOUT --require-toctou-safe issued {} ACL-strip syscall(s), so every ordinary \
         copy now funds a containment guarantee it did not ask for:\n{}",
        plain.len(),
        plain.join("\n")
    );
}

// ── The source-root warning (§3.3) ─────────────────────────────────────────────────────────────
//
// `all` not preserving ACLs is only defensible if a user who did not read this document finds out.
// One `listxattr` on the SOURCE ROOT per run — a constant, not a per-entry probe — says so.
//
// It is a HEURISTIC and the tests below say so: a root without an ACL proves nothing about its
// children, and the alternative (probing enough entries to be sure) IS the per-entry cost that made
// `acl` opt-in in the first place.

/// Run `rcp` at the DEFAULT verbosity — no `-v` — and return everything it wrote.
///
/// The absence of `-v` is the assertion. The global default level is `error`, so the notice is
/// only visible because it carries its own tracing target with its own `warn` directive; a change
/// that drops the target, the directive, or the `target:` on the emit re-hides it from exactly the
/// users it exists for, and every test through this helper fails. Both streams are captured
/// because the log layer writes through the progress bar's writer, which targets stdout.
fn rcp_log(args: &[&str]) -> String {
    let output = rcp(args);
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The marker every form of the warning shares, so a wording change does not silently make these
/// tests assert nothing.
const ROOT_WARNING: &str = "carries a POSIX ACL that this copy will NOT preserve";

#[test]
fn warns_when_the_source_root_carries_an_acl_that_is_not_preserved() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--preserve-settings=all",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("warned").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "no warning for a source root carrying an ACL that `all` drops, so a user who does not \
         know `all` excludes ACLs never finds out:\n{log}"
    );
    assert!(
        log.contains("all+acl"),
        "the warning must name the fix:\n{log}"
    );
    // and it names the root, so a multi-operand or nested run says WHICH path it is talking about
    assert!(
        log.contains(src_tree.to_str().unwrap()),
        "the warning must name the source root:\n{log}"
    );
}

#[test]
fn stays_silent_when_the_copy_asks_for_no_preservation() {
    // a copy left at the shipped default already drops uid, gid, timestamps and the
    // setuid/setgid/sticky bits without a word — it reproduces the source's `rwx` bits like `cp`
    // and claims nothing more. Singling ACLs out there would be advice about a completeness the
    // run never asked for, so the notice is armed by ASKING (`--preserve*`, or the strict flag),
    // not by an ACL merely existing. `--preserve-settings=none` resolves to the same settings and
    // must behave the same: it is the explicit spelling of "I did not ask".
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    for (label, args) in [
        ("default", vec![]),
        ("none", vec!["--preserve-settings=none"]),
        // spelled-out longhand of the default: the gate reads the RESOLVED settings, so this must
        // not behave differently from writing nothing
        ("longhand", vec!["--preserve-settings=f:0777 d:0777"]),
    ] {
        let dst = dst_dir.path().join(label);
        let mut argv = args.clone();
        argv.push(src_tree.to_str().unwrap());
        argv.push(dst.to_str().unwrap());
        let log = rcp_log(&argv);
        assert!(
            !log.contains(ROOT_WARNING),
            "`{label}` asked for no preservation, so the ACL notice is noise about a fidelity the \
             copy never claimed:\n{log}"
        );
    }
    // non-vacuous: the SAME tree under settings that do ask must still warn, or this test would
    // also pass with the notice deleted outright
    let log = rcp_log(&[
        "--preserve-settings=all",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("asked").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "the same root under `all` did not warn either, so the silence above proves nothing:\n{log}"
    );
}

#[test]
fn any_attribute_worth_preserving_arms_the_root_notice() {
    // the gate is "did this run ask for metadata fidelity at all", not "did it ask for the mode".
    // A run preserving only uid is making a claim about the destination's metadata just as much as
    // `all` is, and an ACL it silently drops undermines that claim the same way.
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--preserve-settings=d:uid",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("uid_only").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "a run preserving one attribute did not arm the notice, so the gate is keyed to a preset \
         rather than to asking:\n{log}"
    );
}

#[test]
fn strict_mode_arms_the_root_notice_without_any_preserve_flag() {
    // the two flags do not imply each other (§10.2). `--require-toctou-safe` is a request ABOUT
    // permissions whatever the preserve settings say, so it arms the notice on its own — and it is
    // the case where the user is most likely to assume the flag covered the source's ACLs too.
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&src_base);
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--require-toctou-safe",
        src_tree.to_str().unwrap(),
        dst_base.join("strict_only").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "--require-toctou-safe alone did not warn, so a user who reached for it — and may well \
         assume it carries source ACLs across — is told nothing:\n{log}"
    );
    assert!(
        log.contains("does not carry the SOURCE's"),
        "the strict wording must survive being armed by the flag alone:\n{log}"
    );
}

#[test]
fn quiet_suppresses_the_root_notice() {
    // `--quiet` installs no tracing subscriber at all, so it silences the notice like everything
    // else. That is the supported way to turn it off, and it must keep working — a notice the user
    // cannot switch off is a notice they learn to ignore.
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--quiet",
        "--preserve-settings=all",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("quiet").to_str().unwrap(),
    ]);
    assert!(
        !log.contains(ROOT_WARNING),
        "--quiet did not suppress the root notice:\n{log}"
    );
}

#[test]
fn the_notice_target_does_not_unmute_ordinary_warnings() {
    // The notice is visible at the default verbosity because of a directive scoped to ONE tracing
    // target, not because the global level was raised. The difference matters: 14 `warn!` sites sit
    // in per-entry paths, so a global `warn` default would print thousands of lines for one failed
    // subtree. `--preserve` alongside `--preserve-settings` emits an ordinary (non-notice) warning,
    // which must stay hidden at the default verbosity exactly as it was before the notice existed.
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    const IGNORED: &str = "ignored when --preserve-settings";
    let default_dst = dst_dir.path().join("default");
    let default_log = rcp_log(&[
        "--preserve",
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        default_dst.to_str().unwrap(),
    ]);
    assert!(
        !default_log.contains(IGNORED),
        "an ordinary warning became visible at the default verbosity, so the notice directive is \
         not target-scoped and every per-entry warning is now printed too:\n{default_log}"
    );
    // non-vacuous: the same run at -v must show it, or the assertion above proves nothing
    let verbose_dst = dst_dir.path().join("verbose");
    let verbose_log = rcp_log(&[
        "-v",
        "--preserve",
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        verbose_dst.to_str().unwrap(),
    ]);
    assert!(
        verbose_log.contains(IGNORED),
        "`-v` did not show the ordinary warning either, so the check above cannot distinguish \
         'still hidden' from 'never emitted':\n{verbose_log}"
    );
}

#[test]
fn stays_silent_when_the_source_root_acl_is_preserved() {
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--preserve-settings=all+acl",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("quiet").to_str().unwrap(),
    ]);
    assert!(
        !log.contains(ROOT_WARNING),
        "warned about an ACL the copy is preserving:\n{log}"
    );
}

#[test]
fn the_root_warning_consults_the_setting_for_the_roots_own_kind() {
    // `f:acl` and `d:acl` are independent, so the warning must ask about the kind the ROOT actually
    // is. Getting this wrong in either direction is silent: one way it warns about an ACL the copy
    // is preserving, the other it stays quiet about one being dropped.
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let file_only = rcp_log(&[
        "--preserve-settings=f:uid,gid,time,acl,7777 d:uid,gid,time,7777",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("file_only").to_str().unwrap(),
    ]);
    assert!(
        file_only.contains(ROOT_WARNING),
        "`f:acl` alone leaves a DIRECTORY root's ACL unpreserved, so the warning is still due:\n{file_only}"
    );
    let dir_only = rcp_log(&[
        "--preserve-settings=f:uid,gid,time,7777 d:uid,gid,time,acl,7777",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("dir_only").to_str().unwrap(),
    ]);
    assert!(
        !dir_only.contains(ROOT_WARNING),
        "`d:acl` preserves a DIRECTORY root's ACL, so there is nothing to warn about:\n{dir_only}"
    );
}

#[test]
fn a_root_that_cannot_warn_does_not_spend_the_probe_budget() {
    // The one probe per process is claimed AFTER the root's kind and the per-kind settings are
    // known, so a root that could never produce a notice does not consume it. Here the first
    // operand is a symlink — no ACL is possible on one — and the second is a directory that does
    // carry an ACL. If the symlink burned the budget, the directory would be silent and a user
    // copying several trees at once would be told nothing about any of them but the first.
    let (src_dir, dst_dir) = setup_test_env();
    std::os::unix::fs::symlink("/nonexistent", src_dir.path().join("alink")).unwrap();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let into = format!("{}/", dst_dir.path().to_str().unwrap());
    let log = rcp_log(&[
        "--preserve-settings=all",
        src_dir.path().join("alink").to_str().unwrap(),
        src_tree.to_str().unwrap(),
        &into,
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "the symlink operand consumed the one per-process probe, so the directory operand that \
         actually had an ACL to report was silenced:\n{log}"
    );
}

#[test]
fn stays_silent_when_the_source_root_has_no_acl() {
    // the heuristic's own boundary: the root is what is probed, so a tree whose ACLs live BELOW the
    // root produces no warning. Documented as a heuristic precisely because of this.
    let (src_dir, dst_dir) = setup_test_env();
    let src_tree = build_aclless_source(src_dir.path());
    set_acl(&src_tree.join("a.txt"), ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--preserve-settings=all",
        src_tree.to_str().unwrap(),
        dst_dir.path().join("unprobed").to_str().unwrap(),
    ]);
    assert!(
        !log.contains(ROOT_WARNING),
        "the probe is one syscall on the ROOT; warning about a child means it is walking the \
         tree, which is the per-entry cost `acl` is opt-in to avoid:\n{log}"
    );
}

#[test]
fn the_strict_mode_warning_says_the_flag_does_not_preserve_source_acls() {
    // §10.2: a user reaching for --require-toctou-safe may reasonably assume it covers both bugs.
    // It closes §1.2 (inherited destination ACLs) and leaves §1.1 (dropped source ACLs) open, so
    // the warning under that flag has to say so rather than repeat the generic wording.
    if strict_mode_unusable() {
        return;
    }
    let (src_dir, dst_dir) = setup_test_env();
    let src_base = src_dir.path().canonicalize().unwrap();
    let dst_base = dst_dir.path().canonicalize().unwrap();
    let src_tree = build_aclless_source(&src_base);
    set_acl(&src_tree, ACL_ACCESS, &denying_acl());
    let log = rcp_log(&[
        "--require-toctou-safe",
        "--preserve-settings=all",
        src_tree.to_str().unwrap(),
        dst_base.join("strict_warned").to_str().unwrap(),
    ]);
    assert!(
        log.contains(ROOT_WARNING),
        "no warning under --require-toctou-safe:\n{log}"
    );
    assert!(
        log.contains("does not carry the SOURCE's"),
        "the strict-mode warning must say the flag contains the DESTINATION's ACLs but does not \
         preserve the SOURCE's — otherwise it reads as though the flag covered both:\n{log}"
    );
    assert!(
        log.contains("all+acl"),
        "the strict-mode warning must name the fix:\n{log}"
    );
}

#[test]
fn the_root_warning_costs_one_syscall_whatever_the_tree_size() {
    // The warning is only affordable on the default path because it is a CONSTANT. Hold the root
    // fixed and vary the file count: the probe count must not move. (`all_does_not_pay_the_acl_probe`
    // bounds the same number at 1; this one proves the bound is structural rather than a
    // coincidence of that fixture's size.)
    let (src_dir, dst_dir) = setup_test_env();
    let mut counts = Vec::new();
    for (name, files) in [("few", 2usize), ("many", 40usize)] {
        let tree = src_dir.path().join(name);
        std::fs::create_dir(&tree).unwrap();
        for i in 0..files {
            create_test_file(&tree.join(format!("f{i}.txt")), "x", 0o644);
        }
        set_acl(&tree, ACL_ACCESS, &denying_acl());
        counts.push(count_xattr_syscalls(&[
            "--preserve-settings=all",
            tree.to_str().unwrap(),
            dst_dir.path().join(name).to_str().unwrap(),
        ]));
    }
    let (few, many) = (&counts[0], &counts[1]);
    assert_eq!(
        few.len(),
        1,
        "expected exactly the one root probe:\n{}",
        few.join("\n")
    );
    assert_eq!(
        many.len(),
        few.len(),
        "the ACL probe count grew from {} to {} when only the FILE count changed, so the root \
         warning is no longer a constant:\n{}",
        few.len(),
        many.len(),
        many.join("\n")
    );
}
