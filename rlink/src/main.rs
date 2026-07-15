use anyhow::{Context, Result, anyhow};
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rlink",
    version,
    about = "Hard-link large filesets efficiently with optional update path",
    long_about = "`rlink` allows hard-linking large number of files with optional update path for handling deltas.

EXAMPLES:
    # Hard-link contents of one path to another
    rlink /foo /bar --progress --summary

    # Hard-link with update (similar to rsync --link-dest)
    rlink /foo --update /bar /baz --update-exclusive --progress

In the second example, files from /foo are hard-linked to /baz if they match files in /bar. Using --update-exclusive means files present in /foo but not in /bar are ignored."
)]
struct Args {
    // Linking options
    /// Overwrite existing files/directories
    #[arg(short, long, help_heading = "Linking options")]
    overwrite: bool,

    /// File attributes to compare when deciding if files are identical (used with --overwrite)
    ///
    /// Comma-separated list. Available: uid, gid, mode, size, mtime, ctime
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Linking options"
    )]
    overwrite_compare: String,

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Linking options")]
    fail_early: bool,

    /// Skip special files (sockets, FIFOs, devices) without error
    #[arg(long, help_heading = "Linking options")]
    skip_specials: bool,

    /// Delete extraneous files from the destination (mirror the source)
    ///
    /// Removes entries under the destination that have no counterpart in the
    /// source (or, with --update, in the source or update directory). Implies
    /// --overwrite. Excluded files are protected unless --delete-excluded.
    #[arg(long, help_heading = "Linking options")]
    delete: bool,

    /// With --delete, also remove destination entries matching an exclude pattern
    #[arg(long, requires = "delete", help_heading = "Linking options")]
    delete_excluded: bool,

    /// Directory with updated contents of `link`
    #[arg(long, value_name = "PATH", help_heading = "Linking options")]
    update: Option<std::path::PathBuf>,

    /// Hard-link only the files that are in the update directory
    #[arg(long, help_heading = "Linking options")]
    update_exclusive: bool,

    /// Attributes to compare when deciding whether to hard-link or copy from update directory
    ///
    /// Same format as --overwrite-compare. Used with --update flag.
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Linking options"
    )]
    update_compare: String,

    /// Allow --update even when --preserve-settings does not cover all attributes
    /// used by --update-compare
    ///
    /// This is dangerous: attributes used for comparison (e.g. mtime) may not be preserved
    /// on copied files, causing incorrect decisions in future --update runs.
    #[arg(long, help_heading = "Linking options")]
    allow_lossy_update: bool,

    // Preserve options
    /// What file attributes to preserve on directories, symlinks, and files copied during --update
    ///
    /// Defaults to "all" if not specified. Presets: "all" preserves uid, gid, time, and full
    /// mode (0o7777); "none" uses minimal defaults (no uid/gid/time, mode mask 0o0777).
    /// Custom format: "`<type>:<attrs>` ..." where type is f (file), d (directory), l (symlink),
    /// and attrs is a comma-separated list of uid, gid, time, or a 4-digit octal mode mask.
    ///
    /// Hard-linked files always share metadata with their source via the inode - preserve
    /// settings have no effect on them. Settings apply to directories and symlinks in all
    /// modes, and additionally to files that are copied (not linked) during --update operations.
    ///
    /// Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"
    #[arg(long, value_name = "SETTINGS", help_heading = "Preserve options")]
    preserve_settings: Option<String>,

    // Filtering options
    /// Glob pattern for files to include (can be specified multiple times)
    ///
    /// Only files matching at least one include pattern will be linked. Patterns use glob
    /// syntax: * matches anything except /, ** matches anything including /, ? matches single
    /// char, [...] for character classes. Leading / anchors to source root, trailing / matches
    /// only directories. Simple patterns (like *.txt) apply to the source root itself;
    /// anchored patterns (like /src/**) match paths inside the source.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    include: Vec<String>,

    /// Glob pattern for files to exclude (can be specified multiple times)
    ///
    /// Files matching any exclude pattern will be skipped. Excludes are checked before includes.
    /// Simple patterns (like *.log) can exclude the source root itself; anchored patterns
    /// (like /build/) only match paths inside the source.
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    exclude: Vec<String>,

    /// Read filter patterns from file
    #[arg(long, value_name = "PATH", conflicts_with_all = ["include", "exclude"], help_heading = "Filtering")]
    filter_file: Option<std::path::PathBuf>,

    /// Preview mode - show what would be linked without actually linking
    ///
    /// Note: dry-run bypasses --overwrite checks and shows all files that would be
    /// attempted, regardless of whether the destination already exists.
    /// --progress and --summary are suppressed in dry-run mode (use -v to
    /// still see summary output).
    #[arg(long, value_name = "MODE", help_heading = "Filtering")]
    dry_run: Option<common::DryRunMode>,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    /// Maximum number of open files (0 = no limit, unspecified = 80% of system limit)
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Chunk size for calculating I/O operations per file
    ///
    /// Required when using --iops-throttle (must be > 0)
    #[arg(
        long,
        default_value = "0",
        value_name = "SIZE",
        help_heading = "Performance & throttling"
    )]
    chunk_size: u64,

    #[command(flatten)]
    common: common::cli::CommonArgs,

    // TOCTOU safety
    /// Print TOCTOU-safety verdict for this invocation and exit (0 = safe, 1 = not safe)
    ///
    /// Analyzes whether the invocation is hardened against symlink/path-swap races
    /// and exits without performing the link operation.
    #[arg(long, help_heading = "Security")]
    toctou_check: bool,

    /// Refuse to run unless the invocation uses the TOCTOU-hardened walk
    ///
    /// Refuses non-Linux builds (rlink has no --dereference). It does NOT verify the trust
    /// of the operand path's prefix — that is the caller's responsibility (lock paths down
    /// in the sudo rule). See "Scope of TOCTOU safety" in docs/tocttou.md. Intended for
    /// sudo rules: `NOPASSWD: /usr/bin/rlink --require-toctou-safe *`.
    #[arg(long, conflicts_with = "toctou_check", help_heading = "Security")]
    require_toctou_safe: bool,

    // ARGUMENTS
    /// Directory with contents we want to update into `dst`
    #[arg()]
    src: std::path::PathBuf,

    /// Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a file from `new` if it's been modified
    #[arg()]
    dst: String, // must be a string to allow for parsing trailing slash
}

/// Resolve the effective destination ROOT path that the link operation will create, applying the
/// trailing-slash ("copy INTO this directory") rule: a `dst` ending in `/` means the source
/// basename is appended (`dst/<src_basename>`), otherwise `dst` is used verbatim.
fn resolve_dst_root(src: &std::path::Path, dst: &str) -> Result<std::path::PathBuf> {
    if dst.ends_with('/') {
        // Derive the basename via the SAME decomposition the link operation uses:
        // `root_operand_basename` (the sync twin of `split_root_operand`) canonicalizes
        // `.`/`..`/`dir/..` — which have no `Path::file_name()` — so `dst/<name>` names exactly the
        // entry the link will create: `rlink . out/` -> `out/<cwd-name>`, `rlink tree/.. out/` ->
        // `out/<parent-name>`. A plain `file_name()` returns None for those operands and would
        // reject them before the link operation could canonicalize the source.
        let src_name = common::walk::root_operand_basename(src)
            .context(format!("resolving source operand {src:?}"))?;
        Ok(std::path::PathBuf::from(dst).join(src_name))
    } else {
        Ok(std::path::PathBuf::from(dst))
    }
}

async fn async_main(args: Args) -> Result<common::link::Summary> {
    // `.`/`..` (and `dir/.`, `dir/..`) source operands are supported: `common::link` decomposes them
    // via `split_root_operand` (canonicalizing `.`/`..`), so `rlink . dst` hard-links the current
    // directory just like `rrm .`/`rchm .` already work. (`/` is rejected there.) Use a shell glob
    // (`dir/*`) if you want to expand a directory's contents instead.
    let dst = resolve_dst_root(&args.src, &args.dst)?;
    if !args.dst.ends_with('/') && dst.exists() && !(args.overwrite || args.delete) {
        return Err(anyhow!(
            "Destination path {dst:?} already exists! \n\
            If you want to copy INTO it then follow the destination path with a trailing slash (/) or use \
            --overwrite if you want to overwrite it"
        ));
    }
    // parse preserve settings
    let preserve = if let Some(ref settings_str) = args.preserve_settings {
        common::parse_preserve_settings(settings_str)
            .context(format!("parsing --preserve-settings: {settings_str}"))?
    } else {
        common::preserve::preserve_all()
    };
    let update_compare = common::parse_metadata_cmp_settings(&args.update_compare)?;
    // validate --update comparison attributes against preserve settings
    if args.update.is_some()
        && let Err(msg) = common::validate_update_compare_vs_preserve(&update_compare, &preserve)
    {
        if !args.allow_lossy_update {
            return Err(anyhow!("{msg}"));
        }
        tracing::warn!("{msg}");
    }
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )?;
    let delete = if args.delete {
        Some(common::copy::DeleteSettings {
            delete_excluded: args.delete_excluded,
        })
    } else {
        None
    };
    let settings = common::link::Settings {
        copy_settings: common::copy::Settings {
            dereference: false, // currently not supported
            fail_early: args.fail_early,
            overwrite: args.overwrite || args.delete,
            overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
            overwrite_filter: None,
            ignore_existing: false,
            chunk_size: args.chunk_size,
            skip_specials: args.skip_specials,
            remote_copy_buffer_size: 0, // not used for local operations
            filter: filter.clone(),
            dry_run: args.dry_run,
            delete,
        },
        update_compare,
        update_exclusive: args.update_exclusive,
        filter,
        dry_run: args.dry_run,
        preserve,
    };
    let result = common::link(&args.src, &dst, &args.update, &settings).await;
    match result {
        Ok(summary) => Ok(summary),
        Err(error) => {
            tracing::error!("{:#}", &error);
            if args.summary {
                return Err(anyhow!("rlink encountered errors\n\n{}", &error.summary));
            }
            Err(anyhow!("rlink encountered errors"))
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    // TOCTOU linter: must run before the async runtime starts.
    // rlink has no --dereference flag; dereference is always false. Operands
    // (src, dst, and --update when given) are passed so --require-toctou-safe
    // can enforce the strict operand contract (absolute + lexically normal,
    // resolved RESOLVE_NO_SYMLINKS); keeping the directories along them out of
    // a less-privileged actor's write control remains the caller's
    // responsibility (see the "Scope of TOCTOU safety" section of
    // docs/tocttou.md).
    let operand_paths: Vec<std::path::PathBuf> = [
        Some(args.src.clone()),
        Some(std::path::PathBuf::from(&args.dst)),
        args.update.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();
    common::toctou_check::enforce_or_exit(
        false,
        args.toctou_check,
        args.require_toctou_safe,
        &operand_paths,
    );

    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.common.progress_requested(),
            args.summary,
            args.common.verbose,
            args.overwrite,
            !args.include.is_empty() || !args.exclude.is_empty() || args.filter_file.is_some(),
            true,
            false, // rlink has no --ignore-existing
        )
    });
    let is_dry_run = dry_run_warnings.is_some();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = args
        .common
        .output_config(args.quiet, !is_dry_run && args.summary);
    let runtime = args.common.runtime_config();
    let throttle = args
        .common
        .throttle_config(args.max_open_files, args.chunk_size);
    let tracing = common::TracingConfig::local("rlink");
    let progress = if is_dry_run {
        None
    } else {
        args.common
            .user_progress_settings(common::progress::LocalProgressKind::Link)
    };
    let res = common::run(progress, output, runtime, throttle, tracing, func);
    if let Some(warnings) = dry_run_warnings {
        warnings.print();
    }
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
