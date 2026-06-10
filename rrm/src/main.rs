use anyhow::{Result, anyhow};
use clap::Parser;
use tracing::instrument;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rrm",
    version,
    about = "Remove large filesets efficiently - equivalent to `rm -rf`",
    long_about = "`rrm` is a tool for removing large number of files efficiently.

EXAMPLE:
    # Remove a path recursively with progress
    rrm /path/to/remove --progress --summary

Note: Like `rm -rf`, this is a destructive operation. Use with caution."
)]
struct Args {
    // Removal options
    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Removal options")]
    fail_early: bool,

    // Filtering options
    /// Glob pattern for files to include (can be specified multiple times)
    ///
    /// Only files matching at least one include pattern will be removed. Patterns use glob
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

    /// Only remove entries whose modification time is at least this old
    ///
    /// Accepts human-readable durations (humantime format). Examples: `1y`, `6months`, `30d`,
    /// `12h`, `30m`, `45s`. NOTE: `M` means months, lowercase `m` means minutes — they are
    /// different units. This is an entry filter: it applies independently to each file,
    /// symlink, and directory. Directories are always traversed regardless of their own
    /// timestamps; a directory is only removed when its own mtime is old enough AND it
    /// ends up empty after its children have been processed. A directory left non-empty
    /// after filtering (because it contained skipped new children) is logged at info and
    /// left intact — this is not an error. For symlinks the filter uses the symlink's own
    /// timestamps (not the target's). When combined with `--created-before`, both
    /// conditions must hold (AND).
    #[arg(long, value_name = "DURATION", help_heading = "Filtering")]
    modified_before: Option<String>,

    /// Only remove entries whose creation (birth) time is at least this old
    ///
    /// Accepts human-readable durations (humantime format). Examples: `1y`, `6months`, `30d`,
    /// `12h`, `30m`, `45s`. NOTE: `M` means months, lowercase `m` means minutes — they are
    /// different units. This is an entry filter: it applies independently to each file,
    /// symlink, and directory. Directories are always traversed regardless of their own
    /// timestamps; a directory is only removed when its own btime is old enough AND it
    /// ends up empty after its children have been processed. A directory left non-empty
    /// after filtering is logged at info and left intact — this is not an error. For
    /// symlinks the filter uses the symlink's own timestamps (not the target's). Some
    /// Linux filesystems (and most symlinks) do not expose birth time; such entries are
    /// logged and skipped rather than removed. Pass --fail-early to abort on the first
    /// such error instead. NOT AVAILABLE on musl builds — rebuild against glibc to use
    /// this flag.
    #[arg(long, value_name = "DURATION", help_heading = "Filtering")]
    #[cfg_attr(target_env = "musl", arg(hide = true))]
    created_before: Option<String>,

    /// Preview mode - show what would be removed without actually removing
    ///
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

    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system limit
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Chunk size used to calculate number of I/O per file
    ///
    /// Modifying this setting to a value > 0 is REQUIRED when using --iops-throttle.
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
    /// and exits without performing the removal operation.
    #[arg(long, help_heading = "Security")]
    toctou_check: bool,

    /// Refuse to run unless the invocation uses the TOCTOU-hardened walk
    ///
    /// Refuses non-Linux builds (rrm has no --dereference). It does NOT verify the trust
    /// of the operand path's prefix — that is the caller's responsibility (lock paths down
    /// in the sudo rule). See "Scope of TOCTOU safety" in docs/tocttou.md. Intended for
    /// sudo rules: `NOPASSWD: /usr/bin/rrm --require-toctou-safe *`.
    #[arg(long, conflicts_with = "toctou_check", help_heading = "Security")]
    require_toctou_safe: bool,

    // ARGUMENTS
    /// Path(s) to remove
    #[arg()]
    paths: Vec<std::path::PathBuf>,
}

#[instrument]
async fn async_main(args: Args) -> Result<common::rm::Summary> {
    // build filter settings once before the loop
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )?;
    let time_filter = common::filter::TimeFilter::from_cli_args(
        args.modified_before.as_deref(),
        args.created_before.as_deref(),
    )?;
    let mut join_set = tokio::task::JoinSet::new();
    for path in args.paths {
        let settings = common::rm::Settings {
            fail_early: args.fail_early,
            filter: filter.clone(),
            time_filter: time_filter.clone(),
            dry_run: args.dry_run,
        };
        let do_rm = || async move { common::rm(&path, &settings).await };
        join_set.spawn(do_rm());
    }
    let error_collector = common::error_collector::ErrorCollector::default();
    let mut rm_summary = common::rm::Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => rm_summary = rm_summary + summary,
            Err(error) => {
                tracing::error!("{:#}", &error);
                rm_summary = rm_summary + error.summary;
                if args.fail_early {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", error, &rm_summary));
                    }
                    return Err(anyhow!("{}", error));
                }
                error_collector.push(error.source);
            }
        }
    }
    if let Some(err) = error_collector.into_error() {
        if args.summary {
            return Err(anyhow!("{:#}\n\n{}", err, &rm_summary));
        }
        return Err(err);
    }
    Ok(rm_summary)
}

fn main() -> Result<()> {
    let args = Args::parse();
    common::filter::reject_created_before_on_musl("rrm", args.created_before.as_deref())?;
    // TOCTOU linter: must run before the async runtime starts.
    // rrm has no --dereference flag; dereference is always false. The linter
    // does NOT inspect the operand paths — the trust of a path's prefix is the
    // caller's responsibility (see the "Scope of TOCTOU safety" section of
    // docs/tocttou.md).
    common::toctou_check::enforce_or_exit(false, args.toctou_check, args.require_toctou_safe);

    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.common.progress_requested(),
            args.summary,
            args.common.verbose,
            false, // rrm has no --overwrite
            !args.include.is_empty()
                || !args.exclude.is_empty()
                || args.filter_file.is_some()
                || args.modified_before.is_some()
                || args.created_before.is_some(),
            false, // rrm has no destination
            false, // rrm has no --ignore-existing
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
    let tracing = common::TracingConfig::local("rrm");
    let progress = if is_dry_run {
        None
    } else {
        args.common
            .user_progress_settings(common::progress::LocalProgressKind::Remove)
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
