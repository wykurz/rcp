use anyhow::{Result, anyhow};
use clap::Parser;
use tracing::instrument;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rchm",
    version,
    about = "Recursively change permissions/ownership of large filesets - a dchmod replacement",
    long_about = "`rchm` recursively applies chmod/chgrp/chown changes to large filesets.

The --group and --owner options take a per-type DSL: a bare value applies to all
entries (files, directories, and symlinks via lchown); f:/d:/l: prefixes target one
type. --mode uses the same DSL but covers only files and directories (a bare value
sets both) — symlink mode bits aren't settable on Linux, so l: is rejected for --mode.

EXAMPLES:
    # join group 'data' and add group rwx to dirs, rw to files
    rchm --group data --mode 'f:g+rw d:g+rwxs' /data --progress --summary

    # dchmod-style: one mode for everything
    rchm --mode g+rwX /data --progress"
)]
struct Args {
    /// Mode change DSL (e.g. 'g+rwX' or 'f:g+rw d:g+rwxs'). Symbolic or octal.
    #[arg(long, value_name = "DSL", help_heading = "Operation")]
    mode: Option<String>,
    /// Group change DSL: a group name/gid, optionally per type (e.g. 'data' or 'f:data d:wheel')
    #[arg(long, value_name = "DSL", help_heading = "Operation")]
    group: Option<String>,
    /// Owner change DSL: a user name/uid, optionally per type (e.g. 'root' or 'd:root')
    #[arg(long, value_name = "DSL", help_heading = "Operation")]
    owner: Option<String>,
    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Operation")]
    fail_early: bool,
    /// Apply directory changes after their contents (post-order) instead of before
    ///
    /// By default directory changes are applied before descending (like `chmod -R`), so
    /// `--mode d:u+rwx` can recover an unreadable directory. Use this flag when recursively
    /// removing the owner's own read/execute from directories, where pre-order would lock
    /// itself out of the contents.
    #[arg(long, help_heading = "Operation")]
    defer_dir_changes: bool,
    /// Glob pattern for paths to include (can be specified multiple times)
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    include: Vec<String>,
    /// Glob pattern for paths to exclude (can be specified multiple times)
    #[arg(long, value_name = "PATTERN", action = clap::ArgAction::Append, help_heading = "Filtering")]
    exclude: Vec<String>,
    /// Read filter patterns from file
    #[arg(long, value_name = "PATH", conflicts_with_all = ["include", "exclude"], help_heading = "Filtering")]
    filter_file: Option<std::path::PathBuf>,
    /// Only change entries whose modification time is at least this old (e.g. 30d, 12h)
    #[arg(long, value_name = "DURATION", help_heading = "Filtering")]
    modified_before: Option<String>,
    /// Only change entries whose creation (birth) time is at least this old
    #[arg(long, value_name = "DURATION", help_heading = "Filtering")]
    #[cfg_attr(target_env = "musl", arg(hide = true))]
    created_before: Option<String>,
    /// Preview mode - show what would change without changing it
    #[arg(long, value_name = "MODE", help_heading = "Filtering")]
    dry_run: Option<common::DryRunMode>,
    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,
    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,
    /// Maximum number of open files, 0 means no limit, unspecified means 80% of system limit
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
    /// Path(s) to modify
    #[arg()]
    paths: Vec<std::path::PathBuf>,
}

fn parse_duration_arg(flag: &str, value: &str) -> Result<std::time::Duration> {
    humantime::parse_duration(value).map_err(|err| {
        anyhow!(
            "{} value {:?} is not a valid duration: {}\n\
             Hint: use suffixes like 1y, 6months, 30d, 12h, 30m, 45s. \
             Note that 'M' means months and 'm' means minutes.",
            flag,
            value,
            err
        )
    })
}

fn build_time_filter(
    modified_before: Option<&str>,
    created_before: Option<&str>,
) -> Result<Option<common::filter::TimeFilter>> {
    let modified_before = modified_before
        .map(|v| parse_duration_arg("--modified-before", v))
        .transpose()?;
    let created_before = created_before
        .map(|v| parse_duration_arg("--created-before", v))
        .transpose()?;
    if modified_before.is_none() && created_before.is_none() {
        return Ok(None);
    }
    Ok(Some(common::filter::TimeFilter {
        modified_before,
        created_before,
    }))
}

fn build_settings(args: &Args) -> Result<common::chmod::Settings> {
    let mode = args
        .mode
        .as_deref()
        .map(common::chmod::parse_mode_dsl)
        .transpose()?
        .unwrap_or_default();
    let owner = args
        .owner
        .as_deref()
        .map(|s| common::chmod::parse_owner_dsl(s, common::chmod::IdKind::User))
        .transpose()?
        .unwrap_or_default();
    let group = args
        .group
        .as_deref()
        .map(|s| common::chmod::parse_owner_dsl(s, common::chmod::IdKind::Group))
        .transpose()?
        .unwrap_or_default();
    if mode.is_empty() && owner.is_empty() && group.is_empty() {
        return Err(anyhow!(
            "nothing to do: specify at least one of --mode, --group or --owner"
        ));
    }
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )?;
    let time_filter = build_time_filter(
        args.modified_before.as_deref(),
        args.created_before.as_deref(),
    )?;
    Ok(common::chmod::Settings {
        mode,
        owner,
        group,
        fail_early: args.fail_early,
        defer_dir_changes: args.defer_dir_changes,
        filter,
        time_filter,
        dry_run: args.dry_run,
    })
}

#[instrument]
async fn async_main(args: Args) -> Result<common::chmod::Summary> {
    let settings = build_settings(&args)?;
    if args.paths.is_empty() {
        return Err(anyhow!(
            "no paths given: specify at least one path to modify"
        ));
    }
    let mut join_set = tokio::task::JoinSet::new();
    for path in args.paths {
        let settings = settings.clone();
        join_set.spawn(async move { common::chmod(&path, &settings).await });
    }
    let error_collector = common::error_collector::ErrorCollector::default();
    let mut summary = common::chmod::Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(s) => summary = summary + s,
            Err(error) => {
                tracing::error!("{:#}", &error);
                summary = summary + error.summary;
                if args.fail_early {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", error, &summary));
                    }
                    return Err(anyhow!("{}", error));
                }
                error_collector.push(error.source);
            }
        }
    }
    if let Some(err) = error_collector.into_error() {
        if args.summary {
            return Err(anyhow!("{:#}\n\n{}", err, &summary));
        }
        return Err(err);
    }
    Ok(summary)
}

fn main() -> Result<()> {
    let args = Args::parse();
    #[cfg(target_env = "musl")]
    if args.created_before.is_some() {
        return Err(anyhow!(
            "--created-before is not supported on musl builds: birth time (btime) is not \
             readable via std::fs::Metadata::created() under musl, so every entry would \
             fail evaluation and be skipped. Use --modified-before instead, or rebuild \
             rchm against glibc."
        ));
    }
    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.common.progress_requested(),
            args.summary,
            args.common.verbose,
            false, // rchm has no --overwrite
            !args.include.is_empty()
                || !args.exclude.is_empty()
                || args.filter_file.is_some()
                || args.modified_before.is_some()
                || args.created_before.is_some(),
            false, // rchm has no destination
            false, // rchm has no --ignore-existing
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
    let tracing = common::TracingConfig {
        remote_layer: None,
        debug_log_file: None,
        chrome_trace_prefix: None,
        flamegraph_prefix: None,
        trace_identifier: "rchm".to_string(),
        profile_level: None,
        tokio_console: false,
        tokio_console_port: None,
    };
    let progress = if is_dry_run {
        None
    } else {
        args.common
            .user_progress_settings(common::progress::LocalProgressKind::Modify)
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
