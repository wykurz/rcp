use anyhow::{anyhow, Result};
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

    /// Preview mode - show what would be removed without actually removing
    ///
    /// --progress and --summary are suppressed in dry-run mode (use -v to
    /// still see summary output).
    #[arg(long, value_name = "MODE", help_heading = "Filtering")]
    dry_run: Option<common::DryRunMode>,

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Toggles the type of progress to show
    ///
    /// If specified, --progress flag is implied.
    ///
    /// Options are: `ProgressBar` (animated progress bar), `TextUpdates` (appropriate for logging), Auto (default, will
    /// choose between `ProgressBar` or `TextUpdates` depending on the type of terminal attached to stderr)
    #[arg(long, value_name = "TYPE", help_heading = "Progress & output")]
    progress_type: Option<common::ProgressType>,

    /// Sets the delay between progress updates
    ///
    /// - For the interactive (--progress-type=ProgressBar), the default is 200ms.
    /// - For the non-interactive (--progress-type=TextUpdates), the default is 10s.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// This option accepts a human readable duration, e.g. "200ms", "10s", "5min" etc.
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    progress_delay: Option<String>,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    verbose: u8,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    // Performance & throttling
    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system limit
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    ops_throttle: usize,

    /// Throttle the number of I/O operations per second, 0 means no throttle
    ///
    /// I/O is calculated based on provided chunk size -- number of I/O operations for a file is calculated as:
    /// ((file size - 1) / chunk size) + 1
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    iops_throttle: usize,

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

    // Advanced settings
    /// Number of worker threads, 0 means number of cores
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_blocking_threads: usize,

    // ARGUMENTS
    /// Path(s) to remove
    #[arg()]
    paths: Vec<std::path::PathBuf>,
}

#[instrument]
async fn async_main(args: Args) -> Result<common::rm::Summary> {
    // build filter settings once before the loop
    let filter = if let Some(ref path) = args.filter_file {
        Some(common::filter::FilterSettings::from_file(path)?)
    } else if !args.include.is_empty() || !args.exclude.is_empty() {
        let mut filter_settings = common::filter::FilterSettings::new();
        for p in &args.include {
            filter_settings.add_include(p)?;
        }
        for p in &args.exclude {
            filter_settings.add_exclude(p)?;
        }
        Some(filter_settings)
    } else {
        None
    };
    let mut join_set = tokio::task::JoinSet::new();
    for path in args.paths {
        let settings = common::rm::Settings {
            fail_early: args.fail_early,
            filter: filter.clone(),
            dry_run: args.dry_run,
        };
        let do_rm = || async move { common::rm(&path, &settings).await };
        join_set.spawn(do_rm());
    }
    let mut success = true;
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
                success = false;
            }
        }
    }
    if !success {
        if args.summary {
            return Err(anyhow!("rrm encountered errors\n\n{}", &rm_summary));
        }
        return Err(anyhow!("rrm encountered errors"));
    }
    Ok(rm_summary)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.progress || args.progress_type.is_some() || args.progress_delay.is_some(),
            args.summary,
            args.verbose,
            false, // rrm has no --overwrite
            !args.include.is_empty() || !args.exclude.is_empty() || args.filter_file.is_some(),
            false, // rrm has no destination
        )
    });
    let is_dry_run = dry_run_warnings.is_some();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = common::OutputConfig {
        quiet: args.quiet,
        verbose: args.verbose,
        print_summary: if is_dry_run { false } else { args.summary },
    };
    let runtime = common::RuntimeConfig {
        max_workers: args.max_workers,
        max_blocking_threads: args.max_blocking_threads,
    };
    let throttle = common::ThrottleConfig {
        max_open_files: args.max_open_files,
        ops_throttle: args.ops_throttle,
        iops_throttle: args.iops_throttle,
        chunk_size: args.chunk_size,
    };
    let tracing = common::TracingConfig {
        remote_layer: None,
        debug_log_file: None,
        chrome_trace_prefix: None,
        flamegraph_prefix: None,
        trace_identifier: "rrm".to_string(),
        profile_level: None,
        tokio_console: false,
        tokio_console_port: None,
    };
    let res = common::run(
        if !is_dry_run
            && (args.progress || args.progress_type.is_some() || args.progress_delay.is_some())
        {
            Some(common::ProgressSettings {
                progress_type: common::GeneralProgressType::User(
                    args.progress_type.unwrap_or_default(),
                ),
                progress_delay: args.progress_delay,
            })
        } else {
            None
        },
        output,
        runtime,
        throttle,
        tracing,
        func,
    );
    if let Some(warnings) = dry_run_warnings {
        warnings.print();
    }
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
