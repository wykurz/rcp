use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rcmp",
    version,
    about = "Compare large filesets efficiently (metadata only)",
    long_about = "`rcmp` is a tool for comparing large filesets based on metadata attributes.

Currently supports metadata comparison only (no content checking).

By default, differences are printed to stdout. Use --log to write them to a file
instead, or --quiet to suppress stdout output.

EXIT CODES:
    0 - No differences found
    1 - Differences found
    2 - Errors occurred during comparison

EXAMPLES:
    # Compare two directories (differences printed to stdout)
    rcmp /foo /bar --progress --summary

    # Compare and log differences to a file
    rcmp /foo /bar --progress --summary --log compare.log

    # Compare silently (only exit code)
    rcmp /foo /bar --quiet"
)]
struct Args {
    // Comparison options
    /// Attributes to compare when deciding if objects are "identical"
    ///
    /// The format is: "`<type1>:<attributes1> <type2>:<attributes2>` ..."
    /// Where `<type>` is one of: "f" (file), "d" (directory), "l" (symlink)
    /// And `<attributes>` is a comma separated list of: uid, gid, mode, size, mtime, ctime
    ///
    /// Example: "f:mtime,ctime,mode,size d:mtime,ctime,mode l:mtime,ctime,mode"
    #[arg(
        long,
        default_value = "f:mtime,size d:mtime l:mtime",
        value_name = "SETTINGS",
        help_heading = "Comparison options"
    )]
    metadata_compare: String,

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Comparison options")]
    fail_early: bool,

    /// Exit on first mismatch
    #[arg(short = 'm', long = "exit-early", help_heading = "Comparison options")]
    exit_early: bool,

    /// Return non-zero exit code only if there were errors performing the comparison
    #[arg(long, help_heading = "Comparison options")]
    no_check: bool,

    /// File to store comparison output (instead of stdout)
    #[arg(long, value_name = "PATH", help_heading = "Comparison options")]
    log: Option<std::path::PathBuf>,

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Set the type of progress display
    ///
    /// If specified, --progress flag is implied.
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

    /// Quiet mode, suppress stdout output (errors and differences)
    ///
    /// Without --log, differences are printed to stdout. This flag suppresses that.
    /// When used with --log, differences are still written to the log file.
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
    /// File or directory to compare
    #[arg()]
    src: std::path::PathBuf,

    /// File or directory to compare
    #[arg()]
    dst: std::path::PathBuf,
}

async fn async_main(args: Args) -> Result<common::cmp::Summary> {
    // output to stdout if no log file and not quiet
    let use_stdout = args.log.is_none() && !args.quiet;
    let log_handle = common::cmp::LogWriter::new(args.log.as_deref(), use_stdout).await?;
    let summary = common::cmp(
        &args.src,
        &args.dst,
        &log_handle,
        &common::cmp::Settings {
            fail_early: args.fail_early,
            exit_early: args.exit_early,
            compare: common::parse_compare_settings(&args.metadata_compare)?,
        },
    )
    .await?;
    log_handle.flush().await?;
    Ok(summary)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = common::OutputConfig {
        quiet: args.quiet,
        verbose: args.verbose,
        print_summary: args.summary,
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
        trace_identifier: "rcmp".to_string(),
        profile_level: None,
        tokio_console: false,
        tokio_console_port: None,
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
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
    match res {
        Some(summary) => {
            if args.no_check {
                std::process::exit(0)
            } else {
                // if there are any differences, return error code 1
                for (_, cmp_result) in &summary.mismatch {
                    let different = cmp_result[common::cmp::CompareResult::Different] > 0
                        || cmp_result[common::cmp::CompareResult::SrcMissing] > 0
                        || cmp_result[common::cmp::CompareResult::DstMissing] > 0;
                    if different {
                        std::process::exit(1);
                    }
                }
                std::process::exit(0);
            }
        }
        None => std::process::exit(2),
    }
}
