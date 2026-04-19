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

    /// When a directory is missing on one side, list its contents individually
    #[arg(long, help_heading = "Comparison options")]
    expand_missing: bool,

    /// Return non-zero exit code only if there were errors performing the comparison
    #[arg(long, help_heading = "Comparison options")]
    no_check: bool,

    /// File to store comparison output (instead of stdout)
    #[arg(long, value_name = "PATH", help_heading = "Comparison options")]
    log: Option<std::path::PathBuf>,

    // Filtering options
    /// Glob pattern for files to include (can be specified multiple times)
    ///
    /// Only files matching at least one include pattern will be compared. Patterns use glob
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

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, suppress stdout output (errors and differences)
    ///
    /// Without --log, differences are printed to stdout. This flag suppresses that.
    /// When used with --log, differences are still written to the log file.
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    /// Output format for differences and summary
    #[arg(
        long,
        default_value = "json",
        value_name = "FORMAT",
        help_heading = "Progress & output"
    )]
    output_format: common::cmp::OutputFormat,

    /// Maximum number of open files (0 = no limit, unspecified = 80% of system limit)
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

    // ARGUMENTS
    /// File or directory to compare
    #[arg()]
    src: std::path::PathBuf,

    /// File or directory to compare
    #[arg()]
    dst: std::path::PathBuf,
}

async fn async_main(args: Args) -> Result<common::cmp::FormattedSummary> {
    // build filter settings from CLI arguments
    let filter = common::filter::FilterSettings::from_args(
        args.filter_file.as_deref(),
        &args.include,
        &args.exclude,
    )?;
    // output to stdout if no log file and not quiet
    let use_stdout = args.log.is_none() && !args.quiet;
    let log_handle =
        common::cmp::LogWriter::new(args.log.as_deref(), use_stdout, args.output_format).await?;
    let summary = common::cmp(
        &args.src,
        &args.dst,
        &log_handle,
        &common::cmp::Settings {
            fail_early: args.fail_early,
            exit_early: args.exit_early,
            expand_missing: args.expand_missing,
            compare: common::parse_compare_settings(&args.metadata_compare)?,
            filter,
        },
    )
    .await?;
    log_handle.flush().await?;
    Ok(common::cmp::FormattedSummary {
        summary,
        format: args.output_format,
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = common::OutputConfig {
        suppress_runtime_stats: matches!(args.output_format, common::cmp::OutputFormat::Json),
        ..args.common.output_config(args.quiet, args.summary)
    };
    let runtime = args.common.runtime_config();
    let throttle = args
        .common
        .throttle_config(args.max_open_files, args.chunk_size);
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
    // note: rcmp historically does not treat --progress-delay alone as implying
    // --progress (unlike rrm/rlink). preserve that behavior here.
    let progress = if args.common.progress || args.common.progress_type.is_some() {
        Some(common::ProgressSettings {
            progress_type: common::GeneralProgressType::User(
                args.common.progress_type.unwrap_or_default(),
            ),
            progress_delay: args.common.progress_delay.clone(),
        })
    } else {
        None
    };
    let res = common::run(progress, output, runtime, throttle, tracing, func);
    match res {
        Some(formatted) => {
            if args.no_check {
                std::process::exit(0)
            } else {
                // if there are any differences, return error code 1
                for (_, cmp_result) in &formatted.summary.mismatch {
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
