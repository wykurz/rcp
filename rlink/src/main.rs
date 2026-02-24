use anyhow::{anyhow, Context, Result};
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

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Set the type of progress display
    ///
    /// If specified, --progress flag is implied.
    #[arg(long, value_name = "TYPE", help_heading = "Progress & output")]
    progress_type: Option<common::ProgressType>,

    /// Set delay between progress updates
    ///
    /// Default is 200ms for interactive mode (`ProgressBar`) and 10s for non-interactive mode (`TextUpdates`). If specified, --progress flag is implied. Accepts human-readable durations like "200ms", "10s", "5min".
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    progress_delay: Option<String>,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    // Performance & throttling
    /// Maximum number of open files (0 = no limit, unspecified = 80% of system limit)
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second (0 = no throttle)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    ops_throttle: usize,

    /// Limit I/O operations per second (0 = no throttle)
    ///
    /// Requires --chunk-size to calculate I/O operations per file: ((`file_size` - 1) / `chunk_size`) + 1
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    iops_throttle: usize,

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

    // Advanced settings
    /// Number of worker threads (0 = number of CPU cores)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_workers: usize,

    /// Number of blocking worker threads (0 = Tokio default of 512)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_blocking_threads: usize,

    // ARGUMENTS
    /// Directory with contents we want to update into `dst`
    #[arg()]
    src: std::path::PathBuf,

    /// Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a file from `new` if it's been modified
    #[arg()]
    dst: String, // must be a string to allow for parsing trailing slash
}

async fn async_main(args: Args) -> Result<common::link::Summary> {
    for src in &args.src {
        if src == "."
            || src
                .to_str()
                .expect("input path cannot be converted to string?!")
                .ends_with("/.")
        {
            return Err(anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute path or '*' instead",
                std::path::PathBuf::from(src)
            ));
        }
    }
    let dst = if args.dst.ends_with('/') {
        let src_file = args
            .src
            .file_name()
            .context(format!("source {:?} does not have a basename", &args.src))
            .unwrap();
        let dst_dir = std::path::PathBuf::from(args.dst);
        dst_dir.join(src_file)
    } else {
        let dst_path = std::path::PathBuf::from(args.dst);
        if dst_path.exists() && !args.overwrite {
            return Err(anyhow!(
                "Destination path {dst_path:?} already exists! \n\
                If you want to copy INTO it then follow the destination path with a trailing slash (/) or use \
                --overwrite if you want to overwrite it"
            ));
        }
        dst_path
    };
    // parse preserve settings
    let preserve = if let Some(ref settings_str) = args.preserve_settings {
        common::parse_preserve_settings(settings_str)
            .context(format!("parsing --preserve-settings: {settings_str}"))?
    } else {
        common::preserve::preserve_all()
    };
    let update_compare = common::parse_metadata_cmp_settings(&args.update_compare)?;
    // validate --update comparison attributes against preserve settings
    if args.update.is_some() {
        if let Err(msg) = common::validate_update_compare_vs_preserve(&update_compare, &preserve) {
            if !args.allow_lossy_update {
                return Err(anyhow!("{msg}"));
            }
            tracing::warn!("{msg}");
        }
    }
    let result = common::link(&args.src, &dst, &args.update, {
        // build filter settings from CLI arguments
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
        &common::link::Settings {
            copy_settings: common::copy::Settings {
                dereference: false, // currently not supported
                fail_early: args.fail_early,
                overwrite: args.overwrite,
                overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
                chunk_size: args.chunk_size,
                remote_copy_buffer_size: 0, // not used for local operations
                filter: filter.clone(),
                dry_run: args.dry_run,
            },
            update_compare,
            update_exclusive: args.update_exclusive,
            filter,
            dry_run: args.dry_run,
            preserve,
        }
    })
    .await;
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
    let dry_run_warnings = args.dry_run.map(|_| {
        common::DryRunWarnings::new(
            args.progress || args.progress_type.is_some() || args.progress_delay.is_some(),
            args.summary,
            args.verbose,
            args.overwrite,
            !args.include.is_empty() || !args.exclude.is_empty() || args.filter_file.is_some(),
            true,
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
        trace_identifier: "rlink".to_string(),
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
