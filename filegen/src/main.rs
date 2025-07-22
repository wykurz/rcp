use anyhow::{Context, Result};
use async_recursion::async_recursion;
use common::ProgressType;
use rand::Rng;
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tracing::instrument;

#[derive(Clone, Debug)]
struct Dirwidth {
    value: Vec<usize>,
}

impl std::str::FromStr for Dirwidth {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let value = s
            .split(',')
            .map(|s| s.parse::<usize>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Dirwidth { value })
    }
}

#[derive(Clone, StructOpt, Debug)]
#[structopt(name = "filegen")]
struct Args {
    /// Root directory where files are generated
    #[structopt(parse(from_os_str))]
    root: std::path::PathBuf,

    /// Number of sub-directories in the generated directory tree. E.g., "3,2" will generate:
    /// |- d1
    ///    |- d1a
    ///    |- d1b
    /// |- d2
    ///    |- d2a
    ///    |- d2b
    /// |- d3
    ///    |- d3a
    ///    |- d3b
    #[structopt()]
    dirwidth: Dirwidth,

    /// Number of files in each directory
    #[structopt()]
    numfiles: usize,

    /// Size of each file. Accepts suffixes like "1K", "1M", "1G"
    #[structopt()]
    filesize: String,

    /// Size of the buffer used to write to each file. Accepts suffixes like "1K", "1M", "1G"
    #[structopt(default_value = "4K")]
    bufsize: String,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Toggles the type of progress to show.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// Options are: ProgressBar (animated progress bar), TextUpdates (appropriate for logging), Auto (default, will
    /// choose between ProgressBar or TextUpdates depending on the type of terminal attached to stderr)
    #[structopt(long)]
    progress_type: Option<ProgressType>,

    /// Sets the delay between progress updates.
    ///
    /// - For the interactive (--progress-type=ProgressBar), the default is 200ms.
    /// - For the non-interactive (--progress-type=TextUpdates), the default is 10s.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// This option accepts a human readable duration, e.g. "200ms", "10s", "5min" etc.
    #[structopt(long)]
    progress_delay: Option<String>,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,

    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system
    /// limit
    #[structopt(long)]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    ops_throttle: usize,

    /// Throttle the number of I/O operations per second, 0 means no throttle.
    ///
    /// I/O is calculated based on provided chunk size -- number of I/O operations for a file is calculated as:
    /// ((file size - 1) / chunk size) + 1
    #[structopt(long, default_value = "0")]
    iops_throttle: usize,

    /// Chunk size used to calculate number of I/O per file.
    ///
    /// Modifying this setting to a value > 0 is REQUIRED when using --iops-throttle.
    #[structopt(long, default_value = "0")]
    chunk_size: u64,

    /// Throttle the number of bytes per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    tput_throttle: usize,
}

#[instrument]
async fn write_file(
    path: std::path::PathBuf,
    mut filesize: usize,
    bufsize: usize,
    chunk_size: u64,
) -> Result<()> {
    let _permit = throttle::open_file_permit().await;
    throttle::get_ops_token().await;
    if chunk_size > 0 {
        let tokens = 1 + (std::cmp::max(1, filesize) - 1) as u64 / chunk_size;
        if tokens > u32::MAX as u64 {
            tracing::error!(
                "chunk size: {} is too small to limit throughput for files this size: {}",
                chunk_size,
                filesize,
            );
        } else {
            throttle::get_iops_tokens(tokens as u32).await;
        }
    }
    let mut bytes = vec![0u8; bufsize];
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .context(format!("Error opening {:?}", &path))?;
    while filesize > 0 {
        {
            // make sure rng falls out of scope before await
            let mut rng = rand::thread_rng();
            rng.fill(&mut bytes[..]);
        }
        let writesize = std::cmp::min(filesize, bufsize) as usize;
        file.write_all(&bytes[..writesize])
            .await
            .context(format!("Error writing to {:?}", &path))?;
        filesize -= writesize;
    }
    Ok(())
}

#[async_recursion]
#[instrument]
async fn filegen(
    root: &std::path::Path,
    dirwidth: &[usize],
    numfiles: usize,
    filesize: usize,
    writebuf: usize,
    chunk_size: u64,
) -> Result<()> {
    let numdirs = *dirwidth.first().unwrap_or(&0);
    let mut join_set = tokio::task::JoinSet::new();
    // generate directories and recurse into them
    for i in 0..numdirs {
        let path = root.join(format!("dir{i}"));
        let dirwidth = dirwidth[1..].to_owned();
        let recurse = || async move {
            tokio::fs::create_dir(&path)
                .await
                .map_err(anyhow::Error::msg)?;
            filegen(&path, &dirwidth, numfiles, filesize, writebuf, chunk_size).await
        };
        join_set.spawn(recurse());
    }
    // generate files
    for i in 0..numfiles {
        let path = root.join(format!("file{i}"));
        join_set.spawn(write_file(path.clone(), filesize, writebuf, chunk_size));
    }
    while let Some(res) = join_set.join_next().await {
        res??
    }
    Ok(())
}

// TODO: implement a FilegenSummary
#[instrument]
async fn async_main(args: Args) -> Result<String> {
    let filesize = args
        .filesize
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    let writebuf = args.bufsize.parse::<bytesize::ByteSize>().unwrap().as_u64() as usize;
    let root = args.root.join("filegen");
    tokio::fs::create_dir(&root)
        .await
        .context(format!("Error creating {:?}", &root))?;
    filegen(
        &root,
        &args.dirwidth.value,
        args.numfiles,
        filesize,
        writebuf,
        args.chunk_size,
    )
    .await?;
    Ok("OK".to_string())
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
            Some(common::ProgressSettings {
                progress_type: args.progress_type.unwrap_or_default(),
                progress_delay: args.progress_delay,
            })
        } else {
            None
        },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        args.max_open_files,
        args.ops_throttle,
        args.iops_throttle,
        args.chunk_size,
        args.tput_throttle,
        None,
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
