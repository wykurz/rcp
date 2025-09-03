use anyhow::{anyhow, Context};
use common::ProgressType;
use structopt::StructOpt;
use tracing::instrument;

mod path;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcp",
    about = "`rcp` is a tool for copying files similar to `cp` but generally MUCH faster when dealing with a large \
    number of files.

Inspired by tools like `dsync`(1) and `pcp`(2).

1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp"
)]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when when deciding if files are "identical", used with
    /// --overwrite flag.
    /// Options are: uid, gid, mode, size, mtime, ctime
    #[structopt(long, default_value = "size,mtime")]
    overwrite_compare: String,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

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

    /// Preserve file metadata: file owner, group, setuid, setgid, mtime, atime and mode.
    #[structopt(short, long)]
    preserve: bool,

    /// Specify exactly what attributes to preserve.
    ///
    /// If specified, the "preserve" flag is ignored.
    ///
    /// The format is: "<type1>:<attributes1> <type2>:<attributes2> ..."
    /// Where <type> is one of: f (file), d (directory), l (symlink)
    /// And <attributes> is a comma separated list of: uid, gid, time, <mode mask>
    /// Where <mode mask> is a 4 digit octal number
    ///
    /// Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"
    #[structopt(long)]
    preserve_settings: Option<String>,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<String>,

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
    chunk_size: bytesize::ByteSize,

    /// Throttle the number of bytes per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    tput_throttle: usize,

    /// Enable file-based debug logging for rcpd processes with given prefix
    /// (e.g., /tmp/rcpd-log will create /tmp/rcpd-log-YYYY-MM-DDTHH-MM-SS-RANDOM)
    #[structopt(long)]
    rcpd_debug_log_prefix: Option<String>,
}

#[instrument]
async fn run_rcpd_master(
    args: &Args,
    preserve: &common::preserve::Settings,
    src: &path::RemotePath,
    dst: &path::RemotePath,
) -> anyhow::Result<common::copy::Summary> {
    tracing::debug!("running rcpd src/dst");
    // open a port and wait from server & client hello, respond to client with server port
    let server_endpoint = remote::get_server()?;
    let server_addr = remote::get_endpoint_addr(&server_endpoint)?;
    let server_name = remote::get_random_server_name();
    let mut rcpds = vec![];
    let rcpd_config = remote::protocol::RcpdConfig {
        verbose: args.verbose,
        fail_early: args.fail_early,
        max_workers: args.max_workers,
        max_blocking_threads: args.max_blocking_threads,
        max_open_files: args.max_open_files,
        ops_throttle: args.ops_throttle,
        iops_throttle: args.iops_throttle,
        chunk_size: args.chunk_size.0 as usize,
        tput_throttle: args.tput_throttle,
        dereference: args.dereference,
        overwrite: args.overwrite,
        overwrite_compare: args.overwrite_compare.clone(),
        debug_log_prefix: args.rcpd_debug_log_prefix.clone(),
    };
    for _ in 0..2 {
        let rcpd =
            remote::start_rcpd(&rcpd_config, src.session(), &server_addr, &server_name).await?;
        rcpds.push(rcpd);
    }
    tracing::info!("Waiting for connections from rcpd processes...");
    // accept connection from source
    tracing::info!("Waiting for connection from source rcpd...");
    let source_connection = {
        let source_connecting = match server_endpoint.accept().await {
            Some(conn) => conn,
            None => return Err(anyhow!("Server endpoint closed before source connected")),
        };
        tracing::info!("Source rcpd connected");
        remote::streams::Connection::new(source_connecting.await?)
    };
    let mut source_tracing_stream = source_connection
        .accept_uni()
        .await
        .context("Failed to open unidirectional stream with source rcpd")?;
    // receiving some data guarantees that the stream is established in the right order
    source_tracing_stream
        .recv_object::<remote::protocol::TracingHello>()
        .await
        .context("Failed to receive tracing hello from source rcpd")?;
    let source_tracing_task = {
        tokio::spawn(async move {
            if let Err(e) = remote::run_remote_tracing_receiver(source_tracing_stream).await {
                tracing::warn!("Source remote tracing receiver failed: {}", e);
            }
        })
    };
    let (source_send_stream, mut source_recv_stream) = source_connection
        .open_bi()
        .await
        .context("Failed to open bidirectional stream with source rcpd")?;
    {
        let mut source_send_stream = source_send_stream.lock().await;
        source_send_stream
            .send_control_message(&remote::protocol::MasterHello::Source {
                src: src.path().to_path_buf(),
                dst: dst.path().to_path_buf(),
            })
            .await?;
        source_send_stream.close().await?;
    }
    tracing::debug!("Waiting for source rcpd to send hello");
    let source_hello = source_recv_stream
        .recv_object::<remote::protocol::SourceMasterHello>()
        .await?
        .expect("Failed to receive source hello from source rcpd");
    // accept connection from destination
    tracing::info!("Waiting for connection from destination rcpd...");
    let dest_connection = {
        let dest_connecting = match server_endpoint.accept().await {
            Some(conn) => conn,
            None => {
                return Err(anyhow!(
                    "Server endpoint closed before destination connected"
                ))
            }
        };
        tracing::info!("Destination rcpd connected");
        remote::streams::Connection::new(dest_connecting.await?)
    };
    let mut dest_tracing_stream = dest_connection
        .accept_uni()
        .await
        .context("Failed to open unidirectional stream with destination rcpd")?;
    // receiving some data guarantees that the stream is established in the right order
    dest_tracing_stream
        .recv_object::<remote::protocol::TracingHello>()
        .await
        .context("Failed to receive tracing hello from destination rcpd")?;
    let dest_tracing_task = {
        tokio::spawn(async move {
            if let Err(e) = remote::run_remote_tracing_receiver(dest_tracing_stream).await {
                tracing::warn!("Destination remote tracing receiver failed: {}", e);
            }
        })
    };
    // rcpd doesn't know if it's source or destination so we need to match the stream type to source (bidirectional)
    // although a unidirectional stream would be enough here
    let (dest_send_stream, mut dest_recv_stream) = dest_connection
        .open_bi()
        .await
        .context("Failed to open bidirectional stream with destination rcpd")?;
    {
        let mut dest_send_stream = dest_send_stream.lock().await;
        dest_send_stream
            .send_control_message(&remote::protocol::MasterHello::Destination {
                source_addr: source_hello.source_addr,
                server_name: source_hello.server_name.clone(),
                preserve: *preserve,
            })
            .await?;
        dest_send_stream.close().await?;
    }
    tracing::info!("Forwarded source connection info to destination");
    source_recv_stream
        .recv_object::<remote::protocol::RcpdGoodBye>()
        .await?
        .expect("Failed to receive RcpdGoodBye from source rcpd");
    dest_recv_stream
        .recv_object::<remote::protocol::RcpdGoodBye>()
        .await?
        .expect("Failed to receive RcpdGoodBye from destination rcpd");
    tracing::debug!("Received RcpdGoodBye from both source and destination rcpds");
    for rcpd in rcpds {
        tracing::info!("Waiting for rcpd process to finish: {:?}", rcpd);
        remote::wait_for_rcpd_process(rcpd).await?;
    }
    source_tracing_task.await?;
    dest_tracing_task.await?;
    source_connection.close();
    dest_connection.close();
    server_endpoint.wait_idle().await;
    tracing::info!("All rcpd processes finished");
    Ok(common::copy::Summary::default())
}

#[instrument]
async fn async_main(args: Args) -> anyhow::Result<common::copy::Summary> {
    if args.paths.len() < 2 {
        return Err(anyhow!(
            "You must specify at least one source path and one destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    for src in src_strings {
        if src == "." || src.ends_with("/.") {
            return Err(anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute \
                path or '*' instead",
                std::path::PathBuf::from(src))
            );
        }
    }
    // pick the path type of the first source in the list and ensure all other sources match
    let first_src_path_type = path::parse_path(&src_strings[0]);
    for src in src_strings[1..].iter() {
        let path_type = path::parse_path(src);
        if path_type != first_src_path_type {
            return Err(anyhow!(
                "Cannot mix different path types in the source list: {:?} and {:?}",
                first_src_path_type,
                path_type
            ));
        }
    }
    let dst_string = args.paths.last().unwrap();
    let dst_path_type = path::parse_path(dst_string);
    // if any of the src/dst paths are remote, we'll be using the rcpd
    let remote_src_dst = match (first_src_path_type, dst_path_type) {
        (path::PathType::Remote(src_remote), path::PathType::Remote(dst_remote)) => {
            Some((src_remote, dst_remote))
        }
        (path::PathType::Remote(src_remote), path::PathType::Local(src_local)) => {
            Some((src_remote, path::RemotePath::from_local(&src_local)))
        }
        (path::PathType::Local(src_local), path::PathType::Remote(dst_remote)) => {
            Some((path::RemotePath::from_local(&src_local), dst_remote))
        }
        (path::PathType::Local(_), path::PathType::Local(_)) => None,
    };
    if args.preserve_settings.is_some() && args.preserve {
        tracing::warn!("The --preserve flag is ignored when --preserve-settings is specified!");
    }
    let preserve = if let Some(preserve_settings) = &args.preserve_settings {
        common::parse_preserve_settings(preserve_settings)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?
    } else if args.preserve {
        common::preserve::preserve_all()
    } else {
        common::preserve::preserve_default()
    };
    tracing::debug!("preserve settings: {:?}", &preserve);
    if let Some((remote_src, remote_dst)) = remote_src_dst {
        if src_strings.len() > 1 {
            return Err(anyhow!(
                "Multiple sources are currently not supported when using remote paths!"
            ));
        }
        return run_rcpd_master(&args, &preserve, &remote_src, &remote_dst).await;
    }
    let src_dst: Vec<(std::path::PathBuf, std::path::PathBuf)> = if dst_string.ends_with('/') {
        // rcp foo bar baz/ -> copy foo to baz/foo and bar to baz/bar
        let dst_dir = std::path::PathBuf::from(dst_string);
        src_strings
            .iter()
            .map(|src| {
                let src_path = std::path::PathBuf::from(src);
                let src_file = src_path
                    .file_name()
                    .context(format!("source {:?} does not have a basename", &src_path))
                    .unwrap();
                (src_path.to_owned(), dst_dir.join(src_file))
            })
            .collect::<Vec<(std::path::PathBuf, std::path::PathBuf)>>()
    } else {
        if src_strings.len() > 1 {
            return Err(anyhow!(
                "Multiple sources can only be copied INTO a directory; if this is your intent - follow the \
                destination path with a trailing slash"
            ));
        }
        let dst_path = std::path::PathBuf::from(dst_string);
        if dst_path.exists() && !args.overwrite {
            return Err(anyhow!(
                "Destination path {dst_path:?} already exists! \n\
                If you want to copy INTO it, then follow the destination path with a trailing slash (/). Use \
                --overwrite if you want to overwrite it"
            ));
        }
        assert_eq!(src_strings.len(), 1);
        vec![(
            std::path::PathBuf::from(src_strings[0].clone()),
            std::path::PathBuf::from(dst_string),
        )]
    };
    let settings = common::copy::Settings {
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)
            .map_err(|err| common::copy::Error::new(err, Default::default()))?,
        chunk_size: args.chunk_size.0,
    };
    tracing::debug!("copy settings: {:?}", &settings);
    let mut join_set = tokio::task::JoinSet::new();
    for (src_path, dst_path) in src_dst {
        let do_copy =
            || async move { common::copy(&src_path, &dst_path, &settings, &preserve).await };
        join_set.spawn(do_copy());
    }
    let mut success = true;
    let mut copy_summary = common::copy::Summary::default();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => copy_summary = copy_summary + summary,
                Err(error) => {
                    tracing::error!("{:?}", &error);
                    copy_summary = copy_summary + error.summary;
                    if args.fail_early {
                        if args.summary {
                            return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                        }
                        return Err(anyhow!("{}", error));
                    }
                    success = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                    }
                    return Err(anyhow!("{}", error));
                }
            }
        }
    }
    if !success {
        if args.summary {
            return Err(anyhow!("rcp encountered errors\n\n{}", &copy_summary));
        }
        return Err(anyhow!("rcp encountered errors"));
    }
    Ok(copy_summary)
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
        args.chunk_size.0,
        args.tput_throttle,
        None,
        None,
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
