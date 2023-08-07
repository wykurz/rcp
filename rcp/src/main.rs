extern crate log;

use anyhow::{Context, Result};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "rcp")]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Show progress
    #[structopt(short, long)]
    progress: bool,

    /// Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    #[structopt(short, long)]
    preserve: bool,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<String>,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// File copy read buffer size
    #[structopt(long, default_value = "128KiB")]
    read_buffer: String,
}

async fn async_main(args: Args) -> Result<()> {
    if args.paths.len() < 2 {
        return Err(anyhow::anyhow!(
            "You must specify at least one source and destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    let dst_string = args.paths.last().unwrap();
    let src_dst: Vec<(std::path::PathBuf, std::path::PathBuf)> = if dst_string.ends_with('/') {
        // rcp foo bar baz/ -> copy foo to baz/foo and bar to baz/bar
        let dst_dir = std::path::PathBuf::from(dst_string);
        src_strings
            .iter()
            .map(|src| {
                let src_path = std::path::PathBuf::from(src);
                let src_file = src_path
                    .file_name()
                    .context(format!("Source {:?} is not a file", &src_path))
                    .unwrap();
                (src_path.to_owned(), dst_dir.join(src_file))
            })
            .collect()
    } else {
        if src_strings.len() > 1 {
            return Err(anyhow::anyhow!(
                "Multiple sources can only be copied to a directory; if this is your intent follow the destination path with a trailing slash"
            ));
        }
        assert_eq!(src_strings.len(), 1);
        vec![(
            std::path::PathBuf::from(src_strings[0].clone()),
            std::path::PathBuf::from(dst_string),
        )]
    };
    let read_buffer = args
        .read_buffer
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    if !sysinfo::set_open_files_limit(isize::MAX) {
        log::info!("Failed to update the open files limit (expeted on non-linux targets)");
    }
    let mut join_set = tokio::task::JoinSet::new();
    for (src_path, dst_path) in src_dst {
        let do_copy = || async move {
            if dst_path.exists() {
                if args.overwrite {
                    // TODO: is this the right behavior?
                    if tokio::fs::metadata(&dst_path).await?.is_file() {
                        tokio::fs::remove_file(&dst_path).await?;
                    } else {
                        tokio::fs::remove_dir_all(&dst_path).await?;
                    }
                } else {
                    return Err(anyhow::anyhow!(
                        "Destination {:?} already exists, use --overwrite to overwrite",
                        dst_path
                    ));
                }
            }
            common::copy(
                args.progress,
                &src_path,
                &dst_path,
                &common::copy::Settings {
                    preserve: args.preserve,
                    read_buffer,
                    dereference: args.dereference,
                },
            )
            .await
        };
        join_set.spawn(do_copy());
    }
    let mut errors = vec![];
    while let Some(res) = join_set.join_next().await {
        if let Err(error) = res? {
            errors.push(error);
        }
    }
    if !errors.is_empty() {
        return Err(anyhow::anyhow!("{:?}", &errors));
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let quiet = args.quiet;
    if !quiet {
        env_logger::Builder::new()
            .filter_level(match args.verbose {
                0 => log::LevelFilter::Error,
                1 => log::LevelFilter::Info,
                2 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            })
            .init();
    } else {
        assert!(
            args.verbose == 0,
            "Quiet mode and verbose mode are mutually exclusive"
        );
    }
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if args.max_workers > 0 {
        builder.worker_threads(args.max_workers);
    }
    let runtime = builder.build().unwrap();
    let res = runtime.block_on(async_main(args));
    if let Err(error) = res {
        if !quiet {
            eprintln!("{}", error);
        }
        std::process::exit(1);
    }
    std::process::exit(0);
}
