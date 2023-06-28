#[macro_use]
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

    /// Preserve additional file attributes: file owner, group and setuid and setgid bits
    #[structopt(short, long)]
    preserve: bool,

    /// Input file/directory
    #[structopt(parse(from_os_str))]
    src: std::path::PathBuf,

    /// Output file/directory
    #[structopt()]
    dst: String,

    /// Maximum number of parallel file copies from within a single directory, 0 means unlimited
    #[structopt(long, default_value = "100000")]
    max_width: usize,

    /// File copy read buffer size
    #[structopt(long, default_value = "128KiB")]
    read_buffer: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::from_args();
    info!("Copy: {:?} -> {:?}", args.src, args.dst);
    let dst = if args.dst.ends_with('/') {
        // rcp foo bar/ -> copy foo to bar/foo
        let dst_dir = std::path::PathBuf::from(args.dst);
        let src_file = args
            .src
            .file_name()
            .context(format!("Source {:?} is not a file", args.src))?;
        dst_dir.join(src_file)
    } else {
        std::path::PathBuf::from(args.dst)
    };
    if dst.exists() {
        if args.overwrite {
            // TODO: is this the right behavior?
            if tokio::fs::metadata(&dst).await?.is_file() {
                tokio::fs::remove_file(&dst).await?;
            } else {
                tokio::fs::remove_dir_all(&dst).await?;
            }
        } else {
            return Err(anyhow::anyhow!(
                "Destination {:?} already exists, use --overwrite to overwrite",
                dst
            ));
        }
    }
    let max_width = if args.max_width == 0 {
        usize::MAX
    } else {
        args.max_width
    };
    let read_buffer = args
        .read_buffer
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    common::copy(
        args.progress,
        &args.src,
        &dst,
        args.preserve,
        max_width,
        read_buffer,
    )
    .await
}
