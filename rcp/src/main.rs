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

    /// Input file/directory
    #[structopt(parse(from_os_str))]
    src: std::path::PathBuf,

    /// Output file/directory
    #[structopt()]
    dst: String,

    /// Maximum number of parallel file copies from within a single directory
    #[structopt(short, long, default_value = "100000")]
    max_width: usize,
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
            tokio::fs::remove_dir_all(&dst).await?;
        } else {
            return Err(anyhow::anyhow!(
                "Destination {:?} already exists, use --overwrite to overwrite",
                dst
            ));
        }
    }
    common::copy(&args.src, &dst, args.max_width).await
}
