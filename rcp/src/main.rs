#[macro_use]
extern crate log;

use async_recursion::async_recursion;
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
}

async fn is_file(path: &std::path::Path) -> Result<bool> {
    let md = tokio::fs::metadata(path).await?;
    Ok(md.is_file())
}

async fn copy_file(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    let mut reader = tokio::fs::File::open(src).await?;
    let mut writer = tokio::fs::File::create(dst).await?;
    tokio::io::copy(&mut reader, &mut writer).await?;
    Ok(())
}

#[async_recursion]
async fn copy(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    if is_file(src).await? {
        return copy_file(src, dst).await;
    }
    tokio::fs::create_dir(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        let dst_path = dst.join(entry_name);
        copy(&entry_path, &dst_path).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::from_args();
    info!("Copy: {:?} -> {:?}", args.src, args.dst);
    let dst = if args.dst.ends_with('/') {
        // rcp foo bar/ -> copy foo to bar/foo
        let dst_dir = std::path::PathBuf::from(args.dst);
        let src_file = args.src.file_name().context(format!("Source {:?} is not a file", args.src))?;
        dst_dir.join(src_file)
    } else {
        std::path::PathBuf::from(args.dst)
    };
    if dst.exists() {
        if args.overwrite {
            tokio::fs::remove_dir_all(&dst).await?;
        } else {
            return Err(anyhow::anyhow!("Destination {:?} already exists, use --overwrite to overwrite", dst));
        }
    }
    copy(&args.src, &dst).await
}
