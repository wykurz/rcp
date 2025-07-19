use crate::streams;
use tracing::{event, Level};

#[derive(Debug)]
pub struct DirectoryTracker {
    remaining_dir_entries: std::collections::HashMap<std::path::PathBuf, usize>,
    control_send_stream: streams::SharedSendStream,
}

impl DirectoryTracker {
    pub fn new(control_send_stream: streams::SharedSendStream) -> Self {
        Self {
            remaining_dir_entries: std::collections::HashMap::new(),
            control_send_stream,
        }
    }

    pub async fn add_directory(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
        num_entries: usize,
    ) -> anyhow::Result<()> {
        if num_entries > 0 {
            self.remaining_dir_entries
                .insert(dst.to_path_buf(), num_entries);
            event!(
                Level::DEBUG,
                "Added directory tracking: {:?} with {} entries",
                dst,
                num_entries
            );
        }
        let confirmation = remote::protocol::SrcDst {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DestinationMessage::DirectoryCreated(confirmation);
        {
            let mut stream = self.control_send_stream.lock().await;
            stream.send_object(&message).await?;
            stream.flush().await?;
        }
        event!(
            Level::INFO,
            "Sent directory creation confirmation: {:?} -> {:?}",
            src,
            dst
        );
        // If there are no entries, we can immediately send completion
        if num_entries == 0 {
            event!(Level::INFO, "Directory completed: {:?}", dst);
            self.send_completion(src, dst).await?;
        }
        Ok(())
    }

    async fn send_completion(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let completion = remote::protocol::SrcDst {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DestinationMessage::DirectoryComplete(completion);
        let mut stream = self.control_send_stream.lock().await;
        stream.send_object(&message).await?;
        stream.flush().await?;
        event!(
            Level::INFO,
            "Sent directory completion notification: {:?} -> {:?}",
            src,
            dst
        );
        Ok(())
    }

    pub async fn decrement_entry(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let dst_parent_dir = dst.parent().unwrap();
        let remaining = self
            .remaining_dir_entries
            .get_mut(dst_parent_dir)
            .ok_or_else(|| anyhow::anyhow!("Directory {:?} not being tracked", dst_parent_dir))?;
        assert!(
            *remaining > 0,
            "Entry count for {dst_parent_dir:?} is already zero"
        );
        *remaining -= 1;
        event!(
            Level::DEBUG,
            "Decremented entry count for {:?}, remaining: {}",
            dst_parent_dir,
            *remaining
        );
        if *remaining == 0 {
            self.remaining_dir_entries.remove(dst_parent_dir);
            event!(Level::INFO, "Directory completed: {:?}", dst_parent_dir);
            self.send_completion(src.parent().unwrap(), dst_parent_dir)
                .await?;
        }
        Ok(())
    }
}

pub type SharedDirectoryTracker = std::sync::Arc<tokio::sync::Mutex<DirectoryTracker>>;

pub fn make_shared(control_send_stream: streams::SharedSendStream) -> SharedDirectoryTracker {
    std::sync::Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
        control_send_stream,
    )))
}
