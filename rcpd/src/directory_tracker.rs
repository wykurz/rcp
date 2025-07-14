use crate::streams::SendStream;
use tracing::{event, Level};

#[derive(Debug)]
pub struct DirectoryTracker {
    remaining_dir_entries: tokio::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>>,
    dir_created_send_stream: tokio::sync::Mutex<SendStream>,
}

impl DirectoryTracker {
    pub fn new(dir_created_send_stream: SendStream) -> Self {
        Self {
            remaining_dir_entries: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            dir_created_send_stream: tokio::sync::Mutex::new(dir_created_send_stream),
        }
    }

    pub async fn add_directory(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
        num_entries: usize,
    ) -> anyhow::Result<()> {
        // First, add directory to tracking (acquire and release entries lock)
        {
            let mut entries = self.remaining_dir_entries.lock().await;
            entries.insert(dst.to_path_buf(), num_entries);
            event!(
                Level::DEBUG,
                "Added directory tracking: {:?} with {} entries",
                dst,
                num_entries
            );
        }
        // Now send confirmation (acquire sender lock)
        let confirmation = remote::protocol::DirectoryCreated {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DirectoryMessage::Created(confirmation);
        let mut sender = self.dir_created_send_stream.lock().await;
        sender.send_object(&message).await?;
        event!(
            Level::INFO,
            "Sent directory creation confirmation: {:?} -> {:?}",
            src,
            dst
        );
        Ok(())
    }

    async fn send_completion(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let completion = remote::protocol::DirectoryComplete {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DirectoryMessage::Complete(completion);
        let mut sender = self.dir_created_send_stream.lock().await;
        sender.send_object(&message).await?;
        event!(
            Level::INFO,
            "Sent directory completion notification: {:?} -> {:?}",
            src,
            dst
        );
        Ok(())
    }

    pub async fn decrement_entry(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let parent_dir = dst.parent().unwrap();
        let mut entries = self.remaining_dir_entries.lock().await;
        let remaining = entries
            .get_mut(parent_dir)
            .ok_or_else(|| anyhow::anyhow!("Directory {:?} not being tracked", parent_dir))?;
        *remaining -= 1;
        event!(
            Level::DEBUG,
            "Decremented entry count for {:?}, remaining: {}",
            parent_dir,
            *remaining
        );
        if *remaining == 0 {
            entries.remove(parent_dir);
            drop(entries); // Release lock before sending completion
            event!(Level::INFO, "Directory completed: {:?}", parent_dir);
            self.send_completion(src.parent().unwrap(), parent_dir)
                .await?;
        }
        Ok(())
    }

    pub async fn finish(&self) -> anyhow::Result<()> {
        // Stream will be closed automatically when dropped
        assert!(
            self.remaining_dir_entries.lock().await.is_empty(),
            "Not all directories were processed before finishing"
        );
        Ok(())
    }
}
