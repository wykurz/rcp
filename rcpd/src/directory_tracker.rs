use crate::streams::SendStream;
use tracing::{event, Level};

#[derive(Debug)]
pub struct DirectoryTracker {
    remaining_dir_entries: tokio::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>>,
    dir_created_send_stream: tokio::sync::Mutex<Option<SendStream>>,
}

impl DirectoryTracker {
    pub fn new(dir_created_send_stream: SendStream) -> Self {
        Self {
            remaining_dir_entries: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            dir_created_send_stream: tokio::sync::Mutex::new(Some(dir_created_send_stream)),
        }
    }

    pub async fn add_directory(
        &self,
        src: &std::path::Path,
        dst: &std::path::Path,
        num_entries: usize,
    ) -> anyhow::Result<()> {
        // First, add directory to tracking (acquire and release entries lock)
        if num_entries > 0 {
            let mut entries = self.remaining_dir_entries.lock().await;
            entries.insert(dst.to_path_buf(), num_entries);
            event!(
                Level::DEBUG,
                "Added directory tracking: {:?} with {} entries",
                dst,
                num_entries
            );
        }
        let confirmation = remote::protocol::DirectoryCreated {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DirectoryMessage::Created(confirmation);
        {
            let mut send_stream_opt = self.dir_created_send_stream.lock().await;
            let send_stream = send_stream_opt
                .as_mut()
                .expect("Send stream should be initialized");
            send_stream.send_object(&message).await?;
            event!(
                Level::INFO,
                "Sent directory creation confirmation: {:?} -> {:?}",
                src,
                dst
            );
        } // release the stream lock
        if num_entries == 0 {
            event!(Level::INFO, "Directory completed: {:?}", dst);
            self.send_completion(src, dst).await?;
        }
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
        let mut send_stream_opt = self.dir_created_send_stream.lock().await;
        let send_stream = send_stream_opt
            .as_mut()
            .expect("Send stream should be initialized");
        send_stream.send_object(&message).await?;
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
        let dst_parent_dir = dst.parent().unwrap();
        let mut entries = self.remaining_dir_entries.lock().await;
        let remaining = entries
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
            entries.remove(dst_parent_dir);
            drop(entries); // Release lock before sending completion
            event!(Level::INFO, "Directory completed: {:?}", dst_parent_dir);
            self.send_completion(src.parent().unwrap(), dst_parent_dir)
                .await?;
        }
        Ok(())
    }

    pub async fn done_creating_directories(&self) -> anyhow::Result<()> {
        // TODO: call this after create_directory_structure completes
        // if there are no more tracked directories we are done (call finish)
        // if not -- any call that removes a directory should check if it's the last one and call finish
        event!(Level::INFO, "All directories created");
        Ok(())
    }

    async fn finish(&self) -> anyhow::Result<()> {
        let mut send_stream_opt = self.dir_created_send_stream.lock().await;
        send_stream_opt
            .take()
            .expect("Send stream should be initialized");
        assert!(
            self.remaining_dir_entries.lock().await.is_empty(),
            "Not all directories were processed before finishing"
        );
        Ok(())
    }
}
