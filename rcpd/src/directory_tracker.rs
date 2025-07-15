use crate::streams::SendStream;
use tracing::{event, Level};

#[derive(Debug)]
pub struct DirectoryTracker {
    remaining_dir_entries: std::collections::HashMap<std::path::PathBuf, usize>,
    // Use Option to allow dropping the stream when done, separately from releasing the tracker
    dir_created_send_stream: Option<SendStream>,
    done_creating_directories: bool,
}

impl DirectoryTracker {
    pub fn new(dir_created_send_stream: SendStream) -> Self {
        Self {
            remaining_dir_entries: std::collections::HashMap::new(),
            dir_created_send_stream: Some(dir_created_send_stream),
            done_creating_directories: false,
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
        let confirmation = remote::protocol::DirectoryCreated {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DirectoryMessage::Created(confirmation);
        self.dir_created_send_stream
            .as_mut()
            .expect("Send stream should be initialized")
            .send_object(&message)
            .await?;
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
            if self.done_creating_directories && self.remaining_dir_entries.is_empty() {
                self.finish().await?;
            }
        }
        Ok(())
    }

    async fn send_completion(
        &mut self,
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> anyhow::Result<()> {
        let completion = remote::protocol::DirectoryComplete {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
        };
        let message = remote::protocol::DirectoryMessage::Complete(completion);
        self.dir_created_send_stream
            .as_mut()
            .expect("Send stream should be initialized")
            .send_object(&message)
            .await?;
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
            if self.done_creating_directories && self.remaining_dir_entries.is_empty() {
                self.finish().await?;
            }
        }
        Ok(())
    }

    pub async fn done_creating_directories(&mut self) -> anyhow::Result<()> {
        event!(Level::INFO, "All directories created");
        self.done_creating_directories = true;
        if self.remaining_dir_entries.is_empty() {
            self.finish().await?;
        } else {
            event!(
                Level::DEBUG,
                "Not all directories were processed, remaining: {}",
                self.remaining_dir_entries.len()
            );
        }
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        event!(Level::INFO, "No more directories to process, finishing");
        self.dir_created_send_stream
            .take()
            .expect("Send stream should be initialized");
        assert!(
            self.remaining_dir_entries.is_empty(),
            "Not all directories were processed before finishing"
        );
        Ok(())
    }
}

pub type SharedDirectoryTracker = std::sync::Arc<tokio::sync::Mutex<DirectoryTracker>>;

pub fn make_shared(dir_created_send_stream: SendStream) -> SharedDirectoryTracker {
    std::sync::Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
        dir_created_send_stream,
    )))
}
