use anyhow::Context;

#[derive(Debug)]
pub struct DirectoryTracker {
    remaining_dir_entries: std::collections::HashMap<std::path::PathBuf, usize>,
    control_send_stream: remote::streams::SharedSendStream,
}

impl DirectoryTracker {
    pub fn new(control_send_stream: remote::streams::SharedSendStream) -> Self {
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
            tracing::debug!(
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
            stream.send_control_message(&message).await?;
        }
        tracing::info!(
            "Sent directory creation confirmation: {:?} -> {:?}",
            src,
            dst
        );
        // if there are no entries, we can immediately send completion
        if num_entries == 0 {
            tracing::info!("Directory completed: {:?}", dst);
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
        stream
            .send_control_message(&message)
            .await
            .context("Failed to send directory completion notification")?;
        tracing::info!(
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
        tracing::debug!(
            "Decremented entry count for {:?}, remaining: {}",
            dst_parent_dir,
            *remaining
        );
        if *remaining == 0 {
            self.remaining_dir_entries.remove(dst_parent_dir);
            tracing::info!("Directory completed: {:?}", dst_parent_dir);
            self.send_completion(src.parent().unwrap(), dst_parent_dir)
                .await?;
        }
        Ok(())
    }
}

pub type SharedDirectoryTracker = std::sync::Arc<tokio::sync::Mutex<DirectoryTracker>>;

pub fn make_shared(
    control_send_stream: remote::streams::SharedSendStream,
) -> SharedDirectoryTracker {
    std::sync::Arc::new(tokio::sync::Mutex::new(DirectoryTracker::new(
        control_send_stream,
    )))
}
