use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::Mutex;
use tokio_util::codec::FramedWrite;
use tracing::{event, Level};

#[derive(Debug)]
pub struct DirectoryTracker {
    entries: Mutex<HashMap<PathBuf, usize>>,
    sender: Mutex<FramedWrite<quinn::SendStream, tokio_util::codec::LengthDelimitedCodec>>,
}

impl DirectoryTracker {
    pub fn new(send_stream: quinn::SendStream) -> Self {
        let framed_send = FramedWrite::new(send_stream, tokio_util::codec::LengthDelimitedCodec::new());
        Self {
            entries: Mutex::new(HashMap::new()),
            sender: Mutex::new(framed_send),
        }
    }

    pub async fn add_directory(&self, dst: PathBuf, num_entries: usize) -> anyhow::Result<()> {
        let mut entries = self.entries.lock().await;
        entries.insert(dst.clone(), num_entries);
        event!(Level::DEBUG, "Added directory tracking: {:?} with {} entries", dst, num_entries);
        Ok(())
    }

    pub async fn send_directory_created(&self, confirmation: remote::protocol::DirectoryCreated) -> anyhow::Result<()> {
        let confirmation_bytes = bincode::serialize(&confirmation)?;
        let mut sender = self.sender.lock().await;
        futures::SinkExt::send(&mut *sender, bytes::Bytes::from(confirmation_bytes)).await?;
        event!(Level::INFO, "Sent directory creation confirmation: {:?} -> {:?}", confirmation.src, confirmation.dst);
        Ok(())
    }

    pub async fn decrement_entry(&self, src: &PathBuf, dst: &PathBuf) -> anyhow::Result<()> {
        let parent_dir = dst.parent().unwrap().to_path_buf();
        let mut entries = self.entries.lock().await;
        
        if let Some(remaining) = entries.get_mut(&parent_dir) {
            *remaining -= 1;
            event!(Level::DEBUG, "Decremented entry count for {:?}, remaining: {}", parent_dir, *remaining);
            
            if *remaining == 0 {
                entries.remove(&parent_dir);
                event!(Level::INFO, "Directory completed: {:?}", parent_dir);
                
                // Send directory complete notification
                let completion = remote::protocol::DirectoryComplete {
                    src: src.parent().unwrap().to_path_buf(),
                    dst: parent_dir,
                };
                let completion_bytes = bincode::serialize(&completion)?;
                
                let mut sender = self.sender.lock().await;
                futures::SinkExt::send(&mut *sender, bytes::Bytes::from(completion_bytes)).await?;
                event!(Level::INFO, "Sent directory completion notification: {:?} -> {:?}", completion.src, completion.dst);
            }
        }
        
        Ok(())
    }

    pub async fn get_remaining_entries(&self, dst: &PathBuf) -> Option<usize> {
        let entries = self.entries.lock().await;
        entries.get(dst).copied()
    }

    pub async fn finish(&self) -> anyhow::Result<()> {
        let mut sender = self.sender.lock().await;
        sender.get_mut().finish().await?;
        Ok(())
    }
}