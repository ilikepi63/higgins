use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::trace;

use crate::storage::index::INDEX_SIZE;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct IndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl IndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await?;
        if file_exists {
            file.sync_all().await?;

            let actual_index_size = file.metadata().await?.len();

            index_size_bytes.store(actual_index_size, Ordering::Release);
        }

        trace!(
            "Opened index file for writing: {file_path}, size: {}",
            index_size_bytes.load(Ordering::Acquire)
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    /// Appends multiple index buffer to the index file in a single operation.
    pub async fn save_indexes(&mut self, indexes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        if indexes.is_empty() {
            return Ok(());
        }

        let count = indexes.len() / INDEX_SIZE;

        self.file.write_all(indexes).await?;
        self.index_size_bytes
            .fetch_add(indexes.len() as u64, Ordering::Release);

        if self.fsync {
            let _ = self.fsync().await;
        }
        trace!(
            "Saved {count} indexes of size {} to file: {}",
            INDEX_SIZE * count,
            self.file_path
        );

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.file.sync_all().await?;
        Ok(())
    }
}
