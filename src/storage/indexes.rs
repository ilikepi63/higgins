use std::{
    alloc::System,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
    time::SystemTime,
};

use riskless::{
    batch_coordinator::{CommitBatchResponse, CommitFile, TopicIdPartition},
    messages::CommitBatchRequest,
};

use crate::storage::index::{
    Index,
    index_writer::{self, IndexWriter},
};

use super::index::index_reader::{self, IndexReader};

/// A struct representing the management of indexes for all of higgins' record batches.
#[derive(Debug)]
pub struct IndexDirectory(PathBuf);

impl IndexDirectory {
    pub fn new(directory: PathBuf) -> Self {
        assert!(directory.is_dir());

        Self(directory)
    }

    pub fn create_topic_dir(&self, topic: &str) {
        let mut topic_path = std::env::current_dir().unwrap();
        topic_path.push(topic);

        if !topic_path.exists() {
            std::fs::create_dir(topic_path);
        }
    }
}

#[async_trait::async_trait]
impl CommitFile for IndexDirectory {
    async fn commit_file(
        &self,
        object_key: [u8; 16],
        uploader_broker_id: u32,
        file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        let mut responses = vec![];

        tracing::info!("We are here!");

        for batch in batches {
            let TopicIdPartition(topic, partition) = batch.topic_id_partition.clone();

            self.create_topic_dir(&topic);

            let index_file_path = &format!("{:0>20}.index", partition);

            let index_file_exists = std::fs::exists(index_file_path).unwrap();

            let mut index_writer = IndexWriter::new(
                index_file_path,
                Arc::new(AtomicU64::new(u64::max_value())),
                true,
                index_file_exists,
            )
            .await
            .unwrap();

            let index_size_bytes = std::fs::metadata(index_file_path).unwrap().size();

            let index_reader =
                IndexReader::new(index_file_path, Arc::new(AtomicU64::new(index_size_bytes)))
                    .await
                    .unwrap();

            let indexes = index_reader.load_all_indexes_from_disk().await.unwrap();

            let offset = indexes.count() + 1;
            let position = batch.byte_offset;
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let index = Index {
                offset,
                object_key,
                position: position.try_into().unwrap(),
                timestamp,
            }
            .to_bytes();

            index_writer.save_indexes(&index).await.unwrap();

            responses.push(CommitBatchResponse {
                errors: vec![],
                assigned_base_offset: 1,
                log_append_time: 1,
                log_start_offset: 1,
                is_duplicate: false,
                request: batch.clone(),
            });
        }

        vec![]
    }
}
