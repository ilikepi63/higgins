use std::{
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{Arc, atomic::AtomicU64},
    time::SystemTime,
};

use riskless::{
    batch_coordinator::{
        BatchInfo, BatchMetadata, CommitBatchResponse, CommitFile, FindBatchRequest,
        FindBatchResponse, FindBatches, TopicIdPartition,
    },
    messages::CommitBatchRequest,
};

use crate::storage::index::{Index, index_writer::IndexWriter};

use super::index::index_reader::IndexReader;

/// A struct representing the management of indexes for all of higgins' record batches.
#[derive(Debug)]
pub struct IndexDirectory(pub PathBuf);

impl IndexDirectory {
    pub fn new(directory: PathBuf) -> Self {
        assert!(directory.is_dir());

        Self(directory)
    }

    pub fn create_topic_dir(&self, topic: &str) -> PathBuf {
        let mut topic_path = std::env::current_dir().unwrap();
        topic_path.push(topic);

        if !topic_path.exists() {
            std::fs::create_dir(&topic_path).unwrap();
        }

        topic_path
    }

    pub fn index_file_path_from_partition(partition_key: &[u8]) -> String {
        format!(
            "{:0>20}.index",
            partition_key.iter().map(|b| b.to_string()).collect::<String>()
        )
    }
}

#[async_trait::async_trait]
impl CommitFile for IndexDirectory {
    async fn commit_file(
        &self,
        object_key: [u8; 16],
        _uploader_broker_id: u32,
        _file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        let mut responses = vec![];

        for batch in batches {
            let TopicIdPartition(topic, partition) = batch.topic_id_partition.clone();

            let mut topic_dir = self.create_topic_dir(&topic);

            let index_file_path = Self::index_file_path_from_partition(&partition);

            topic_dir.push(index_file_path);

            let index_file_path = topic_dir.to_string_lossy().to_string();

            let index_file_exists = std::fs::exists(&index_file_path).unwrap();

            let mut index_writer = IndexWriter::new(
                &index_file_path,
                Arc::new(AtomicU64::new(u64::MAX)),
                true,
                index_file_exists,
            )
            .await
            .unwrap();

            let index_size_bytes = std::fs::metadata(&index_file_path).unwrap().size();

            let index_reader =
                IndexReader::new(&index_file_path, Arc::new(AtomicU64::new(index_size_bytes)))
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
                size: batch.size.into(),
            }
            .to_bytes();

            tracing::info!("Saving Index: {:#?}", index);

            index_writer.save_indexes(&index).await.unwrap();

            tracing::info!("Successfully saved Index: {:#?}", index);

            responses.push(CommitBatchResponse {
                errors: vec![],
                assigned_base_offset: 0,
                log_append_time: timestamp,
                log_start_offset: offset.into(),
                is_duplicate: false,
                request: batch.clone(),
            });
        }

        responses
    }
}

#[async_trait::async_trait]
impl FindBatches for IndexDirectory {
    async fn find_batches(
        &self,
        batch_requests: Vec<FindBatchRequest>,
        _size: u32,
    ) -> Vec<FindBatchResponse> {
        let mut responses = vec![];

        for batch_request in batch_requests {
            let FindBatchRequest {
                topic_id_partition,
                // offset,
                // max_partition_fetch_bytes,
                ..
            } = batch_request;

            let TopicIdPartition(topic, partition) = topic_id_partition.clone();

            let mut topic_dir = self.create_topic_dir(&topic);

            let index_file_path = Self::index_file_path_from_partition(&partition);

            topic_dir.push(index_file_path);

            let index_file_path = topic_dir.to_string_lossy().to_string();

            let index_size_bytes = std::fs::metadata(&index_file_path).unwrap().size();

            let index_reader =
                IndexReader::new(&index_file_path, Arc::new(AtomicU64::new(index_size_bytes)))
                    .await
                    .unwrap();

            let indexes = index_reader.load_all_indexes_from_disk().await.unwrap();

            tracing::info!("Reading at offset: {}", 0);

            let index = indexes.get(0); // offset.try_into().unwrap());

            tracing::info!("Reading index: {:#?}", index);

            // tracing::info!("SIZE: {}", index.unwrap().size());

            match index {
                Some(index) => {
                    let object_key = uuid::Uuid::from_bytes(index.object_key()).to_string();

                    tracing::info!("Reading from object: {:#?}", object_key);

                    let batch = BatchInfo {
                        batch_id: 1,
                        object_key,
                        metadata: BatchMetadata {
                            topic_id_partition,
                            byte_offset: index.position().into(),
                            byte_size: index.size().try_into().unwrap(),
                            base_offset: 0,
                            last_offset: 0,
                            log_append_timestamp: 0,
                            batch_max_timestamp: 0,
                            timestamp_type: riskless::batch_coordinator::TimestampType::Dummy,
                            producer_id: 0,
                            producer_epoch: 0,
                            base_sequence: 0,
                            last_sequence: 0,
                        },
                    };

                    let response = FindBatchResponse {
                        errors: vec![],
                        batches: vec![batch],
                        log_start_offset: 0,
                        high_watermark: 0,
                    };

                    responses.push(response);
                }
                None => {
                    tracing::error!("No Index found at offset {}", 0);
                    let response = FindBatchResponse {
                        errors: vec!["Failed to find index for Topic and offset".to_string()],
                        batches: vec![],
                        log_start_offset: 0,
                        high_watermark: 0,
                    };
                    responses.push(response);
                }
            }
        }

        responses
    }
}
