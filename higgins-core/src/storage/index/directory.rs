use std::{path::PathBuf, sync::Arc, time::SystemTime};

use super::IndexError;
use super::IndexesMut;
use super::file::IndexFile;

use super::default::DefaultIndex;
use riskless::{
    batch_coordinator::{
        BatchInfo, BatchMetadata, CommitBatchResponse, CommitFile, FindBatchRequest,
        FindBatchResponse, FindBatches, TopicIdPartition,
    },
    messages::CommitBatchRequest,
};
use tokio::io::AsyncWriteExt;

// use crate::storage::index::index_reader::load_all_indexes_from_disk;

/// A struct representing the management of indexes for all of higgins' record batches.
#[derive(Debug)]
pub struct IndexDirectory(pub PathBuf);

impl IndexDirectory {
    pub fn new(directory: PathBuf) -> Result<Self, IndexError> {
        if !directory.is_dir() {
            return Err(IndexError::IndexFileIsNotADirectory);
        }

        Ok(Self(directory))
    }

    pub fn create_topic_dir(&self, topic: &str) -> PathBuf {
        let mut topic_path = self.0.clone();
        topic_path.push(topic);

        if !topic_path.exists() {
            std::fs::create_dir(&topic_path).unwrap();
        }

        topic_path
    }

    pub fn index_file_path_from_partition(partition_key: &[u8]) -> String {
        format!(
            "{:0>20}.index",
            partition_key
                .iter()
                .map(|b| b.to_string())
                .collect::<String>()
        )
    }

    pub fn index_file_from_stream_and_partition(&self, stream: String, partition: &[u8]) -> String {
        let mut topic_dir = self.create_topic_dir(&stream);

        let index_file_path = Self::index_file_path_from_partition(partition);

        topic_dir.push(index_file_path);

        topic_dir.to_string_lossy().to_string()
    }

    /// Retrieves the timestamp before the given one.
    pub async fn get_by_timestamp(
        &self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Vec<FindBatchResponse> {
        let mut responses = vec![];

        let stream_str = String::from_utf8_lossy(stream).to_string();

        let topic_id_partition = TopicIdPartition(stream_str.clone(), partition.to_owned());

        let index_file_path = self.index_file_from_stream_and_partition(stream_str, partition);

        if !std::fs::exists(&index_file_path).unwrap_or(false) {
            return vec![];
        }

        let read_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&index_file_path)
            .unwrap();

        let index_file = IndexFile::new(&index_file_path).unwrap();
        let indexes = IndexesMut {
            buffer: index_file.as_slice(),
        };

        let index = indexes.find_by_timestamp(timestamp);

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

        responses
    }

    /// Retrieves the latest value to be placed on this partition.
    pub async fn get_latest_offset(
        &self,
        stream: &[u8],
        partition: &[u8],
    ) -> Vec<FindBatchResponse> {
        let mut responses = vec![];

        let stream_str = String::from_utf8_lossy(stream).to_string();

        let topic_id_partition = TopicIdPartition(stream_str.clone(), partition.to_owned());

        let index_file_path = self.index_file_from_stream_and_partition(stream_str, partition);

        if !std::fs::exists(&index_file_path).unwrap_or(false) {
            return vec![];
        }

        let read_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&index_file_path)
            .unwrap();

        let index_file = IndexFile::new(&index_file_path).unwrap();
        let indexes = IndexesMut {
            buffer: index_file.as_slice(),
        };

        let index = indexes.last();

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

        responses
    }

    /// Retrieves the offset by its offset number.
    #[allow(unused)]
    pub async fn get_by_offset(&self, stream: &[u8], partition: &[u8]) -> Vec<FindBatchResponse> {
        vec![]
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

            let index_file_path = self.index_file_from_stream_and_partition(topic, &partition);

            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&index_file_path)
                .await
                .unwrap();

            let read_file = std::fs::OpenOptions::new()
                .read(true)
                .open(&index_file_path)
                .unwrap();

            let index_file = IndexFile::new(&index_file_path).unwrap();
            let indexes = IndexesMut {
                buffer: index_file.as_slice(),
            };

            let offset = indexes.count() + 1;
            let position = batch.byte_offset;
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let index = DefaultIndex {
                offset,
                object_key,
                position: position.try_into().unwrap(),
                timestamp,
                size: batch.size.into(),
            }
            .to_bytes();

            tracing::info!("Saving Index: {:#?}", index);

            file.write_all(&index).await.unwrap();

            file.sync_all().await.unwrap();

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
                offset,
                // max_partition_fetch_bytes,
                ..
            } = batch_request;

            let TopicIdPartition(topic, partition) = topic_id_partition.clone();

            let index_file_path = self.index_file_from_stream_and_partition(topic, &partition);

            tracing::info!("Reading metadata for file : {index_file_path}");

            let read_file = std::fs::OpenOptions::new()
                .read(true)
                .open(&index_file_path)
                .unwrap();

            let index_file = IndexFile::new(&index_file_path).unwrap();
            let indexes = IndexesMut {
                buffer: index_file.as_slice(),
            };

            tracing::info!("Reading at offset: {}", 0);

            let index = indexes.get(offset.try_into().unwrap()); // offset.try_into().unwrap());

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
