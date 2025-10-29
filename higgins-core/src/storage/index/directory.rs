use std::{path::PathBuf, time::SystemTime};

use crate::broker::Broker;
use crate::storage::batch_coordinate::BatchCoordinate;
use crate::storage::dereference::Reference;
use crate::storage::dereference::S3Reference;
use crate::storage::index::index_size_from_index_type_and_definition;
use crate::subscription::error;
use crate::topography::Key;
use crate::topography::StreamDefinition;

use super::IndexError;
use super::IndexFile;
use super::IndexType;
use super::IndexesView;

use super::default::DefaultIndex;
use riskless::{
    batch_coordinator::{
        BatchInfo, BatchMetadata, CommitBatchResponse, FindBatchRequest, FindBatchResponse,
        TopicIdPartition,
    },
    messages::CommitBatchRequest,
};

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

        tracing::trace!("Stream path: {:#?}", topic_path);

        if !topic_path.exists() {
            tracing::trace!("Stream path does not exist, creating: {:#?}", topic_path);

            std::fs::create_dir_all(&topic_path).unwrap();
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

    pub fn index_file_name_from_stream_and_partition(
        &self,
        stream: String,
        partition: &[u8],
    ) -> String {
        let mut topic_dir = self.create_topic_dir(&stream);

        let index_file_path = Self::index_file_path_from_partition(partition);

        topic_dir.push(index_file_path);

        topic_dir.to_string_lossy().to_string()
    }

    /// Retrieves an index file instance given a stream and partition.
    pub fn index_file_from_stream_and_partition(
        &self,
        stream: String,
        partition: &[u8],
        element_size: usize,
        index_type: IndexType,
    ) -> Result<IndexFile, IndexError> {
        let index_file_name = self.index_file_name_from_stream_and_partition(stream, partition);

        tracing::trace!("Index file: {index_file_name}");

        tracing::info!("Retrieveing Index file with name: {:#?}", index_file_name);

        let index_file: IndexFile = IndexFile::new(&index_file_name, element_size, index_type)
            .inspect_err(|err| {
                tracing::error!("Error whilst trying to open index file: {:#?}", err);
            })?;

        Ok(index_file)
    }

    /// Retrieves the timestamp before the given one.
    pub async fn get_by_timestamp(
        &self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
        index_type: IndexType,
    ) -> Vec<FindBatchResponse> {
        todo!();
        let mut responses = vec![];

        let stream_str = String::from_utf8_lossy(stream).to_string();

        let topic_id_partition = TopicIdPartition(stream_str.clone(), partition.to_owned());

        let index_file = self
            .index_file_from_stream_and_partition(
                stream_str,
                partition,
                DefaultIndex::size_of(),
                index_type,
            )
            .unwrap();

        let indexes: IndexesView = IndexesView {
            buffer: index_file.as_slice(),
            element_size: size_of::<usize>(),
            index_type,
        };

        let index = indexes.find_by_timestamp(timestamp);

        match index {
            Some(index) => {
                let index_reference = index.get_reference();

                let index_reference = match index_reference {
                    Reference::S3(r) => r,
                    _ => unimplemented!(),
                };

                let object_key = uuid::Uuid::from_bytes(index_reference.object_key).to_string();

                tracing::info!("Reading from object: {:#?}", object_key);

                let batch = BatchInfo {
                    batch_id: 1,
                    object_key,
                    metadata: BatchMetadata {
                        topic_id_partition,
                        byte_offset: index_reference.position,
                        byte_size: index_reference.size.try_into().unwrap(),
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
        index_type: &IndexType,
        stream_definition: &StreamDefinition,
    ) -> Vec<Reference> {
        let mut responses = vec![];

        let stream_str = String::from_utf8_lossy(stream).to_string();

        let topic_id_partition = TopicIdPartition(stream_str.clone(), partition.to_owned());

        let index_file = self
            .index_file_from_stream_and_partition(
                stream_str,
                partition,
                index_size_from_index_type_and_definition(index_type, stream_definition),
                index_type.clone(),
            )
            .unwrap();

        let indexes: IndexesView = IndexesView {
            buffer: index_file.as_slice(),
            element_size: size_of::<usize>(),
            index_type: index_type.clone(),
        };

        let index = indexes.last();

        let index = index.map(DefaultIndex::of);

        match index {
            Some(index) => {
                responses.push(index.reference());
            }
            None => {
                tracing::error!("No Index found at offset {}", 0);
                todo!()
                // let response = FindBatchResponse {
                //     errors: vec!["Failed to find index for Topic and offset".to_string()],
                //     batches: vec![],
                //     log_start_offset: 0,
                //     high_watermark: 0,
                // };
                // responses.push(response);
            }
        }

        responses
    }

    /// Retrieves the offset by its offset number.
    pub async fn get_by_offset(
        &self,
        stream: &[u8],
        partition: &[u8],
        offset: u64,
        index_type: IndexType,
        stream_definition: &StreamDefinition,
    ) -> Result<Reference, Box<dyn std::error::Error>> {
        let stream_str = String::from_utf8_lossy(stream).to_string();

        let topic_id_partition = TopicIdPartition(stream_str.clone(), partition.to_owned());

        let index_size = index_size_from_index_type_and_definition(&index_type, stream_definition);

        let index_file = self
            .index_file_from_stream_and_partition(
                stream_str,
                partition,
                index_size,
                index_type.clone(),
            )
            .unwrap();

        let indexes = IndexesView {
            buffer: index_file.as_slice(),
            element_size: index_size.clone(),
            index_type,
        };

        let index = indexes
            .get(offset.try_into().unwrap())
            .map(DefaultIndex::of);

        match index {
            Some(index) => {
                let reference = index.reference();

                // let object_key = match reference {
                //     crate::storage::index::Reference::S3(val) => {
                //         uuid::Uuid::from_bytes(index.object_key()).to_string()
                //     }
                //     _ => {
                //         tracing::error!("Currently S3 References are only implemented.");
                //         panic!("Expected an S3 reference, got something different.");
                //     }
                // };

                // tracing::info!("Reading from object: {:#?}", object_key);

                // let batch = BatchInfo {
                //     batch_id: 1,
                //     object_key,
                //     metadata: BatchMetadata {
                //         topic_id_partition,
                //         byte_offset: index.position().into(),
                //         byte_size: index.size().try_into().unwrap(),
                //         base_offset: 0,
                //         last_offset: 0,
                //         log_append_timestamp: 0,
                //         batch_max_timestamp: 0,
                //         timestamp_type: riskless::batch_coordinator::TimestampType::Dummy,
                //         producer_id: 0,
                //         producer_epoch: 0,
                //         base_sequence: 0,
                //         last_sequence: 0,
                //     },
                // };

                // let response = FindBatchResponse {
                //     errors: vec![],
                //     batches: vec![batch],
                //     log_start_offset: 0,
                //     high_watermark: 0,
                // };

                Ok(reference)
            }
            None => {
                // TODO: handle error here.
                tracing::error!("No Index found at offset {}", 0);
                unimplemented!();

                // let response = FindBatchResponse {
                //     errors: vec!["Failed to find index for Topic and offset".to_string()],
                //     batches: vec![],
                //     log_start_offset: 0,
                //     high_watermark: 0,
                // };
                // response
            }
        }
    }

    pub async fn find_batches(
        &self,
        batch_requests: Vec<FindBatchRequest>,
        _size: u32,
        index_type: &IndexType,
        stream_definition: &StreamDefinition,
    ) -> Vec<Reference> {
        let mut responses = vec![];

        for batch_request in batch_requests {
            let FindBatchRequest {
                topic_id_partition,
                offset,
                // max_partition_fetch_bytes,
                ..
            } = batch_request;

            let TopicIdPartition(topic, partition) = topic_id_partition.clone();

            let mut index_file = self
                .index_file_from_stream_and_partition(
                    topic,
                    &partition,
                    index_size_from_index_type_and_definition(index_type, stream_definition),
                    index_type.clone(),
                )
                .unwrap();

            let indexes = IndexesView {
                buffer: index_file.as_slice(),
                element_size: index_size_from_index_type_and_definition(
                    index_type,
                    stream_definition,
                ),
                index_type: index_type.clone(),
            };

            tracing::info!("Reading at offset: {}", 0);

            let index: Option<DefaultIndex> = indexes
                .get(offset.try_into().unwrap())
                .map(DefaultIndex::of);

            match index {
                Some(index) => {
                    let reference = index.reference();

                    responses.push(reference);
                }
                None => {
                    // TODO: Handle errors here?

                    tracing::error!("No Index found at offset {}", 0);
                    // let response = FindBatchResponse {
                    //     errors: vec!["Failed to find index for Topic and offset".to_string()],
                    //     batches: vec![],
                    //     log_start_offset: 0,
                    //     high_watermark: 0,
                    // };
                    // responses.push(response);
                }
            }
        }

        responses
    }

    /// Adds a default index to the given type.
    pub async fn put_default_index(
        &self,
        stream: String,
        partition: &[u8],
        reference: Reference,
        batch_coord: BatchCoordinate,
        index_type: &IndexType,
        stream_def: &StreamDefinition,
    ) {
        let mut index_file = self
            .index_file_from_stream_and_partition(
                stream,
                &partition,
                index_size_from_index_type_and_definition(&index_type, &stream_def),
                index_type.clone(),
            )
            .unwrap();

        let indexes = IndexesView {
            buffer: index_file.as_slice(),
            element_size: size_of::<DefaultIndex>(),
            index_type: index_type.clone(),
        };

        let offset = (indexes.count() + 1) as u64;
        let position = batch_coord.offset;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut val = vec![0; index_size_from_index_type_and_definition(&index_type, &stream_def)];

        // TODO: change this to reflect the new reference API
        DefaultIndex::put(
            offset,
            reference,
            position.try_into().unwrap(),
            timestamp,
            batch_coord.size.into(),
            &mut val,
        );

        let index = DefaultIndex::of(&val).to_bytes();

        tracing::info!("Saving Index: {:#?}", index);

        index_file.append(&index).unwrap();

        tracing::info!("Successfully saved Index: {:#?}", index);
    }

    /// Commit this file to the ObjectStore.
    pub async fn commit_file(
        &self,
        object_key: [u8; 16],
        batches: Vec<CommitBatchRequest>,
        broker: std::sync::Arc<tokio::sync::RwLock<Broker>>,
    ) -> Vec<CommitBatchResponse> {
        let mut responses = vec![];

        for batch in batches {
            let TopicIdPartition(topic, partition) = batch.topic_id_partition.clone();

            let (index_type, stream_def) = {
                let broker = broker.write().await;

                let (_, stream_def) = broker
                    .get_topography_stream(&Key(topic.as_bytes().to_owned()))
                    .unwrap();

                (
                    IndexType::try_from(stream_def).unwrap(),
                    stream_def.to_owned(),
                )
            };

            let mut index_file = self
                .index_file_from_stream_and_partition(
                    topic,
                    &partition,
                    index_size_from_index_type_and_definition(&index_type, &stream_def),
                    index_type.clone(),
                )
                .unwrap();

            let indexes = IndexesView {
                buffer: index_file.as_slice(),
                element_size: size_of::<DefaultIndex>(),
                index_type: index_type.clone(),
            };

            let offset = (indexes.count() + 1) as u64;
            let position = batch.byte_offset;
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let reference = Reference::S3(S3Reference {
                object_key,
                size: batch.size.into(),
                position,
            });

            let mut val = [0; DefaultIndex::size_of()];

            DefaultIndex::put(
                offset,
                reference,
                position.try_into().unwrap(),
                timestamp,
                batch.size.into(),
                &mut val,
            );

            let index = DefaultIndex::of(&val).to_bytes();

            tracing::info!("Saving Index: {:#?}", index);

            index_file.append(&index).unwrap();

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
