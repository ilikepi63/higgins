use riskless::{
    batch_coordinator::{CommitBatchResponse, CommitFile},
    messages::CommitBatchRequest,
};

/// A struct representing the management of indexes for all of higgins' record batches.
#[derive(Debug)]
pub struct Indexes {}

#[async_trait::async_trait]
impl CommitFile for Indexes {
    async fn commit_file(
        &self,
        object_key: [u8; 16],
        uploader_broker_id: u32,
        file_size: u64,
        batches: Vec<CommitBatchRequest>,
    ) -> Vec<CommitBatchResponse> {
        todo!()
    }
}
