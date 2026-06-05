use crate::topography::StreamDefinition;

use super::Broker;

use arrow::{array::RecordBatch, datatypes::Schema};
use higgins_shared::StreamName;
use std::sync::Arc;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

impl Broker {
    pub fn get_stream(&self, stream_name: &StreamName) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        tracing::trace!("[GET_STREAM] Retrieving streams from {:#?}", self.streams);
        self.streams.get(stream_name)
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &StreamName, schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams.insert(stream_name.clone(), (schema, tx, rx));
    }

    /// Get a stream inside of the topography.
    pub fn get_topography_stream(
        &self,
        key: &StreamName,
    ) -> Option<(StreamName, &StreamDefinition)> {
        self.topography
            .get_stream_definition_by_key(key.clone())
            .map(|stream_def| (key.clone(), stream_def))
    }

    pub fn get_schema(&self, key: &String) -> Option<&Arc<arrow::datatypes::Schema>> {
        self.topography.get_schema_by_key(key.clone())
    }
}
