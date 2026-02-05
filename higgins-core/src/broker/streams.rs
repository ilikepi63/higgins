use crate::topography::{Key, StreamDefinition};

use super::Broker;

use arrow::{array::RecordBatch, datatypes::Schema};
use std::sync::Arc;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

impl Broker {
    pub fn get_stream(&self, stream_name: &[u8]) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        tracing::trace!("[GET_STREAM] Retrieving streams from {:#?}", self.streams);
        self.streams.get(stream_name)
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &[u8], schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_owned(), (schema, tx, rx));
    }

    /// Get a stream inside of the topography.
    pub fn get_topography_stream(&self, key: &Key) -> Option<(Key, &StreamDefinition)> {
        self.topography
            .get_stream_definition_by_key(String::from_utf8(key.0.to_owned()).ok()?)
            .map(|stream_def| (key.clone(), stream_def))
    }

    pub fn get_schema(&self, key: &Key) -> Option<&Arc<arrow::datatypes::Schema>> {
        self.topography
            .get_schema_by_key(String::from_utf8(key.0.to_owned()).ok()?)
    }
}
