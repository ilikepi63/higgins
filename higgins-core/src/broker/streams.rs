use crate::topography::{Key, StreamDefinition};

use super::Broker;

use arrow::{array::RecordBatch, datatypes::Schema};
use std::sync::Arc;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

impl Broker {
    pub fn get_stream(&self, stream_name: &[u8]) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        self.streams.get(stream_name)
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &[u8], schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_owned(), (schema, tx, rx));
    }

    /// Get a stream inside of the topography.
    pub fn get_topography_stream(&self, key: &Key) -> Option<(&Key, &StreamDefinition)> {
        self.topography.streams.iter().find(|(k, _)| *k == key)
    }

    pub fn get_schema(&self, key: &Key) -> Option<&Arc<arrow::datatypes::Schema>> {
        self.topography.schema.get(key)
    }
}
