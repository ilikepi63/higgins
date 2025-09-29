use super::Broker;
use crate::storage::index::IndexFile;
use std::sync::Arc;

pub struct BrokerIndexFile<T> {
    index_file: IndexFile<T>,
    mutex: Arc<tokio::sync::Mutex<()>>,
}

impl<T> BrokerIndexFile<T> {
    pub fn new(index_file: IndexFile<T>, mutex: Arc<tokio::sync::Mutex<()>>) -> Self {
        Self { index_file, mutex }
    }

    pub fn append() {}
}

impl Broker {
    pub fn get_index_file<T>(
        &mut self,
        stream: String,
        partition: &[u8],
    ) -> Option<BrokerIndexFile<T>> {
        let index_file_get_result = self
            .indexes
            .index_file_from_stream_and_partition(stream.clone(), partition);

        match index_file_get_result {
            Ok(index_file) => {
                let broker_index = match self
                    .broker_indexes
                    .iter()
                    .find(|(s, p, _)| s == &stream && p == partition)
                {
                    Some(val) => val,
                    None => {
                        // We are guaranteed to be Sync here because we hold a mutable reference on the broker.
                        self.broker_indexes.push((
                            stream.to_owned(),
                            partition.to_owned(),
                            Arc::new(tokio::sync::Mutex::new(())),
                        ));

                        self.broker_indexes.last().unwrap()
                    }
                };

                Some(BrokerIndexFile::new(index_file, broker_index.2.clone()))
            }
            Err(err) => {
                tracing::error!(
                    "Failure retrieving index file, returning None. Error: {:#?}",
                    err
                );
                None
            }
        }
    }
}
