use super::Broker;
use crate::storage::index::{IndexError, IndexFile, IndexesMut, Timestamped};
use rkyv::Portable;
use std::sync::Arc;

pub struct BrokerIndexFile<T: Portable + Timestamped> {
    index_file: IndexFile<T>,
    mutex: Arc<tokio::sync::Mutex<()>>,
}

impl<T: Portable + Timestamped> BrokerIndexFile<T> {
    /// Create a new instance of a BrokerIndexFile.
    pub fn new(index_file: IndexFile<T>, mutex: Arc<tokio::sync::Mutex<()>>) -> Self {
        Self { index_file, mutex }
    }

    pub async fn lock<'a>(&'a mut self) -> BrokerIndexFileLock<'a, T> {
        let lock = self.mutex.lock().await;

        BrokerIndexFileLock {
            index_file: &mut self.index_file,
            mutex: self.mutex.clone(),
            lock_guard: lock,
        }
    }
}

#[allow(unused)]
pub struct BrokerIndexFileLock<'a, T: Portable + Timestamped> {
    index_file: &'a mut IndexFile<T>,
    mutex: Arc<tokio::sync::Mutex<()>>,
    lock_guard: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a, T: Portable + Timestamped> BrokerIndexFileLock<'a, T> {
    /// Append a new T to this index file.
    pub async fn append(&mut self, val: &[u8]) -> Result<(), IndexError> {
        // Append this data to the underlying file.
        self.index_file.append(val)?;

        Ok(())
    }

    pub fn as_indexes_mut(&mut self) -> IndexesMut<T> {
        self.index_file.as_index_mut()
    }
}

impl Broker {
    pub fn get_index_file<T: Portable + Timestamped>(
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
