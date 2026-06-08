use super::Broker;
use crate::storage::index::{IndexFile, IndexType, IndexesView};
use higgins_shared::{IndexError, PartitionName, StreamName};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub struct BrokerIndexFile {
    index_file: IndexFile,
    mutex: Arc<tokio::sync::Mutex<()>>,
}

impl BrokerIndexFile {
    /// Create a new instance of a BrokerIndexFile.
    pub fn new(index_file: IndexFile, mutex: Arc<tokio::sync::Mutex<()>>) -> Self {
        Self { index_file, mutex }
    }

    pub async fn lock<'a>(&'a mut self) -> BrokerIndexFileLock<'a> {
        let lock = self.mutex.lock().await;

        BrokerIndexFileLock {
            index_file: &mut self.index_file,
            mutex: self.mutex.clone(),
            lock_guard: lock,
        }
    }

    /// `put_at` is not meant to be used in a multithread environment if multiple
    /// concurrent processes have access to this file. This operations swaps out bytes
    /// at a specific offset with another set of bytes.
    ///
    /// You will run into race conditions if you attempt to use this concurrently.
    pub fn put_at(&mut self, index: u64, val: &mut [u8]) -> Result<(), IndexError> {
        self.index_file.put_at(index, val)
    }

    pub fn view<'a>(&'a self) -> IndexesView<'a> {
        self.index_file.as_view()
    }
}

#[allow(unused)]
pub struct BrokerIndexFileLock<'a> {
    index_file: &'a mut IndexFile,
    mutex: Arc<tokio::sync::Mutex<()>>,
    lock_guard: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a> Deref for BrokerIndexFileLock<'a> {
    type Target = IndexFile;

    fn deref(&self) -> &Self::Target {
        self.index_file
    }
}

impl<'a> DerefMut for BrokerIndexFileLock<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.index_file
    }
}

impl<'a> BrokerIndexFileLock<'a> {
    pub fn as_indexes_mut(&'a self) -> IndexesView<'a> {
        self.index_file.as_view()
    }

    pub fn as_index(&mut self) -> &mut IndexFile {
        self.index_file
    }
}

impl Broker {
    pub fn get_index_file(
        &mut self,
        stream: StreamName,
        partition: &PartitionName,
    ) -> Option<BrokerIndexFile> {
        let stream_def = self
            .topography
            .get_stream_definition_by_key(stream.clone())
            .unwrap();

        let element_size = stream_def.index_size();

        let index_file_get_result = self.indexes.index_file_from_stream_and_partition(
            stream.clone(),
            partition,
            element_size,
            IndexType::try_from(stream_def).unwrap(),
        );

        match index_file_get_result {
            Ok(index_file) => {
                let broker_index = match self.broker_indexes.iter().find(|(s, p, _)| {
                    s == &stream && PartitionName::try_from(&p[..]).unwrap() == *partition
                }) {
                    Some(val) => val,
                    None => {
                        // We are guaranteed to be Sync here because we hold a mutable reference on the broker.
                        self.broker_indexes.push((
                            stream.clone(),
                            partition.to_vec(),
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
