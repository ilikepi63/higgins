//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

mod completion;
pub mod join;
pub mod mapping;
pub mod opts;

use crate::broker::{Broker, BrokerIndexFile};
use crate::derive::joining::completion::complete_from;
use crate::derive::joining::join::JoinDefinition;
use crate::derive::operation::OperationData;
use crate::storage::dereference::Reference;
use crate::storage::index::joined_index::JoinedIndex;
use crate::subscription::helpers::push_subscriptions;
use crate::utils::epoch;
use higgins_shared::HigginsError;
use opts::amalgamate_join;

#[derive(Debug)]
pub struct JoinOperation {
    pub data: OperationData,
    pub definition: JoinDefinition,
    /// Offset at which this index is trying to place.
    pub optimistic_offset: Option<usize>,
    pub optimistic_index: Option<Vec<u8>>,
}

impl JoinOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        let stream = self.definition.clone().base.0;
        let n_offsets = self.definition.joins.len();

        // Retrieve the Index file, given the stream name and partition key.
        let mut index_file = {
            let mut broker = self.data.broker.write().await;
            let index_file: BrokerIndexFile =
                broker.get_index_file(stream, &self.data.partition)?; // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
            tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
            drop(broker);
            index_file
        };

        // we first make a voodoo index.
        let optimistic_offset = {
            let guard = index_file.lock().await;
            guard.len()?
        };

        // Create the index.
        let mut optimistic_index = vec![0_u8; JoinedIndex::size_of(n_offsets)];

        tracing::debug!("Waiting for the offsets..");

        let offsets = self.data.offsets.get().await?;

        tracing::debug!("We retrieved the offsets: {:#?}", offsets);

        JoinedIndex::put(
            optimistic_offset as u64,
            Reference::Null,
            epoch(),
            &(0..n_offsets)
                .map(|i| {
                    if i == self.data.join_index? as usize {
                        Some(offsets.start)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            &mut optimistic_index,
        )
        .inspect_err(|err| tracing::error!("Error: {:#?}", err))?;

        tracing::debug!("Completed index: {:#?}", offsets);

        if optimistic_offset > 0 {
            tracing::trace!("Completing the index from the previous index.");
            let last_completed_index = {
                let mut guard = index_file.lock().await;
                // TODO: Fix this, if there is no previous index, just complete the current index.
                let mut buf = vec![0_u8; JoinedIndex::size_of(n_offsets)];
                guard.read_at(optimistic_offset.saturating_sub(1), &mut buf)?;
                buf
            };

            complete_from(&mut optimistic_index, &last_completed_index, n_offsets)?;
        } else {
            tracing::trace!("Completing the index without a previous index..");

            JoinedIndex::set_completed(&mut optimistic_index);
        }

        let data = amalgamate_join(
            &optimistic_index,
            self.definition.clone(),
            self.data.partition.clone(),
            self.data.broker.clone(),
        )
        .await?;

        tracing::debug!("Got the data {:#?}", offsets);

        self.data.records_setter.set(vec![data.clone()]).await;

        tracing::trace!("Completed amalmagamation: {:#?}", data);

        let stream = String::from_utf8_lossy(self.definition.base.0.as_bytes()).to_string();

        let broker_guard = self.data.broker.write().await;

        let backing_store = broker_guard
            .backing_store
            .as_ref()
            .ok_or(HigginsError::ObjectStoreNotConfigured)?
            .clone();
        // CREATE REFERENCE
        let reference =
            Broker::put_data_store(backing_store, stream, &self.data.partition, data).await?;

        tracing::trace!("Created the Reference: {:#?}", reference);

        JoinedIndex::put_reference_static(reference, &mut optimistic_index)?;

        self.optimistic_index = Some(optimistic_index);
        self.optimistic_offset = Some(optimistic_offset);

        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let stream = self.definition.base.0.clone();

        let mut index_file = {
            let mut broker = self.data.broker.write().await;
            let index_file: BrokerIndexFile =
                broker.get_index_file(stream.clone(), &self.data.partition)?; // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
            tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
            drop(broker);
            index_file
        };

        let mut index_file_guard = index_file.lock().await;

        tracing::info!(
            "Retrieved indexfile for stream {stream} and partition {:#?}",
            self.data.partition
        );

        match (
            self.optimistic_index.as_mut(),
            self.optimistic_offset.as_ref(),
        ) {
            (Some(optimistic_index), Some(optimistic_offset)) => {
                index_file_guard
                    .try_range_put_at(
                        *optimistic_offset..optimistic_offset.saturating_add(1),
                        optimistic_index,
                    )
                    .inspect_err(|err| {
                        tracing::error!("{:#?}", err);
                    })?;

                let offsets = *optimistic_offset as u64..optimistic_offset.saturating_add(1) as u64;
                self.data.offsets_setter.set(offsets.clone()).await;

                let mut broker_guard = self.data.broker.write().await;
                push_subscriptions(
                    self.data.stream.clone(),
                    self.data.partition.clone(),
                    offsets,
                    &mut broker_guard,
                )
                .await?;

                tracing::debug!("Completed join. Length: {:#?}", index_file_guard.len());
            }
            _ => {
                tracing::error!("Attempted to place optimistic index without having one.");
            }
        }

        Ok(())
    }
}
