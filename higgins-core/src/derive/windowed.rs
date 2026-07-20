//! All liveness work on the windowed specific derived streams

use crate::broker::{Broker, BrokerIndexFile};
use crate::derive::operation::OperationData;
use crate::derive::windowed::definition::WindowValue;
use crate::storage::index::file::windowed_index_file::WindowedIndexFile;
use crate::storage::windowing::assign_sliding_windows_range;
use crate::subscription::helpers::push_subscriptions;
use definition::WindowedStreamDefinition;
use higgins_shared::{HigginsError, PartitionName, StreamName};
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod definition;

#[derive(Debug)]
pub struct WindowOperation(pub OperationData);

impl WindowOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Initing window operation : {}", self.0.stream.as_str());

        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        tracing::debug!("Committing window operation : {}", self.0.stream.as_str());

        // let stream_key: Key = self.0.stream.clone().into();
        let definition = WindowedStreamDefinition::try_from(self.0.definition.clone())?;

        match &definition.window_type {
            WindowValue::Count(count) => {
                let resultant_stream = self.0.stream.to_string();

                tracing::info!("Retrieving index file for stream {resultant_stream}");

                // TODO: maybe paralellize these?
                let mut resultant_index_file =
                    get_index_file_handle(&self.0.stream, &self.0.partition, self.0.broker.clone())
                        .await?;

                tracing::info!("Retrieved index file..");

                let offsets = self.0.offsets.get().await?;

                let mut new_ranges = assign_sliding_windows_range(
                    offsets.clone(),
                    *count,
                    definition.slide.normalize(),
                    0,
                );

                let mut guard = resultant_index_file.lock().await;
                let index_file = guard.as_index();

                let mut windowed_index_file = WindowedIndexFile::of(index_file);

                tracing::info!("Applying ranges {:#?} to windowed index file.", new_ranges);

                windowed_index_file.put_ranges(&mut new_ranges)?;

                tracing::debug!(
                    "Retrieving broker lock for subscription pushing for windowed operation."
                );

                let mut broker_guard = self.0.broker.write().await;
                tracing::debug!("Retrieving the broker lock.");

                push_subscriptions(
                    self.0.stream.clone(),
                    self.0.partition.clone(),
                    offsets,
                    &mut broker_guard,
                )
                .await?;

                tracing::debug!("Pushed the subscriptions.");

                // // acknowledge me!
                // {
                //     let mut guard = self
                //         .0
                //         .subscription
                //         .as_ref()
                //         .ok_or(HigginsError::Arbitrary(
                //             "Failed to retrieve subscription for given acknoweledgement"
                //                 .to_string(),
                //         ))?
                //         .write()
                //         .await;
                //     tracing::info!("Acknowledging ranges {:#?}.", offsets);
                //     guard.acknowledge(&self.0.partition, &offsets)?;
                // }

                tracing::info!("Successfully applied ranges to windowed function.");
            }
            WindowValue::Timed((_count, _time_unit)) => {
                tracing::error!("TIMED STREAM IS NOT AVAILABLE");

                // ON timestamp type
                //
                //  -> We'd need to check the resultant offset's timestamp.
                //  -> We would check if there are any "open" windows where this timestamp >
                //  -> We add in these values and close the open ones that can be closed

                todo!()
            }
        }

        Ok(())
    }
}

async fn get_index_file_handle(
    stream: &StreamName,
    key: &PartitionName,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<BrokerIndexFile, HigginsError> {
    let mut broker = broker_ref.write().await;
    broker.get_index_file(stream.clone(), key)
}
