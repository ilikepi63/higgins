//! All liveness work on the windowed specific derived streams

use super::joining::opts::eager_take_from_subscription_or_wait;
use crate::broker::{Broker, BrokerIndexFile};
use crate::error::HigginsError;
use crate::storage::index::windowed_index::WindowedIndex;
use crate::task::SpawnTaskConfig;
use definition::WindowedStreamDefinition;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod definition;

pub async fn create_windowed_stream_from_definition(
    definition: WindowedStreamDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) {
    let (client_id, condvar, subscription) = {
        // let mut broker = broker.write().await;
        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();
        let left_subscription =
            broker.create_subscription(definition.base_stream.base.unwrap().as_bytes());
        let stream = ""; // let stream = .stream.clone();
        let (left_notify, left_subscription) = broker
            .get_subscription_by_key(stream.as_bytes(), &left_subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
            .unwrap();

        tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");

        (client_id, left_notify, left_subscription)
    };

    broker
        .task_handler
        .spawn(&SpawnTaskConfig::new("windowing", false), async move {
            let offsets = eager_take_from_subscription_or_wait(
                subscription.clone(),
                condvar.clone(),
                client_id,
            )
            .await
            .unwrap();

            // Retrieve the Index file, given the stream name and partition key.
            // let mut index_file = {
            //     let mut broker = broker_ref.write().await;
            //     let index_file: BrokerIndexFile = broker
            //         .get_index_file(
            //             String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
            //             &partition,
            //             WindowedIndex::size_of(),
            //         )
            //         .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
            //     tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
            //     drop(broker);
            //     index_file
            // };

            // Once we have the offsets, we just need to add it to the derivative stream
        })
        .unwrap();
}
