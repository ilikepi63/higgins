//! All liveness work on the windowed specific derived streams

use super::joining::opts::eager_take_from_subscription_or_wait;
use crate::error::HigginsError;
use crate::{broker::Broker, topography::StreamDefinition};
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod definition;

pub async fn create_windowed_stream_from_definition(
    definition: StreamDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) {
    let (client_id, condvar, subscription) = {
        // let mut broker = broker.write().await;
        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();
        let left_subscription = broker.create_subscription(definition.base.unwrap().as_bytes());
        let stream = ""; // let stream = .stream.clone();
        let (left_notify, left_subscription) = broker
            .get_subscription_by_key(stream.as_bytes(), &left_subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
            .unwrap();

        tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");
        drop(broker); // Explicitly drop the lock.

        (client_id, left_notify, left_subscription)
    };

    let offsets =
        eager_take_from_subscription_or_wait(subscription.clone(), condvar.clone(), client_id)
            .await
            .unwrap();

    // Once we have the offsets, we just need to add it to the derivative stream
}
