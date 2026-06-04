use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    broker::Broker, client::ClientRef, subscription::Subscription, topography::StreamName,
};

pub async fn create_derived_stream_subscription_ref(
    stream: StreamName,
    broker: &mut Broker,
) -> (u64, Arc<RwLock<Subscription>>) {
    let client_id = broker.clients.insert(ClientRef::NoOp).unwrap();
    let (_, sub) = broker.create_non_reactive_subscription(&stream);
    tracing::debug!("Created the subscription, returning.");

    (client_id, sub)
}
