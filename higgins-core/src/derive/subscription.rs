use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{
    broker::Broker, client::ClientRef, error::HigginsError, subscription::Subscription,
    topography::StreamName,
};

pub async fn create_derived_stream_subscription(
    stream: StreamName,
    broker_ref: Arc<RwLock<Broker>>,
) -> (u64, Arc<RwLock<Subscription>>) {
    let mut broker = broker_ref.write().await;
    let client_id = broker.clients.insert(ClientRef::NoOp).unwrap();
    let (_, sub) = broker.create_non_reactive_subscription(&stream);

    tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");
    drop(broker);

    (client_id, sub)
}
