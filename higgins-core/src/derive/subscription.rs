use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{broker::Broker, client::ClientRef, subscription::Subscription};
use higgins_shared::{HigginsError, StreamName};

pub async fn create_derived_stream_subscription_ref(
    stream: StreamName,
    broker: &mut Broker,
) -> Result<(u64, Arc<RwLock<Subscription>>), HigginsError> {
    let client_id = broker
        .clients
        .insert(ClientRef::NoOp)
        .ok_or(HigginsError::Arbitrary(
            "Failed to insert a client into the shared client structure.".to_string(),
        ))?;
    let (_, sub) = broker.create_non_reactive_subscription(&stream);
    tracing::debug!("Created the subscription, returning.");

    Ok((client_id, sub))
}
