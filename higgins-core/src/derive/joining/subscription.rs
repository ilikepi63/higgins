use crate::task::SpawnTaskConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::join::JoinWithStream;
use super::opts::eager_take_from_subscription_or_wait;
use crate::broker::Broker;
use crate::error::HigginsError;
use higgins_shared::PartitionName;

pub fn start_join_subscription_task(
    broker: &mut Broker,
    task_broker: Arc<RwLock<Broker>>,
    join_stream: JoinWithStream,
    derivative_channel_tx: tokio::sync::mpsc::Sender<(usize, Vec<(PartitionName, u64)>)>,
    i: usize,
) {
    let _handle = broker.task_handler.spawn(
        &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
        async move {
            // Create a subscription on each derivative
            let (client_id, condvar, subscription) = {
                let mut broker = task_broker.write().await;
                let client_id = broker.clients.insert(super::ClientRef::NoOp).unwrap();
                let left_subscription = broker.create_subscription(join_stream.stream.0.as_bytes());
                let stream = join_stream.stream.clone();
                let (left_notify, left_subscription) = broker
                    .get_subscription_by_key(stream.0.as_bytes(), &left_subscription)
                    .ok_or(HigginsError::SubscriptionRetrievalFailed)
                    .unwrap();

                tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");
                drop(broker); // Explicitly drop the lock.

                (client_id, left_notify, left_subscription)
            };

            loop {
                let offsets = eager_take_from_subscription_or_wait(
                    subscription.clone(),
                    condvar.clone(),
                    client_id,
                )
                .await
                .unwrap();

                tracing::trace!("Retrieved offsets {:#?} from {client_id}.", offsets);

                derivative_channel_tx
                    .send((i, offsets))
                    .await
                    .inspect_err(|err| {
                        tracing::error!(
                            "Error attempting to send to derivative channel: {:#?}",
                            err
                        );
                    })
                    .unwrap();
            }
        },
    );
}
