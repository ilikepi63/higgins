pub fn create_derived_stream_subscription() {
    let (client_id, condvar, subscription) = {
        let mut broker = broker_ref.write().await;
        let client_id = broker.clients.insert(ClientRef::NoOp).unwrap();
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
}
