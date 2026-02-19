use std::collections::BTreeMap;

use bytes::BytesMut;
use higgins_codec::Message;
use prost::Message as _;

use crate::error::HigginsError;

#[derive(Debug, Clone)]
pub enum ClientRef {
    AsyncTcpSocket(tokio::sync::mpsc::Sender<BytesMut>),
    NoOp,
}

impl ClientRef {
    pub async fn send(&self, message: Message) -> Result<(), HigginsError> {
        let mut result = BytesMut::new();

        message.encode(&mut result).unwrap(); // TODO: Make this catchable from HigginsError.

        match self {
            ClientRef::AsyncTcpSocket(sender) => {
                sender.send(result).await.unwrap();
            }
            ClientRef::NoOp => {}
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ClientCollection(BTreeMap<u64, ClientRef>);

impl ClientCollection {
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    fn get_smallest_unused(&self) -> Option<u64> {
        let mut expected = 0;

        for (&id, _) in &self.0 {
            if id > expected {
                return Some(expected);
            }
            expected = id + 1;
        }

        Some(expected)
    }

    pub fn insert(&mut self, client: ClientRef) -> Result<u64, HigginsError> {
        let id = self
            .get_smallest_unused()
            .ok_or(HigginsError::TooManyClientsConnnectedToBroker)?;

        self.0.insert(id, client);

        Ok(id)
    }

    pub fn remove(&mut self, id: u64) {
        self.0.remove(&id);
    }

    pub fn get(&self, client_id: u64) -> Option<&ClientRef> {
        self.0.get(&client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio::sync::mpsc;

    fn dummy_client() -> ClientRef {
        let (tx, _) = mpsc::channel::<BytesMut>(1);
        ClientRef::AsyncTcpSocket(tx)
    }

    #[test]
    fn starts_from_zero() {
        let mut c = ClientCollection::empty();
        assert_eq!(c.insert(dummy_client()).unwrap(), 0);
        assert_eq!(c.insert(dummy_client()).unwrap(), 1);
        assert!(c.get(0).is_some());
        assert!(c.get(1).is_some());
    }

    #[test]
    fn reuses_smallest_available_id() {
        let mut c = ClientCollection::empty();
        c.insert(dummy_client()).unwrap();
        c.insert(dummy_client()).unwrap();
        c.insert(dummy_client()).unwrap();

        c.remove(1);
        assert_eq!(c.insert(dummy_client()).unwrap(), 1);

        c.remove(0);
        c.remove(2);
        assert_eq!(c.insert(dummy_client()).unwrap(), 0);
    }

    #[test]
    fn len_and_is_empty() {
        let mut c = ClientCollection::empty();
        assert_eq!(c.0.len(), 0);

        let id = c.insert(dummy_client()).unwrap();
        assert_eq!(c.0.len(), 1);
        assert_eq!(id, 0);

        c.remove(id);
        assert_eq!(c.0.len(), 0);
    }

    #[test]
    fn get_returns_none_for_missing() {
        let mut c = ClientCollection::empty();
        c.insert(dummy_client()).unwrap();
        assert!(c.get(0).is_some());
        assert!(c.get(42).is_none());
    }
}
