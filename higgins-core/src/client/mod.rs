use bytes::BytesMut;
use higgins_codec::Message;
use higgins_shared::{HigginsError, UniqueCollection};
use prost::Message as _;

#[derive(Debug, Clone)]
pub enum ClientRef {
    AsyncTcpSocket(tokio::sync::mpsc::Sender<BytesMut>),
    NoOp,
}

impl ClientRef {
    pub async fn send(&self, message: Message) -> Result<(), HigginsError> {
        let mut result = BytesMut::new();

        message.encode(&mut result)?; // TODO: Make this catchable from HigginsError.

        match self {
            ClientRef::AsyncTcpSocket(sender) => {
                sender.send(result).await.map_err(|err| {
                    HigginsError::Arbitrary(format!(
                        "Failed to write offsets to client: {:#?}",
                        err
                    ))
                })?;
            }
            ClientRef::NoOp => {}
        }

        Ok(())
    }
}

pub type ClientCollection = UniqueCollection<ClientRef>;
