use crate::{
    configuration::upload_configuration,
    error::HigginsClientError,
    functions::upload_module,
    ping::ping,
    produce::produce_sync,
    query::{query_by_timestamp, query_latest},
    subscription::{create_subscription, take},
};
use higgins_codec::{CreateConfigurationResponse, ProduceResponse, Record, TakeRecordsResponse};
use tokio::net::{TcpStream, ToSocketAddrs};
pub struct Client(tokio::net::TcpStream);

impl Client {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, HigginsClientError> {
        let stream = futures::executor::block_on(TcpStream::connect(addr))?;

        Ok(Self(stream))
    }

    pub fn produce(
        &mut self,
        stream: &str,
        partition: &[u8],
        payload: &[u8],
    ) -> Result<ProduceResponse, HigginsClientError> {
        futures::executor::block_on(produce_sync(
            stream.as_bytes(),
            partition,
            payload,
            &mut self.0,
        ))
    }

    pub fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<TakeRecordsResponse, HigginsClientError> {
        futures::executor::block_on(take(sub_id, stream_name, n, &mut self.0))
    }

    pub fn ping(&mut self) -> Result<(), HigginsClientError> {
        futures::executor::block_on(ping(&mut self.0))
    }

    pub fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Result<Vec<Record>, HigginsClientError> {
        futures::executor::block_on(query_by_timestamp(
            stream,
            partition,
            &mut self.0,
            timestamp,
        ))
    }

    pub fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &[u8],
    ) -> Result<Vec<Record>, HigginsClientError> {
        futures::executor::block_on(query_latest(stream, partition, &mut self.0))
    }

    pub fn create_subscription(&mut self, stream: &[u8]) -> Result<Vec<u8>, HigginsClientError> {
        futures::executor::block_on(create_subscription(stream, &mut self.0))
    }

    pub fn upload_module(&mut self, name: &str, module: &[u8]) -> Result<(), HigginsClientError> {
        futures::executor::block_on(upload_module(name, module, &mut self.0))
    }

    pub fn upload_configuration(
        &mut self,
        config: &[u8],
    ) -> Result<CreateConfigurationResponse, HigginsClientError> {
        futures::executor::block_on(upload_configuration(config, &mut self.0))
    }
}
