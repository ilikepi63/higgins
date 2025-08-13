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
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, HigginsClientError> {
        let stream = TcpStream::connect(addr).await?;

        Ok(Self(stream))
    }

    pub async fn produce(
        &mut self,
        stream: &str,
        partition: &[u8],
        payload: &[u8],
    ) -> Result<ProduceResponse, HigginsClientError> {
        produce_sync(stream.as_bytes(), partition, payload, &mut self.0).await
    }

    pub async fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<TakeRecordsResponse, HigginsClientError> {
        take(sub_id, stream_name, n, &mut self.0).await
    }

    pub async fn ping(&mut self) -> Result<(), HigginsClientError> {
        ping(&mut self.0).await
    }

    pub async fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Result<Vec<Record>, HigginsClientError> {
        query_by_timestamp(stream, partition, &mut self.0, timestamp).await
    }

    pub async fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &[u8],
    ) -> Result<Vec<Record>, HigginsClientError> {
        query_latest(stream, partition, &mut self.0).await
    }

    pub async fn create_subscription(
        &mut self,
        stream: &[u8],
    ) -> Result<Vec<u8>, HigginsClientError> {
        create_subscription(stream, &mut self.0).await
    }

    pub async fn upload_module(
        &mut self,
        name: &str,
        module: &[u8],
    ) -> Result<(), HigginsClientError> {
        upload_module(name, module, &mut self.0).await
    }

    pub async fn upload_configuration(
        &mut self,
        config: &[u8],
    ) -> Result<CreateConfigurationResponse, HigginsClientError> {
        upload_configuration(config, &mut self.0).await
    }
}
