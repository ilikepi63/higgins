use crate::{
    configuration::{get_current_topography, upload_configuration},
    error::HigginsClientError,
    functions::upload_module,
    ping::ping,
    produce::produce,
    query::{query_by_timestamp, query_latest},
    subscription::{create_subscription, take},
};
use higgins_shared::PartitionName;
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct Client(pub(crate) tokio::net::TcpStream, pub(crate) Duration);

macro_rules! timeout {
    ($future: expr, $dur: expr) => {
        tokio::time::timeout($dur, $future)
    };
}

impl Client {
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        dur: Option<Duration>,
    ) -> Result<Self, HigginsClientError> {
        let duration = dur.unwrap_or(Duration::from_secs(3));

        let stream = timeout!(TcpStream::connect(addr), duration).await??;

        Ok(Self(stream, duration))
    }

    pub async fn produce(
        &mut self,
        stream: &str,
        partition: &PartitionName,
        payload: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(
            produce(stream.as_bytes(), partition, payload, &mut self.0),
            self.1
        )
        .await?;
        Ok(())
    }

    pub async fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<(), HigginsClientError> {
        timeout!(take(sub_id, stream_name, n, &mut self.0), self.1).await?
    }

    pub async fn ping(&mut self) -> Result<(), HigginsClientError> {
        timeout!(ping(&mut self.0), self.1).await?
    }

    pub async fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
        timestamp: u64,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            query_by_timestamp(stream, partition, &mut self.0, timestamp),
            self.1
        )
        .await?
    }

    pub async fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
    ) -> Result<(), HigginsClientError> {
        timeout!(query_latest(stream, partition, &mut self.0), self.1).await?
    }

    pub async fn create_subscription(&mut self, stream: &[u8]) -> Result<(), HigginsClientError> {
        timeout!(create_subscription(stream, &mut self.0), self.1).await?
    }

    pub async fn upload_module(
        &mut self,
        name: &str,
        module: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(upload_module(name, module, &mut self.0), self.1).await?
    }

    pub async fn upload_configuration(&mut self, config: &[u8]) -> Result<(), HigginsClientError> {
        timeout!(upload_configuration(config, &mut self.0), self.1).await?
    }

    pub async fn get_current_topography(&mut self) -> Result<(), HigginsClientError> {
        timeout!(get_current_topography(&mut self.0), self.1).await?
    }
}
