use std::time::Duration;

use crate::{
    configuration::upload_configuration,
    error::HigginsClientError,
    functions::upload_module,
    ping::{ping, ping_sync},
    produce::produce_sync,
    query::{query_by_timestamp, query_latest},
    subscription::{create_subscription, take},
};
use higgins_codec::{CreateConfigurationResponse, ProduceResponse, Record, TakeRecordsResponse};
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct Client(tokio::net::TcpStream, Duration);

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
        partition: &[u8],
        payload: &[u8],
    ) -> Result<ProduceResponse, HigginsClientError> {
        timeout!(
            produce_sync(stream.as_bytes(), partition, payload, &mut self.0),
            self.1
        )
        .await?
    }

    pub async fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<TakeRecordsResponse, HigginsClientError> {
        timeout!(take(sub_id, stream_name, n, &mut self.0), self.1).await?
    }

    pub async fn ping(&mut self) -> Result<(), HigginsClientError> {
        tracing::trace!("Able to Write...");

        let result = timeout!(ping_sync(&mut self.0), self.1).await?;

        tracing::trace!("Written..");

        result
    }

    pub async fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Result<Vec<Record>, HigginsClientError> {
        timeout!(
            query_by_timestamp(stream, partition, &mut self.0, timestamp),
            self.1
        )
        .await?
    }

    pub async fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &[u8],
    ) -> Result<Vec<Record>, HigginsClientError> {
        timeout!(query_latest(stream, partition, &mut self.0), self.1).await?
    }

    pub async fn create_subscription(
        &mut self,
        stream: &[u8],
    ) -> Result<Vec<u8>, HigginsClientError> {
        timeout!(create_subscription(stream, &mut self.0), self.1).await?
    }

    pub async fn upload_module(
        &mut self,
        name: &str,
        module: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(upload_module(name, module, &mut self.0), self.1).await?
    }

    pub async fn upload_configuration(
        &mut self,
        config: &[u8],
    ) -> Result<CreateConfigurationResponse, HigginsClientError> {
        timeout!(upload_configuration(config, &mut self.0), self.1).await?
    }
}
