use std::time::Duration;

use crate::{error::HigginsClientError, recv::Response};
use higgins_codec::{CreateConfigurationResponse, ProduceResponse, Record};
use higgins_shared::PartitionName;
use tokio::net::ToSocketAddrs;
pub struct Client(crate::Client, tokio::runtime::Runtime);

impl Client {
    pub fn connect<A: ToSocketAddrs>(
        addr: A,
        duration: Option<Duration>,
    ) -> Result<Self, HigginsClientError> {
        let rt = tokio::runtime::Runtime::new()?;

        let client = rt.block_on(crate::Client::connect(addr, duration))?;

        Ok(Self(client, rt))
    }

    pub fn produce(
        &mut self,
        stream: &str,
        partition: &PartitionName,
        payload: &[u8],
    ) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.produce(stream, partition, payload))
    }

    pub fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.take(sub_id, stream_name, n))
    }

    pub fn ping(&mut self) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.ping())
    }

    pub fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
        timestamp: u64,
    ) -> Result<(), HigginsClientError> {
        self.1
            .block_on(self.0.query_by_timestamp(stream, partition, timestamp))
    }

    pub fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
    ) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.query_latest(stream, partition))
    }

    pub fn create_subscription(&mut self, stream: &[u8]) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.create_subscription(stream))
    }

    pub fn upload_module(&mut self, name: &str, module: &[u8]) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.upload_module(name, module))
    }

    pub fn upload_configuration(&mut self, config: &[u8]) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.upload_configuration(config))
    }

    pub fn recv(&mut self) -> Result<Response, HigginsClientError> {
        self.1.block_on(self.0.recv())
    }
}
