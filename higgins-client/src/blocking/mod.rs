use std::time::Duration;

use crate::error::HigginsClientError;
use higgins_codec::{CreateConfigurationResponse, ProduceResponse, Record, TakeRecordsResponse};
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
        partition: &[u8],
        payload: &[u8],
    ) -> Result<ProduceResponse, HigginsClientError> {
        self.1.block_on(self.0.produce(stream, partition, payload))
    }

    pub fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<TakeRecordsResponse, HigginsClientError> {
        self.1.block_on(self.0.take(sub_id, stream_name, n))
    }

    pub fn ping(&mut self) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.ping())
    }

    pub fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &[u8],
        timestamp: u64,
    ) -> Result<Vec<Record>, HigginsClientError> {
        self.1
            .block_on(self.0.query_by_timestamp(stream, partition, timestamp))
    }

    pub fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &[u8],
    ) -> Result<Vec<Record>, HigginsClientError> {
        self.1.block_on(self.0.query_latest(stream, partition))
    }

    pub fn create_subscription(&mut self, stream: &[u8]) -> Result<Vec<u8>, HigginsClientError> {
        self.1.block_on(self.0.create_subscription(stream))
    }

    pub fn upload_module(&mut self, name: &str, module: &[u8]) -> Result<(), HigginsClientError> {
        self.1.block_on(self.0.upload_module(name, module))
    }

    pub fn upload_configuration(
        &mut self,
        config: &[u8],
    ) -> Result<CreateConfigurationResponse, HigginsClientError> {
        self.1.block_on(self.0.upload_configuration(config))
    }
}
