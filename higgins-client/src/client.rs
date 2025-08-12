use crate::{
    produce::produce_sync,
    error::HigginsClientError
};
use higgins_codec::ProduceResponse;

pub struct Client (tokio::net::TcpStream);

impl Client {
    pub async fn produce(&mut self, stream: &str, partition: &[u8], payload: &[u8]) -> Result<ProduceResponse, HigginsClientError> {
        produce_sync(stream.as_bytes(), partition, payload, &mut self.0).await
    }

    pub async fn consume(&self) {}

    pub async fn ping(&self) {}

    pub async fn query_by_timestamp(&self) {}

    pub async fn query_latest(&self) {}

    pub async fn create_subscription(&self) {}

    pub async fn upload_module(&self) {}

    pub async fn upload_configuration(&self) {}

}
