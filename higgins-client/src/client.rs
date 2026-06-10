use crate::{
    configuration::{get_current_topography, upload_configuration},
    error::HigginsClientError,
    functions::upload_module,
    ping::ping,
    produce::produce,
    query::{query_at, query_by_timestamp, query_latest},
    subscription::{acknowledge, create_subscription, get_subscription, take},
};
use arrow_schema::SchemaRef;
use higgins_shared::{PartitionName, UniqueCollection};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct Client(
    pub(crate) tokio::net::TcpStream,
    pub(crate) Duration,
    pub(crate) UniqueCollection<u64>,
);

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

        Ok(Self(stream, duration, UniqueCollection::empty()))
    }

    pub async fn produce(
        &mut self,
        stream: &str,
        payload: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(
            produce(
                stream.as_bytes(),
                payload,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await??;
        Ok(())
    }

    pub async fn produce_json(
        &mut self,
        stream: &str,
        payload: &[u8],
        schema: SchemaRef,
    ) -> Result<(), HigginsClientError> {
        let cursor = std::io::Cursor::new(payload);

        let mut reader = arrow_json::ReaderBuilder::new(schema.clone())
            .build(cursor)
            .map_err(|err| {
                tracing::error!("Error occurred attempting to produce json: {:#?}", err);
                HigginsClientError::MissingPayload
            })?;

        let batch = reader
            .next()
            .ok_or(HigginsClientError::MissingPayload)?
            .map_err(|err| {
                tracing::error!("Produce failed due to {:#?}", err);
                HigginsClientError::MissingPayload
            })?;

        let payload = higgins_shared::write_arrow(&batch)?;

        timeout!(
            produce(
                stream.as_bytes(),
                &payload,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await??;

        Ok(())
    }

    pub async fn take(
        &mut self,
        sub_id: Vec<u8>,
        stream_name: &[u8],
        n: u64,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            take(
                sub_id,
                stream_name,
                n,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn ping(&mut self) -> Result<(), HigginsClientError> {
        timeout!(
            ping(
                &mut self.0,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
            ),
            self.1
        )
        .await?
    }

    pub async fn query_by_timestamp(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
        timestamp: u64,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            query_by_timestamp(
                stream,
                partition,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0,
                timestamp
            ),
            self.1
        )
        .await?
    }

    pub async fn query_latest(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            query_latest(
                stream,
                partition,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn query_at(
        &mut self,
        stream: &[u8],
        partition: &PartitionName,
        index: u64,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            query_at(
                stream,
                partition,
                index,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn create_subscription(&mut self, stream: &[u8]) -> Result<(), HigginsClientError> {
        timeout!(
            create_subscription(
                stream,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn upload_module(
        &mut self,
        name: &str,
        module: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(
            upload_module(
                name,
                module,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn upload_configuration(&mut self, config: &[u8]) -> Result<(), HigginsClientError> {
        timeout!(
            upload_configuration(
                config,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn get_current_topography(&mut self) -> Result<(), HigginsClientError> {
        timeout!(
            get_current_topography(
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }
    pub async fn get_subscription(
        &mut self,
        stream: &str,
        subscription_id: &[u8],
    ) -> Result<(), HigginsClientError> {
        timeout!(
            get_subscription(
                subscription_id,
                stream,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }

    pub async fn acknowledge(
        &mut self,
        stream: &str,
        subscription_id: &[u8],
        offsets: Vec<(PartitionName, std::ops::Range<u64>)>,
    ) -> Result<(), HigginsClientError> {
        timeout!(
            acknowledge(
                subscription_id,
                stream,
                offsets,
                self.2
                    .insert(0)
                    .ok_or(HigginsClientError::TooManyConcurrentRequests)?,
                &mut self.0
            ),
            self.1
        )
        .await?
    }
}
