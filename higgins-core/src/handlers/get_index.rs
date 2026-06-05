use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{GetIndexResponse, Message, Record, message::Type};
use higgins_shared::read_arrow;
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use higgins_shared::PartitionName;
use tokio::sync::mpsc::Sender;

pub async fn handle_get_index(
    message: Message,
    broker: Arc<RwLock<Broker>>,
    writer_tx: Sender<BytesMut>,
) {
    tracing::trace!("Trying to retrieve the broker lock..");

    let mut broker_lock = broker.write().await;

    tracing::trace!("Retrieved the GetIndexRequest");

    let request = message.get_index_request.unwrap(); // TODO: error response here.
    tracing::trace!("Retrieved the GetIndexRequest: {:#?}", request);

    for index in request.indexes {
        // We can potentially query in three different ways using this request, so
        // this match arm reflects that.
        match index.r#type() {
            higgins_codec::index::Type::Timestamp => {
                let values = broker_lock
                    .get_by_timestamp(
                        &index.stream,
                        &PartitionName::try_from(&index.partition[..]).unwrap(),
                        index.timestamp(),
                    )
                    .await
                    .unwrap();

                let response = GetIndexResponse {
                    records: values
                        .batches
                        .iter()
                        .map(|batch| {
                            let stream_reader = read_arrow(&batch.data);

                            let batches =
                                stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                            let batch_refs = batches.first().unwrap();

                            let data = higgins_shared::write_arrow(batch_refs);

                            Record {
                                data,
                                stream: batch.topic.as_bytes().to_vec(),
                                offset: batch.offset,
                                partition: batch.partition.clone(),
                            }
                        })
                        .collect::<Vec<_>>(),
                };

                let mut result = BytesMut::new();

                Message {
                    correlation_id: message.correlation_id,
                    r#type: Type::Getindexresponse as i32,
                    get_index_response: Some(response),
                    ..Default::default()
                }
                .encode(&mut result)
                .unwrap();

                writer_tx.send(result).await.unwrap();
                // }
            }
            higgins_codec::index::Type::Latest => {
                tracing::trace!("Retrieved a Latest GetIndexRequest",);

                let partition = &PartitionName::try_from(&index.partition[..]).unwrap();

                let response = broker_lock.get_latest(&index.stream, partition).await;

                let response = response.unwrap().await.unwrap();

                tracing::trace!("Response for GetIndexRequest: {:#?}", response);

                let index_response = GetIndexResponse {
                    records: vec![Record {
                        data: response,
                        stream: vec![],
                        partition: vec![],
                        offset: 0,
                    }],
                };

                let mut result = BytesMut::new();

                Message {
                    correlation_id: message.correlation_id,
                    r#type: Type::Getindexresponse as i32,
                    get_index_response: Some(index_response),
                    ..Default::default()
                }
                .encode(&mut result)
                .unwrap();

                writer_tx.send(result).await.unwrap();
            }
            higgins_codec::index::Type::Offset => {
                tracing::trace!("Retrieved a At Offset GetIndexRequest",);

                let offset = index.index.unwrap();

                let partition = &PartitionName::try_from(&index.partition[..]).unwrap();

                let response = broker_lock
                    .get_at(&index.stream, partition, offset)
                    .await
                    .ok()
                    .flatten()
                    .unwrap();

                let index_response = GetIndexResponse {
                    records: vec![Record {
                        data: response,
                        stream: vec![],
                        partition: vec![],
                        offset: 0,
                    }],
                };

                let mut result = BytesMut::new();

                Message {
                    correlation_id: message.correlation_id,
                    r#type: Type::Getindexresponse as i32,
                    get_index_response: Some(index_response),
                    ..Default::default()
                }
                .encode(&mut result)
                .unwrap();

                writer_tx.send(result).await.unwrap();
            }
        }
    }
}
