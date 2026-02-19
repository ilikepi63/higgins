use std::sync::Arc;

use crate::storage::arrow_ipc::read_arrow;
use bytes::BytesMut;
use higgins_codec::{Error, GetIndexResponse, Message, Record, message::Type};
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

                            let batch_refs = batches.iter().collect::<Vec<_>>();

                            // Infer the batches
                            let buf = Vec::new();
                            let mut writer = arrow_json::LineDelimitedWriter::new(buf);
                            writer.write_batches(&batch_refs).unwrap();
                            writer.finish().unwrap();

                            // Get the underlying buffer back,
                            let buf = writer.into_inner();

                            Record {
                                data: buf,
                                stream: batch.topic.as_bytes().to_vec(),
                                offset: batch.offset,
                                partition: batch.partition.clone(),
                            }
                        })
                        .collect::<Vec<_>>(),
                };

                let mut result = BytesMut::new();

                Message {
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

                let responses = broker_lock.get_latest(&index.stream, partition).await;

                for response in responses {
                    let response = response.await.unwrap();

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
                        r#type: Type::Getindexresponse as i32,
                        get_index_response: Some(index_response),
                        ..Default::default()
                    }
                    .encode(&mut result)
                    .unwrap();

                    writer_tx.send(result).await.unwrap();
                }
            }
            higgins_codec::index::Type::Offset => {
                let mut result = BytesMut::new();

                let mut error = Error::default();

                error.set_type(higgins_codec::error::Type::Unimplemented);

                Message {
                    r#type: Type::Error as i32,
                    error: Some(error),
                    ..Default::default()
                }
                .encode(&mut result)
                .unwrap();

                writer_tx.send(result).await.unwrap();
            }
        }
    }
}
