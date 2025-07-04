use std::{io::Cursor, sync::Arc};

use arrow_json::ReaderBuilder;
use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, CreateSubscriptionRequest,
    CreateSubscriptionResponse, Message, Pong, ProduceRequest, ProduceResponse, Record,
    TakeRecordsRequest, TakeRecordsResponse, message::Type,
};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::{broker::Broker, storage::arrow_ipc::read_arrow};
pub mod broker;
pub mod storage;
pub mod subscription;
pub mod topography;
pub mod utils;

mod error;

async fn process_socket(mut socket: TcpStream, broker: Arc<RwLock<Broker>>) {
    loop {
        let mut buffer = vec![0; 1024];

        match socket.read(&mut buffer).await {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(
                        "Received {n} bytes from client {}",
                        socket.peer_addr().unwrap()
                    );
                } else {
                    break;
                }

                let slice = &buffer[0..n];

                let message = Message::decode(slice).unwrap();

                match Type::try_from(message.r#type).unwrap() {
                    Type::Ping => {
                        tracing::info!("Received Ping, sending Pong.");

                        let mut result = BytesMut::new();

                        let pong = Pong::default();

                        Message {
                            r#type: Type::Pong as i32,
                            pong: Some(pong),
                            ..Default::default()
                        }
                        .encode(&mut result)
                        .unwrap();

                        tracing::info!("Responding with: {:#?}", result.clone().to_vec());

                        socket.write_all(&result).await.unwrap();
                        socket.flush().await.unwrap();
                    }
                    Type::Createsubscriptionrequest => {
                        tracing::trace!(
                            "Received CreateSubscriptionRequest: {:#?}",
                            message.create_subscription_request
                        );

                        let CreateSubscriptionRequest {
                            stream_name,..
                            // offset_type,
                            // timestamp,
                            // offset,
                        } = message.create_subscription_request.unwrap();

                        let mut broker = broker.write().await;

                        let subscription_id = broker.create_subscription(&stream_name);

                        let resp = CreateSubscriptionResponse {
                            errors: vec![],
                            subscription_id: Some(subscription_id),
                        };

                        let mut result = BytesMut::new();

                        Message {
                            r#type: Type::Createsubscriptionresponse as i32,
                            create_subscription_response: Some(resp),
                            ..Default::default()
                        }
                        .encode(&mut result)
                        .unwrap();

                        socket.write_all(&result).await.unwrap();
                    }
                    Type::Createsubscriptionresponse => {
                        // We don't handle this.
                    }
                    Type::Producerequest => {
                        let ProduceRequest {
                            stream_name,
                            partition_key,
                            payload,
                        } = message.produce_request.unwrap();

                        let mut broker = broker.write().await;

                        if let Err(err) = broker.create_partition(&stream_name, &partition_key) {
                            tracing::error!(
                                "Failed to create partition inside of broker: {:#?}",
                                err
                            );
                        };

                        let (schema, _tx, _rx) = broker
                            .get_stream(&stream_name)
                            .expect("Could not find stream for stream_name.");

                        let cursor = Cursor::new(payload);
                        let mut reader = ReaderBuilder::new(schema.clone()).build(cursor).unwrap();
                        let batch = reader.next().unwrap().unwrap();

                        let _ = broker.produce(&stream_name, &partition_key, batch).await;

                        drop(broker);

                        let mut result = BytesMut::new();

                        let resp = ProduceResponse::default();

                        Message {
                            r#type: Type::Produceresponse as i32,
                            produce_response: Some(resp),
                            ..Default::default()
                        }
                        .encode(&mut result)
                        .unwrap();

                        socket.write_all(&result).await.unwrap();
                    }
                    Type::Produceresponse => {}
                    Type::Metadatarequest => todo!(),
                    Type::Metadataresponse => todo!(),
                    Type::Pong => todo!(),
                    Type::Takerecordsrequest => {
                        let mut result = BytesMut::new();

                        let TakeRecordsRequest {
                            n,
                            stream_name,
                            subscription_id,
                        } = message.take_records_request.unwrap();

                        let mut broker = broker.write().await;

                        let offsets = broker
                            .take_from_subscription(&stream_name, &subscription_id, n)
                            .unwrap();

                        tracing::info!("We've received offsets: {:#?}", offsets);

                        for (partition, offset) in offsets {
                            let mut consumption = broker
                                .consume(&stream_name, &partition, offset, 50_000)
                                .await;

                            while let Some(val) = consumption.recv().await {
                                let resp = TakeRecordsResponse {
                                    records: val
                                        .batches
                                        .iter()
                                        .map(|batch| {
                                            let stream_reader = read_arrow(&batch.data);

                                            let batches = stream_reader
                                                .filter_map(|val| val.ok())
                                                .collect::<Vec<_>>();

                                            let batch_refs = batches.iter().collect::<Vec<_>>();

                                            // Infer the batches
                                            let buf = Vec::new();
                                            let mut writer =
                                                arrow_json::LineDelimitedWriter::new(buf);
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

                                Message {
                                    r#type: Type::Takerecordsresponse as i32,
                                    take_records_response: Some(resp),
                                    ..Default::default()
                                }
                                .encode(&mut result)
                                .unwrap();

                                socket.write_all(&result).await.unwrap();
                            }
                        }
                    }
                    Type::Takerecordsresponse => {
                        // we don't handle this.
                    }
                    Type::Createconfigurationrequest => {
                        let mut broker = broker.write().await;

                        if let Some(CreateConfigurationRequest { data }) =
                            message.create_configuration_request
                        {
                            let result = broker.apply_configuration(&data);

                            if let Err(err) = result {
                                let create_configuration_response = CreateConfigurationResponse {
                                    errors: vec![err.to_string()],
                                };

                                let mut result = BytesMut::new();

                                Message {
                                    r#type: Type::Createconfigurationresponse as i32,
                                    create_configuration_response: Some(
                                        create_configuration_response,
                                    ),
                                    ..Default::default()
                                }
                                .encode(&mut result)
                                .unwrap();

                                tracing::info!("Responding with: {:#?}", result.clone().to_vec());

                                socket.write_all(&result).await.unwrap();
                                socket.flush().await.unwrap();
                            } else {
                                let create_configuration_response =
                                    CreateConfigurationResponse { errors: vec![] };

                                let mut result = BytesMut::new();

                                Message {
                                    r#type: Type::Createconfigurationresponse as i32,
                                    create_configuration_response: Some(
                                        create_configuration_response,
                                    ),
                                    ..Default::default()
                                }
                                .encode(&mut result)
                                .unwrap();

                                tracing::info!("Responding with: {:#?}", result.clone().to_vec());

                                socket.write_all(&result).await.unwrap();
                                socket.flush().await.unwrap();
                            }
                        } else {
                            let create_configuration_response = CreateConfigurationResponse {
                                errors: vec!["Malformed request for creating configuration. Please include CreateConfigurationRequest in body.".into()]
                            };

                            let mut result = BytesMut::new();

                            Message {
                                r#type: Type::Createconfigurationresponse as i32,
                                create_configuration_response: Some(create_configuration_response),
                                ..Default::default()
                            }
                            .encode(&mut result)
                            .unwrap();

                            tracing::info!("Responding with: {:#?}", result.clone().to_vec());

                            socket.write_all(&result).await.unwrap();
                            socket.flush().await.unwrap();
                        }
                    }
                    Type::Createconfigurationresponse => todo!(),
                    Type::Deleteconfigurationrequest => todo!(),
                    Type::Deleteconfigurationresponse => todo!(),
                }
            }
            Err(err) => {
                tracing::trace!("Received error when trying to process socket: {:#?}", err);
            }
        }
    }
}

pub async fn run_server(port: u16) {
    let broker = Arc::new(RwLock::new(Broker::new()));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    tracing::info!("Connected on {}", port);

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tracing::info!("Received connection from: {addr}");
        process_socket(socket, broker.clone()).await;
    }
}
