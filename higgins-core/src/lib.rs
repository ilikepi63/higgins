use std::{io::Cursor, sync::Arc};

use arrow_json::ReaderBuilder;
use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, CreateSubscriptionRequest,
    CreateSubscriptionResponse, Error, Message, Pong, ProduceRequest, ProduceResponse,
    TakeRecordsRequest, message::Type,
};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use utils::consumption::collect_consume_responses;

use crate::{broker::Broker, client::ClientRef};
pub mod broker;
pub mod client;
pub mod storage;
pub mod subscription;
pub mod topography;
pub mod utils;

mod error;

async fn process_socket(tcp_socket: TcpStream, broker: Arc<RwLock<Broker>>) {
    let (mut read_socket, mut write_socket) = tcp_socket.into_split();

    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(100);

    let client_id = {
        let mut broker_lock = broker.write().await;

        let client_id = broker_lock
            .clients
            .insert(ClientRef::AsyncTcpSocket(writer_tx.clone()));

        client_id
    };

    let _read_handle = tokio::spawn(async move {
        loop {
            let mut buffer = vec![0; 1024]; // TODO: consider how we are goingg to size this buffer.

            match read_socket.read(&mut buffer).await {
                Ok(n) => {
                    if n > 0 {
                        tracing::trace!(
                            "Received {n} bytes from client {}",
                            read_socket.peer_addr().unwrap()
                        );
                    } else {
                        break;
                    }

                    let slice = &buffer[0..n];

                    let message = Message::decode(slice).unwrap();

                    tracing::info!("Received a message, responding.");

                    match Type::try_from(message.r#type).unwrap() {
                        Type::Ping => {
                            tracing::trace!("Received Ping, sending Pong.");

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

                            writer_tx.send(result).await;
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

                            writer_tx.send(result).await.unwrap();
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

                            if let Err(err) =
                                broker.create_partition(&stream_name, &partition_key).await
                            {
                                tracing::error!(
                                    "Failed to create partition inside of broker: {:#?}",
                                    err
                                );
                            };

                            let (schema, _tx, _rx) = broker
                                .get_stream(&stream_name)
                                .expect("Could not find stream for stream_name.");

                            let cursor = Cursor::new(payload);
                            let mut reader =
                                ReaderBuilder::new(schema.clone()).build(cursor).unwrap();
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

                            writer_tx.send(result).await;
                        }
                        Type::Produceresponse => {}
                        Type::Metadatarequest => todo!(),
                        Type::Metadataresponse => todo!(),
                        Type::Pong => todo!(),
                        Type::Takerecordsrequest => {
                            let broker_ref = broker.clone();

                            let TakeRecordsRequest {
                                n,
                                stream_name,
                                subscription_id,
                            } = message.take_records_request.unwrap();

                            let mut broker = broker.write().await;

                            let _ = broker
                                .take_from_subscription(
                                    client_id,
                                    &stream_name,
                                    &subscription_id,
                                    writer_tx.clone(),
                                    broker_ref,
                                    n,
                                )
                                .await
                                .unwrap();
                        }
                        Type::Takerecordsresponse => {
                            // we don't handle this.
                        }
                        Type::Createconfigurationrequest => {
                            let mut broker = broker.write().await;

                            tracing::info!("Applying configuration..");

                            if let Some(CreateConfigurationRequest { data }) =
                                message.create_configuration_request
                            {
                                let result = broker.apply_configuration(&data);

                                if let Err(err) = result {
                                    let create_configuration_response =
                                        CreateConfigurationResponse {
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

                                    let result = writer_tx.send(result).await;
                                } else {
                                    let create_configuration_response =
                                        CreateConfigurationResponse { errors: vec![] };

                                    tracing::info!(
                                        "Responding with: {:#?}",
                                        create_configuration_response
                                    );

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

                                    let result = writer_tx.send(result).await;
                                    tracing::info!("Result from writing: {:#?}", result);
                                }
                            } else {
                                let create_configuration_response = CreateConfigurationResponse {
                                errors: vec!["Malformed request for creating configuration. Please include CreateConfigurationRequest in body.".into()]
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

                                writer_tx.send(result).await;
                            }
                        }
                        Type::Createconfigurationresponse => todo!(),
                        Type::Deleteconfigurationrequest => todo!(),
                        Type::Deleteconfigurationresponse => todo!(),
                        Type::Error => {}
                        Type::Getindexrequest => {
                            let broker = broker.read().await;

                            let request = message.get_index_request.unwrap(); // TODO: error response here.

                            for index in request.indexes {
                                // We can potentially query in three different ways using this request, so
                                // this match arm reflects that.
                                match index.r#type() {
                                    higgins_codec::index::Type::Timestamp => {
                                        let values = broker
                                            .get_by_timestamp(
                                                &index.stream,
                                                &index.partition,
                                                index.timestamp(),
                                            )
                                            .await
                                            .unwrap();

                                        let responses = collect_consume_responses(values).await;

                                        for response in responses {
                                            let mut result = BytesMut::new();

                                            Message {
                                                r#type: Type::Getindexresponse as i32,
                                                get_index_response: Some(response),
                                                ..Default::default()
                                            }
                                            .encode(&mut result)
                                            .unwrap();

                                            writer_tx.send(result).await;
                                        }
                                    }
                                    higgins_codec::index::Type::Latest => {
                                        let values = broker
                                            .get_latest(&index.stream, &index.partition)
                                            .await
                                            .unwrap();

                                        let responses = collect_consume_responses(values).await;

                                        for response in responses {
                                            let mut result = BytesMut::new();

                                            Message {
                                                r#type: Type::Getindexresponse as i32,
                                                get_index_response: Some(response),
                                                ..Default::default()
                                            }
                                            .encode(&mut result)
                                            .unwrap();

                                            writer_tx.send(result).await;
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

                                        writer_tx.send(result).await;
                                    }
                                }
                            }
                        }
                        Type::Getindexresponse => {}
                    }
                }
                Err(err) => {
                    tracing::trace!("Received error when trying to process socket: {:#?}", err);
                }
            }
        }
    });

    let _write_handle = tokio::spawn(async move {
        tracing::info!("Starting writing task..");

        while let Some(val) = writer_rx.recv().await {
            tracing::info!("Received: {:#?} on the writing side", val);

            let result = write_socket.write_all(&val).await;
            write_socket.flush().await;
        }
    });
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
