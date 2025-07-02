use std::{
    io::{Cursor, Read},
    sync::Arc,
};

use arrow_json::ReaderBuilder;
use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, Message, Pong, ProduceRequest,
    ProduceResponse, message::Type,
};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::broker::Broker;
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



                    }
                    Type::Createsubscriptionresponse => {
                        // We don't handle this.
                    }
                    Type::Producerequest => {
                        let ProduceRequest {
                            topic,
                            partition_key,
                            payload,
                        } = message.produce_request.unwrap();

                        let mut broker = broker.write().await;

                        let (schema, _tx, _rx) = broker
                            .get_stream(topic.as_bytes())
                            .expect("Could not find stream for stream_name.");

                        let cursor = Cursor::new(payload);
                        let mut reader = ReaderBuilder::new(schema.clone()).build(cursor).unwrap();
                        let batch = reader.next().unwrap().unwrap();

                        println!("You are producing! {:#?}", batch);

                        let _ = broker
                            .produce(topic.as_bytes(), &partition_key, batch)
                            .await;

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
                    Type::Takerecordsrequest => {}
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
                                let create_configuration_response = CreateConfigurationResponse {
                                    errors: vec![],
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
