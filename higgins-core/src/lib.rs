use std::{path::PathBuf, sync::Arc};

use higgins_codec::{Message, frame::Frame, message::Type};
use prost::Message as _;
use task::SpawnTaskConfig;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::{broker::Broker, client::ClientRef};
pub mod broker;
pub mod client;
mod derive;
mod error;
pub mod functions;
pub mod storage;
pub mod subscription;
pub mod task;
pub mod topography;
pub mod utils;

mod handlers;

async fn process_socket(tcp_socket: TcpStream, broker: Arc<RwLock<Broker>>) {
    let (mut read_socket, mut write_socket) = tcp_socket.into_split();

    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(100);

    let spawning_broker = broker.clone();

    let mut broker_lock = spawning_broker.write().await;

    let client_id = broker_lock
        .clients
        .insert(ClientRef::AsyncTcpSocket(writer_tx.clone()));

    let _read_handle = broker_lock.task_handler.spawn(
        &SpawnTaskConfig::new(
            "tcp_read", true, // Needs to be unique as this is a socket per client.
        ),
        async move {
            loop {
                let frame = match Frame::try_read_async(&mut read_socket).await {
                    Ok(frame) => frame,
                    Err(_) => {
                        // Usually means that EOF was received on the socket, terminating this.
                        break;
                    }
                };

                let message = Message::decode(&mut frame.inner()).unwrap();

                tracing::info!("Received a message {:#?}, responding.", message);

                let t = Type::try_from(message.r#type);

                tracing::info!("Request Type: {:#?}", t);

                match Type::try_from(message.r#type).unwrap() {
                    Type::Ping => {
                        handlers::handle_ping(writer_tx.clone()).await;
                    }
                    Type::Createsubscriptionrequest => {
                        handlers::handle_create_subscription(
                            message,
                            broker.clone(),
                            writer_tx.clone(),
                        )
                        .await;
                    }
                    Type::Producerequest => {
                        handlers::handle_produce(message, broker.clone(), writer_tx.clone()).await;
                    }
                    Type::Createconfigurationrequest => {
                        handlers::handle_create_configuration(
                            broker.clone(),
                            message,
                            writer_tx.clone(),
                        )
                        .await;
                    }
                    Type::Takerecordsrequest => {
                        handlers::handle_take_records(
                            broker.clone(),
                            message,
                            client_id,
                            writer_tx.clone(),
                        )
                        .await;
                    }
                    Type::Getindexrequest => {
                        handlers::handle_get_index(message, broker.clone(), writer_tx.clone())
                            .await;
                    }
                    Type::Uploadmodulerequest => {
                        handlers::handle_upload_module(message, broker.clone(), writer_tx.clone())
                            .await;
                    }
                    Type::Metadatarequest => todo!(),
                    Type::Getcurrenttopographyrequest => {
                        handlers::handle_get_topography(broker.clone(), writer_tx.clone()).await;
                    }
                    Type::Produceresponse
                    | Type::Metadataresponse
                    | Type::Pong
                    | Type::Takerecordsresponse
                    | Type::Createconfigurationresponse
                    | Type::Deleteconfigurationrequest
                    | Type::Deleteconfigurationresponse
                    | Type::Createsubscriptionresponse
                    | Type::Error
                    | Type::Getindexresponse
                    | Type::Uploadmoduleresponse
                    | Type::Getcurrenttopographyresponse => {
                        handlers::errors::handle_incorrect_message_received(writer_tx.clone())
                            .await;
                    }
                }
            }
        },
    );

    let _write_handle =
        broker_lock
            .task_handler
            .spawn(&SpawnTaskConfig::new("tcp_write", false), async move {
                tracing::info!("Starting writing task..");

                while let Some(val) = writer_rx.recv().await {
                    tracing::info!("Received: {:#?} on the writing side", val);

                    Frame::new(val.to_vec())
                        .try_write_async(&mut write_socket)
                        .await
                        .unwrap();
                    // let _result = write_socket.write_all(&val).await;
                    write_socket.flush().await.unwrap();
                }
            });

    drop(broker_lock);
}

pub async fn run_server(dir: PathBuf, port: u16) {
    let broker = Arc::new(RwLock::new(Broker::new(dir)));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    tracing::info!("Connected on {}", port);

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        tracing::info!("Received connection from: {addr}");

        process_socket(socket, broker.clone()).await;
    }
}
