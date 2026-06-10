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
pub mod functions;
mod handlers;
pub mod storage;
pub mod subscription;
pub mod task;
pub mod topography;
pub mod utils;

async fn process_socket(tcp_socket: TcpStream, broker: Arc<RwLock<Broker>>) {
    let (mut read_socket, mut write_socket) = tcp_socket.into_split();

    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(100);

    let spawning_broker = broker.clone();

    let mut broker_lock = spawning_broker.write().await;

    let client_id = broker_lock
        .clients
        .insert(ClientRef::AsyncTcpSocket(writer_tx.clone()))?;

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

                let message = Message::decode(&mut frame.inner())?;

                tracing::info!("Received a message {:#?}, responding.", message);

                let t = Type::try_from(message.r#type);

                tracing::info!("Request Type: {:#?}", t);

                match Type::try_from(message.r#type)? {
                    Type::Ping => {
                        handlers::handle_ping(message, writer_tx.clone()).await;
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
                        handlers::handle_get_topography(message, broker.clone(), writer_tx.clone())
                            .await;
                    }
                    Type::Getsubscriptionrequest => {
                        handlers::handle_get_subscription(
                            message,
                            broker.clone(),
                            writer_tx.clone(),
                        )
                        .await;
                    }
                    Type::Acknowledgerequest => {
                        handlers::handle_acknowledge(message, broker.clone(), writer_tx.clone())
                            .await;
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
                    | Type::Getcurrenttopographyresponse
                    | Type::Getsubscriptionresponse
                    | Type::Acknowledgeresponse => {
                        handlers::errors::handle_incorrect_message_received(
                            message,
                            writer_tx.clone(),
                        )
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
                        .await?;
                    // let _result = write_socket.write_all(&val).await;
                    write_socket.flush().await?;
                }
            });

    drop(broker_lock);
}

pub struct ServerHandle(tokio::sync::oneshot::Sender<()>);

impl ServerHandle {
    pub fn close(self) {
        self.0.send(())?;
    }
}

pub async fn run_server(dir: PathBuf, port: u16) {
    let broker = Arc::new(RwLock::new(Broker::new(dir)));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    tracing::info!("Connected on {}", port);

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::info!("Received connection from: {addr}");

        process_socket(socket, broker.clone()).await;
    }
}

// #[cfg(test)]
pub fn run_server_returning(dir: PathBuf, port: u16) -> ServerHandle {
    let broker = Arc::new(RwLock::new(Broker::new(dir)));

    let (tx, mut rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async move {
            let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

            tracing::info!("Connected on {}", port);

            loop {
                tokio::select! {
                    socket = listener.accept() => {
                        let (socket, addr) = socket?;
                        tracing::info!("Received connection from: {addr}");

                        process_socket(socket, broker.clone()).await;

                    },
                    _ = &mut rx => {
                        tracing::info!("Received close, will stop the server.");
                        break;
                    }
                }
            }
        });

        tracing::info!("Thread is no longer blocked.. terminating");
    });

    ServerHandle(tx)
}
