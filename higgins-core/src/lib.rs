#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
use std::{path::PathBuf, sync::Arc};

use higgins_codec::{Message, frame::Frame, message::Type};
use higgins_shared::HigginsError;
use prost::Message as _;
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

async fn process_socket(
    tcp_socket: TcpStream,
    broker: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    let (mut read_socket, mut write_socket) = tcp_socket.into_split();

    let (writer_tx, mut writer_rx) = tokio::sync::mpsc::channel(100);

    let spawning_broker = broker.clone();

    let mut broker_lock = spawning_broker.write().await;

    let client_id = broker_lock
        .clients
        .insert(ClientRef::AsyncTcpSocket(writer_tx.clone()))
        .ok_or(HigginsError::Arbitrary(
            "Unable to create client ref for socket".to_string(),
        ))?;

    let _read_handle = tokio::spawn(async move {
        loop {
            let frame = match Frame::try_read_async(&mut read_socket).await {
                Ok(frame) => frame,
                Err(_) => {
                    // Usually means that EOF was received on the socket, terminating this.
                    break;
                }
            };

            let broker = broker.clone();
            let writer_tx = writer_tx.clone();
            // Move this to task_handler.
            tokio::spawn(async move {
                let message = match Message::decode(&mut frame.inner()) {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::error!("{:#?}", err);
                        return;
                    }
                };

                let t = match Type::try_from(message.r#type) {
                    Ok(t) => t,
                    Err(err) => {
                        tracing::error!("{:#?}", err);
                        return;
                    }
                };

                tracing::info!("Request Type: {:#?}", t);

                if let Err(err) = match t {
                    Type::Ping => handlers::handle_ping(message, writer_tx.clone()).await,
                    Type::Createsubscriptionrequest => {
                        handlers::handle_create_subscription(
                            message,
                            broker.clone(),
                            writer_tx.clone(),
                        )
                        .await
                    }
                    Type::Producerequest => {
                        tokio::spawn(async move {
                            handlers::handle_produce(message, broker.clone(), writer_tx.clone())
                                .await
                        });

                        Ok(())
                    }
                    Type::Createconfigurationrequest => {
                        handlers::handle_create_configuration(
                            broker.clone(),
                            message,
                            writer_tx.clone(),
                        )
                        .await
                    }
                    Type::Takerecordsrequest => {
                        handlers::handle_take_records(
                            broker.clone(),
                            message,
                            client_id,
                            writer_tx.clone(),
                        )
                        .await
                    }
                    Type::Getindexrequest => {
                        handlers::handle_get_index(message, broker.clone(), writer_tx.clone()).await
                    }
                    Type::Uploadmodulerequest => {
                        handlers::handle_upload_module(message, broker.clone(), writer_tx.clone())
                            .await
                    }
                    Type::Metadatarequest => todo!(),
                    Type::Getcurrenttopographyrequest => {
                        handlers::handle_get_topography(message, broker.clone(), writer_tx.clone())
                            .await
                    }
                    Type::Getsubscriptionrequest => {
                        handlers::handle_get_subscription(
                            message,
                            broker.clone(),
                            writer_tx.clone(),
                        )
                        .await
                    }
                    Type::Acknowledgerequest => {
                        handlers::handle_acknowledge(message, broker.clone(), writer_tx.clone())
                            .await
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
                        .await
                    }
                } {
                    tracing::error!("{:#?}", err);
                }
            });
        }
    });

    let _write_handle = tokio::spawn(async move {
        tracing::info!("Starting writing task..");

        while let Some(val) = writer_rx.recv().await {
            tracing::info!("Received: {:#?} on the writing side", val);

            if let Err(err) = Frame::new(val.to_vec())
                .try_write_async(&mut write_socket)
                .await
            {
                tracing::error!("{:#?}", err);
            };
            // let _result = write_socket.write_all(&val).await;
            if let Err(err) = write_socket.flush().await {
                tracing::error!("{:#?}", err);
            };
        }
    });

    drop(broker_lock);

    Ok(())
}

pub struct ServerHandle(tokio::sync::oneshot::Sender<()>);

impl ServerHandle {
    pub fn close(self) {
        if let Err(err) = self.0.send(()) {
            tracing::trace!("{:#?}", err);
        };
    }
}

pub async fn run_server(dir: PathBuf, port: u16) -> Result<(), HigginsError> {
    let broker = Arc::new(RwLock::new(Broker::new(dir)?));

    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;

    tracing::info!("Connected on {}", port);

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::info!("Received connection from: {addr}");

        process_socket(socket, broker.clone()).await?;
    }
}

// #[cfg(test)]
pub fn run_server_returning(dir: PathBuf, port: u16) -> Result<ServerHandle, HigginsError> {
    let broker = Arc::new(RwLock::new(Broker::new(dir)?));

    let (tx, mut rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        if let Ok(rt) = tokio::runtime::Runtime::new() {
            rt.block_on(async move {
                if let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{port}")).await {
                    tracing::info!("Connected on {}", port);

                    loop {
                        tokio::select! {
                            socket = listener.accept() => {

                                if let Ok(socket) = socket {
                                    let (socket, addr) = socket;
                                    tracing::info!("Received connection from: {addr}");

                                    if let Err(err) = process_socket(socket, broker.clone()).await{
                                        tracing::error!("{:#?}", err);
                                    };

                                };

                            },
                            _ = &mut rx => {
                                tracing::info!("Received close, will stop the server.");
                                break;
                            }
                        }
                    }
                }
            });
        };

        tracing::info!("Thread is no longer blocked.. terminating");
    });

    Ok(ServerHandle(tx))
}
