use bytes::BytesMut;
use clap::{Parser, Subcommand, arg, command};
use higgins_codec::{Message, Ping, Pong, ProduceRequest, message::Type};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn handle_ping_cmd(socket: &mut TcpStream) {
    // Send a ping command to the server;

    let mut read_buf = BytesMut::zeroed(20);

    let mut write_buf = BytesMut::new();

    let ping = Ping::default();

    Message {
        r#type: Type::Ping as i32,
        ping: Some(ping),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    tracing::info!("Writing: {:#?}", write_buf);

    let result = socket.write_all(&write_buf).await.unwrap();

    let n = socket.read(&mut read_buf).await.unwrap();

    tracing::info!("Received: {:#?}", socket.peer_addr());

    tracing::info!("Reading: {:#?}", read_buf.clone().to_vec());

    let slice = &read_buf[0..n];

    if n == 0 {
        tracing::info!("No bytes read, continuing.");
        return;
    }

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Ping => {
            let mut result = BytesMut::new();

            let pong = Pong::default();

            Message {
                r#type: Type::Pong as i32,
                pong: Some(pong),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            socket.write(&result).await.unwrap();
        }
        Type::Consumerequest => todo!(),
        Type::Consumeresponse => todo!(),
        Type::Producerequest => todo!(),
        Type::Produceresponse => todo!(),
        Type::Metadatarequest => todo!(),
        Type::Metadataresponse => todo!(),
        Type::Pong => {
            tracing::info!("Received Pong!")
        }
        Type::Consumerecordsrequest => todo!(),
        Type::Consumerecordsresponse => todo!(),
        Type::Createconfigurationrequest => todo!(),
        Type::Createconfigurationresponse => todo!(),
        Type::Deleteconfigurationrequest => todo!(),
        Type::Deleteconfigurationresponse => todo!(),
    }

    let result = socket.write_all(&write_buf).await.unwrap();
    let n = socket.read(&mut read_buf).await.unwrap();

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    match Type::try_from(message.r#type).unwrap() {
        Type::Ping => {
            let mut result = BytesMut::new();

            let pong = Pong::default();

            Message {
                r#type: Type::Pong as i32,
                pong: Some(pong),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            socket.write(&result).await.unwrap();
        }
        Type::Consumerequest => todo!(),
        Type::Consumeresponse => todo!(),
        Type::Producerequest => todo!(),
        Type::Produceresponse => todo!(),
        Type::Metadatarequest => todo!(),
        Type::Metadataresponse => todo!(),
        Type::Pong => {
            tracing::info!("Received Pong!")
        }
        Type::Consumerecordsrequest => todo!(),
        Type::Consumerecordsresponse => todo!(),
        Type::Createconfigurationrequest => todo!(),
        Type::Createconfigurationresponse => todo!(),
        Type::Deleteconfigurationrequest => todo!(),
        Type::Deleteconfigurationresponse => todo!(),
    }
}
