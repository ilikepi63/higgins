#![allow(clippy::unused_io_amount)]

use std::time::Duration;

use bytes::BytesMut;
use clap::{Parser, Subcommand};
use higgins_client::Client;
use higgins_codec::{Message, ProduceRequest, message::Type};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::ping::handle_ping_cmd;

mod ping;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Ping {},
    Produce {
        #[arg(long, require_equals = true)]
        topic: String,
        #[arg(long, require_equals = true)]
        key: Vec<u8>,
        #[arg(long, require_equals = true)]
        file_name: String,
    },
    CreateConsumer {
        #[arg(long, require_equals = true)]
        topic: String,
        // partitions?
    },
    CreateConfiguration {
        #[arg(long, require_equals = true)]
        file: String,
        // partitions?
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .with_max_level(tracing::Level::TRACE)
        .init();

    let mut client = Client::connect("http://127.0.0.1:8080", Some(Duration::from_secs(3)))
        .await
        .unwrap();

    let args = Args::parse();

    match args.command {
        Commands::Ping {} => {
            let result = client.ping().await.unwrap();
        }
        Commands::Produce {
            topic,
            key,
            file_name,
        } => {
            // let data = std::fs::read_to_string(&file_name).unwrap();

            // let request = ProduceRequest {
            //     stream_name: topic.as_bytes().to_vec(),
            //     partition_key: key,
            //     payload: data.as_bytes().to_vec(),
            // };

            // let mut write_buf = BytesMut::new();
            // let mut read_buf = BytesMut::new();

            // Message {
            //     r#type: Type::Producerequest as i32,
            //     produce_request: Some(request),
            //     ..Default::default()
            // }
            // .encode(&mut write_buf)
            // .unwrap();

            // tracing::info!("Writing: {:#?}", write_buf);

            // socket.write_all(&write_buf).await.unwrap();

            // let n = socket.read(&mut read_buf).await.unwrap();

            // let slice = &read_buf[0..n];

            // let message = Message::decode(slice).unwrap();

            // match Type::try_from(message.r#type).unwrap() {
            //     Type::Ping => {}
            //     Type::Createsubscriptionrequest => {
            //         tracing::info!("Received Consume Response!");
            //     }
            //     Type::Createsubscriptionresponse => todo!(),
            //     Type::Producerequest => {}
            //     Type::Produceresponse => {
            //         tracing::info!("Received Produce Response!");
            //     }
            //     Type::Metadatarequest => todo!(),
            //     Type::Metadataresponse => todo!(),
            //     Type::Pong => {}
            //     Type::Takerecordsrequest => todo!(),
            //     Type::Takerecordsresponse => todo!(),
            //     Type::Createconfigurationrequest => todo!(),
            //     Type::Createconfigurationresponse => todo!(),
            //     Type::Deleteconfigurationrequest => todo!(),
            //     Type::Deleteconfigurationresponse => todo!(),
            //     Type::Getindexrequest => todo!(),
            //     Type::Getindexresponse => todo!(),
            //     Type::Uploadmodulerequest => todo!(),
            //     Type::Uploadmoduleresponse => todo!(),
            //     Type::Error => todo!(),
            // }
        }
        _ => todo!(),
    }

    Ok(())
}
