use bytes::BytesMut;
use clap::{Parser, Subcommand, arg, command};
use higgins_codec::{Message, Ping, Pong, ProduceRequest, message::Type};
use prost::Message as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();

    let mut socket = TcpStream::connect("127.0.0.1:8080").await.unwrap();

    let args = Args::parse();

    match args.command {
        Some(cmd) if matches!(cmd, Commands::Ping {}) => {
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
                return Ok(());
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
                Type::Metadataesponse => todo!(),
                Type::Pong => {
                    tracing::info!("Received Pong!")
                }
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
                Type::Metadataesponse => todo!(),
                Type::Pong => {
                    tracing::info!("Received Pong!")
                }
            }
        }
        Some(cmd)
            if matches!(
                &cmd,
                Commands::Produce {
                    topic,
                    key,
                    file_name
                }
            ) =>
        {
            if let Commands::Produce {
                topic,
                key,
                file_name,
            } = cmd
            {
                let data = std::fs::read_to_string(&file_name).unwrap();

                let request = ProduceRequest {
                    topic,
                    partition_key: key,
                    payload: data.as_bytes().to_vec(),
                };

                let mut write_buf = BytesMut::new();
                let mut read_buf = BytesMut::new();

                Message {
                    r#type: Type::Producerequest as i32,
                    produce_request: Some(request),
                    ..Default::default()
                }
                .encode(&mut write_buf)
                .unwrap();

                tracing::info!("Writing: {:#?}", write_buf);

                let result = socket.write_all(&write_buf).await.unwrap();

                let n = socket.read(&mut read_buf).await.unwrap();

                let slice = &read_buf[0..n];

                let message = Message::decode(slice).unwrap();

                match Type::try_from(message.r#type).unwrap() {
                    Type::Ping => {}
                    Type::Consumerequest => {
                        tracing::info!("Received Consume Response!");
                    }
                    Type::Consumeresponse => todo!(),
                    Type::Producerequest => {}
                    Type::Produceresponse => {
                        tracing::info!("Received Produce Response!");
                    }
                    Type::Metadatarequest => todo!(),
                    Type::Metadataesponse => todo!(),
                    Type::Pong => {}
                }
            }
        }
        Some(_) => todo!(),
        None => todo!(),
    }

    Ok(())
}
