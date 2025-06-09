use bytes::BytesMut;
use clap::{Parser, Subcommand, arg, command};
use higgins_codec::{Message, Ping, Pong, message::Type};
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

            let mut buf = BytesMut::new();

            let ping = Ping::default();

            Message {
                r#type: Type::Ping as i32,
                consume_request: None,
                consume_response: None,
                produce_request: None,
                produce_response: None,
                metadata_request: None,
                metadata_response: None,
                ping: Some(ping),
                pong: None,
            }
            .encode(&mut buf)
            .unwrap();

            tracing::info!("Sending: {:#?}", buf);
            tracing::info!("Byte size: {:#?}", buf.len());

            let result = socket.write_all(&buf).await.unwrap();

            let n = socket.read(&mut buf).await.unwrap();

            let slice = &buf[0..n];

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
                        consume_request: None,
                        consume_response: None,
                        produce_request: None,
                        produce_response: None,
                        metadata_request: None,
                        metadata_response: None,
                        ping: None,
                        pong: Some(pong),
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
                },
            }


            // tracing::info!("Wrote {} bytes to socket", result);
        }
        Some(_) => todo!(),
        None => todo!(),
    }

    Ok(())
}
