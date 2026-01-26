#![allow(clippy::unused_io_amount)]

use std::time::Duration;

use clap::{Parser, Subcommand};
use higgins_client::{Client, Response};
use higgins_shared::PartitionName;

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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .init();

    let mut client = Client::connect("127.0.0.1:8080", Some(Duration::from_secs(3)))
        .await
        .unwrap();

    let args = Args::parse();

    match args.command {
        Commands::Ping {} => {
            client.ping().await.unwrap();

            let result = client.recv().await.unwrap();

            match result {
                Response::Pong(_) => {
                    println!("Pong!");
                }
                _ => {
                    println!("Didn't receive a pong response.");
                }
            }
        }

        Commands::CreateConfiguration { file } => {
            let configuration = std::fs::read_to_string(&file).unwrap();

            client
                .upload_configuration(configuration.as_bytes())
                .await
                .unwrap();

            client.recv().await.unwrap();

            println!("Successfully uploaded result!");
        }
        Commands::Produce {
            topic,
            key,
            file_name,
        } => {
            let payload = std::fs::read(&file_name).unwrap();

            client
                .produce(
                    &topic,
                    &PartitionName::try_from(key.as_slice()).unwrap(),
                    &payload,
                )
                .await
                .unwrap();

            let result = client.recv().await.unwrap();

            println!("Result: {:#?}", result);

            println!("Successfully Produced!");

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
