#![allow(clippy::unused_io_amount)]

use std::time::Duration;

use clap::{Parser, Subcommand};
use higgins_client::{Client, ResponseBody};
use higgins_shared::HigginsError;

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

    let mut client = Client::connect("127.0.0.1:8080", Some(Duration::from_secs(3))).await?;

    let args = Args::parse();

    match args.command {
        Commands::Ping {} => {
            let result: Result<(), HigginsError> = {
                client.ping().await?;

                let result = client.recv(None).await?;

                match result.body {
                    ResponseBody::Pong(_) => {
                        tracing::trace!("Pong!");
                    }
                    _ => {
                        tracing::trace!("Didn't receive a pong response.");
                    }
                }

                Ok(())
            };

            if let Err(err) = result {
                tracing::error!("Error occurred when trying to ping: {:#?}", err);
            }
        }

        Commands::CreateConfiguration { file } => {
            let result: Result<(), HigginsError> = {
                let configuration = std::fs::read_to_string(&file)?;

                client
                    .upload_configuration(configuration.as_bytes())
                    .await?;

                client.recv(None).await?;

                tracing::trace!("Successfully uploaded result!");

                Ok(())
            };

            if let Err(err) = result {
                tracing::error!(
                    "Error occurred when trying to create configuration: {:#?}",
                    err
                );
            }
        }
        Commands::Produce { topic, file_name } => {
            let result: Result<(), HigginsError> = {
                let payload = std::fs::read(&file_name)?;

                client.produce(&topic, &payload).await?;

                let result = client.recv(None).await?;

                tracing::trace!("Result: {:#?}", result);

                tracing::trace!("Successfully Produced!");

                Ok(())
            };

            if let Err(err) = result {
                tracing::error!("Error occurred when trying to produce: {:#?}", err);
            }
        }
        _ => todo!(),
    }

    Ok(())
}
