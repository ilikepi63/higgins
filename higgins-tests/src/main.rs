#![allow(clippy::unused_io_amount)]

use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use clap::{Parser, Subcommand};
use higgins_client::{Client, ResponseBody};
use higgins_shared::HigginsError;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The port to connect to.
    #[arg(short, long)]
    port: u16,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Multiple Produces sequentially.
    MultiProduce {
        /// How many of said produce elements there should be.
        #[arg(long, short, require_equals = true)]
        count: u64,
    },
    /// Create the given configuration.
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

    let args = Args::parse();

    let port = args.port;

    let mut client =
        Client::connect(format!("127.0.0.1:{port}"), Some(Duration::from_secs(3))).await?;

    match args.command {
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
        Commands::MultiProduce { count } => {
            static CONFIG: &str = r#"[storage.memory]
            type="memory"

            [schema.update_customer_event]
            id = "string"
            first_name = "string"
            last_name = "string"
            age = "int32"

            [streams.update_customer]
            schema = "update_customer_event"
            partition_key = "id"
            "#;

            static PAYLOAD: &str = r#"
            {
                "id": "1",
                "first_name": "John",
                "last_name": "Doe",
                "age": 21
            }"#;

            static STREAM: &str = "update_customer";

            let result: Result<(), HigginsError> = {
                // Upload a basic configuration with one stream.
                client
                    .upload_configuration(CONFIG.as_bytes())
                    .await
                    .unwrap();

                match client.recv(None).await.unwrap().body {
                    ResponseBody::CreateConfiguration(_) => {
                        println!("Retrieved create configuration!");
                    } //create_subscription_response.subscription_id.unwrap(),
                    _ => panic!("Retrieved unexpected result."),
                };

                pub fn customer_schema() -> Schema {
                    Schema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("first_name", DataType::Utf8, false),
                        Field::new("last_name", DataType::Utf8, false),
                        Field::new("age", DataType::Int32, false),
                    ])
                }

                for _n in 0..count {
                    client
                        .produce_json(
                            STREAM,
                            PAYLOAD.as_bytes(),
                            std::sync::Arc::new(customer_schema()),
                        )
                        .await
                        .unwrap();
                }

                for _n in 0..count {
                    match client.recv(None).await.unwrap().body {
                        ResponseBody::Produce(_) => {
                            tracing::trace!("Retrieved Produce!");
                        } //create_subscription_response.subscription_id.unwrap(),
                        _ => panic!("Retrieved unexpected result."),
                    };
                }

                Ok(())
            };

            if let Err(err) = result {
                tracing::error!("Error occurred when trying to produce: {:#?}", err);
            }
        }
    }

    Ok(())
}
