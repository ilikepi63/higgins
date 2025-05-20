use std::{
    io::{BufReader, Write, stdin, stdout},
    sync::{Arc, atomic::AtomicU64},
};

use broker::Broker;
use config::Configuration;
use storage::index::{Index, IndexView, index_reader::IndexReader};

pub mod broker;
pub mod config;
pub mod storage;
pub mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();

    let config = Configuration::from_env();

    let mut broker = Broker::from_config(config);

    tracing::info!("Created Broker: {:#?}", broker);

    let mut args = std::env::args();

    let first_arg = args.nth(1);

    if first_arg.as_ref().is_some_and(|first_arg| first_arg == "L") {
        let path = "update_customer/00000000000000000001.index";

        let fs_md = std::fs::metadata(&path).unwrap();

        let reader = IndexReader::new(path, Arc::new(AtomicU64::new(fs_md.len()))).await?;

        let indexes = reader.load_all_indexes_from_disk().await.unwrap();

        let mut current_index = 0;

        while let Some(index) = indexes.get(current_index) {
            tracing::info!("Found index: {}", index);

            current_index += 1;
        }

        return Ok(());
    }

    if first_arg.as_ref().is_some_and(|first_arg| first_arg == "P") {
        let name = "update_customer";

        let data_file = "customer.json";

        let data = std::fs::File::open(data_file).expect("No data found.");

        println!("Finding stream for name: {name}");

        let (schema, _tx, _rx) = broker
            .get_stream(name)
            .expect("Could not find stream for stream_name.");

        let mut json = arrow_json::ReaderBuilder::new(schema.clone())
            .build(BufReader::new(data))
            .unwrap();

        let batch = json.next().unwrap().unwrap();

        broker.produce(name, "partition_key", batch).await;

        return Ok(());
    }

    if first_arg.is_some_and(|first_arg| first_arg == "C") {
        let name = "update_customer";

        let (schema, _tx, _rx) = broker
            .get_stream(name)
            .expect("Could not find stream for stream_name.");

        let mut result = broker.consume(name, b"partition", 1, 1000).await;

        match result.recv().await {
            Some(result) => {
                tracing::info!("Received: {:#?}", result);
            },
            None => {
                tracing::error!("Did not receive any results for given key.");
            }
        }

        return Ok(());
    }

    loop {
        print!("> ");
        stdout().flush().unwrap();
        if let Some(Ok(input)) = stdin().lines().next() {
            if input.trim() == "exit" {
                break;
            }
            if input.trim().is_empty() {
                continue;
            }

            let tokens = input.split_whitespace();

            let tokens = tokens.take(3).map(|s| s.to_string()).collect::<Vec<_>>();

            let command = tokens.first().expect("NO Command Given.");

            match command.as_ref() {
                "produce" => {
                    let name = tokens.get(1).expect("Invalid Message.");

                    let data_file = tokens.get(2).expect("Invalid Data Path.");

                    let data = std::fs::File::open(data_file).expect("No data found.");

                    println!("Finding stream for name: {name}");

                    let (schema, _tx, _rx) = broker
                        .get_stream(name)
                        .expect("Could not find stream for stream_name.");

                    let mut json = arrow_json::ReaderBuilder::new(schema.clone())
                        .build(BufReader::new(data))
                        .unwrap();

                    let batch = json.next().unwrap().unwrap();

                    println!("You are producing! {:#?}", batch);

                    broker.produce(name, "partition_key", batch).await;
                }
                "listen" => {
                    let name = tokens.get(1).expect("Invalid Message.").clone();

                    let mut rx = broker
                        .get_receiver(&name)
                        .expect("Could not find stream for stream_name.");

                    tokio::spawn(async move {
                        while let Ok(value) = rx.recv().await {
                            println!("Received value {:#?} on stream {name}", value);
                        }
                    });
                }
                _ => {
                    println!("Invalid Command: {:#?}", tokens);
                    continue;
                }
            }
        }
    }

    Ok(())
}
