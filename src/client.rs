use std::io::{BufReader, Write, stdin, stdout};

use broker::Broker;
use config::Configuration;

pub mod broker;
pub mod config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Configuration::from_env();

    let broker = Broker::from_config(config);

    println!("Created Broker: {:#?}", broker);

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

            let command = tokens.get(0).expect("NO Command Given.");

            match command.as_ref() {
                "produce" => {
                    let name = tokens.get(1).expect("Invalid Message.");

                    let data_file = tokens.get(2).expect("Invalid Data Path.");

                    let data = std::fs::File::open(data_file).expect("No data found.");

                    println!("Finding stream for name: {name}");

                    let (schema, tx, rx) = broker
                        .get_stream(name)
                        .expect("Could not find stream for stream_name.");

                    let mut json = arrow_json::ReaderBuilder::new(schema.clone())
                        .build(BufReader::new(data))
                        .unwrap();

                    let batch = json.next().unwrap().unwrap();

                    println!("You are producing! {:#?}", batch);

                    broker.produce(name, batch).await;
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
