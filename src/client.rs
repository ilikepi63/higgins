use std::{collections::BTreeMap, fs};

use broker::Broker;
use config::Configuration;
use serde::{Deserialize, Serialize};

pub mod broker;
pub mod config;

use easy_repl::{CommandStatus, Repl, command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Configuration::from_env();

    let broker = Broker::from_config(config);

    println!("Created Broker: {:#?}", broker);

    let mut repl = Repl::builder()
        .add(
            "produce",
            command! {
                "Produce JSON to a given stream",
                (stream_name: String, data: String) => |name, data| {
                    tracing::info!("Hello {}!", name);
                    tracing::info!("data: {:#?}", data);


                    // broker.produce(stream_name, record);

                    Ok(CommandStatus::Done)
                }
            },
        )
        .add(
            "consume",
            command! {
                "Consume from a Stream.",
                (stream_name: String) => |name| {

                    Ok(CommandStatus::Done)
                }
            },
        )
        .build()
        .expect("Failed to create repl");

    repl.run().expect("CriHI Hi tical REPL error");

    Ok(())
}
