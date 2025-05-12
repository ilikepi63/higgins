use std::{collections::BTreeMap, fs};

use serde::{Deserialize, Serialize};

pub mod broker;

#[derive(Serialize, Deserialize, Debug)] 
pub struct Configuration {
    schema: BTreeMap<String, BTreeMap<String, String>>,
}

use easy_repl::{Repl, CommandStatus, command};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let mut repl = Repl::builder()
        .add("produce", command! {
            "Produce JSON to a given stream",
            (stream_name: String, data: String) => |name, data| {
                println!("Hello {}!", name);
                println!("data: {:#?}", data);

                Ok(CommandStatus::Done)
            }
        })
        .add("consume", command! {
            "Consume from a Stream.",
            (stream_name: String) => |name| {

                Ok(CommandStatus::Done)
            }
        })
        .build().expect("Failed to create repl");
    
    repl.run().expect("CriHI Hi tical REPL error");


    Ok(())
}
