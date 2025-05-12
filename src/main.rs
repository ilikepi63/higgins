use std::{collections::BTreeMap, fs};

use serde::{Deserialize, Serialize};

pub mod broker;

#[derive(Serialize, Deserialize, Debug)] 
pub struct Configuration {
    schema: BTreeMap<String, BTreeMap<String, String>>,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string("config.yaml")?;

    let config: Configuration = serde_yaml::from_str(&config)?;

    println!("{:#?}", config);


    Ok(())
}
