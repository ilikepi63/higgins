#[deny(clippy::unwrap_used)]
#[deny(clippy::expect_used)]
pub mod broker;
pub mod client;
mod derive;
mod error;
pub mod functions;
pub mod storage;
pub mod subscription;
pub mod task;
pub mod topography;
pub mod utils;

use std::{path::PathBuf, str::FromStr};

use higgins::run_server;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, require_equals = true)]
    topic: String,
    #[arg(long, require_equals = true)]
    key: Vec<u8>,
    #[arg(long, require_equals = true)]
    file_name: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();

    let port = 8080; // TODO: this needs to go to env vars.

    let dir = PathBuf::from_str("higgins_data").unwrap();

    run_server(dir, port).await;
}
