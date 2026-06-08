#[deny(clippy::unwrap_used)]
#[deny(clippy::expect_used)]
pub mod broker;
pub mod client;
mod derive;
pub mod functions;
pub mod storage;
pub mod subscription;
pub mod task;
pub mod topography;
pub mod utils;

use clap::Parser;
use higgins::run_server;
use std::{path::PathBuf, str::FromStr};

static DEFAULT_PORT: u16 = 4932;
static DEFAULT_DIR: &str = "data";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, require_equals = true)]
    port: Option<u16>,
    #[arg(long, require_equals = true)]
    dir: Option<String>,
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

    let args = Args::parse();

    let port = args.port.unwrap_or(DEFAULT_PORT);

    let dir = PathBuf::from_str(&args.dir.unwrap_or(DEFAULT_DIR.to_string()));

    match dir {
        Ok(dir) => {
            run_server(dir, port).await;
        }
        Err(_) => {
            tracing::error!("Incorrect directory name given.");
        }
    };
}
