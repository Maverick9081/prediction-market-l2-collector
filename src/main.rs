use std::path::PathBuf;
use std::time::Duration;
use engineering_challenge::logger::init_logger;
use engineering_challenge::run::run;
use engineering_challenge::discovery::run_discovery;
use engineering_challenge::websockets::{Pool, WsEvent};
use engineering_challenge::writer::{WriterCommand, run_sbe_writer};
use reqwest::Client;
use tokio::sync::mpsc::{self, channel};

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

/// This is a dummy example where we define a snapshot and price level update,
/// write them to a file using [`AlignedSbeWriter`], and then read them back
/// from the same file using [`AlignedSbeReader`], asserting that what we read
/// back is the exact same as what we wrote.
///
/// You should replace this main function with your actual solution, and split
/// up your code into modules in `src/` in a way that makes sense to you.

#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>> {
  
    let _ = init_logger(Some("collector.log".to_string()));

    let (writer_tx, writer_rx) = mpsc::channel::<WriterCommand>(10_000);
    let sbe_output_path = PathBuf::from("orderbook.sbe");

    let writer_handle = tokio::spawn(run_sbe_writer(sbe_output_path, writer_rx));

    let (event_tx,event_rx) = channel::<WsEvent>(10000);
    let ws_pool = Pool::new(6, event_tx);

    let (diff_tx,diff_rx) = channel(100);

    let client = Client::builder().connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(20)).build()?;

    let discovery_handle = tokio::spawn(run_discovery(client, Duration::from_secs(60),diff_tx));


    let _ = run(diff_rx, ws_pool, writer_tx, event_rx).await;

    Ok(())
}
