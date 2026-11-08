use std::path::PathBuf;
use std::time::Duration;
use engineering_challenge::logger::init_logger;
use engineering_challenge::polymarket::PolyMarketDiscovery;
use engineering_challenge::run::run;
use engineering_challenge::discovery::run_discovery;
use engineering_challenge::websockets::{Pool, WsEvent};
use engineering_challenge::writer::{WriterCommand, run_sbe_writer};
use log::error;
use reqwest::Client;
use tokio::sync::mpsc::{self, channel};

// const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[tokio::main]
async fn main() -> Result<(),Box<dyn std::error::Error>> {
  
    let _ = init_logger(Some("collector.log".to_string()));

    let (writer_tx, writer_rx) = mpsc::channel::<WriterCommand>(10_000);
    let sbe_output_path = PathBuf::from("orderbook.sbe");

    let mut writer_handle = tokio::spawn(run_sbe_writer(sbe_output_path, writer_rx));

    let (event_tx,event_rx) = channel::<WsEvent>(10000);
    let ws_pool = Pool::new(6, event_tx);

    let (diff_tx,diff_rx) = channel(100);

    let client = Client::builder().connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(20)).build()?;

    let discovery = PolyMarketDiscovery::new(client);

    let mut discovery_handle = tokio::spawn(run_discovery(discovery, Duration::from_secs(60),diff_tx));


    tokio::select! {
        res = run(diff_rx,ws_pool,writer_tx,event_rx) => {
            if let Err(e) = res {
                error!("collecter run loop error : {e}");
            }
        }
        res = &mut writer_handle => {
            log::error!("SBE Writer task terminated unexpectedly: {:?}", res);
        }
        res = &mut discovery_handle => {
            log::error!("Discovery task terminated unexpectedly: {:?}", res);
        }
    }

    discovery_handle.abort();
    let _ = writer_handle.await;

    Ok(())
}
