use std::path::PathBuf;

use log::{error, info};
use tokio::sync::{mpsc::Receiver, oneshot};

use crate::util::{AlignedSbeWriter, dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain}};

const CHUNK_SIZE: usize = 64 *1024 * 1024;

pub enum WriterCommand {
    Delta(PriceLevelUpdateDomain),
    Snapshot(SnapshotDomain),
    Flush(oneshot::Sender<()>),
}

pub async fn run_sbe_writer(file_path: PathBuf,mut rx : Receiver<WriterCommand>) {

    let mut writer = match AlignedSbeWriter::create(&file_path, CHUNK_SIZE) {
        Ok(w) => w,
        Err(e) => {
            error!("[SBE Writer] Failed to create output file: {}", e);
            return;
        }
    };


    while let Some(cmd) = rx.recv().await {
        match cmd {
            WriterCommand::Delta(delta) => {
                if let Err(e) = writer.write_msg(&delta) {
                    error!("[SBE Writer] Error writing delta: {}", e);
                }
            }
            WriterCommand::Flush(done_tx) => {
                info!("[SBE Writer] Flushing remaining buffer to disk...");
                if let Err(e) = writer.flush() {
                    error!("[SBE Writer] Flush error: {}", e);
                }
                let _ = done_tx.send(());
                break;
            }
            WriterCommand::Snapshot(snapshot) => {
                if let Err(e) = writer.write_msg(&snapshot) {
                    error!("[SBE Writer] Error writing snapshot: {}", e);
                }
            }
        }
    }
}