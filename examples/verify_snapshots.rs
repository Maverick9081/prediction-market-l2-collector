use std::path::PathBuf;
use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, SnapshotDomain},
};

fn main() -> anyhow::Result<()> {
    let sbe_path = PathBuf::from("orderbook.sbe");
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&sbe_path)?;

    let mut snap_count = 0usize;
    let mut total_scanned = 0usize;

    println!("Searching for snapshots in SBE file...");

    while let Some(msg) = reader.read_msg()? {
        total_scanned += 1;
        if let AnyMessage::Snapshot(decoder) = msg {
            let snap = SnapshotDomain::try_from_decoder(decoder)?;
            snap_count += 1;
            if snap_count <= 3 {
                println!("[Snapshot #{}] Symbol: {}, Bids: {}, Asks: {}, ExTs: {}", 
                    snap_count, snap.symbol, snap.bids.len(), snap.asks.len(), snap.exchange_timestamp);
            }
        }
        if snap_count >= 10 || total_scanned >= 1_500_000 {
            break;
        }
    }

    println!("\nFound {} snapshots across {} scanned messages!", snap_count, total_scanned);
    Ok(())
}
