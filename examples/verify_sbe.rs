use std::path::PathBuf;
use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

fn main() -> anyhow::Result<()> {
    let sbe_path = PathBuf::from("orderbook.sbe");
    println!("Opening {:?}...", sbe_path);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&sbe_path)?;

    let mut snapshot_count = 0usize;
    let mut delta_count = 0usize;
    let mut first_snapshot = None;
    let mut first_delta = None;

    let max_to_scan = 100_000;

    println!("Scanning first {} messages from SBE file...", max_to_scan);

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(decoder) => {
                let snap = SnapshotDomain::try_from_decoder(decoder)?;
                if first_snapshot.is_none() {
                    first_snapshot = Some(snap.clone());
                }
                snapshot_count += 1;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let delta = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                if first_delta.is_none() {
                    first_delta = Some(delta.clone());
                }
                delta_count += 1;
            }
            _ => {}
        }

        if snapshot_count + delta_count >= max_to_scan {
            break;
        }
    }

    println!("--- Verification Results ---");
    println!("Successfully decoded {} total messages without errors!", snapshot_count + delta_count);
    println!("Snapshots decoded: {}", snapshot_count);
    println!("Price level updates decoded: {}", delta_count);

    if let Some(snap) = first_snapshot {
        println!("\nSample Snapshot: Symbol: {}, Bids: {}, Asks: {}, ExTs: {}", 
            snap.symbol, snap.bids.len(), snap.asks.len(), snap.exchange_timestamp);
    }
    if let Some(delta) = first_delta {
        println!("Sample Delta: Symbol: {}, Side: {:?}, Price: {}, Size: {}, ExTs: {}", 
            delta.symbol, delta.side, delta.price_level, delta.quantity, delta.exchange_timestamp);
    }

    println!("\nSBE File verification: SUCCESS!");
    Ok(())
}
