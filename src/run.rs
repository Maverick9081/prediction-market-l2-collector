use std::{error::Error, sync::atomic::Ordering, time::Duration};

use chrono::Utc;
use log::{error, warn};
use tokio::{
    signal::ctrl_c,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    time::{Instant, interval},
};

use crate::{
    discovery::DiscoveryDiff,
    order_book::OrderBookManager,
    util::dtos::challenge_orderbook::PriceLevelUpdateDomain,
    websockets::{ConnectionHealth, ConnectionRole, MarketEvent, Pool, WsEvent},
    writer::WriterCommand,
};

pub async fn run(
    mut diff_rx: Receiver<DiscoveryDiff>,
    ws_pool: Pool,
    writer_tx: Sender<WriterCommand>,
    mut event_rx: Receiver<WsEvent>,
) -> Result<(), Box<dyn Error>> {
    let mut order_book_manager = OrderBookManager::new();
    let metrics = ws_pool.metrics().clone();

    let mut source_selectors = ws_pool.create_source_selectors();
    let num_workers = source_selectors.len();

    let mut snapshot_interval = interval(Duration::from_secs(60));
    snapshot_interval.tick().await;
    let mut health_check_interval = interval(Duration::from_millis(10000));

    loop {
        tokio::select! {

        Some(diff) = diff_rx.recv() => {
            if !diff.added.is_empty(){
                for token in &diff.added {
                    order_book_manager.insert_new_token(token);
                }
                ws_pool.subscribe(diff.added).await;
            }

            if !diff.removed.is_empty() {
                for token in & diff.removed {
                    order_book_manager.remove_token(token);
                }

                ws_pool.unsubscribe(diff.removed).await;
            }
        }


        Some(ws_msg) = event_rx.recv() => {
            // match ws_msg {

                let WsEvent { worker_id, role, event, received_at } = ws_msg;

                let applied = order_book_manager.apply_event(role, &event);

                if let MarketEvent::Delta(ref delta) = event {
                    if let Some(latency_us) = received_at
                    .signed_duration_since(delta.exchange_timestamp)
                    .num_microseconds()
                    .filter(|&lat| lat >= 0 && lat < 10_000_000) 
                {
                    metrics.record_feed_latency(worker_id, role, latency_us as u64);
                }
                }


                if applied && role == source_selectors[worker_id].active_role {
                    if let MarketEvent::Delta(delta) = event {
                        let now = Utc::now();
                        let latency = (now - delta.exchange_timestamp).num_milliseconds().max(0) as u64;
                        metrics.record_latency(latency);

                        let sbe_delta = PriceLevelUpdateDomain {
                            backtest_timestamp: now,
                            exchange_timestamp: delta.exchange_timestamp,
                            price_level: delta.price as i64,
                            quantity: delta.size as i64,
                            side: delta.side,
                            symbol: delta.asset_id,
                        };
                        if let Err(e) = writer_tx.send(WriterCommand::Delta(sbe_delta)).await{
                            error!("SBE Writer channel closed {}, Stopping collecter",e);
                            return Err(anyhow::anyhow!("SBE Write unavailable").into());
                        }
                    }
                }
        }

        _ = health_check_interval.tick() => {


            for selector in &mut source_selectors {
                let w_id = selector.worker_id;
                let current_role = selector.active_role;
                let candidate_role = current_role.switch();
                let current_health = match current_role {
                    ConnectionRole::Primary => *selector.primary_health.borrow(),
                    ConnectionRole::Secondary => *selector.secondary_health.borrow(),
                };
                let candidate_health = match candidate_role {
                    ConnectionRole::Primary => *selector.primary_health.borrow(),
                    ConnectionRole::Secondary => *selector.secondary_health.borrow(),
                };

                let now_instant = Instant::now();
                let mut trigger_switch = false;
                let mut switch_reason = "";

                if current_health != ConnectionHealth::Connected && candidate_health == ConnectionHealth::Connected {
                    trigger_switch = true;
                    switch_reason = "CONNECTION DISCONNECTED";
                    selector.better_since = None;
                }else if current_health == ConnectionHealth::Connected && candidate_health ==ConnectionHealth::Connected {

                    let candidate_ready = order_book_manager.is_partition_ready(w_id, num_workers, candidate_role);

                    if candidate_ready  {
                        if let (Some(active_p50), Some(cand_p50)) = (
                            metrics.get_worker_median(w_id, current_role, 100),
                            metrics.get_worker_median(w_id, candidate_role, 100),
                        ) {
                            if selector.latency_check(active_p50, cand_p50, now_instant) {
                                trigger_switch = true;
                                switch_reason = "1-HOUR LATENCY SUPERIORITY (>= 10% faster)";
                            }
                            else {
                                selector.better_since = None;
                            }
                        }
                    }
                    else {
                        selector.better_since = None;
                    }
                }
                else {
                    selector.better_since = None;
                }

                    if trigger_switch {
                        if order_book_manager.is_partition_ready(w_id, num_workers, candidate_role) {
                            selector.active_role = candidate_role;
                            metrics.total_switches.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                "[Worker {}] Switch triggered [{}]! Active {:?} ({:?}) -> Promoted {:?} (ready). Emitting handoff snapshots...",
                                  w_id, switch_reason, current_role, current_health, candidate_role
                              );
                            let handoff_snaps = order_book_manager.generate_handoff_snapshots(
                                w_id,
                                num_workers,
                                candidate_role,
                                Utc::now(),
                            );
                            for snap in handoff_snaps {
                                if writer_tx.send(WriterCommand::Snapshot(snap)).await.is_err() {
                                    error!("Failed to write handoff snapshot: writer closed");
                                }
                            }
                        } else {
                            warn!(
                                "[Worker {}] Switch condition met ({}) but candidate {:?} books NOT ready yet. Waiting for initial snapshots...",
                                  w_id, switch_reason, candidate_role
                            );
                        }

                    }


            }

        }
             _ = snapshot_interval.tick() => {
                    let now = Utc::now();
                    
                    let active_roles: Vec<ConnectionRole> = source_selectors
                    .iter()
                    .map(|s| s.active_role)
                    .collect();

                    let periodic_snaps = order_book_manager.generate_periodic_snapshots(now,num_workers,&active_roles,);
                    for sbe_snapshot in periodic_snaps {
                        let _ = writer_tx.send(WriterCommand::Snapshot(sbe_snapshot)).await;
                    }

                    metrics.print_report(order_book_manager.len());
                }

            _ = ctrl_c() => {

                    ws_pool.shutdown().await;

                    while let Ok(ws_msg) = event_rx.try_recv() {
                        let WsEvent { worker_id, role, event,received_at } = ws_msg;
                        let applied = order_book_manager.apply_event(role, &event);
                        if applied && role == source_selectors[worker_id].active_role {
                            if let MarketEvent::Delta(delta) = event {
                                if let (Ok(p), Ok(s)) = (i64::try_from(delta.price), i64::try_from(delta.size)) {
                                    let _ = writer_tx.send(WriterCommand::Delta(PriceLevelUpdateDomain {
                                        backtest_timestamp: Utc::now(),
                                        exchange_timestamp: delta.exchange_timestamp,
                                        price_level: p,
                                        quantity: s,
                                        side: delta.side,
                                        symbol: delta.asset_id,
                                    })).await;
                                }
                            }
                        }
                    }

                    let (done_tx,done_rx) =  oneshot::channel();


                    if let Err(e) = writer_tx.send(WriterCommand::Flush(done_tx)).await {
                        error!("Failed to send flush to writer: {}", e);
                    } else {
                        let _ = done_rx.await;
                    }
                    break;
                }

            }
    }

    Ok(())
}
