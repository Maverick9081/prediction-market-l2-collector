use std::{
    collections::HashSet, hash::{DefaultHasher, Hash, Hasher}, str::FromStr, sync::atomic::Ordering, time::{Duration},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, channel},
        watch::{self, Receiver as WatchReceiver},
    }, task::JoinHandle, time::{Instant, interval, interval_at, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{Side, metrics::Metrics};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionRole {
    Primary,
    Secondary,
}

impl ConnectionRole {
    pub fn switch(&self) -> Self {
        match self {
            ConnectionRole::Primary => ConnectionRole::Secondary,
            ConnectionRole::Secondary => ConnectionRole::Primary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    Connecting,
    Connected,
    Disconnected,
    Recycling,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Snapshot(BookSnapshot),
    Delta(PriceDelta),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "event_type")]
enum RawWsMsg {
    #[serde(rename = "book")]
    Book {
        asset_id: String,
        bids: Vec<RawLevel>,
        asks: Vec<RawLevel>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(rename = "price_change")]
    PriceChange {
        market: Option<String>,
        price_changes: Vec<RawPriceChange>,
        #[serde(default)]
        timestamp: Option<String>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize, Debug)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize, Debug)]
struct RawPriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub asset_id: String,
    pub bids: Vec<(u64, u64)>,
    pub asks: Vec<(u64, u64)>,
    pub exchange_timestamp: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct PriceDelta {
    pub asset_id: String,
    pub side: Side,
    pub price: u64,
    pub size: u64,
    pub exchange_timestamp: DateTime<Utc>,
}

// #[derive(Debug, Clone)]
// pub enum WsEvent {
//     Snapshot(BookSnapshot),
//     Delta(PriceDelta),
// }

#[derive(Debug, Clone)]
pub struct WsEvent {
    pub worker_id: usize,
    pub role: ConnectionRole,
    pub event: MarketEvent,
    pub received_at : DateTime<Utc>
}
pub enum WorkerCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Shutdown,
}

fn parse_scaled(val: &str) -> Result<u64> {
    let decimal = Decimal::from_str(val)?;

    let scaled = decimal
        .checked_mul(Decimal::from(1_000_000u64))
        .ok_or_else(|| anyhow::anyhow!("Value overflow: {}", val))?;

    let result: u64 = scaled
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid or out-of-range value: {}", val))?;

    Ok(result)
}

fn parse_timestamp(ts: Option<&str>) -> Result<DateTime<Utc>> {
    let raw = ts.ok_or_else(|| anyhow::anyhow!("Missing exchange timestamp"))?;
    let millis: i64 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("Malformed timestamp: {}", raw))?;
    let dt = if millis > 1_000_000_000_000 {
        DateTime::from_timestamp_millis(millis)
    } else {
        DateTime::from_timestamp(millis, 0)
    };
    dt.ok_or_else(|| anyhow::anyhow!("Out of bounds timestamp: {}", millis))
}

fn parse_market_event(worker_id: usize, role: ConnectionRole, text: &str) -> Vec<MarketEvent> {
    let mut events = Vec::new();
    if let Ok(raw) = serde_json::from_str::<RawWsMsg>(text) {
        match raw {
            RawWsMsg::Book {
                asset_id,
                bids,
                asks,
                timestamp,
                ..
            } => {
                let ex_ts = match parse_timestamp(timestamp.as_deref()) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(
                            "[Worker {} - {:?}] Dropping snapshot {}: {}",
                            worker_id, role, asset_id, e
                        );
                        return events;
                    }
                };

                let mut parsed_bids = Vec::with_capacity(bids.len());
                for l in bids {
                    match (parse_scaled(&l.price), parse_scaled(&l.size)) {
                        (Ok(p), Ok(s)) => parsed_bids.push((p, s)),
                        (Err(e), _) | (_, Err(e)) => {
                            warn!(
                                "[Worker {} - {:?}] Rejecting incomplete snapshot for {}: invalid bid level: {}",
                                worker_id, role, asset_id, e
                            );
                            return events;
                        }
                    }
                }
                let mut parsed_asks = Vec::with_capacity(asks.len());
                for l in asks {
                    match (parse_scaled(&l.price), parse_scaled(&l.size)) {
                        (Ok(p), Ok(s)) => parsed_asks.push((p, s)),
                        (Err(e), _) | (_, Err(e)) => {
                            warn!(
                                "[Worker {} - {:?}] Rejecting incomplete snapshot for {}: invalid ask level: {}",
                                worker_id, role, asset_id, e
                            );
                            return events;
                        }
                    }
                }
                events.push(MarketEvent::Snapshot(BookSnapshot {
                    asset_id,
                    bids: parsed_bids,
                    asks: parsed_asks,
                    exchange_timestamp: ex_ts,
                }));
            }
            RawWsMsg::PriceChange {
                price_changes,
                timestamp,
                ..
            } => {
                let ex_ts = match parse_timestamp(timestamp.as_deref()) {
                    Ok(t) => t,
                    Err(_) => return events,
                };
                for pc in price_changes {
                    let side = match pc.side.as_str() {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        unknown => {
                            warn!(
                                "[Worker {} - {:?}] Unknown side '{}' on token {}. Skipping delta.",
                                worker_id, role, unknown, pc.asset_id
                            );
                            continue;
                        }
                    };
                    if let (Ok(p), Ok(s)) = (parse_scaled(&pc.price), parse_scaled(&pc.size)) {
                        events.push(MarketEvent::Delta(PriceDelta {
                            asset_id: pc.asset_id,
                            side,
                            price: p,
                            size: s,
                            exchange_timestamp: ex_ts,
                        }));
                    }
                }
            }
            RawWsMsg::Other => {}
        }
    }
    events
}

pub async fn run_worker(
    worker_id: usize,
    role: ConnectionRole,
    mut cmd_rx: Receiver<WorkerCommand>,
    event_tx: Sender<WsEvent>,
    health_tx: watch::Sender<ConnectionHealth>,
    recycle_offset: Duration,
    metrics: Metrics,
) {
    let mut assigned_tokens: HashSet<String> = HashSet::new();
    loop {
        let _ = health_tx.send(ConnectionHealth::Connecting);
        let (ws_stream, _) = match connect_async(WS_URL).await {
            Ok(res) => res,
            Err(e) => {
                error!("[Worker {}] Failed to connect: {}", worker_id, e);
                let _ = health_tx.send(ConnectionHealth::Disconnected);

                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(WorkerCommand::Shutdown) => return,
                            Some(WorkerCommand::Subscribe(tokens)) => {
                                metrics.on_subscribe_success(tokens.len());
                                assigned_tokens.extend(tokens);
                            }
                            Some(WorkerCommand::Unsubscribe(tokens)) => {
                                metrics.on_unsubscribe_success(tokens.len());
                                assigned_tokens.retain(|t| !tokens.contains(t));
                            }
                            None => {}
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
        
          
                continue;
            }
        };

        info!("[Worker {}] Connected successfully!", worker_id);
        let _ = health_tx.send(ConnectionHealth::Connected);
        metrics.on_reconnect();

        let (mut ws_write, mut ws_read) = ws_stream.split();

        if !assigned_tokens.is_empty() {
            let list: Vec<String> = assigned_tokens.iter().cloned().collect();
            for chunk in list.chunks(500) {
                let sub = serde_json::json!({
                    "assets_ids": chunk,
                    "type": "market"
                });
                if let Err(e) = ws_write.send(Message::Text(sub.to_string().into())).await {
                    error!("[Worker {}] Failed to re-subscribe: {}", worker_id, e);
                    tokio::select! {
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(WorkerCommand::Shutdown) => return,
                                Some(WorkerCommand::Subscribe(tokens)) => {
                                    metrics.on_subscribe_success(tokens.len());
                                    assigned_tokens.extend(tokens);
                                }
                                Some(WorkerCommand::Unsubscribe(tokens)) => {
                                    metrics.on_unsubscribe_success(tokens.len());
                                    assigned_tokens.retain(|t| !tokens.contains(t));
                                }
                                None => {}
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
            
              
                    continue;
                }
            }
        }

        let mut recycle_interval = interval_at(
            Instant::now() + Duration::from_secs(30 * 60) + recycle_offset,
            Duration::from_secs(30 * 60),
        );

        let mut ping_interval = interval(Duration::from_secs(10));
        ping_interval.tick().await;

        loop {
            tokio::select! {
            _ = ping_interval.tick() => {

                if !assigned_tokens.is_empty() {

                if let Err(e) = ws_write.send(Message::Text("PING".into())).await {
                    error!("Failed to send PING: {}", e);
                    break;
                }
            }

            }

            _ = recycle_interval.tick() => {
                info!("[Worker {} - {:?}] Periodic recycling triggered. Refreshing connection...", worker_id, role);
                metrics.total_recycles.fetch_add(1, Ordering::Relaxed);
                 let _ = health_tx.send(ConnectionHealth::Recycling);
                let _ = ws_write.send(Message::Close(None)).await;
                break;
            }

            Some(cmd) = cmd_rx.recv() => {
                match cmd {

                    WorkerCommand::Subscribe(tokens) => {

                        for  chunk in tokens.chunks(500) {

                            let sub = serde_json::json!({
                                "operation": "subscribe",
                                "assets_ids": chunk
                            });

                            if let Err(e) = ws_write.send(Message::Text(sub.to_string().into())).await {
                                metrics.on_subscribe_failure(tokens.len());

                                error!("[Worker {}] Failed to send dynamic subscribe: {}", worker_id, e);
                                break;
                            }
                        }
                        metrics.on_subscribe_success(tokens.len());

                        assigned_tokens.extend(tokens);

                    }

                    WorkerCommand::Unsubscribe(tokens) => {

                        for  chunk in tokens.chunks(500) {

                            let unsub = serde_json::json!({
                                "operation": "unsubscribe",
                                "assets_ids": chunk
                            });

                            debug!("Removing {} token from subscriptions",tokens.len());

                            if let Err(e) = ws_write.send(Message::Text(unsub.to_string().into())).await {
                                metrics.on_unsubscribe_failure(tokens.len());

                                error!("[Worker {}] Failed to send dynamic unsubscribe: {}", worker_id, e);
                                break;
                            }
                        }
                        metrics.on_unsubscribe_success(tokens.len());

                        assigned_tokens.retain(|token| !tokens.contains(token));
                    }

                    WorkerCommand::Shutdown => {
                        let _ = ws_write.send(Message::Close(None)).await;
                        info!("Shutting down!");
                        return;
                    }
                }
            }

            msg = ws_read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "PONG" {
                            continue;
                        }
                        let received_at = Utc::now();
                        let events = parse_market_event(worker_id, role, &text);
                        for ev in events {
                            if event_tx.send(WsEvent { worker_id, role, event: ev ,received_at : received_at}).await.is_err() {
                                return;
                            }
                        }
                    }

                    Some(Ok(Message::Close(frame))) => {
                        warn!("[Worker {} - {:?}] Server closed connection: {:?}", worker_id, role, frame);
                       
                        break;
                    }
                    Some(Err(e)) => {
                        error!("WebSocket transport error: {}", e);
                        break;
                    }
                    None => {
                        warn!("WebSocket stream ended");
                        break;
                    }
                    _ => {}
                }
            }
            }
        }
        warn!(
            "[Worker {}] Connection lost. Reconnecting in 2s...",
            worker_id
        );
        let _ = health_tx.send(ConnectionHealth::Disconnected);
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(WorkerCommand::Shutdown) => return,
                    Some(WorkerCommand::Subscribe(tokens)) => {
                        metrics.on_subscribe_success(tokens.len());
                        assigned_tokens.extend(tokens);
                    }
                    Some(WorkerCommand::Unsubscribe(tokens)) => {
                        metrics.on_unsubscribe_success(tokens.len());
                        assigned_tokens.retain(|t| !tokens.contains(t));
                    }
                    None => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }

  
    }
}

pub struct ConnectionHandle {
    pub role: ConnectionRole,
    pub command_tx: Sender<WorkerCommand>,
    pub health_rx: WatchReceiver<ConnectionHealth>,
}

pub struct FeedManager {
    pub worker_id: usize,
    pub primary: ConnectionHandle,
    pub secondary: ConnectionHandle,
}

impl FeedManager {
    pub async fn subscribe(&self, tokens: Vec<String>) {
        let _ = self
            .primary
            .command_tx
            .send(WorkerCommand::Subscribe(tokens.clone()))
            .await;
        let _ = self
            .secondary
            .command_tx
            .send(WorkerCommand::Subscribe(tokens))
            .await;
    }
    pub async fn unsubscribe(&self, tokens: Vec<String>) {
        let _ = self
            .primary
            .command_tx
            .send(WorkerCommand::Unsubscribe(tokens.clone()))
            .await;
        let _ = self
            .secondary
            .command_tx
            .send(WorkerCommand::Unsubscribe(tokens))
            .await;
    }
    pub async fn shutdown(&self) {
        let _ = self.primary.command_tx.send(WorkerCommand::Shutdown).await;
        let _ = self
            .secondary
            .command_tx
            .send(WorkerCommand::Shutdown)
            .await;
    }
}

pub struct SourceSelector {
    pub worker_id: usize,
    pub active_role: ConnectionRole,
    pub primary_health: WatchReceiver<ConnectionHealth>,
    pub secondary_health: WatchReceiver<ConnectionHealth>,
    pub better_since : Option<Instant>
}

impl SourceSelector {
    pub fn new(
        worker_id: usize,
        primary_health: WatchReceiver<ConnectionHealth>,
        secondary_health: WatchReceiver<ConnectionHealth>,
    ) -> Self {
        Self {
            worker_id,
            active_role: ConnectionRole::Primary,
            primary_health,
            secondary_health,
            better_since:None
        }
    }
    pub fn check_failover(&mut self) -> Option<ConnectionRole> {
        let p_ok = *self.primary_health.borrow() == ConnectionHealth::Connected;
        let s_ok = *self.secondary_health.borrow() == ConnectionHealth::Connected;
        if self.active_role == ConnectionRole::Primary && !p_ok && s_ok {
            self.active_role = ConnectionRole::Secondary;
            Some(ConnectionRole::Secondary)
        } else if self.active_role == ConnectionRole::Secondary && !s_ok && p_ok {
            self.active_role = ConnectionRole::Primary;
            Some(ConnectionRole::Primary)
        } else {
            None
        }
    }

    pub fn latency_check(&mut self,active_median:u64,candidate_median: u64,now :Instant) -> bool {
        if active_median == 0 {
            self.better_since = None;
            return false;
        }

        // 5% faster
        if !(candidate_median.saturating_mul(100) <= active_median.saturating_mul(95)) {
            self.better_since = None;
            return false;
        }
        let since = self.better_since.get_or_insert(now);
        now.duration_since(*since) >= Duration::from_secs(60 * 60)
    }
}

pub struct Pool {
    feed_managers: Vec<FeedManager>,
    worker_handles: Vec<JoinHandle<()>>,
    metrics: Metrics,
}

impl Pool {
    pub fn new(workers: usize, event_tx: Sender<WsEvent>) -> Self {
        info!(
            "Initializing WebSocket pool with {} workers for redunduncy ",
            workers * 2
        );

        let mut feed_managers = Vec::with_capacity(workers);
        let mut worker_handles = Vec::with_capacity(workers * 2);

        let metrics = Metrics::new(workers);

        for i in 0..workers {
            let (p_cmd_tx, p_cmd_rx) = channel::<WorkerCommand>(500);
            let (p_health_tx, p_health_rx) = watch::channel(ConnectionHealth::Connecting);
            let p_event_tx = event_tx.clone();
            let p_metrics = metrics.clone();

            let p_handle = tokio::spawn(async move {
                sleep(Duration::from_millis(i as u64 * 300)).await;
                run_worker(
                    i,
                    ConnectionRole::Primary,
                    p_cmd_rx,
                    p_event_tx,
                    p_health_tx,
                    Duration::from_secs(0),
                    p_metrics,
                )
                .await;
            });

            let (s_cmd_tx, s_cmd_rx) = channel::<WorkerCommand>(500);
            let (s_health_tx, s_health_rx) = watch::channel(ConnectionHealth::Connecting);
            let s_event_tx = event_tx.clone();
            let s_metrics = metrics.clone();
            let s_handle = tokio::spawn(async move {
                sleep(Duration::from_millis(i as u64 * 300 + 150)).await;
                run_worker(
                    i,
                    ConnectionRole::Secondary,
                    s_cmd_rx,
                    s_event_tx,
                    s_health_tx,
                    Duration::from_secs(15 * 60),
                    s_metrics,
                )
                .await;
            });
            worker_handles.push(p_handle);
            worker_handles.push(s_handle);

            feed_managers.push(FeedManager {
                worker_id: i,
                primary: ConnectionHandle {
                    role: ConnectionRole::Primary,
                    command_tx: p_cmd_tx,
                    health_rx: p_health_rx,
                },
                secondary: ConnectionHandle {
                    role: ConnectionRole::Secondary,
                    command_tx: s_cmd_tx,
                    health_rx: s_health_rx,
                },
            });
        }

        Self {
            feed_managers,
            worker_handles,
            metrics,
        }
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn create_source_selectors(&self) -> Vec<SourceSelector> {
        self.feed_managers
            .iter()
            .map(|fm| {
                SourceSelector::new(
                    fm.worker_id,
                    fm.primary.health_rx.clone(),
                    fm.secondary.health_rx.clone(),
                )
            })
            .collect()
    }

    pub async fn subscribe(&self, tokens: Vec<String>) {
        let num_workers = self.feed_managers.len();
        info!(
            "balancing {} tokens across {} workers",
            tokens.len(),
            num_workers
        );
        let mut buckets: Vec<Vec<String>> = vec![Vec::new(); num_workers];

        for token in tokens {
            let mut hasher = DefaultHasher::new();

            token.hash(&mut hasher);
            let id = (hasher.finish() as usize) % num_workers;
            buckets[id].push(token);
        }
        for (id, worker_tokens) in buckets.into_iter().enumerate() {
            if !worker_tokens.is_empty() {
                let _ = self.feed_managers[id].subscribe(worker_tokens).await;
            }
        }
    }

    pub async fn unsubscribe(&self, tokens: Vec<String>) {
        let num_workers = self.feed_managers.len();

        let mut buckets: Vec<Vec<String>> = vec![Vec::new(); num_workers];

        for token in tokens {
            let mut hasher = DefaultHasher::new();

            token.hash(&mut hasher);
            let id = (hasher.finish() as usize) % num_workers;
            buckets[id].push(token);
        }

        for (id, worker) in buckets.into_iter().enumerate() {
            if !worker.is_empty() {
                let _ = self.feed_managers[id].unsubscribe(worker).await;
            }
        }
    }

    pub async fn shutdown(mut self) {
        info!("Shutting down all {} workers...", self.feed_managers.len());
        for tx in &self.feed_managers {
            let _ = tx.shutdown().await;
        }

        for handle in self.worker_handles {
            let _ = handle.await;
        }
    }
}
