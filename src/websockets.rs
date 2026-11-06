use std::{
    collections::HashSet, hash::{DefaultHasher, Hash, Hasher}, sync::atomic::Ordering, time::{Duration},
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, channel},
        watch::{self, Receiver as WatchReceiver},
    }, task::JoinHandle, time::{Instant, interval, interval_at, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{ market_data::{ConnectionRole, MarketEvent}, metrics::Metrics, order_book::SourceSelector, polymarket::{POLYMARKET_WS_URL, parse_market_event}, traits::FeedPool};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    Connecting,
    Connected,
    Disconnected,
    Recycling,
}

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
        let (ws_stream, _) = match connect_async(POLYMARKET_WS_URL).await {
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


impl FeedPool for Pool {
    async  fn subscribe(&self,tokens : Vec<String>)  {
        self.subscribe(tokens).await;
    }
    async fn unsubscribe(&self,tokens : Vec<String>) {
        self.unsubscribe(tokens).await;
    }

    async fn shutdown(self) {
        self.shutdown().await;
    }

    fn metrics(&self) -> Metrics {
        self.metrics().clone()
    }

    fn create_source_selectors(&self) -> Vec<SourceSelector> {
        self.create_source_selectors()
    }
}