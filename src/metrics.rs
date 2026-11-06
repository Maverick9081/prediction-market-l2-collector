use std::{collections::VecDeque, sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};

use log::info;

use crate::market_data::ConnectionRole;


pub const ARRAY_SIZE :usize = 5000000;

const LATENCY_WINDOW: Duration = Duration::from_secs(5 * 60);
const MAX_LATENCY_SAMPLES: usize = 50_000;

#[derive(Default,Debug,Clone)]
pub struct LatencyTracker {
    primary: VecDeque<(Instant, u64)>,
    secondary: VecDeque<(Instant, u64)>,
}

impl LatencyTracker {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &mut self,
        role: ConnectionRole,
        latency_us: u64,
    ) {
        let samples = match role {
            ConnectionRole::Primary => &mut self.primary,
            ConnectionRole::Secondary => &mut self.secondary,
        };

        samples.push_back((Instant::now(), latency_us));

        while samples.len() > MAX_LATENCY_SAMPLES {
            samples.pop_front();
        }
    }

    pub fn get_median(&mut self, role: ConnectionRole, min_samples: usize) -> Option<u64> {
        let now = Instant::now();
        let samples = match role {
            ConnectionRole::Primary => &mut self.primary,
            ConnectionRole::Secondary => &mut self.secondary,
        };
    
        while let Some(&(timestamp, _)) = samples.front() {
            if now.duration_since(timestamp) > LATENCY_WINDOW {
                samples.pop_front();
            } else {
                break;
            }
        }
        
        if samples.len() < min_samples {
            return None;
        }
        // 3. Compute P50 (median)
        let mut values: Vec<u64> = samples.iter().map(|&(_, v)| v).collect();
        values.sort_unstable();
        Some(values[values.len() / 2])
    }
}


#[derive(Debug,Clone)]
pub struct Metrics {

    pub current_subbed : Arc<AtomicUsize>,
    pub total_unsubbed : Arc<AtomicUsize>,
    pub failed_subs : Arc<AtomicUsize>,
    pub failed_unsubs : Arc<AtomicUsize>,
    pub total_reconnects : Arc<AtomicUsize>,
    pub total_subbed : Arc<AtomicUsize>,
    pub total_updates: Arc<AtomicUsize>,
    pub total_switches: Arc<AtomicUsize>,  
    pub total_recycles: Arc<AtomicUsize>, 
    latencies : Arc<Mutex<Vec<u64>>>,
    worker_latencies: Arc<Mutex<Vec<LatencyTracker>>>,
}


impl Metrics{
    pub fn new(num_workers : usize) -> Self {
        let mut trackers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            trackers.push(LatencyTracker::new());
        }
      Self{  
        current_subbed: Arc::new(AtomicUsize::new(0)),
        total_unsubbed: Arc::new(AtomicUsize::new(0)),
        failed_subs: Arc::new(AtomicUsize::new(0)),
        failed_unsubs: Arc::new(AtomicUsize::new(0)),
        total_subbed: Arc::new(AtomicUsize::new(0)),
        total_reconnects: Arc::new(AtomicUsize::new(0)),
        total_updates: Arc::new(AtomicUsize::new(0)),
        total_switches: Arc::new(AtomicUsize::new(0)),
        total_recycles: Arc::new(AtomicUsize::new(0)),
        latencies: Arc::new(Mutex::new(Vec::with_capacity(ARRAY_SIZE))),
        worker_latencies: Arc::new(Mutex::new(trackers)),
    }}


    pub fn on_subscribe_success(&self, count: usize) {
        self.current_subbed.fetch_add(count, Ordering::Relaxed);
        self.total_subbed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn on_subscribe_failure(&self, count: usize) {
        self.failed_subs.fetch_add(count, Ordering::Relaxed);
    }

    pub fn on_unsubscribe_success(&self, count: usize) {
     
        let _ = self.current_subbed.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some(cur.saturating_sub(count))
        });
        self.total_unsubbed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn on_unsubscribe_failure(&self, count: usize) {
        self.failed_unsubs.fetch_add(count, Ordering::Relaxed);
    }
    pub fn on_reconnect(&self){
        self.total_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self,latency_ms :u64) {

        self.total_updates.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut lock) = self.latencies.lock() {
            if lock.len() < ARRAY_SIZE {
                lock.push(latency_ms);
            }
        }
    }

    pub fn record_feed_latency(&self, worker_id: usize, role: ConnectionRole, latency_us: u64) {
        if let Ok(mut trackers) = self.worker_latencies.lock() {
            if let Some(tracker) = trackers.get_mut(worker_id) {
                tracker.record(role, latency_us);
            }
        }
    }

    pub fn get_worker_median(&self, worker_id: usize, role: ConnectionRole, min_samples: usize) -> Option<u64> {
        if let Ok(mut trackers) = self.worker_latencies.lock() {
            if let Some(tracker) = trackers.get_mut(worker_id) {
                return tracker.get_median(role, min_samples);
            }
        }
        None
    }

    
    pub fn print_report(&self, unique_tokens: usize) {
        let current_subs = self.current_subbed.load(Ordering::Relaxed);

        let expected_socket_subs = unique_tokens * 2;
        let in_sync = current_subs == expected_socket_subs;
        let primary_subs = current_subs / 2;
        let secondary_subs = current_subs - primary_subs;


        info!("=================== TELEMETRY REPORT ===================");
        info!(
            "Active Universe: {} tokens | Redundant Feeds: [Primary: {} | Secondary: {}] | [Sync: {}]",
            unique_tokens,
            primary_subs,
            secondary_subs,
            if in_sync { "YES" } else { "MISMATCH" }
        );
        info!(
            "Lifetime Stats: Total Updates: {} | Subbed: {} | Unsubbed: {} | Reconnects: {} | Failovers: {} | Recycles: {}",
            self.total_updates.load(Ordering::Relaxed),
            self.total_subbed.load(Ordering::Relaxed),
            self.total_unsubbed.load(Ordering::Relaxed),
            self.total_reconnects.load(Ordering::Relaxed),
            self.total_switches.load(Ordering::Relaxed),
            self.total_recycles.load(Ordering::Relaxed),
        );
        let mut samples = {
            if let Ok(mut lock) = self.latencies.lock() {
                std::mem::take(&mut *lock)
            } else {
                Vec::new()
            }
        };
        if !samples.is_empty() {
            samples.sort_unstable();
            let count = samples.len();
            let min_ms = samples[0];
            let max_ms = samples[count - 1];
           
            let avg_ms = (samples.iter().map(|&s| s as u128).sum::<u128>() / count as u128) as u64;
            let p50_ms = samples[count * 50 / 100];
            let p95_ms = samples[count * 95 / 100];
            let rate_per_sec = count / 60;
            info!(
                "Active Feed Latency ({} samples, ~{}/s): min: {}ms | avg: {}ms | p50: {}ms | p95: {}ms | max: {}ms",
                count, rate_per_sec, min_ms, avg_ms, p50_ms, p95_ms, max_ms
            );
        } else {
            info!("Active Feed Latency: No price deltas recorded in this 60s window");
        }
        info!("========================================================");
    }
}