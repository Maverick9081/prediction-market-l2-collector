use std::{collections::VecDeque, sync::{Arc, Mutex, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::{Duration, Instant}};

use log::info;

use crate::market_data::ConnectionRole;

const LATENCY_WINDOW: Duration = Duration::from_secs(5 * 60);
const MAX_LATENCY_SAMPLES: usize = 50_000;

const HISTOGRAM_BUCKETS:usize = 5000;

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
    latencies : Arc<Vec<AtomicU64>>,
    worker_latencies: Arc<Vec<Mutex<LatencyTracker>>>,
}


impl Metrics{
    pub fn new(num_workers : usize) -> Self {
        let mut trackers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            trackers.push(Mutex::new(LatencyTracker::new()));
        }

        let histogram = (0..=HISTOGRAM_BUCKETS).map(|_| AtomicU64::new(0)).collect();
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
        latencies: Arc::new(histogram),
        worker_latencies: Arc::new(trackers),
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
        let bucket = (latency_ms as usize).min(HISTOGRAM_BUCKETS);
        self.latencies[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_feed_latency(&self, worker_id: usize, role: ConnectionRole, latency_us: u64) {
        if let Some(tracker) = self.worker_latencies.get(worker_id) {
            if let Ok(mut tracker) = tracker.lock() {
                tracker.record(role, latency_us);
        }
        }
       
    }

    pub fn get_worker_median(&self, worker_id: usize, role: ConnectionRole, min_samples: usize) -> Option<u64> {
        if let Some(tracker) = self.worker_latencies.get(worker_id) {
            if let Ok(mut lock) = tracker.lock() {
                return lock.get_median(role, min_samples);
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

        let mut counts = Vec::with_capacity(HISTOGRAM_BUCKETS+1);
        let mut sum_ms = 0u128;
        let mut min_ms = None;
        let mut max_ms = 0u64;
        let mut total_samples = 0u64;

        for (ms, bucket) in self.latencies.iter().enumerate(){
            let count = bucket.swap(0, Ordering::Relaxed);
            counts.push(count);


            if count > 0 {
                total_samples += count;
                sum_ms += ms as u128 * count as u128;

                min_ms.get_or_insert(ms as u64);
                max_ms = ms as u64;
            }
        }

        if total_samples > 0 {
            let avg_ms = (sum_ms / total_samples as u128) as u64;
            let rate_per_sec = total_samples/60;

            let p50_target = (total_samples * 50).div_ceil(100);
            let p95_target = (total_samples * 95).div_ceil(100);

            let mut running = 0u64;
             let mut p50_ms = 0;
            let mut p95_ms = 0;

            for (ms ,count) in counts.iter().enumerate() {

                running += count;

                if running >= p50_target && p50_ms == 0 {
                    p50_ms = ms as u64;
                }

                if running >= p95_target {
                    p95_ms = ms as u64;
                    break;
                }
            }

            info!(
                "Active Feed Latency ({} samples, ~{}/s): min: {}ms | avg: {}ms | p50: {}ms | p95: {}ms | max: {}ms",
                total_samples, rate_per_sec, min_ms.unwrap_or(0), avg_ms, p50_ms, p95_ms, max_ms
            );
        }
        info!("========================================================");
    }
}