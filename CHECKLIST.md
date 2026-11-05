# Production-Readiness Bug Fix & Hardening Checklist

**Target Codebase:** `~/engineering-challenge-template`  
**Review Focus:** Order-Book Correctness, Failover Reliability, Latency Metrics Trustworthiness, and Structured Task Supervision  
**Author:** Voluntas Engineering Challenge  

---

## Priority 1: Critical Order-Book Correctness (Data Integrity)

These bugs directly affect data validity and can corrupt emitted order books or cause desynchronization.

- [ ] **1.1 Stop Initializing Books From Deltas (`src/order_book.rs`)**
  - **Issue:** `apply_delta()` sets `self.initialized = true`. If a delta arrives before the initial snapshot, an incomplete book is marked valid.
  - **Fix:** 
    - Check `if !self.initialized { return false; }` at the top of `apply_delta()`.
    - Enforce timestamp monotonicity: `if ex_ts < last_ts { return false; }`.
    - Only `apply_snapshot()` should ever transition `initialized` to `true`.
  - **Target Function:** `OrderBook::apply_delta()`
  - **Verification:** Unit test where a delta arrives on an uninitialized book; verify delta is rejected and book remains uninitialized.

- [ ] **1.2 Fix Partition Readiness Check (`src/order_book.rs`)**
  - **Issue:** `is_partition_ready()` currently uses `books.values().any(|b| b.initialized)`, returning `true` if even a single token in the partition has a snapshot.
  - **Fix:**
    - Iterate over all tokens belonging to `worker_id` (using the partition hash).
    - If any assigned token has `!book.initialized`, return `false`.
    - Ensure `has_tokens == true` before returning `true`.
  - **Target Function:** `OrderBookManager::is_partition_ready()`
  - **Verification:** Unit test with 10 tokens assigned to partition: 9 initialized, 1 uninitialized $\to$ must return `false`.

- [ ] **1.3 Centralize Partition Hashing Logic (`src/order_book.rs`, `src/websockets.rs`)**
  - **Issue:** Token-to-worker hash formula `(hasher.finish() as usize) % num_workers` is duplicated across multiple functions with potential hasher divergence.
  - **Fix:**
    - Create a single deterministic helper:
      ```rust
      pub fn assign_worker(token: &str, num_workers: usize) -> usize {
          let mut hasher = std::hash::DefaultHasher::new();
          std::hash::Hash::hash(token, &mut hasher);
          (std::hash::Hasher::finish(&hasher) as usize) % num_workers
      }
      ```
    - Use this helper consistently in `websockets.rs`, `order_book.rs`, and `run.rs`.

- [ ] **1.4 Stale Snapshot Rejection & Level Validation (`src/order_book.rs`)**
  - **Issue:** Snapshots with timestamps older than the newest applied delta can overwrite fresh book state. Snapshots can also contain negative or zero-sized levels.
  - **Fix:**
    - Reject snapshots where `ex_ts < current_ts`.
    - Sanitize snapshot levels: filter out `size == 0`, invalid prices, and verify `best_bid < best_ask` (no crossed books).
  - **Target Function:** `OrderBook::apply_snapshot()`

---

## Priority 2: Latency-Based Source Selection & Metrics Hardening

These bugs distort feed latency measurements and can trigger false or unstable source switches.

- [ ] **2.1 Reject Negative Latencies (`src/run.rs`, `src/metrics.rs`)**
  - **Issue:** `(received_at - ex_ts).max(Some(0)).unwrap_or(0)` turns negative clock skew into `0 µs`, making an out-of-sync feed appear instantaneously fast.
  - **Fix:**
    ```rust
    if let Some(latency_us) = received_at
        .signed_duration_since(delta.exchange_timestamp)
        .num_microseconds()
        .filter(|&lat| lat >= 0 && lat < 10_000_000) // Filter negative & > 10s outliers
    {
        metrics.record_feed_latency(worker_id, role, latency_us as u64);
    }
    ```

- [ ] **2.2 Separate Wire Latency from Book-Eligible Latency (`src/run.rs`)**
  - **Issue:** Latency samples are recorded before checking if the event was accepted by the book state machine.
  - **Fix:**
    - Only pass samples to `metrics.record_feed_latency()` if `applied == true`.
    - Keep general network observation counters separate from switching metrics.

- [ ] **2.3 Eliminate Global Latency Mutex Contention (`src/metrics.rs`)**
  - **Issue:** All 12 workers acquire a single `Arc<Mutex<Vec<LatencyTracker>>>`. Calculating medians copies and sorts 50,000 samples under the lock every health tick.
  - **Fix:**
    - Shard trackers: give each worker partition its own lock: `Vec<Mutex<LatencyTracker>>`.
    - Decouple median calculation: update cached percentiles every 5 seconds rather than sorting on every 250ms tick.
    - Reduce buffer size or use a fixed circular ring buffer.

- [ ] **2.4 Gating Latency Stability Timer by Candidate Readiness (`src/run.rs`, `SourceSelector`)**
  - **Issue:** The 1-hour superiority timer starts even when the candidate's books are not yet initialized, allowing immediate promotion once snapshots arrive.
  - **Fix:**
    - In `SourceSelector`, require:
      1. Candidate is `Connected`.
      2. Candidate partition is fully ready (`is_partition_ready == true`).
      3. Candidate has $\ge N$ fresh valid latency samples.
      4. Candidate $p50$ is $\ge 10\%$ faster than active $p50$.
    - If ANY condition fails, explicitly reset `better_since = None`.

- [ ] **2.5 Align Switching Threshold & Log Messages (`src/run.rs`, `SourceSelector`)**
  - **Issue:** Log claims `>= 10% faster`, while internal check used a 5% threshold.
  - **Fix:** Standardize constant `const LATENCY_SUPERIORITY_THRESHOLD: f64 = 0.10;` (10%) across code and logs.

---

## Priority 3: Concurrency, Shutdown Deadlock & Error Propagation

These issues cause silent failures or hangs during process termination and I/O pressure.

- [ ] **3.1 Prevent Shutdown Deadlock Under Full Channels (`src/run.rs`)**
  - **Issue:** If `event_rx` is full, workers block on `send().await`. Calling `ws_pool.shutdown().await` deadlocks because workers cannot receive the shutdown command.
  - **Fix:**
    - Concurrently drain `event_rx` in a loop while awaiting `ws_pool.shutdown()`.
    - Signal workers via a broadcast / watch channel, then drain remaining messages until workers terminate.

- [ ] **3.2 Supervise Tasks in `main.rs` With Cancellation Tokens (`src/main.rs`)**
  - **Issue:** `tokio::spawn(run_sbe_writer)` and `tokio::spawn(run_discovery)` handles are discarded. If either task crashes, the main loop runs unaware.
  - **Fix:**
    - Store `JoinHandle`s for discovery and writer.
    - Pass a `CancellationToken` (from `tokio_util`) or watch channel to signal graceful cancellation.
    - Implement the structured shutdown order:
      1. Cancel discovery task $\to$ join.
      2. Shutdown WebSocket pool while draining `event_rx` $\to$ join workers.
      3. Send `WriterCommand::Flush` $\to$ join writer task.
      4. Exit process with code 0.

- [ ] **3.3 Propagate Fatal SBE Writer Channel Errors (`src/run.rs`, `src/writer.rs`)**
  - **Issue:** `let _ = writer_tx.send(...)` silently drops write failures if the writer channel closes.
  - **Fix:**
    ```rust
    if let Err(e) = writer_tx.send(WriterCommand::Delta(sbe_delta)).await {
        error!("FATAL: SBE writer channel closed! Stopping collector: {}", e);
        return Err(anyhow::anyhow!("SBE writer unavailable"));
    }
    ```
  - **In `src/writer.rs`:** Propagate file write and flush errors back or panic safely rather than silently continuing.

---

## Priority 4: Discovery & REST API Hardening

- [ ] **4.1 Use Strongly-Typed Query Parameters (`src/discovery.rs`)**
  - **Issue:** String formatting `format!("{}?next_cursor={}", URL, cursor)` can fail on special characters.
  - **Fix:**
    ```rust
    let mut req = client.get(POLYMARKET_MARKETS_URL);
    if !cursor.is_empty() {
        req = req.query(&[("next_cursor", &cursor)]);
    }
    let res = req.send().await?.error_for_status()?;
    ```

- [ ] **4.2 Cursor Infinite Loop Detection (`src/discovery.rs`)**
  - **Issue:** If the API cycles `A -> B -> C -> A`, discovery enters an infinite loop.
  - **Fix:** Maintain `let mut seen_cursors: HashSet<String> = HashSet::new();`. If `!seen_cursors.insert(next.clone())`, terminate pagination with a warning.

- [ ] **4.3 Explicit HTTP Client Timeouts (`src/discovery.rs`)**
  - **Issue:** Default `Client::new()` can hang indefinitely on network stalls.
  - **Fix:**
    ```rust
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()?;
    ```

- [ ] **4.4 Token ID Format Validation (`src/discovery.rs`)**
  - **Issue:** Malformed API responses can inject empty or invalid token IDs into the subscription set.
  - **Fix:** Verify `!token.is_empty()` and token consists solely of numeric digits / valid hex characters before inserting into the token list.

---

## Priority 5: Metrics & Telemetry Accuracy

- [ ] **5.1 Independent Primary & Secondary Subscription Counters (`src/metrics.rs`)**
  - **Issue:** `let primary_subs = current_subs / 2;` guesses subscription parity. If secondary fails to subscribe, the report lies.
  - **Fix:** Add `pub primary_subbed: AtomicUsize` and `pub secondary_subbed: AtomicUsize`, incremented explicitly on confirmed WebSocket subscriptions.

- [ ] **5.2 Remove 40 MB Memory Waste (`ARRAY_SIZE = 5_000_000`) (`src/metrics.rs`)**
  - **Issue:** Allocates an unneeded 40 MB vector and stops recording after 5M elements.
  - **Fix:** Replace with a compact bounded ring buffer (e.g. 5,000 samples) or rolling exponential moving average.

- [ ] **5.3 Accurate Event Rate Per Second (`src/metrics.rs`)**
  - **Issue:** `count / 60` assumes an exact 60-second window.
  - **Fix:** Track `let elapsed = last_report_time.elapsed();` and compute `count as f64 / elapsed.as_secs_f64()`.

---

## Priority 6: SBE Serialization Safety & Logger Efficiency

- [ ] **6.1 Checked Integer Conversion in SBE Encoding (`src/order_book.rs`)**
  - **Issue:** `price as i64` and `quantity as i64` wrap silently if values exceed `i64::MAX`.
  - **Fix:** Use `i64::try_from(price)?` and `i64::try_from(quantity)?` to reject overflows deterministically.

- [ ] **6.2 Preallocate Snapshot Vectors (`src/order_book.rs`, `src/run.rs`)**
  - **Issue:** `Vec::new()` causes multiple dynamic reallocations during 27,000-token periodic snapshots.
  - **Fix:** `Vec::with_capacity(num_tokens)` and chunk writes into the SBE channel.

- [ ] **6.3 Remove Synchronous Disk Flush on Every Log Line (`src/logger.rs`)**
  - **Issue:** `file.flush()` on every message causes thread stalls on Tokio runtime workers.
  - **Fix:** Wrap in `std::io::BufWriter` and flush periodically or during shutdown.
  - **Propagate errors:** Don't ignore `init_logger()` failure in `main.rs`.

---

## Priority 7: Required Verification Test Suite (`tests/`)

Implement automated Rust integration tests in `tests/correctness_tests.rs`:

| Test Name | Action Tested | Expected Assertion |
| :--- | :--- | :--- |
| `test_delta_before_snapshot_rejected` | Feed delta to uninitialized book | Returns `false`, `initialized == false`, levels empty |
| `test_stale_delta_rejected` | Send delta with `ex_ts < last_ts` | Returns `false`, levels unchanged |
| `test_stale_snapshot_rejected` | Send snapshot with `ex_ts < delta_ts` | Snapshot rejected, current book preserved |
| `test_partition_readiness_all_tokens` | 9 of 10 partition tokens initialized | `is_partition_ready()` returns `false` |
| `test_negative_latency_rejected` | Inject `ex_ts` in future of local clock | No latency sample recorded, metrics clean |
| `test_latency_timer_resets_on_unready` | Candidate faster but partition unready | `better_since` remains `None`, no switch |
| `test_latency_timer_resets_on_disconnect`| Candidate disconnects at $t=59\text{m}$ | Timer resets to zero immediately |
| `test_writer_channel_closed_fatal` | Drop writer receiver | `run()` returns `Err(WriterUnavailable)` |
| `test_shutdown_full_channel_no_deadlock` | Fill event channel and trigger shutdown | Exits cleanly in $< 500\text{ms}$ |
| `test_discovery_cursor_loop_detection` | Feed recurring cursor `A -> B -> A` | Returns cursor loop error, no hang |
| `test_sbe_integer_overflow_rejected` | Level with price $> \text{i64::MAX}$ | Handled gracefully with error, no wrap |
