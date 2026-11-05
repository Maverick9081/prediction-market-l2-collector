```markdown
# Latency & Transport Notes — Polymarket CLOB Collector

**Exchange:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`  
**Run:** 2.6 hours continuous production data  

---

## 1. Hardware & Operating Environment

All production tests and evaluations were run on a contabo VPS server in France with these specs:

| Parameter | System Specification |
| :--- | :--- |
| **Location / Datacenter** | Lauterbourg, Grand Est, France (`48.9751° N, 8.1785° E`) |
| **CPU** | 3 vCPUs, Intel Core (Broadwell, IBRS) @ 2.00 GHz |
| **CPU Cache** | 16 MiB L3 |
| **RAM** | 7.8 GiB (0B swap) |
| **Network** | 1 Gbps full-duplex virtual ethernet |

---

## 2. Concurrency Model

Tokio on 3 CPUs → 3 runtime worker threads.  
**15 async tasks total:**

- 12 WS workers (6 partitions × primary + secondary)
- 1 SBE writer (64 MiB chunks)
- 1 discovery loop (`/sampling-markets` every 60s)
- 1 central event loop (books, failover, snapshots)

```
              Tokio runtime (3 OS threads)
                        |
    +-------------------+-------------------+
    |                   |                   |
 OS thread 1        OS thread 2         OS thread 3
    |                   |                   |
    +-------- 12 WS workers + SBE + discovery --------+
```

---

## 3. Production Run (2.6 hours)

```
Runtime:              2h 37m 48s
Updates:              23,733,760
Active tokens:        26,890 (peak 27,376)
Dual-feed sync:       Primary 26,890 | Secondary 26,890 [YES]
Subscribed / unsub:   58,812 / 5,032
SBE output:           orderbook.sbe ~3.3 GB (52 flushes)
Recycles:             17 (30 min staggered)
Failovers:            1
Feed latency:         min 8ms | avg 10ms | p50 10ms | p95 12ms
RSS:                  212–230 MB (flat)
```

**Latency definition:** wall time when the WS text frame was read minus exchange timestamp in the message. Includes path + exchange clock; excludes internal queue / SBE write.

**Failover:** Worker 3 hit send-buffer backpressure (`08:29:43`). Secondary promoted, handoff snapshots emitted, ingestion continued on the standby feed. Health poll is 10s, so detect is bounded by that; role switch after detect is in-process and fast.

**Recycles:** 17 clean 30-minute recycles across partitions; TLS re-setup + resubscribe without process growth.

---

## 4. Connection Breakdown

Probe (`examples/latency.rs`) from the same host:

```
DNS → TCP → TLS 1.3 / WS 101 → subscribe → first book
~6ms   ~8ms      ~180–210ms           ~2ms        ~20ms
```

| Stage | Mean (5 trials) |
| :--- | :--- |
| DNS | 5.8ms (then cached) |
| TCP SYN/ACK | 8.9ms |
| TLS + WS upgrade | 194ms |
| Sub → first book | 22.6ms |
| Steady tick lag | ~12ms |

TCP to the Anycast edge is ~8ms from FR. Most of connect cost is TLS + WS handshake.

---

## 5. Extension 4 Experiments

### A. Source port / path variance

Same target, different bind ports (TCP connect RTT):

| Port | RTT |
| :--- | :--- |
| 15000 | 8.14ms |
| 25000 | 7.86ms |
| 35000 | 10.85ms |
| 45000 | 7.81ms |
| 55000 | 7.81ms |

### B. Parallel connect race (3 sockets)

I used 3 parallel connection to check latency for each port. It had arounfd ~140ms of time difference between 1st and 2nd. So parallel connections does make a difference

| Rank | First book | WS handshake | TCP |
| :--- | :--- | :--- | :--- |
| 1 | 18.8ms | 203.9ms | 8.3ms |
| 2 | 20.2ms | 344.1ms | 7.7ms |
| 3 | 24.7ms | 330.4ms | 8.7ms |




### Final thoughts

Latency tracking still uses a Mutex; something lighter would be cleaner.
More workers might help on a bigger box — with 3 CPUs I kept partitions modest on purpose.

Health check started at 250ms and starved the runtime, so I moved it to 10s.
Faster polling would mean quicker failover detect; 10s was the stable tradeoff here.

Redundant sockets help more for uptime than for shaving steady-state lag.
Most of the ~10ms p50 is path + exchange time from FR, not local processing.

