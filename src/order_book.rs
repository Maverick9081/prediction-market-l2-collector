use std::{collections::{BTreeMap, HashMap}, hash::{DefaultHasher, Hash, Hasher}};

use chrono::{DateTime, Utc};

use crate::{Side, util::dtos::challenge_orderbook::{SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain}, websockets::{ConnectionRole, MarketEvent}};

pub const USDC_DENOMINATION:u64 = 1000000;

pub struct OrderBook {
    pub symbol : String,
    pub bids : BTreeMap<u64,u64>,
    pub asks : BTreeMap<u64,u64>,
    pub last_updated : Option<DateTime<Utc>>,
    pub exchange_timestamp : Option<DateTime<Utc>>,
    pub initialized: bool,
}

pub fn assign_worker(token: &str, num_workers: usize) -> usize {
    if num_workers == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    (hasher.finish() as usize) % num_workers
}


impl OrderBook {
    pub fn new(symbol : String) -> Self {
        Self { symbol, bids: BTreeMap::new(), asks: BTreeMap::new(), last_updated: None,exchange_timestamp:None,initialized:false }

    }

    pub fn apply_snapshot(&mut self,bids: Vec<(u64,u64)>,asks:Vec<(u64,u64)>,ex_ts: DateTime<Utc>) -> bool {

        // Reject stale snapshot 
        if let Some(curent_ts) = self.exchange_timestamp {
            if ex_ts < curent_ts {
                return false;
            }
        }

        self.bids.clear();

        for (price,size) in bids {
            self.bids.insert(price,size);
        }

        self.asks.clear();

        for (prize,size) in asks {
            self.asks.insert(prize,size);
        }

        self.last_updated = Some(Utc::now());
        self.exchange_timestamp = Some(ex_ts);
        self.initialized = true;
        true
    }

    pub fn apply_delta(&mut self,side : Side,price: u64,size:u64,ex_ts: DateTime<Utc>)->bool {
        if !self.initialized {
            return false;
        }

        if let Some(last_ts) = self.exchange_timestamp {
            if ex_ts < last_ts {
                return false;
            }
        }

        let side = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks
        };

        if size == 0 {
            side.remove(&price);
        }else {
            side.insert(price, size);
        }

        self.last_updated = Some(Utc::now());
        self.exchange_timestamp = Some(ex_ts);

        true
    }


    pub fn to_sbe_snapshot(&self, time : DateTime<Utc>) -> Option<SnapshotDomain> {

        if !self.initialized {
            return None;
        }
        let ex_ts = self.exchange_timestamp?;

        let bids = self.bids.iter().
                rev()
                .map(|(&price,&quantity)| SnapshotBidsEntryDomain {price_level: price as i64,quantity : quantity as i64}).collect();

        let asks = self.asks.iter()
                .map(|(&price,&quantity)| SnapshotAsksEntryDomain {price_level: price as i64,quantity : quantity as i64}).collect();

        Some(SnapshotDomain { received_at: time, exchange_timestamp: ex_ts, bids, asks, symbol: self.symbol.clone() })
    
    }
}

pub struct OrderBookManager {
    pub primary_books : HashMap<String,OrderBook>,
    pub secondary_books : HashMap<String,OrderBook>
}

impl OrderBookManager {
    pub fn new() -> Self {
        Self { primary_books: HashMap::new(), secondary_books: HashMap::new() }
    }

    pub fn insert_new_token(&mut self,token : &str) {
        self.primary_books.entry(token.to_string()).or_insert_with(|| OrderBook::new(token.to_string()));
        self.secondary_books.entry(token.to_string()).or_insert_with(|| OrderBook::new(token.to_string()));
    }

    pub fn remove_token(&mut self, token: &str) {
        self.primary_books.remove(token);
        self.secondary_books.remove(token);
    }

    pub fn len(&self) -> usize {
        self.primary_books.len()
    }

    pub fn apply_event(&mut self, role: ConnectionRole, event: &MarketEvent) -> bool{
        let books = match role {
            ConnectionRole::Primary => &mut self.primary_books,
            ConnectionRole::Secondary => &mut self.secondary_books,
        };
        match event {
            MarketEvent::Snapshot(snap) => {
                if let Some(book) = books.get_mut(&snap.asset_id) {
                    book.apply_snapshot(
                        snap.bids.clone(),
                        snap.asks.clone(),
                        snap.exchange_timestamp,
                    )
                } else {
                    false
                }
            }
            MarketEvent::Delta(delta) => {
                if let Some(book) = books.get_mut(&delta.asset_id) {
                    book.apply_delta(
                        delta.side,
                        delta.price,
                        delta.size,
                        delta.exchange_timestamp,
                    )
                } else {
                    false
                }
            }
        }
    }


    pub fn is_partition_ready(
        &self,
        worker_id: usize,
        num_workers: usize,
        role: ConnectionRole,
    ) -> bool {
        if num_workers == 0 {
            return false;
        }
        let books = match role {
            ConnectionRole::Primary => &self.primary_books,
            ConnectionRole::Secondary => &self.secondary_books,
        };
        let mut count = 0;
        let mut initialized_count = 0;
        for (token, book) in books {

            let worker = assign_worker(token, num_workers);
            if worker == worker_id {
                count += 1;
                if book.initialized {
                    initialized_count += 1;
                }
            }
        }
        // books.values().any(|b| b.initialized)
        
        count > 0 && count == initialized_count
    }

    pub fn generate_periodic_snapshots(&self, now: DateTime<Utc>,  num_workers: usize,active_roles: &[ConnectionRole]) -> Vec<SnapshotDomain> {
        if num_workers == 0 || active_roles.len() != num_workers {
            return Vec::new();
        }
        let mut snapshots = Vec::new();
       
        for token in self.primary_books.keys() {
           
            let worker_id = assign_worker(token, num_workers);
            let active_role = active_roles[worker_id];
            let authoritative_books = match active_role {
                ConnectionRole::Primary => &self.primary_books,
                ConnectionRole::Secondary => &self.secondary_books,
            };
            if let Some(book) = authoritative_books.get(token) {
                if let Some(sbe_snap) = book.to_sbe_snapshot(now) {
                    snapshots.push(sbe_snap);
                }
            }
        }
        snapshots
    }
    pub fn generate_handoff_snapshots(
        &self,
        worker_id: usize,
        num_workers: usize,
        new_role: ConnectionRole,
        now: DateTime<Utc>,
    ) -> Vec<SnapshotDomain> {
        let books = match new_role {
            ConnectionRole::Primary => &self.primary_books,
            ConnectionRole::Secondary => &self.secondary_books,
        };
        books
            .iter()
            .filter_map(|(token, book)| {

               let worker = assign_worker(token, num_workers);
                if worker == worker_id {
                    book.to_sbe_snapshot(now)
                } else {
                    None
                }
            })
            .collect()
    }
}