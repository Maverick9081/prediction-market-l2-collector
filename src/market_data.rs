use chrono::{DateTime, Utc};

use crate::{Side};


#[derive(Debug, Clone)]
pub enum MarketEvent {
    Snapshot(BookSnapshot),
    Delta(PriceDelta),
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub asset_id: String,
    pub bids: Vec<(u64, u64)>,
    pub asks: Vec<(u64, u64)>,
    pub exchange_timestamp: DateTime<Utc>,
}
#[derive(Debug,Clone, Copy)]
pub enum QuantityUpdate {
    Set(u64),
    Change(i64),
}

#[derive(Debug, Clone)]
pub struct PriceDelta {
    pub asset_id: String,
    pub side: Side,
    pub price: u64,
    pub size: QuantityUpdate,
    pub exchange_timestamp: DateTime<Utc>,
}

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