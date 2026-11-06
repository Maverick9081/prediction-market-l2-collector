use std::{collections::HashSet, str::FromStr};

use anyhow::{Error, Result, anyhow};
use chrono::{DateTime, Utc};
use log::{error, warn};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{Side, market_data::{BookSnapshot, ConnectionRole, MarketEvent, PriceDelta, QuantityUpdate}, traits::MarketDiscovery};

pub const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_MARKETS_URL: &str = "https://clob.polymarket.com/sampling-markets";


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


pub fn parse_scaled(val: &str) -> Result<u64> {
    let decimal = Decimal::from_str(val)?;

    let scaled = decimal
        .checked_mul(Decimal::from(1_000_000u64))
        .ok_or_else(|| anyhow::anyhow!("Value overflow: {}", val))?;

    let result: u64 = scaled
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid or out-of-range value: {}", val))?;

    Ok(result)
}

pub fn parse_timestamp(ts: Option<&str>) -> Result<DateTime<Utc>> {
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

pub fn parse_market_event(worker_id: usize, role: ConnectionRole, text: &str) -> Vec<MarketEvent> {
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
                            size: QuantityUpdate::Set(s),    
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


#[derive(Debug, Deserialize)]
pub struct SamplingResponse {
    pub data: Vec<Market>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MarketToken {
    pub token_id: String,
    pub outcome: String,
}

#[derive(Debug, Deserialize)]
pub struct Market {
    pub condition_id: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub accepting_orders: bool,
    #[serde(default)]
    pub tokens: Vec<MarketToken>,
}

pub struct PolyMarketDiscovery{
    pub client : Client
}

impl PolyMarketDiscovery {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl MarketDiscovery for PolyMarketDiscovery {
    async fn discover_active(&self) -> anyhow::Result<HashSet<String>> {
        fetch_active(&self.client).await.map_err(|e| anyhow!(e))
    }
}


pub async fn fetch_active(client:&Client) -> Result<HashSet<String>,Error> {

    let mut active = HashSet::new();
    let mut cursor = String::new();
    let mut seen_cursors:HashSet<String> = HashSet::new();

    loop {
       
        let mut req = client.get(POLYMARKET_MARKETS_URL);
        if !cursor.is_empty() {
            req = req.query(&[("next_cursor", cursor.as_str())]);
        }
        let res = req.send().await?.error_for_status()?;
        let sampling: SamplingResponse = res.json().await?;
        for market in sampling.data {
            if market.active && !market.closed && market.accepting_orders {
                for token in market.tokens {
                    if !token.token_id.trim().is_empty() {
                        active.insert(token.token_id);
                    }
                }
            }
        }

        match sampling.next_cursor {
            Some(c) if !c.is_empty() && c != "LTE=" => {
                if !seen_cursors.insert(c.clone()) {
                    error!("Pagination cursor repeated: {}", c);
                    break;
                }
        
                cursor = c;
            }
            _ => break
        }

    }


    Ok(active)
}