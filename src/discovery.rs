use std::{collections::HashSet, time::Duration};

use log::{error, info};
use reqwest::{Client, Error};
use serde::Deserialize;
use tokio::{sync::mpsc::Sender, time::interval};



const POLYMARKET_MARKETS_URL: &str = "https://clob.polymarket.com/sampling-markets";


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

#[derive(Debug, Clone)]
pub struct DiscoveryDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
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




pub async fn run_discovery(client: Client,time_interval : Duration, diff_channel_sender:Sender<DiscoveryDiff>) {

    let mut known_tokens :HashSet<String> = HashSet::new();

    let mut interval = interval(time_interval);

    loop {
        interval.tick().await;

        match fetch_active(&client).await {
            Ok(tokens) => {

                let added :Vec<String>= tokens.difference(&known_tokens).cloned().collect();

                let removed : Vec<String> = known_tokens.difference(&tokens).cloned().collect();

                info!("Adding  {} new tokens, and removing {} tokens",added.len(),removed.len());

                if !added.is_empty() || !removed.is_empty() {

                    let diff = DiscoveryDiff{ added, removed};

                    if diff_channel_sender.send(diff).await.is_err(){
                        error!("Error sending diff through chaneel");
                        break;
                    }
                }
                // sending the updated list through channel 

                known_tokens = tokens;


            }
            Err(e) => {

                error!("Failed to fetch markets : {}",e);
            }
        }
    }
}