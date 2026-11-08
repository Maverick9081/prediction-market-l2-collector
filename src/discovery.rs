use std::{collections::HashSet, time::Duration};
use log::{error, info};
use tokio::{sync::mpsc::Sender, time::interval};

use crate::{ traits::MarketDiscovery};


#[derive(Debug, Clone)]
pub struct DiscoveryDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

pub async fn run_discovery<D:MarketDiscovery>(discovery:D,time_interval : Duration, diff_channel_sender:Sender<DiscoveryDiff>) {

    let mut known_tokens :HashSet<String> = HashSet::new();
    let mut interval = interval(time_interval);

    loop {
        interval.tick().await;

        match discovery.discover_active().await {
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
                known_tokens = tokens;

            }
            Err(e) => {

                error!("Failed to fetch markets : {}",e);
            }
        }
    }
}