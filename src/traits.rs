use std::collections::HashSet;

use anyhow::Result;

use crate::{metrics::Metrics, order_book::SourceSelector};

pub trait MarketDiscovery:Send + Sync +'static {
    async fn discover_active(&self) -> Result<HashSet<String>>;
}


pub trait FeedPool : Send + Sync + 'static {
    async fn subscribe(&self,tokens : Vec<String>);
    async fn unsubscribe(&self,tokens : Vec<String>);
    async fn shutdown(self);
    fn metrics(&self) -> Metrics;
    fn create_source_selectors(&self) -> Vec<SourceSelector>;
}
