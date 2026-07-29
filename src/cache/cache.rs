use crate::cache::cache_redis::CacheRedis;
use crate::config::AppConfig;
use anyhow::Result;
use futures::future::BoxFuture;
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub trait CacheTrait: Send + Sync + 'static {
    fn set_user_route<'a>(
        &'a self,
        uid: i64,
        node_id: &'a str,
        grpc_addr: &'a str,
    ) -> BoxFuture<'a, Result<()>>;
    fn del_user_route(&self, uid: i64) -> BoxFuture<'_, Result<()>>;
    fn get_user_route(&self, uid: i64) -> BoxFuture<'_, Result<serde_json::Value>>;
    fn report_node_heartbeat<'a>(&'a self, node_id: &'a str) -> BoxFuture<'a, Result<()>>;
}

#[derive(Debug, Clone)]
pub enum CacheType {
    Redis,
}

pub fn new_cache(
    typ: CacheType,
    app_config: Arc<AppConfig>,
) -> anyhow::Result<Box<dyn CacheTrait>> {
    let instance: Box<dyn CacheTrait> = match typ {
        CacheType::Redis => {
            let cl = RedisClient::open(app_config.redis.addr.as_str())?;
            Box::new(CacheRedis::new(
                cl,
                app_config.redis.online_ttl_sec,
                app_config.redis.online_ttl_sec,
            ))
        }
    };

    Ok(instance)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RouteInfo {
    pub node_id: String,
    pub listen_addr: String,
    pub grpc_addr: String,
}
