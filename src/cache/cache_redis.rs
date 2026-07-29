use super::cache::CacheTrait;
use super::cache::RouteInfo;
use crate::registry::etcd::NodeInfo;
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use redis::AsyncCommands;
use serde::Serialize;
use serde::de::Unexpected::Str;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct CacheRedis {
    redis_client: Arc<redis::Client>,
    ttl_user_route: u64,
    ttl_node_heartbeat: u64,
}

impl CacheRedis {
    pub fn new(client: redis::Client, ttl_user_route: u64, ttl_node_heartbeat: u64) -> Self {
        Self {
            redis_client: Arc::new(client),
            ttl_user_route,
            ttl_node_heartbeat,
        }
    }

    fn user_route_key(&self, uid: i64) -> String {
        format!("users:route:{}", uid)
    }

    async fn get_conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow!("redis connect fail: {}", e))
    }
}

impl CacheTrait for CacheRedis {
    fn set_user_route<'a>(
        &'a self,
        uid: i64,
        node_id: &'a str,
        grpc_addr: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut conn = self.get_conn().await?;
            let key = self.user_route_key(uid);
            let info = RouteInfo {
                node_id: node_id.to_string(),
                grpc_addr: grpc_addr.to_string(),
                listen_addr: String::new(),
            };
            let val = serde_json::to_string(&info)?;
            let () = AsyncCommands::set_ex(&mut conn, key, val, self.ttl_user_route).await?;
            Ok(())
        })
    }

    fn del_user_route(&self, uid: i64) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut conn = self.get_conn().await?;
            let key = self.user_route_key(uid);
            let () = AsyncCommands::del(&mut conn, key).await?;
            Ok(())
        })
    }

    fn get_user_route(&self, uid: i64) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let mut conn = self.get_conn().await?;
            let key = self.user_route_key(uid);
            let val: Option<String> = AsyncCommands::get(&mut conn, key).await?;
            match val {
                None => Ok(json!({})),
                Some(s) => Ok(serde_json::from_str(&s)?),
            }
        })
    }

    fn report_node_heartbeat<'a>(&'a self, node_id: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut conn = self.get_conn().await?;
            let key = format!("comet:node:{}", node_id);
            let () = AsyncCommands::set_ex(&mut conn, key, "", self.ttl_node_heartbeat).await?;
            Ok(())
        })
    }
}
