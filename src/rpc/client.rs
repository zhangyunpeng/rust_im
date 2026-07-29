use anyhow::{Result, anyhow};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

// 匹配当前生成结构
use crate::pb::CometPushServiceClient;
use crate::pb::Packet;
use crate::pb::RemotePushReq;

#[derive(Clone)]
pub struct RpcClientPool {
    pool: Arc<RwLock<HashMap<String, CometPushServiceClient<Channel>>>>,
}

impl RpcClientPool {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get_client(&self, grpc_addr: &str) -> Result<CometPushServiceClient<Channel>> {
        // let mut pool = self.pool.write().await;
        // let key = grpc_addr.to_string();
        // if let Some(cli) = pool.get(&key) {
        //     return Ok(cli.clone());
        // }
        // let channel = Channel::builder(grpc_addr.parse()?)
        //     .connect_timeout(std::time::Duration::from_secs(3))
        //     .connect()
        //     .await
        //     .map_err(|e| {
        //         anyhow!("连接远端RPC节点失败: {e}")
        //     })?;
        // let cli = CometPushServiceClient::new(channel);
        // pool.insert(key, cli.clone());
        // Ok(cli)

        let channel = Channel::builder(grpc_addr.parse()?)
            .connect_timeout(std::time::Duration::from_secs(3))
            .connect()
            .await
            .map_err(|e| {
                anyhow!("连接远端RPC节点失败: {e}")
            })?;
        let cli = CometPushServiceClient::new(channel);
        Ok(cli)
    }

    /// 远程跨网关推送
    pub async fn remote_push(&self, grpc_addr: &str, uid: i64, pkt: Packet) -> Result<bool> {
        tracing::info!("666 : {:?}", self.get_client(grpc_addr).await);
        let mut cli = self.get_client(grpc_addr).await?;
        let req = RemotePushReq {
            uid,
            packet_bin: pkt.encode_to_vec(),
        };
        tracing::info!("remote_push req: {:?}", req);
        let resp = cli.remote_push(req).await?.into_inner();
        tracing::info!("remote_push resp = {resp:?}");
        Ok(resp.success)
    }
}
