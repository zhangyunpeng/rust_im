use anyhow::{Result, anyhow};
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper, PutOptions};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::{AppConfig, RegistryConfig};

#[derive(Clone)]
pub struct RegistryEtcdClient {
    cfg: RegistryConfig,
    node_id: String,
    client: Arc<Mutex<Client>>,
    lease_id: i64,
    // 心跳保活句柄
    keeper: Arc<Mutex<Option<LeaseKeeper>>>,
}

impl RegistryEtcdClient {
    pub async fn new(app_cfg: &AppConfig) -> Result<Self> {
        let reg_cfg = &app_cfg.registry;
        let node_id = app_cfg.comet.node_id.clone();
        let endpoints: Vec<&str> = reg_cfg.endpoints.split(",").collect();
        let client = Client::connect(endpoints, None).await?;
        Ok(Self {
            cfg: reg_cfg.clone(),
            node_id,
            client: Arc::new(Mutex::new(client)),
            lease_id: 0,
            keeper: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn register(&mut self) -> Result<()> {
        let ttl = (self.cfg.heartbeat_interval_ms / 1000) as i64 * 3;
        let mut client = self.client.lock().await;

        // 创建租约
        let lease_resp = client.lease_grant(ttl, None).await?;
        self.lease_id = lease_resp.id();

        drop(client);

        // 注册 key
        let register_key = format!("{}{}", self.cfg.service_prefix, self.node_id);
        let node_data = self.build_node_info();
        let put_opt = PutOptions::new().with_lease(self.lease_id);
        let mut client = self.client.lock().await;
        client.put(register_key, node_data, Some(put_opt)).await?;
        drop(client);

        // 心跳续租
        let mut client = self.client.lock().await;
        let (keeper, stream) = client.lease_keep_alive(self.lease_id).await?;
        drop(client);
        let mut self_keeper = self.keeper.lock().await;
        *self_keeper = Some(keeper);
        drop(self_keeper);
        tokio::spawn(Self::keep_alive_task(stream));

        Ok(())
    }

    fn build_node_info(&self) -> String {
        format!(
            r#"{{"node_id":"{}","listen_addr":"{}","grpc_addr":"{}"}}"#,
            self.node_id,
            // todo!
            "".to_string(),
            "".to_string()
        )
    }

    /// 续租后台任务
    async fn keep_alive_task(mut stream: LeaseKeepAliveStream) {
        while let Some(resp) = stream.message().await.transpose() {
            match resp {
                Ok(_) => continue,
                Err(e) => {
                    tracing::error!("keep etcd alive task {:?}", e);
                    break;
                }
            }
        }
        tracing::info!("etcd lease task exit, node will be offline");
    }

    pub async fn get_remote_node(&self, node_id: &str) -> Result<String> {
        let key = format!("{}{}", self.cfg.service_prefix, node_id);
        let mut client = self.client.lock().await;
        let resp = client.get(key, None).await?;
        drop(client);
        let kv = resp
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("node not found"))?;
        Ok(kv.value_str()?.to_string())
    }

    pub async fn list_all_nodes(&self) -> Result<Vec<String>> {
        let prefix = &self.cfg.service_prefix;
        let mut client = self.client.lock().await;
        // 关键修复：使用 GetOptions 开启前缀匹配
        let get_opt = etcd_client::GetOptions::new().with_prefix();
        let resp = client.get(prefix.as_str(), Some(get_opt)).await?;
        drop(client);

        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            nodes.push(kv.value_str()?.to_string());
        }
        Ok(nodes)
    }

    /// 主动注销节点（优雅停机）
    pub async fn unregister(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.lease_revoke(self.lease_id).await?;
        Ok(())
    }
}
