use crate::cache::cache::CacheTrait;
use crate::config::AppConfig;
use crate::connect::room::RoomState;
use crate::pb::{Message as ImMessage, Packet};
use crate::registry::etcd::RegistryEtcdClient;
use anyhow::Result;
use dashmap::DashMap;
use prost::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// 单个链接下行通道
pub type ConnSender = UnboundedSender<Packet>;

#[derive(Clone)]
pub struct CometState {
    pub config: Arc<AppConfig>,
    pub registry: Arc<RegistryEtcdClient>,
    pub cache: Arc<Box<dyn CacheTrait>>,
    // uid => 多设备
    online: Arc<DashMap<i64, Vec<ConnSender>>>,
    room: RoomState,
    kafka_producer: Arc<FutureProducer>,
    heartbeat_ms: u64,
}

impl CometState {
    pub fn new(
        producer: FutureProducer,
        heartbeat_ms: u64,
        cfg: Arc<AppConfig>,
        registry_client: RegistryEtcdClient,
        cache: Box<dyn CacheTrait>,
    ) -> Self {
        let room = RoomState::new(cfg.clone());
        CometState {
            config: cfg,
            registry: Arc::new(registry_client),
            cache: Arc::new(cache),
            online: Arc::new(DashMap::new()),
            room,
            kafka_producer: Arc::new(producer),
            heartbeat_ms,
        }
    }

    /// 注册新链接
    pub async fn add_conn(&self, uid: i64, tx: ConnSender) -> Result<()> {
        self.online.entry(uid).or_default().push(tx);
        self.cache
            .set_user_route(
                uid,
                &self.config.comet.node_id,
                &self.config.comet.grpc_addr,
            )
            .await?;
        Ok(())
    }

    /// 移除单条链接
    pub async fn remove_conn(&self, uid: i64, tx: &ConnSender) -> Result<()> {
        let mut entry = match self.online.get_mut(&uid) {
            Some(v) => v,
            None => return Ok(()),
        };

        entry.retain(|channel| channel.same_channel(tx));
        if entry.is_empty() {
            self.online.remove(&uid);
        }
        self.cache.del_user_route(uid).await?;
        Ok(())
    }

    /// 批量推送消息给目标用户
    pub async fn push_users(&self, uids: &[i64], pkt: Packet) -> anyhow::Result<()> {
        for uid in uids {
            // 1. 本地推送
            if let Some(channels) = self.online.get(uid) {
                for channel in channels.iter() {
                    let _ = channel.send(pkt.clone());
                }
            }

            // 2. 本地无连接，查询所有在线网关， RPC转发推送
            // let all_nodes = self.registry.list_all_nodes().await?;
            // for _node_json in all_nodes {
            //     // 解析节点grpc地址，发起rpc推送
            //     // self.rpc_client.remote_push(node_json, pkt.clone()).await?;
            // }
        }
        Ok(())
    }

    /// 上行消息投递 job kafka
    pub async fn send_job_kafka(&self, msg: ImMessage) -> anyhow::Result<()> {
        let data = msg.encode_to_vec();
        let record = FutureRecord::to("im-push").payload(&data).key(b"msg");
        self.kafka_producer
            .send(record, None)
            .await
            .map_err(|(e, _)| e)?;
        Ok(())
    }

    pub fn heartbeat_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.heartbeat_ms * 2)
    }

    pub async fn push_room(&self, room_id: i64, pkt: Packet) -> anyhow::Result<()> {
        let room_uids = self.room.room_uids(room_id);
        self.push_users(&room_uids, pkt).await
    }
}
