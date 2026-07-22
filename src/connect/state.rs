use crate::pb::Packet;
use dashmap::DashMap;
use prost::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// 单个链接下行通道
pub type ConnSender = UnboundedSender<Packet>;

#[derive(Clone)]
pub struct CometState {
    // uid => 多设备
    online: Arc<DashMap<i64, Vec<ConnSender>>>,
    kafka_producer: Arc<FutureProducer>,
    heartbeat_ms: u64,
}

impl CometState {
    pub fn new(producer: FutureProducer, heartbeat_ms: u64) -> Self {
        CometState {
            online: Arc::new(DashMap::new()),
            kafka_producer: Arc::new(producer),
            heartbeat_ms,
        }
    }

    /// 注册新链接
    pub fn add_conn(&self, uid: i64, tx: ConnSender) {
        self.online.entry(uid).or_default().push(tx);
    }

    /// 移除单条链接
    pub fn remove_conn(&self, uid: i64, tx: &ConnSender) {
        let mut entry = match self.online.get_mut(&uid) {
            Some(v) => v,
            None => return,
        };

        entry.retain(|channel| channel.same_channel(tx));
        if entry.is_empty() {
            self.online.remove(&uid);
        }
    }

    /// 批量推送消息给目标用户
    pub async fn push_users(&self, uids: &[i64], pkt: Packet) -> anyhow::Result<()> {
        for uid in uids {
            tracing::debug!("开始查询在线用户 uid={uid}");
            if let Some(channels) = self.online.get(uid) {
                for channel in channels.iter() {
                    let _ = channel.send(pkt.clone());
                }
            }
        }
        Ok(())
    }

    /// 上行消息投递 job kafka
    pub async fn send_job_kafka(&self, pkt: Packet) -> anyhow::Result<()> {
        let data = pkt.encode_to_vec();
        let record = FutureRecord::to("im-job").payload(&data).key(b"msg");
        self.kafka_producer
            .send(record, None)
            .await
            .map_err(|(e, _)| e)?;
        Ok(())
    }

    pub fn heartbeat_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.heartbeat_ms * 2)
    }
}
