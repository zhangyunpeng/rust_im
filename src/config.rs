use serde::Deserialize;
use std::time::Duration;

/// 全局应用总配置
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// 网关基础配置
    pub comet: CometConfig,
    /// Redis 分布式缓存/路由存储
    pub redis: RedisConfig,
    /// Kafka 消息队列
    pub kafka: KafkaConfig,
    /// 注册中心 Etcd/Nacos 服务发现
    pub registry: RegistryConfig,
    /// 分布式房间本地缓存策略
    pub room_cache: RoomCacheConfig,
}

/// Comet网关基础配置
#[derive(Debug, Deserialize, Clone)]
pub struct CometConfig {
    /// 网关唯一节点ID，分布式区分不同实例
    pub node_id: String,
    /// TCP/WebSocket 监听地址
    pub listen_addr: String,
    /// gRPC 跨节点推送监听端口
    pub grpc_addr: String,
    /// 心跳超时毫秒
    pub heartbeat_ms: u64,
    /// 是否开启分布式模式 false=单机模式
    pub enable_distributed: bool,
}

/// Redis 配置（全局在线路由、房间成员存储）
#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    /// redis连接地址，集群多个用逗号分隔
    pub addr: String,
    pub password: Option<String>,
    pub db: u8,
    /// 连接池大小
    pub pool_size: u32,
    /// 全局key过期时间（在线用户TTL 秒）
    pub online_ttl_sec: u64,
}

/// Kafka 生产者配置（上行消息、推送任务队列）
#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    /// broker地址，多节点逗号分隔
    pub brokers: String,
    /// 上行消息投递topic
    pub up_topic: String,
    /// 分布式推送任务topic
    pub push_task_topic: String,
    /// kafka生产队列缓冲区大小
    pub queue_buffering_max_messages: usize,
}

/// 注册中心 Etcd 配置（服务发现、节点心跳）
#[derive(Debug, Deserialize, Clone)]
pub struct RegistryConfig {
    pub endpoints: String,
    /// 节点心跳上报间隔 毫秒
    pub heartbeat_interval_ms: u64,
    /// 服务注册前缀
    pub service_prefix: String,
}

/// 分布式房间本地一级缓存策略
#[derive(Debug, Deserialize, Clone)]
pub struct RoomCacheConfig {
    /// 本地缓存最大房间数量，LRU淘汰
    pub max_local_room: usize,
    /// 本地缓存同步Redis间隔 秒
    pub sync_redis_interval_sec: u64,
    /// 空房间自动删除开关
    pub auto_clear_empty_room: bool,
}

impl AppConfig {
    /// 从toml文件加载配置
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let cfg: Self = toml::from_str(&content)?;
        Ok(cfg)
    }

    pub fn load_with_env(path: &str) -> anyhow::Result<Self> {
        let mut cfg = Self::load(path)?;
        if let Ok(node_id) = std::env::var("COMET_NODE_ID") {
            cfg.comet.node_id = node_id;
        };
        Ok(cfg)
    }

    /// 转换心跳超时Duration
    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.comet.heartbeat_ms * 2)
    }
}
