use axum::{Router, serve};
use rdkafka::producer::FutureProducer;
use rust_im::cache::cache;
use rust_im::cache::cache::CacheType;
use rust_im::config::AppConfig;
use rust_im::connect::push_consumer::start_push_consumer;
use rust_im::connect::session::handle_tcp_stream;
use rust_im::connect::state::CometState;
use rust_im::connect::ws::build_ws_router;
use rust_im::registry::etcd::RegistryEtcdClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 日志初始化
    fmt().with_env_filter(EnvFilter::new("info")).init();
    tracing::info!("rust-im im-comet 启动中...");

    // 2. 加载配置
    let app_cfg = AppConfig::load_with_env("./config.toml")?;

    // 3. 初始化 Kafka Producer 配置
    let kafka_producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .create()?;

    // 4. 初始化 etcd 注册中心客户端
    let mut registry_client = RegistryEtcdClient::new(&app_cfg).await?;

    // 5. 分布式模式：注册当前节点
    if app_cfg.comet.enable_distributed {
        registry_client.register().await?;
        println!("success register comet node to etcd");
    }

    let arc_app_cfg = Arc::new(app_cfg);
    // 6. 缓存初始化
    let cache_instance = cache::new_cache(CacheType::Redis, arc_app_cfg.clone())?;
    
    // 7. 构建全局Comet状态，心跳间隔30000ms
    let comet_state = CometState::new(
        kafka_producer,
        30000,
        arc_app_cfg,
        registry_client,
        cache_instance,
    );

    // 8. 优雅停机信号监听，主动注销etcd节点
    let state_clone = comet_state.clone();
    tokio::spawn(async move {
        let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("tokio singal spawn failed");
        sig.recv().await;
        println!("receive terminate signal, unregister etcd node");
        let _ = state_clone.registry.unregister().await;
        std::process::exit(0);
    });

    // 1. 启动Kafka Push消息消费协程
    let state_push = comet_state.clone();
    tokio::spawn(async move {
        if let Err(e) = start_push_consumer(state_push).await {
            tracing::info!("push consumer 异常退出: {}", e);
        }
    });

    // 2. TCP长连接监听 :8090
    let state_tcp = comet_state.clone();
    tokio::spawn(async move {
        let addr: SocketAddr = "0.0.0.0:8090".parse().expect("addr parse fail");
        let listener = TcpListener::bind(addr)
            .await
            .expect("tcp bind 0.0.0.0:8090 failed");
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let s = state_tcp.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_stream(stream, s).await {
                            tracing::info!("TCP会话关闭: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("tcp accept error: {}", e);
                }
            }
        }
    });

    // 3. WebSocket服务监听 :8091
    let app: Router = build_ws_router(comet_state.clone());
    tracing::info!("WebSocket 监听 0.0.0.0:8091");

    let ws_listener = tokio::net::TcpListener::bind("0.0.0.0:8091").await?;
    serve(ws_listener, app.into_make_service()).await?;

    Ok(())
}
