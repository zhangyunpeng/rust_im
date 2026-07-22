use axum::{Router, serve};
use rdkafka::producer::FutureProducer;
use tracing_subscriber::{EnvFilter, fmt};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpSocket};

use rust_im::connect::push_consumer::start_push_consumer;
use rust_im::connect::session::handle_tcp_stream;
use rust_im::connect::state::CometState;
use rust_im::connect::ws::build_ws_router;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 日志初始化
    // tracing_subscriber::fmt::init();
    fmt().with_env_filter(EnvFilter::new("info")).init();
    tracing::info!("rust-im im-comet 启动中...");

    // Kafka Producer 配置
    let kafka_producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .create()?;

    // 构建全局Comet状态，心跳间隔30000ms
    let comet_state = CometState::new(kafka_producer, 30000);

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
        let listener = create_reuse_listener(addr).await
            .expect("tcp bind 0.0.0.0:8090 failed");
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::info!("新TCP连接: {}", addr);
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

/// 创建支持端口复用的TCP监听器
pub async fn create_reuse_listener(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let socket = TcpSocket::new_v4()?;
    // 开启 SO_REUSEADDR：允许TIME_WAIT端口立刻重新绑定
    socket.set_reuseaddr(true)?;
    #[cfg(target_family = "unix")]
    socket.set_reuseport(true)?;

    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    Ok(listener)
}
