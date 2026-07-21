use axum::{Router, serve};
use rdkafka::producer::FutureProducer;
use rust_im::connect::push_consumer::start_push_consumer;
use rust_im::connect::session::handle_tcp_stream;
use rust_im::connect::state::CometState;
use rust_im::connect::ws::build_ws_router;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 日志初始化
    tracing_subscriber::fmt::init();
    tracing::info!("rust-im goim-comet 启动中...");

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
            tracing::error!("push consumer 异常退出: {}", e);
        }
    });

    // 2. TCP长连接监听 :8090
    let state_tcp = comet_state.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8090")
            .await
            .expect("tcp bind 0.0.0.0:8090 failed");
        tracing::info!("TCP 长连接监听 0.0.0.0:8090");

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!("新TCP连接: {}", addr);
                    let s = state_tcp.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_stream(stream, s).await {
                            tracing::debug!("TCP会话关闭: {}", e);
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
