use anyhow::Result;
use axum::Router;
use rust_im::config::AppConfig;
use rust_im::db::mysql::init_mysql_pool;
use tracing_subscriber::EnvFilter;
// use rust_im::registry::etcd::RegistryEtcdClient;
use rust_im::route::user as userRoute;

#[tokio::main]
async fn main() -> Result<()> {
    // 1 日志
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();
    tracing::info!("rust im API starting...");

    // 2. 加载配置
    let app_cfg = AppConfig::load_with_env("./config.toml")?;

    // 3. 初始化 etcd 注册中心客户端
    // let mut registry_client = RegistryEtcdClient::new(&app_cfg).await?;

    // 4. 初始化 mysql
    let arc_mysql_cfg = app_cfg.clone();
    init_mysql_pool(&arc_mysql_cfg.mysql).await?;

    // 5. http
    let router = Router::new();
    let router = router.merge(userRoute::user_routes());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, router).await?;

    Ok(())
}
