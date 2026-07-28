use anyhow::{Result, anyhow};
use once_cell::sync::OnceCell;
use sqlx::{MySqlPool, mysql::MySqlPoolOptions};

use crate::config::MysqlConfig;

pub static GLOBAL_MYSQL_POOL: OnceCell<MySqlPool> = OnceCell::new();

pub fn get_mysql_pool() -> &'static MySqlPool {
    GLOBAL_MYSQL_POOL
        .get()
        .expect("GLOBAL MYSQL_POOL not initialized")
}

pub async fn init_mysql_pool(cfg: &MysqlConfig) -> Result<()> {
    let pool = MySqlPoolOptions::new()
        .max_connections(cfg.max_connections)
        .connect(&cfg.dsn)
        .await
        .map_err(|e| anyhow!("Failed to create mysql pool: {}", e))?;
    GLOBAL_MYSQL_POOL
        .set(pool)
        .map_err(|_| anyhow!("MySQL pool is already initialized"))?;
    Ok(())
}
