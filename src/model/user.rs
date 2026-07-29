use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// 数据库用户模型
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password: String,
    pub nickname: String,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub create_at: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub update_at: DateTime<Utc>,
}

/// 登录请求体
#[derive(Debug, Deserialize, Serialize)]
pub struct LoginReq {
    pub username: String,
    pub password: String,
}

/// 登录返回
#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResp {
    pub uid: i64,
    pub nickname: String,
    pub token: String,
}

/// 登录请求体
#[derive(Debug, Deserialize)]
pub struct RegisterReq {
    pub username: String,
    pub password: String,
    pub nickname: String,
}

/// 登录返回
#[derive(Debug, Serialize)]
pub struct RegisterRsp {
    pub id: i64,
}
