use axum::{extract::State, http::StatusCode, response::Json, routing::post, Router};
use sqlx::MySqlPool;

use crate::model::user::{LoginReq, RegisterReq};
use crate::service::user_services::{register_user, user_login};
use crate::db::mysql::get_mysql_pool;

#[derive(Clone)]
pub struct AppState {
    pub mysql_pool: MySqlPool,
}

impl AppState {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        AppState { mysql_pool }
    }
}


pub fn user_routes() -> Router {
    let rs = Router::new()
        .route("/login", post(login_handler))
        .route("/register", post(register_handler));
    Router::new().nest("/user", rs)
}

async fn login_handler(Json(req): Json<LoginReq>) -> (StatusCode, Json<serde_json::Value>) {
    match user_login(get_mysql_pool(), &req).await {
        Ok(resp) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "code": 0,
                "msg": "success",
                "data": resp
            })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": -1,
                "msg": e.to_string(),
                "data": null
            })),
        ),
    }
}

async fn register_handler(Json(req): Json<RegisterReq>) -> (StatusCode, Json<serde_json::Value>) {
    match register_user(get_mysql_pool(), &req).await {
        Ok(resp) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "code": 0,
                "msg": "success",
                "data": resp
            })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": -1,
                "msg": e.to_string(),
                "data": null
            })),
        ),
    }
}