use anyhow::Result;
use bcrypt::{DEFAULT_COST, hash, verify};
use jsonwebtoken::{EncodingKey, Header, encode};
use sqlx::{self, MySqlPool};

use crate::model::user::{LoginReq, LoginResp, RegisterReq, RegisterRsp, User};

const JWT_SECRET: &[u8] = b"rust_im_2026";
const JWT_EXPIRE_SECONDS: i64 = 7 * 24 * 3600;

pub async fn get_user_by_name(pool: &MySqlPool, name: &str) -> Result<Option<User>> {
    let user = sqlx::query_as::<_, User>("SELECT id, username, password, nickname, create_at, update_at FROM `user` WHERE username = ?").bind(name).fetch_optional(pool).await?;
    match user {
        Some(user) => Ok(Some(user)),
        None => Ok(None),
    }
}

pub async fn user_login(pool: &MySqlPool, login_req: &LoginReq) -> Result<LoginResp> {
    let user = get_user_by_name(pool, &login_req.username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("User not found"))?;

    let ok = verify(&login_req.password, &user.password)
        .map_err(|_| anyhow::anyhow!("User wrong password"))?;
    if !ok {
        return Err(anyhow::anyhow!("User wrong password"));
    }

    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "uid": user.id,
        "exp": now + JWT_EXPIRE_SECONDS,
    });
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )?;

    Ok(LoginResp {
        uid: user.id,
        nickname: user.nickname,
        token,
    })
}

pub async fn register_user(pool: &MySqlPool, req: &RegisterReq) -> Result<RegisterRsp> {
    if get_user_by_name(pool, req.username.as_str())
        .await?
        .is_some()
    {
        return Err(anyhow::anyhow!("User already exists"));
    }

    let pwd_hash = hash(req.password.as_str(), DEFAULT_COST)?;
    let result =
        sqlx::query(r#"INSERT INTO `user` (username, password, nickname) VALUES (?, ?, ?)"#)
            .bind(req.username.as_str())
            .bind(pwd_hash)
            .bind(req.nickname.as_str())
            .execute(pool)
            .await?;

    Ok(RegisterRsp {
        id: result.last_insert_id() as i64,
    })
}
