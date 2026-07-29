use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

pub mod user_services;

const JWT_SECRET: &[u8] = b"rust_im_2026";
const JWT_EXPIRE_SECONDS: i64 = 7 * 24 * 3600;

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub uid: i64,
    pub exp: i64,
}
pub fn verify_token(token: &str) -> anyhow::Result<i64> {
    // 解码配置
    let mut validation = Validation::default();
    // 自动校验exp过期时间
    validation.validate_exp = true;

    let token_data = decode::<TokenClaims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET),
        &validation,
    )
    .map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => anyhow::anyhow!("token expired"),
        jsonwebtoken::errors::ErrorKind::InvalidSignature => {
            anyhow::anyhow!("invalid token signature")
        }
        _ => anyhow::anyhow!("token invalid: {}", e),
    })?;

    Ok(token_data.claims.uid)
}
