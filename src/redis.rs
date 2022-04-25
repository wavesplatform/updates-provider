//! Re-exports `redis` crate so that it can be uniformly used throughout the application

pub use bb8_redis::redis::*;

use bb8::Pool;
use bb8_redis::RedisConnectionManager;

pub type RedisPool = Pool<RedisConnectionManager>;

pub async fn new_redis_pool(redis_connection_url: String) -> Result<RedisPool, crate::Error> {
    let redis_pool_manager = RedisConnectionManager::new(redis_connection_url)?;
    let pool = Pool::builder().build(redis_pool_manager).await?;
    Ok(pool)
}
