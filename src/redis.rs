//! Re-exports `redis` crate so that it can be uniformly used throughout the application

pub use deadpool_redis::redis::aio::Connection as RedisConnection;
pub use deadpool_redis::redis::*;
pub use deadpool_redis::Connection as RedisPoolConnection;
pub use deadpool_redis::CreatePoolError as RedisPoolCreateError;
pub use deadpool_redis::Pool as RedisPool;
pub use deadpool_redis::PoolError as RedisPoolError;

use deadpool_redis::{Config, Runtime};

use async_trait::async_trait;
use prometheus::IntGauge;

pub async fn new_redis_pool(
    redis_connection_url: String,
    gauge: IntGauge,
) -> Result<RedisPoolWithStats, crate::Error> {
    let cfg = Config::from_url(redis_connection_url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
    let test_conn = pool.get().await.map_err(|err| {
        crate::Error::RedisPoolInitError(
            "failed to create initial pooled connection on startup",
            err,
        )
    })?;
    drop(test_conn); // Not needed, just verify that Redis server is responding
    Ok(RedisPoolWithStats::new(pool, gauge))
}

#[async_trait]
pub trait DedicatedConnection {
    type Connection;
    type Error;

    async fn dedicated_connection(&self) -> Result<Self::Connection, Self::Error>;
}

#[async_trait]
impl DedicatedConnection for RedisPool {
    type Connection = RedisConnection;
    type Error = RedisPoolError;

    async fn dedicated_connection(&self) -> Result<RedisConnection, RedisPoolError> {
        let conn = self.get().await?;
        let detached = RedisPoolConnection::take(conn);
        Ok(detached)
    }
}

#[derive(Clone)]
pub struct RedisPoolWithStats {
    pool: RedisPool,
    gauge: IntGauge,
}

impl RedisPoolWithStats {
    pub fn new(pool: RedisPool, gauge: IntGauge) -> Self {
        RedisPoolWithStats { pool, gauge }
    }

    pub async fn get(&self) -> Result<RedisPoolConnection, RedisPoolError> {
        let res = self.pool.get().await;
        self.gauge.set(self.pool.status().available as i64);
        res
    }
}

#[async_trait]
impl DedicatedConnection for RedisPoolWithStats {
    type Connection = RedisConnection;
    type Error = RedisPoolError;

    async fn dedicated_connection(&self) -> Result<RedisConnection, RedisPoolError> {
        self.pool.dedicated_connection().await
    }
}
