use super::{Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;

pub struct SubscriptionsRepoImpl {
    pool: Pool<RedisConnectionManager>,
}

impl SubscriptionsRepoImpl {
    pub fn new(pool: Pool<RedisConnectionManager>) -> SubscriptionsRepoImpl {
        SubscriptionsRepoImpl { pool }
    }
}

#[async_trait]
impl SubscriptionsRepo for SubscriptionsRepoImpl {
    async fn get_subscriptions(&self) -> Result<Subscriptions, Error> {
        let mut con = self.pool.get().await?;

        let subscriptions = con.keys("sub:*").await?;

        Ok(subscriptions)
    }
}
