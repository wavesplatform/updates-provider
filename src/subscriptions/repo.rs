use super::{Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use crate::redis::{AsyncCommands, RedisPool, RedisPoolWithStats};
use async_trait::async_trait;

pub struct SubscriptionsRepoImpl {
    pool: RedisPoolWithStats,
}

impl SubscriptionsRepoImpl {
    pub fn new(pool: RedisPool) -> SubscriptionsRepoImpl {
        SubscriptionsRepoImpl {
            pool: RedisPoolWithStats::new(pool),
        }
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
