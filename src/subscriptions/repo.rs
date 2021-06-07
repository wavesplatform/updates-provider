use super::{Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;

pub struct SubscriptionsRepoImpl {
    pool: Pool<RedisConnectionManager>,
}

impl SubscriptionsRepoImpl {
    pub fn new(pool: Pool<RedisConnectionManager>) -> SubscriptionsRepoImpl {
        SubscriptionsRepoImpl { pool }
    }
}

impl SubscriptionsRepo for SubscriptionsRepoImpl {
    fn get_subscriptions(&self) -> Result<Subscriptions, Error> {
        let mut con = self.pool.get()?;

        let subscriptions = con.keys("sub:*")?;

        Ok(subscriptions)
    }
}
