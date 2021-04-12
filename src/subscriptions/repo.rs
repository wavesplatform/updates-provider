use super::{Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;

pub struct SubscriptionsRepoImpl {
    subscriptions_key: String,
    pool: Pool<RedisConnectionManager>,
}

impl SubscriptionsRepoImpl {
    pub fn new(
        pool: Pool<RedisConnectionManager>,
        subscriptions_key: impl AsRef<str>,
    ) -> SubscriptionsRepoImpl {
        SubscriptionsRepoImpl {
            pool,
            subscriptions_key: subscriptions_key.as_ref().to_owned(),
        }
    }
}

impl SubscriptionsRepo for SubscriptionsRepoImpl {
    fn get_subscriptions(&self) -> Result<Subscriptions, Error> {
        let mut redis_con = self.pool.get()?;

        let subscriptions: std::collections::HashMap<String, String> =
            redis_con.hgetall(&self.subscriptions_key)?;

        let subscriptions = subscriptions
            .into_iter()
            .filter_map(|(key, value)| match value.parse::<i64>() {
                Ok(subscribers_count) => Some((key, subscribers_count)),
                Err(_) => None,
            })
            .collect();

        Ok(subscriptions)
    }
}
