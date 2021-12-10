use super::{Subscription, Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use crate::subscriptions::SubscriptionContext;
use lazy_static::lazy_static;
use r2d2::{Pool, PooledConnection};
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;
use std::collections::HashMap;

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

        let subscription_keys: Vec<String> = con.keys("sub:*")?;
        let values = mget(&mut con, &subscription_keys)?;

        let subscriptions = subscription_keys
            .into_iter()
            .zip(values.into_iter())
            .filter_map(|(key, value)| {
                if let Some(key) = key.strip_prefix("sub:") {
                    Some(Subscription {
                        subscription_key: key.to_string(),
                        context: value.map(parse_context).flatten(),
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(subscriptions)
    }

    fn get_subscription_context(
        &self,
        subscription_key: &str,
    ) -> Result<Option<SubscriptionContext>, Error> {
        let mut con = self.pool.get()?;
        let key = "sub:".to_string() + subscription_key;
        let value: Option<String> = con.get(key)?;
        let context = value.map(parse_context).flatten();
        Ok(context)
    }
}

fn mget(
    con: &mut PooledConnection<RedisConnectionManager>,
    keys: &[String],
) -> Result<Vec<Option<String>>, Error> {
    // Need to explicitly handle case with keys.len() == 1
    // due to the issue https://github.com/mitsuhiko/redis-rs/issues/336
    match keys.len() {
        0 => Ok(vec![]),
        1 => Ok(vec![con.get(keys)?]),
        _ => Ok(con.get(keys)?),
    }
}

lazy_static! {
    /// Subscription keys' values in Redis currently have the form `<integer-id>:<json-context>`,
    /// and we only need json context from it, so we have to strip the ID prefix.
    static ref CONTEXT_PREFIX: regex::Regex =
        regex::Regex::new("^\\d+:").expect("internal error - bad regexp");
}

fn parse_context(context: impl Into<String>) -> Option<SubscriptionContext> {
    let context = context.into();
    CONTEXT_PREFIX
        .find(&context)
        .filter(|m| m.start() == 0)
        .map(|m| &context[m.end()..])
        .map(|json_context| serde_json::from_str::<HashMap<String, String>>(json_context).ok())
        .flatten()
        .map(|hash_map| SubscriptionContext {
            tracing_context: hash_map,
        })
}

#[test]
fn test_parse_context() {
    assert_eq!(parse_context(""), None);
    assert_eq!(parse_context("abc"), None);
    assert_eq!(parse_context("123"), None);
    assert_eq!(parse_context("123:"), None);
    assert_eq!(parse_context("123:foo"), None);
    assert_eq!(parse_context("abc:{}"), None);
    assert_eq!(
        parse_context("123:{}"),
        Some(SubscriptionContext {
            tracing_context: HashMap::new()
        })
    );
    assert_eq!(
        parse_context(r#"123:{"foo":"bar"}"#),
        Some(SubscriptionContext {
            tracing_context: HashMap::from([("foo".to_string(), "bar".to_string())])
        })
    );
}
