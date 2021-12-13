use crate::error::Error;
use crate::subscriptions::{
    Subscription, SubscriptionContext, SubscriptionEvent, Subscriptions, SubscriptionsRepo,
    SubscriptionsStream, SubscriptionsUpdateFetcher,
};
use lazy_static::lazy_static;
use r2d2::{Error as R2d2Error, Pool, PooledConnection};
use r2d2_redis::{redis, redis::Commands, redis::RedisError, RedisConnectionManager};
use std::{collections::HashMap, convert::TryFrom};
use wavesexchange_log::{debug, warn};
use wavesexchange_topic::Topic;

type RedisConnectionPool = Pool<RedisConnectionManager>;
type RedisPooledConnection = PooledConnection<RedisConnectionManager>;

pub struct RedisSubscriptionsRepo {
    redis_pool: RedisConnectionPool,

    // r2d2 cannot extract dedicated connection for use with redis pubsub
    // therefore it is needed to use a separate redis client
    redis_client: redis::Client,
}

impl RedisSubscriptionsRepo {
    pub fn new(
        redis_pool: RedisConnectionPool,
        redis_client: redis::Client,
    ) -> RedisSubscriptionsRepo {
        RedisSubscriptionsRepo {
            redis_pool,
            redis_client,
        }
    }

    fn get_pooled_connection(&self) -> Result<RedisPooledConnection, R2d2Error> {
        self.redis_pool.get()
    }

    fn get_unpooled_connection(&self) -> Result<redis::Connection, RedisError> {
        self.redis_client.get_connection()
    }
}

impl SubscriptionsRepo for RedisSubscriptionsRepo {
    fn get_existing_subscriptions(&self) -> Result<Subscriptions, Error> {
        let mut con = self.get_pooled_connection()?;

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

    fn get_updates_fetcher(&self) -> Result<Box<dyn SubscriptionsUpdateFetcher>, Error> {
        let conn = self.get_unpooled_connection()?;
        let fetcher = SubscriptionsUpdateFetcherImpl {
            conn,
            redis_pool: self.redis_pool.clone(),
        };
        Ok(Box::new(fetcher))
    }
}

struct SubscriptionsUpdateFetcherImpl {
    conn: redis::Connection,
    redis_pool: RedisConnectionPool,
}

impl SubscriptionsUpdateFetcher for SubscriptionsUpdateFetcherImpl {
    fn updates_stream(&mut self) -> Result<Box<dyn SubscriptionsStream + '_>, Error> {
        let mut pubsub = self.conn.as_pubsub();

        let subscription_pattern = "__keyspace*__:sub:*".to_string();
        pubsub
            .psubscribe(subscription_pattern.clone())
            .unwrap_or_else(|_| {
                panic!(
                    "Cannot subscribe for the redis keyspace updates on pattern {}",
                    subscription_pattern
                )
            });

        let stream = SubscriptionsStreamImpl {
            pubsub,
            redis_pool: &self.redis_pool,
        };

        Ok(Box::new(stream))
    }
}

struct SubscriptionsStreamImpl<'a> {
    pubsub: redis::PubSub<'a>,
    redis_pool: &'a RedisConnectionPool,
}

impl SubscriptionsStreamImpl<'_> {
    fn get_pooled_connection(&self) -> Result<RedisPooledConnection, R2d2Error> {
        self.redis_pool.get()
    }

    fn get_subscription_context(
        &self,
        subscription_key: &str,
    ) -> Result<Option<SubscriptionContext>, Error> {
        let mut con = self.get_pooled_connection()?;
        let key = "sub:".to_string() + subscription_key;
        let value: Option<String> = con.get(key)?;
        let context = value.map(parse_context).flatten();
        Ok(context)
    }
}

impl SubscriptionsStream for SubscriptionsStreamImpl<'_> {
    fn next_event(&mut self) -> Result<SubscriptionEvent, Error> {
        loop {
            let msg = self.pubsub.get_message()?;
            let payload = msg.get_payload::<String>().unwrap();
            let event_name = payload.as_str();
            if let "set" | "del" | "expired" = event_name {
                let channel = msg.get_channel::<String>().unwrap();
                debug!("[REDIS] Event '{}' on channel '{}'", event_name, channel);
                let subscribe_key = channel
                    .strip_prefix("__keyspace@0__:sub:")
                    .unwrap_or_else(|| panic!("wrong redis subscribe channel: {:?}", channel));
                let topic = Topic::try_from(subscribe_key).expect("bad sub: key");
                let update = if let "set" = event_name {
                    let context = {
                        match self.get_subscription_context(subscribe_key) {
                            Ok(context) => context,
                            Err(_) => {
                                warn!(
                                    "Failed to read subscription context for key {}",
                                    subscribe_key
                                );
                                None
                            }
                        }
                    };
                    SubscriptionEvent::Updated { topic, context }
                } else {
                    SubscriptionEvent::Removed { topic }
                };
                return Ok(update);
            }
        }
    }
}

fn mget(con: &mut RedisPooledConnection, keys: &[String]) -> Result<Vec<Option<String>>, Error> {
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
