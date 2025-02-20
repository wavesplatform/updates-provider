use super::{SubscriptionEvent, SubscriptionsRepo};
use crate::error::Error;
use crate::metrics::REDIS_INPUT_QUEUE_SIZE;
use crate::redis::{DedicatedConnection, RedisPoolWithStats};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use wavesexchange_log::{debug, info, warn};
use wx_topic::Topic;

pub struct PullerImpl {
    subscriptions_repo: Arc<dyn SubscriptionsRepo + Send + Sync + 'static>,
    redis_pool: RedisPoolWithStats,
}

impl PullerImpl {
    pub fn new<S: SubscriptionsRepo + Send + Sync + 'static>(
        subscriptions_repo: Arc<S>,
        redis_pool: RedisPoolWithStats,
    ) -> Self {
        Self {
            subscriptions_repo,
            redis_pool,
        }
    }

    // NB: redis server have to be configured to publish keyspace notifications:
    // https://redis.io/topics/notifications
    pub async fn run(self) -> Result<tokio::sync::mpsc::Receiver<SubscriptionEvent>, Error> {
        let (subscriptions_updates_sender, subscriptions_updates_receiver) =
            tokio::sync::mpsc::channel(100);

        tokio::task::spawn(async move {
            let mut panic_strategy = PanicStrategy::new(3, Duration::from_secs(10));

            loop {
                let con = self
                    .redis_pool
                    .dedicated_connection()
                    .await
                    .expect("failed to create dedicated Redis connection");
                let mut pubsub = con.into_pubsub();

                let subscription_pattern = "__keyspace*__:sub:*".to_string();
                pubsub
                    .psubscribe(subscription_pattern.clone())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "Cannot subscribe for the redis keyspace updates on pattern {}",
                            subscription_pattern
                        )
                    });

                let initial_subscriptions_updates =
                    get_initial_subscriptions(&self.subscriptions_repo)
                        .await
                        .unwrap();

                for update in initial_subscriptions_updates.into_iter() {
                    REDIS_INPUT_QUEUE_SIZE.inc();
                    subscriptions_updates_sender.send(update).await.unwrap();
                }

                let mut messages = pubsub.on_message();
                while let Some(msg) = messages.next().await {
                    let payload = msg.get_payload::<String>().unwrap();
                    let event_name = payload.as_str();
                    if let "set" | "del" | "expired" = event_name {
                        let channel = msg.get_channel::<String>().unwrap();
                        debug!("[REDIS] Event '{}' on channel '{}'", event_name, channel);
                        let subscribe_key = channel
                            .strip_prefix("__keyspace@0__:sub:")
                            .unwrap_or_else(|| {
                                panic!("wrong redis subscribe channel: {:?}", channel)
                            });
                        let topic = Topic::parse_str(subscribe_key).unwrap();
                        let update = if let "set" = event_name {
                            SubscriptionEvent::Updated { topic }
                        } else {
                            SubscriptionEvent::Removed { topic }
                        };
                        debug!("Subscription event: {:?}", update);
                        let subscriptions_updates_sender_ref = &subscriptions_updates_sender;
                        REDIS_INPUT_QUEUE_SIZE.inc();
                        subscriptions_updates_sender_ref.send(update).await.unwrap();
                    }
                }

                warn!("redis connection was closed");

                panic_strategy.add_failure();

                if panic_strategy.should_panic() {
                    panic!("redis connection fails too often");
                }
            }
        });

        Ok(subscriptions_updates_receiver)
    }
}

async fn get_initial_subscriptions(
    subscriptions_repo: &Arc<dyn SubscriptionsRepo + Send + Sync>,
) -> Result<Vec<SubscriptionEvent>, Error> {
    let current_subscriptions = subscriptions_repo.get_subscriptions().await?;

    let initial_subscriptions_updates: Vec<SubscriptionEvent> = current_subscriptions
        .iter()
        .filter_map(|subscriptions_key| {
            if let Some(key) = subscriptions_key.strip_prefix("sub:") {
                if let Ok(topic) = Topic::parse_str(key) {
                    return Some(SubscriptionEvent::Updated { topic });
                }
            }
            None
        })
        .collect();

    info!(
        "initial subscriptions count: {}",
        initial_subscriptions_updates.len()
    );

    Ok(initial_subscriptions_updates)
}

struct PanicStrategy {
    last_failure_ts: Option<Instant>,
    failures_count: i32,
    failures_count_to_panic: i32,
    failures_min_delay_to_clean: Duration,
}

impl PanicStrategy {
    fn new(failures_count_to_panic: i32, failures_min_delay_to_clean: Duration) -> Self {
        Self {
            failures_count_to_panic,
            failures_min_delay_to_clean,
            last_failure_ts: None,
            failures_count: 0,
        }
    }

    fn add_failure(&mut self) {
        let new_failure_ts = Instant::now();

        if let Some(last_failure_ts) = self.last_failure_ts {
            let failures_delay = new_failure_ts - last_failure_ts;

            self.failures_count = if failures_delay < self.failures_min_delay_to_clean {
                self.failures_count + 1
            } else {
                1
            };
        } else {
            self.failures_count = 1;
        }

        self.last_failure_ts = Some(new_failure_ts);
    }

    fn should_panic(&self) -> bool {
        self.failures_count >= self.failures_count_to_panic
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_not_tell_to_panic() {
        let mut strategy = PanicStrategy::new(2, Duration::from_secs(5));
        strategy.add_failure();

        assert!(!strategy.should_panic());

        let mut strategy = PanicStrategy::new(2, Duration::from_secs(1));
        strategy.add_failure();
        tokio::time::sleep(Duration::from_secs(2)).await;
        strategy.add_failure();

        assert!(!strategy.should_panic());
    }

    #[test]
    fn should_tell_to_panic() {
        let mut strategy = PanicStrategy::new(1, Duration::from_secs(5));
        strategy.add_failure();

        assert!(strategy.should_panic());

        let mut strategy = PanicStrategy::new(2, Duration::from_secs(5));
        strategy.add_failure();
        strategy.add_failure();

        assert!(strategy.should_panic());
    }
}
