use super::{SubscriptionEvent, SubscriptionsRepo};
use crate::error::Error;
use crate::metrics::REDIS_INPUT_QUEUE_SIZE;
use itertools::Itertools;
use std::sync::Arc;
use std::{convert::TryFrom, time::Duration};
use tokio::time::Instant;
use wavesexchange_log::{debug, info, warn};
use wavesexchange_topic::Topic;

pub struct PullerImpl {
    subscriptions_repo: Arc<dyn SubscriptionsRepo + Send + Sync + 'static>,
}

impl PullerImpl {
    pub fn new<S: SubscriptionsRepo + Send + Sync + 'static>(subscriptions_repo: Arc<S>) -> Self {
        Self { subscriptions_repo }
    }

    // NB: redis server have to be configured to publish keyspace notifications:
    // https://redis.io/topics/notifications
    pub async fn run(self) -> Result<tokio::sync::mpsc::Receiver<SubscriptionEvent>, Error> {
        let (subscriptions_updates_sender, subscriptions_updates_receiver) =
            tokio::sync::mpsc::channel(100);

        tokio::task::spawn_blocking(move || {
            let mut panic_strategy = PanicStrategy::new(3, Duration::from_secs(10));

            loop {
                // Start receiving updates before getting currently active subscriptions
                let mut updates_fetcher = self.subscriptions_repo.get_updates_fetcher().unwrap();
                let mut subscription_stream = updates_fetcher.updates_stream().unwrap();

                // Get current subscriptions, so that no updates will be missing
                let initial_subscriptions_updates = self.get_initial_subscriptions().unwrap();

                tokio::runtime::Handle::current().block_on(async {
                    for update in initial_subscriptions_updates.into_iter() {
                        REDIS_INPUT_QUEUE_SIZE.inc();
                        subscriptions_updates_sender.send(update).await.unwrap();
                    }
                });

                while let Ok(update) = subscription_stream.next_event() {
                    debug!("Subscription event: {:?}", update);
                    let subscriptions_updates_sender_ref = &subscriptions_updates_sender;
                    tokio::runtime::Handle::current().block_on(async {
                        REDIS_INPUT_QUEUE_SIZE.inc();
                        subscriptions_updates_sender_ref.send(update).await.unwrap();
                    })
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

    fn get_initial_subscriptions(&self) -> Result<Vec<SubscriptionEvent>, Error> {
        let current_subscriptions = self.subscriptions_repo.get_existing_subscriptions()?;

        let initial_subscriptions_updates = current_subscriptions
            .into_iter()
            .filter_map(|subscription| {
                debug!("+ Initial subscription: {:?}", subscription);
                if let Ok(topic) = Topic::try_from(subscription.subscription_key.as_str()) {
                    let context = subscription.context;
                    Some(SubscriptionEvent::Updated { topic, context })
                } else {
                    None
                }
            })
            .collect_vec();

        info!(
            "initial subscriptions count: {}",
            initial_subscriptions_updates.len()
        );

        Ok(initial_subscriptions_updates)
    }
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
    async fn shoild_not_tell_to_panic() {
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
