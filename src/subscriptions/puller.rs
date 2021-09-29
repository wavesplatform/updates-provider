use super::{SubscriptionEvent, SubscriptionsRepo};
use crate::error::Error;
use r2d2_redis::redis;
use std::convert::TryFrom;
use std::sync::Arc;
use wavesexchange_log::{debug, info};
use wavesexchange_topic::Topic;

pub struct PullerImpl {
    subscriptions_repo: Arc<dyn SubscriptionsRepo + Send + Sync + 'static>,
    redis_client: redis::Client,
}

impl PullerImpl {
    pub fn new<S: SubscriptionsRepo + Send + Sync + 'static>(
        subscriptions_repo: Arc<S>,
        redis_client: redis::Client,
    ) -> Self {
        Self {
            subscriptions_repo,
            redis_client,
        }
    }

    // NB: redis server have to be configured to publish keyspace notifications:
    // https://redis.io/topics/notifications
    pub async fn run(self) -> Result<tokio::sync::mpsc::Receiver<SubscriptionEvent>, Error> {
        let (subscriptions_updates_sender, subscriptions_updates_receiver) =
            tokio::sync::mpsc::channel(100);

        tokio::task::spawn_blocking(move || {
            let mut con = self.redis_client.get_connection().unwrap();
            let mut pubsub = con.as_pubsub();

            let subscription_pattern = "__keyspace*__:sub:*".to_string();
            pubsub
                .psubscribe(subscription_pattern.clone())
                .unwrap_or_else(|_| {
                    panic!(
                        "Cannot subscribe for the redis keyspace updates on pattern {}",
                        subscription_pattern
                    )
                });

            let initial_subscriptions_updates =
                get_initial_subscriptions(&self.subscriptions_repo).unwrap();

            tokio::runtime::Handle::current().block_on(async {
                for update in initial_subscriptions_updates.into_iter() {
                    subscriptions_updates_sender.send(update).await.unwrap()
                }
            });

            while let Ok(msg) = pubsub.get_message() {
                let payload = msg.get_payload::<String>().unwrap();
                let event_name = payload.as_str();
                if let "set" | "del" | "expired" = event_name {
                    let channel = msg.get_channel::<String>().unwrap();
                    debug!("[REDIS] Event '{}' on channel '{}'", event_name, channel);
                    let subscribe_key = channel
                        .strip_prefix("__keyspace@0__:sub:")
                        .unwrap_or_else(|| panic!("wrong redis subscribe channel: {:?}", channel));
                    let topic = Topic::try_from(subscribe_key).unwrap();
                    let update = if let "set" = event_name {
                        SubscriptionEvent::Updated { topic }
                    } else {
                        SubscriptionEvent::Removed { topic }
                    };
                    debug!("Subscription event: {:?}", update);
                    let subscriptions_updates_sender_ref = &subscriptions_updates_sender;
                    tokio::runtime::Handle::current().block_on(async {
                        subscriptions_updates_sender_ref.send(update).await.unwrap();
                    })
                }
            }
        });

        Ok(subscriptions_updates_receiver)
    }
}

fn get_initial_subscriptions(
    subscriptions_repo: &Arc<dyn SubscriptionsRepo + Send + Sync>,
) -> Result<Vec<SubscriptionEvent>, Error> {
    let current_subscriptions = subscriptions_repo.get_subscriptions()?;

    let initial_subscriptions_updates: Vec<SubscriptionEvent> = current_subscriptions
        .iter()
        .filter_map(|subscriptions_key| {
            if let Some(key) = subscriptions_key.strip_prefix("sub:") {
                if let Ok(topic) = Topic::try_from(key) {
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
