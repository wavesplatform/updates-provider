use super::{SubscriptionUpdate, SubscriptionUpdateType, Subscriptions, SubscriptionsRepo};
use crate::errors::Error;
use crate::models::Resource;
use r2d2_redis::redis;
use std::convert::TryFrom;
use wavesexchange_log::info;

pub struct PullerImpl {
    subscriptions_repo: std::sync::Arc<dyn SubscriptionsRepo + Send + Sync + 'static>,
    redis_client: redis::Client,
    subscriptions_key: String,
}

impl PullerImpl {
    pub fn new<S: SubscriptionsRepo + Send + Sync + 'static>(
        subscriptions_repo: std::sync::Arc<S>,
        redis_client: redis::Client,
        subscriptions_key: String,
    ) -> Self {
        return Self {
            subscriptions_repo: subscriptions_repo,
            redis_client,
            subscriptions_key,
        };
    }

    // NB: redis server have to be configured to publish keyspace notifications:
    // https://redis.io/topics/notifications
    pub async fn run(
        self,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<SubscriptionUpdate>, Error> {
        let (subscriptions_updates_sender, subscriptions_updates_receiver) =
            tokio::sync::mpsc::unbounded_channel::<SubscriptionUpdate>();

        // includes unactive subscriptions (subscribers_count = 0)
        let mut current_subscriptions: Subscriptions =
            self.subscriptions_repo.get_subscriptions()?;

        let initial_subscriptions_updates: Vec<SubscriptionUpdate> = current_subscriptions
            .iter()
            .filter(|(_, count)| count.to_owned().to_owned() > 0)
            .filter_map(|(subscriptions_key, subscribers_count)| {
                match Resource::try_from(subscriptions_key.as_ref()) {
                    Ok(resource) => Some(SubscriptionUpdate {
                        update_type: SubscriptionUpdateType::New,
                        resource: resource,
                        subscribers_count: subscribers_count.to_owned(),
                    }),
                    _ => None,
                }
            })
            .collect();

        info!(
            "initial subscriptions count: {}",
            initial_subscriptions_updates.len()
        );

        initial_subscriptions_updates
            .iter()
            .try_for_each(|update| subscriptions_updates_sender.send(update.to_owned()))?;

        let redis_client = self.redis_client.clone();
        let subscriptions_repo = self.subscriptions_repo.clone();

        tokio::task::spawn_blocking(move || {
            let mut con = redis_client.get_connection().unwrap();
            let mut pubsub = con.as_pubsub();
            
            let subscription_pattern = format!("__keyspace*__:{}", self.subscriptions_key);
            pubsub
                .psubscribe(subscription_pattern.clone())
                .expect(&format!(
                    "Cannot subscribe for the redis keyspace updates on pattern {}",
                    subscription_pattern
                ));

            while let Ok(_msg) = pubsub.get_message() {
                let updated_subscriptions = subscriptions_repo
                    .get_subscriptions()
                    .expect("Subscriptions has to be a hash map");

                let diff =
                    subscription_updates_diff(&current_subscriptions, &updated_subscriptions)
                        .expect("Cannot calculate subscriptions diff");

                diff.iter()
                    .try_for_each(|update| subscriptions_updates_sender.send(update.to_owned()))
                    .expect("Cannot send a subscription update");

                info!(
                    "subscriptions were updated, new count: {}",
                    updated_subscriptions.len()
                );

                current_subscriptions = updated_subscriptions;
            }
        });

        Ok(subscriptions_updates_receiver)
    }
}

fn subscription_updates_diff(
    current: &Subscriptions,
    new: &Subscriptions,
) -> Result<Vec<SubscriptionUpdate>, Error> {
    new.iter()
        .try_fold(&mut vec![], |acc, (subscription_key, subscribers_count)| {
            if current.contains_key(subscription_key) {
                if current.get(subscription_key).unwrap().to_owned() > subscribers_count.to_owned()
                {
                    if let Ok(resource) = Resource::try_from(subscription_key.as_ref()) {
                        acc.push(SubscriptionUpdate {
                            update_type: SubscriptionUpdateType::Decrement,
                            resource: resource,
                            subscribers_count: subscribers_count.to_owned(),
                        });
                    }
                } else if current.get(subscription_key).unwrap().to_owned()
                    < subscribers_count.to_owned()
                {
                    if let Ok(resource) = Resource::try_from(subscription_key.as_ref()) {
                        acc.push(SubscriptionUpdate {
                            update_type: SubscriptionUpdateType::Increment,
                            resource: resource,
                            subscribers_count: subscribers_count.to_owned(),
                        });
                    }
                }
            } else {
                if let Ok(resource) = Resource::try_from(subscription_key.as_ref()) {
                    acc.push(SubscriptionUpdate {
                        update_type: SubscriptionUpdateType::New,
                        resource: resource,
                        subscribers_count: subscribers_count.to_owned(),
                    });
                }
            }
            Ok(acc)
        })
        .map(|vec| vec.to_owned())
}
