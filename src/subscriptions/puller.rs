use super::{SubscriptionUpdate, SubscriptionUpdateType, Subscriptions};
use crate::error::Error;
use crate::models::Resource;
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;
use futures::StreamExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use wavesexchange_log::debug;

pub struct PullerImpl {
    redis_pool: bb8::Pool<RedisConnectionManager>,
    subscriptions_key: String,
}

impl PullerImpl {
    pub fn new(redis_pool: bb8::Pool<RedisConnectionManager>, subscriptions_key: String) -> Self {
        return Self {
            redis_pool,
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

        let con = bb8::Pool::dedicated_connection(&self.redis_pool).await?;

        let mut current_subscriptions: Subscriptions = self.get_subscriptions().await?;

        debug!("current subscriptions: {:?}", current_subscriptions);

        let initial_subscriptions_updates = current_subscriptions
            .iter()
            .filter(|(_, count)| count.to_owned().to_owned() > 0)
            .try_fold(vec![], |mut acc, (subscription_key, subscribers_count)| {
                Resource::try_from(subscription_key.as_ref()).map(|resource| {
                    acc.push(SubscriptionUpdate {
                        update_type: SubscriptionUpdateType::New,
                        resource: resource,
                        subscribers_count: subscribers_count.to_owned(),
                    });
                    acc
                })
            })?;

        initial_subscriptions_updates
            .iter()
            .try_for_each(|update| subscriptions_updates_sender.send(update.to_owned()))?;

        let mut pubsub = con.into_pubsub();

        pubsub
            .psubscribe(format!("__keyspace*__:{}", self.subscriptions_key))
            .await?;

        tokio::task::spawn(async move {
            while let Some(_) = pubsub.on_message().next().await {
                let updated_subscriptions = self
                    .get_subscriptions()
                    .await
                    .expect("Subscriptions has to be a hash map");

                let diff =
                    subscription_updates_diff(&current_subscriptions, &updated_subscriptions)
                        .expect("Cannot calculate subscriptions diff");

                diff.iter()
                    .try_for_each(|update| subscriptions_updates_sender.send(update.to_owned()))
                    .expect("Cannot send a subscription update");

                current_subscriptions = updated_subscriptions;
            }
        });

        Ok(subscriptions_updates_receiver)
    }

    async fn get_subscriptions(&self) -> Result<Subscriptions, Error> {
        let subscriptions: HashMap<String, String> = self
            .redis_pool
            .get()
            .await?
            .hgetall(&self.subscriptions_key)
            .await?;

        let subscriptions = subscriptions
            .into_iter()
            .filter_map(|(key, value)| match value.parse::<i32>() {
                Ok(subscribers_count) => Some((key, subscribers_count)),
                Err(_) => None,
            })
            .collect();

        Ok(subscriptions)
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
                    let resource = Resource::try_from(subscription_key.as_ref())?;
                    acc.push(SubscriptionUpdate {
                        update_type: SubscriptionUpdateType::Decrement,
                        resource: resource,
                        subscribers_count: subscribers_count.to_owned(),
                    });
                } else if current.get(subscription_key).unwrap().to_owned()
                    < subscribers_count.to_owned()
                {
                    let resource = Resource::try_from(subscription_key.as_ref())?;
                    acc.push(SubscriptionUpdate {
                        update_type: SubscriptionUpdateType::Increment,
                        resource: resource,
                        subscribers_count: subscribers_count.to_owned(),
                    });
                }
            } else {
                let resource = Resource::try_from(subscription_key.as_ref())?;
                acc.push(SubscriptionUpdate {
                    update_type: SubscriptionUpdateType::New,
                    resource: resource,
                    subscribers_count: subscribers_count.to_owned(),
                });
            }
            Ok(acc)
        })
        .map(|vec| vec.to_owned())
}
