use super::{SubscriptionUpdate, Subscriptions, SubscriptionsRepo};
use crate::error::Error;
use r2d2_redis::redis;
use std::convert::TryFrom;
use wavesexchange_log::info;
use wavesexchange_topic::Topic;

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
        Self {
            subscriptions_repo,
            redis_client,
            subscriptions_key,
        }
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
            .filter(|(_, &count)| count > 0)
            .filter_map(|(subscriptions_key, &subscribers_count)| {
                match Topic::try_from(subscriptions_key.as_ref()) {
                    Ok(topic) => Some(SubscriptionUpdate::New {
                        topic,
                        subscribers_count,
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
            .into_iter()
            .try_for_each(|update| subscriptions_updates_sender.send(update))
            .map_err(|error| Error::SendError(format!("{:?}", error)))?;

        let redis_client = self.redis_client.clone();
        let subscriptions_repo = self.subscriptions_repo.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(20);

        tokio::task::spawn_blocking(move || {
            let mut con = redis_client.get_connection().unwrap();
            let mut pubsub = con.as_pubsub();

            let subscription_pattern = format!("__keyspace*__:{}", self.subscriptions_key);
            pubsub
                .psubscribe(subscription_pattern.clone())
                .unwrap_or_else(|_| {
                    panic!(
                        "Cannot subscribe for the redis keyspace updates on pattern {}",
                        subscription_pattern
                    )
                });

            while let Ok(_msg) = pubsub.get_message() {
                let tx_ref = &tx;
                futures::executor::block_on(async move {
                    tx_ref.send(()).await.unwrap();
                })
            }
        });

        tokio::spawn(async move {
            'base: loop {
                let sleep = tokio::time::sleep(std::time::Duration::from_secs(3));
                tokio::pin!(sleep);
                let mut status = ListenStatus::Null;
                loop {
                    tokio::select! {
                        msg = rx.recv() => {
                            if let None = msg {
                                break 'base;
                            }
                            if let ListenStatus::Timeout = status {
                                break;
                            } else {
                                status = ListenStatus::Message
                            }
                        }
                        _ = &mut sleep => {
                            if let ListenStatus::Message = status {
                                break;
                            } else {
                                status = ListenStatus::Timeout
                            }
                        }
                    }
                }

                let updated_subscriptions = subscriptions_repo
                    .get_subscriptions()
                    .expect("Subscriptions has to be a hash map");

                let diff =
                    subscription_updates_diff(&current_subscriptions, &updated_subscriptions)
                        .expect("Cannot calculate subscriptions diff");

                diff.iter()
                    .try_for_each(|update| subscriptions_updates_sender.send(update.to_owned()))
                    .expect("Cannot send a subscription update");

                // info!("subscriptions were updated");

                current_subscriptions = updated_subscriptions;
            }
        });

        Ok(subscriptions_updates_receiver)
    }
}

enum ListenStatus {
    Timeout,
    Message,
    Null,
}

fn subscription_updates_diff(
    current: &Subscriptions,
    new: &Subscriptions,
) -> Result<Vec<SubscriptionUpdate>, Error> {
    let mut updated = new
        .iter()
        .try_fold::<_, _, Result<&mut Vec<SubscriptionUpdate>, Error>>(
            &mut vec![],
            |acc, (subscription_key, &subscribers_count)| {
                if let Ok(topic) = Topic::try_from(subscription_key.as_ref()) {
                    if let Some(&current_count) = current.get(subscription_key) {
                        if current_count != subscribers_count {
                            if subscribers_count > 0 {
                                if current_count == 0 {
                                    acc.push(SubscriptionUpdate::New {
                                        topic,
                                        subscribers_count,
                                    })
                                } else {
                                    acc.push(SubscriptionUpdate::Change {
                                        topic,
                                        subscribers_count,
                                    })
                                }
                            } else {
                                acc.push(SubscriptionUpdate::Delete { topic })
                            }
                        }
                    } else if subscribers_count > 0 {
                        acc.push(SubscriptionUpdate::New {
                            topic,
                            subscribers_count,
                        });
                    }
                }
                Ok(acc)
            },
        )
        .map(|vec| vec.to_owned())?;

    // handle deleted subscriptions
    current.iter().for_each(|(subscription_key, _)| {
        if let Ok(topic) = Topic::try_from(subscription_key.as_ref()) {
            if !new.contains_key(subscription_key) {
                updated.push(SubscriptionUpdate::Delete { topic });
            }
        }
    });

    Ok(updated)
}
