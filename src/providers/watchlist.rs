use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::metrics::{WATCHLISTS_SUBSCRIPTIONS, WATCHLISTS_TOPICS};
use crate::models::Topic;
use crate::subscriptions::SubscriptionUpdate;
use crate::{error::Error, resources::ResourcesRepo};
use std::time::{Duration, Instant};
use std::{collections::HashMap, hash::Hash};

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    items: HashMap<T, i64>,
    deletable_items: HashMap<T, Instant>,
    last_values: TSUpdatesProviderLastValues,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

pub trait WatchListItem: Eq + Hash + Into<Topic> + MaybeFromTopic + ToString + Clone {}

pub trait MaybeFromTopic: Sized {
    fn maybe_item(topic: Topic) -> Option<Self>;
}

impl<T: WatchListItem> WatchList<T> {
    pub fn new(
        repo: TSResourcesRepoImpl,
        last_values: TSUpdatesProviderLastValues,
        delete_timeout: Duration,
    ) -> Self {
        let items = HashMap::new();
        let deletable_items = HashMap::new();
        let type_name = std::any::type_name::<T>().to_string();
        Self {
            repo,
            items,
            last_values,
            delete_timeout,
            type_name,
            deletable_items,
        }
    }

    pub async fn on_update(&mut self, update: SubscriptionUpdate) -> Result<(), Error> {
        match update {
            SubscriptionUpdate::New {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    self.deletable_items.remove(&item);
                    self.items.insert(item, subscribers_count);
                    WATCHLISTS_TOPICS
                        .with_label_values(&[&self.type_name])
                        .inc();
                    WATCHLISTS_SUBSCRIPTIONS
                        .with_label_values(&[&self.type_name])
                        .add(subscribers_count);
                }
            }
            SubscriptionUpdate::Change {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    if let Some(old_count) = self.items.get_mut(&item) {
                        if *old_count != subscribers_count {
                            let increment_subscriptions = subscribers_count - *old_count;
                            *old_count = subscribers_count;
                            WATCHLISTS_SUBSCRIPTIONS
                                .with_label_values(&[&self.type_name])
                                .add(increment_subscriptions);
                        }
                    } else {
                        self.deletable_items.remove(&item);
                        self.items.insert(item, subscribers_count);
                        WATCHLISTS_TOPICS
                            .with_label_values(&[&self.type_name])
                            .inc();
                        WATCHLISTS_SUBSCRIPTIONS
                            .with_label_values(&[&self.type_name])
                            .add(subscribers_count);
                    }
                }
            }
            SubscriptionUpdate::Delete { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    if let Some(old_count) = self.items.get_mut(&item) {
                        WATCHLISTS_SUBSCRIPTIONS
                            .with_label_values(&[&self.type_name])
                            .sub(*old_count);
                        *old_count = 0;
                        let delete_timestamp = Instant::now() + self.delete_timeout;
                        self.deletable_items.insert(item, delete_timestamp);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn delete_old(&mut self) {
        let now = Instant::now();
        let keys = self
            .deletable_items
            .iter()
            .filter_map(|(item, delete_timestamp)| {
                if delete_timestamp < &now {
                    Some(item.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for item in keys {
            self.items.remove(&item);
            WATCHLISTS_TOPICS
                .with_label_values(&[&self.type_name])
                .dec();
            self.deletable_items.remove(&item);
            self.last_values.write().await.remove(&item.to_string());
            let _ = self.repo.del(T::into(item));
        }
    }

    pub fn contains_key(&self, key: &T) -> bool {
        self.items.contains_key(key)
    }
}

impl<'a, T> IntoIterator for &'a WatchList<T>
where
    T: WatchListItem,
{
    type Item = &'a T;
    type IntoIter = std::collections::hash_map::Keys<'a, T, i64>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.keys()
    }
}
