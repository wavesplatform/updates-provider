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
    last_values: TSUpdatesProviderLastValues<T>,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

pub trait WatchListItem: Eq + Hash + Into<Topic> + MaybeFromTopic + ToString + Clone {}

#[derive(Debug, Clone)]
pub enum WatchListUpdate<T: WatchListItem> {
    New { item: T, subscribers_count: i64 },
    Change { item: T, subscribers_count: i64 },
    Delete { item: T },
}

pub trait MaybeFromUpdate: std::fmt::Debug + Send + Sync {
    fn maybe_from_update(update: &SubscriptionUpdate) -> Option<Self>
    where
        Self: Sized;
}

impl<T: WatchListItem + std::fmt::Debug + Send + Sync> MaybeFromUpdate for WatchListUpdate<T> {
    fn maybe_from_update(update: &SubscriptionUpdate) -> Option<Self> {
        match update {
            SubscriptionUpdate::New {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    Some(WatchListUpdate::New {
                        item,
                        subscribers_count: *subscribers_count,
                    })
                } else {
                    None
                }
            }
            SubscriptionUpdate::Change {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    Some(WatchListUpdate::Change {
                        item,
                        subscribers_count: *subscribers_count,
                    })
                } else {
                    None
                }
            }
            SubscriptionUpdate::Delete { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    Some(WatchListUpdate::Delete { item })
                } else {
                    None
                }
            }
        }
    }
}

pub trait MaybeFromTopic: Sized {
    fn maybe_item(topic: &Topic) -> Option<Self>;
}

impl<T: WatchListItem> WatchList<T> {
    pub fn new(
        repo: TSResourcesRepoImpl,
        last_values: TSUpdatesProviderLastValues<T>,
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

    pub fn on_update(&mut self, update: &WatchListUpdate<T>) -> Result<(), Error> {
        match update {
            WatchListUpdate::New {
                item,
                subscribers_count,
            } => {
                self.deletable_items.remove(item);
                self.items.insert(item.to_owned(), *subscribers_count);
                WATCHLISTS_TOPICS
                    .with_label_values(&[&self.type_name])
                    .inc();
                WATCHLISTS_SUBSCRIPTIONS
                    .with_label_values(&[&self.type_name])
                    .add(*subscribers_count);
            }
            WatchListUpdate::Change {
                item,
                subscribers_count,
            } => {
                if let Some(current_count) = self.items.get_mut(item) {
                    if current_count != subscribers_count {
                        let subscriptions_diff = *subscribers_count - *current_count;
                        *current_count = *subscribers_count;
                        WATCHLISTS_SUBSCRIPTIONS
                            .with_label_values(&[&self.type_name])
                            .add(subscriptions_diff);
                    }
                } else {
                    self.deletable_items.remove(item);
                    self.items.insert(item.to_owned(), *subscribers_count);
                    WATCHLISTS_TOPICS
                        .with_label_values(&[&self.type_name])
                        .inc();
                    WATCHLISTS_SUBSCRIPTIONS
                        .with_label_values(&[&self.type_name])
                        .add(*subscribers_count);
                }
            }
            WatchListUpdate::Delete { item } => {
                if let Some(current_count) = self.items.get_mut(item) {
                    WATCHLISTS_SUBSCRIPTIONS
                        .with_label_values(&[&self.type_name])
                        .sub(*current_count);
                    *current_count = 0;
                    let delete_timestamp = Instant::now() + self.delete_timeout;
                    self.deletable_items
                        .insert(item.to_owned(), delete_timestamp);
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
            self.last_values.write().await.remove(&item);
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
