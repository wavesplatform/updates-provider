use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::metrics::WATCHLISTS_TOPICS;
use crate::subscriptions::SubscriptionUpdate;
use crate::{error::Error, resources::ResourcesRepo};
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};
use wavesexchange_topic::Topic;

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    items: HashSet<T>,
    deletable_items: HashMap<T, Instant>,
    last_values: TSUpdatesProviderLastValues<T>,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

pub trait WatchListItem: Eq + Hash + Into<Topic> + MaybeFromTopic + Into<String> + Clone {}

#[derive(Debug, Clone)]
pub enum WatchListUpdate<T: WatchListItem> {
    New { item: T },
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
            SubscriptionUpdate::New { topic } => {
                T::maybe_item(topic).map(|item| WatchListUpdate::New { item })
            }
            SubscriptionUpdate::Delete { topic } => {
                T::maybe_item(topic).map(|item| WatchListUpdate::Delete { item })
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
        let items = HashSet::new();
        let deletable_items = HashMap::new();
        let type_name = std::any::type_name::<T>().to_string();
        Self {
            items,
            deletable_items,
            last_values,
            repo,
            delete_timeout,
            type_name,
        }
    }

    pub fn on_update(&mut self, update: &WatchListUpdate<T>) -> Result<(), Error> {
        match update {
            WatchListUpdate::New { item } => {
                self.deletable_items.remove(item);
                self.items.insert(item.to_owned());
                WATCHLISTS_TOPICS
                    .with_label_values(&[&self.type_name])
                    .inc();
            }
            WatchListUpdate::Delete { item } => {
                if self.items.contains(item) {
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
        self.items.contains(key)
    }
}

impl<'a, T> IntoIterator for &'a WatchList<T>
where
    T: WatchListItem,
{
    type Item = &'a T;
    type IntoIter = std::collections::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}
