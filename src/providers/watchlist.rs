use super::TSResourcesRepoImpl;
use crate::metrics::WATCHLISTS_TOPICS;
use crate::subscriptions::SubscriptionUpdate;
use crate::{error::Error, resources::ResourcesRepo};
use std::time::{Duration, Instant};
use std::{collections::HashMap, hash::Hash};
use wavesexchange_topic::Topic;

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    items: HashMap<T, ItemInfo>,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

#[derive(Debug, Default)]
struct ItemInfo {
    last_value: String,
    maybe_delete: Option<Instant>,
}

impl ItemInfo {
    fn deletable(&mut self, delete_timestamp: Instant) {
        self.maybe_delete = Some(delete_timestamp)
    }
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
    pub fn new(repo: TSResourcesRepoImpl, delete_timeout: Duration) -> Self {
        let items = HashMap::new();
        let type_name = std::any::type_name::<T>().to_string();
        Self {
            items,
            repo,
            delete_timeout,
            type_name,
        }
    }

    pub fn on_update(&mut self, update: &WatchListUpdate<T>) -> Result<(), Error> {
        match update {
            WatchListUpdate::New { item } => {
                self.items.insert(item.to_owned(), ItemInfo::default());
                WATCHLISTS_TOPICS
                    .with_label_values(&[&self.type_name])
                    .inc();
            }
            WatchListUpdate::Delete { item } => {
                if let Some(item_info) = self.items.get_mut(item) {
                    let delete_timestamp = Instant::now() + self.delete_timeout;
                    item_info.deletable(delete_timestamp)
                }
            }
        }
        Ok(())
    }

    pub async fn delete_old(&mut self) {
        let now = Instant::now();
        let keys = self
            .items
            .iter()
            .filter_map(|(item, item_info)| {
                if let Some(ref delete_timestamp) = item_info.maybe_delete {
                    if delete_timestamp < &now {
                        return Some(item.clone());
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        for item in keys {
            self.items.remove(&item);
            WATCHLISTS_TOPICS
                .with_label_values(&[&self.type_name])
                .dec();
            let _ = self.repo.del(T::into(item));
        }
    }

    pub fn contains_key(&self, key: &T) -> bool {
        self.items.contains_key(key)
    }

    pub fn get_value(&self, key: &T) -> Option<&String> {
        self.items.get(key).map(|x| &x.last_value)
    }

    pub fn insert_value(&mut self, key: &T, value: String) {
        if let Some(item_info) = self.items.get_mut(key) {
            item_info.last_value = value;
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a WatchList<T>
where
    T: WatchListItem,
{
    type Item = &'a T;
    type IntoIter = WatchListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        WatchListIter {
            inner: self.items.keys(),
        }
    }
}

pub struct WatchListIter<'a, T: 'a> {
    inner: std::collections::hash_map::Keys<'a, T, ItemInfo>,
}

impl<'a, T> Iterator for WatchListIter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<&'a T> {
        self.inner.next()
    }
}
