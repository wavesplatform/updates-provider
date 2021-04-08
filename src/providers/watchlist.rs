use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::metrics::WATCHLISTS;
use crate::models::Topic;
use crate::subscriptions::SubscriptionUpdate;
use crate::{error::Error, resources::ResourcesRepo};
use std::time::{Duration, Instant};
use std::{collections::HashMap, hash::Hash};

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    pub items: HashMap<T, OnDelete>,
    last_values: TSUpdatesProviderLastValues,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

#[derive(Debug)]
pub struct OnDelete {
    pub delete_flag: bool,
    pub on_time: Instant,
}

impl OnDelete {
    fn new() -> Self {
        Self {
            delete_flag: false,
            on_time: Instant::now(),
        }
    }

    fn delete(delete_timeout: Duration) -> Self {
        Self {
            delete_flag: true,
            on_time: Instant::now() + delete_timeout,
        }
    }

    fn time_to_delete(&self, now: Instant) -> bool {
        self.delete_flag && now > self.on_time
    }

    fn not_delete(&mut self) {
        if self.delete_flag {
            self.delete_flag = false
        }
    }
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
        let type_name = std::any::type_name::<T>().to_string();
        Self {
            repo,
            items,
            last_values,
            delete_timeout,
            type_name,
        }
    }

    pub async fn on_update(&mut self, update: SubscriptionUpdate) -> Result<(), Error> {
        match update {
            SubscriptionUpdate::New { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    if !self.items.contains_key(&item) {
                        WATCHLISTS.with_label_values(&[&self.type_name]).inc();
                    }
                    self.items.insert(item, OnDelete::new());
                }
            }
            SubscriptionUpdate::Increment { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    if let Some(on_delete) = self.items.get_mut(&item) {
                        on_delete.not_delete()
                    } else {
                        self.items.insert(item, OnDelete::new());
                        WATCHLISTS.with_label_values(&[&self.type_name]).inc();
                    }
                }
            }
            SubscriptionUpdate::Decrement {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    if subscribers_count == 0 {
                        if let Some(on_delete) = self.items.get_mut(&item) {
                            *on_delete = OnDelete::delete(self.delete_timeout);
                        }
                    }
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
            .filter_map(|(item, on_delete)| {
                if on_delete.time_to_delete(now) {
                    Some(item.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for item in keys {
            self.items.remove(&item);
            WATCHLISTS.with_label_values(&[&self.type_name]).dec();
            self.last_values.write().await.remove(&item.to_string());
            let _ = self.repo.del(T::into(item));
        }
    }
}
