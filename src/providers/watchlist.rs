use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::models::Topic;
use crate::subscriptions::SubscriptionUpdate;
use crate::{error::Error, resources::ResourcesRepo};
use std::{collections::HashSet, hash::Hash};

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    pub items: HashSet<T>,
    last_values: TSUpdatesProviderLastValues,
    repo: TSResourcesRepoImpl,
}

pub trait WatchListItem: Eq + Hash + Into<Topic> + MaybeFromTopic + ToString {}

pub trait MaybeFromTopic: Sized {
    fn maybe_item(topic: Topic) -> Option<Self>;
}

impl<T: WatchListItem> WatchList<T> {
    pub fn new(repo: TSResourcesRepoImpl, last_values: TSUpdatesProviderLastValues) -> Self {
        let items = HashSet::new();
        Self {
            repo,
            items,
            last_values,
        }
    }

    pub async fn on_update(&mut self, update: SubscriptionUpdate) -> Result<(), Error> {
        match update {
            SubscriptionUpdate::New { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    self.items.insert(item);
                }
            }
            SubscriptionUpdate::Increment { topic } => {
                if let Some(item) = T::maybe_item(topic) {
                    if !self.items.contains(&item) {
                        self.items.insert(item);
                    }
                }
            }
            SubscriptionUpdate::Decrement {
                topic,
                subscribers_count,
            } => {
                if let Some(item) = T::maybe_item(topic) {
                    if subscribers_count == 0 {
                        self.items.remove(&item);
                        self.last_values.write().await.remove(&item.to_string());
                        self.repo.del(T::into(item))?;
                    }
                }
            }
        }
        Ok(())
    }
}
