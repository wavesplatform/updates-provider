use crate::error::Error;
use crate::models::Topic;
use crate::subscriptions::SubscriptionUpdate;
use std::{collections::HashSet, hash::Hash};

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    pub items: HashSet<T>,
}

pub trait WatchListItem: Eq + Hash + MaybeToTopic {}

pub trait MaybeToTopic: Sized {
    fn maybe_item(topic: Topic) -> Option<Self>;
}

impl<T: WatchListItem> WatchList<T> {
    pub fn on_update(&mut self, update: SubscriptionUpdate) -> Result<(), Error> {
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
                    }
                }
            }
        }
        Ok(())
    }
}

impl<T: WatchListItem> Default for WatchList<T> {
    fn default() -> Self {
        WatchList {
            items: HashSet::new(),
        }
    }
}
