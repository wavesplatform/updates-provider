use crate::error::Error;
use crate::models::{TestResource, Topic};
use crate::subscriptions;
use std::collections::HashSet;

#[derive(Debug)]
pub struct WatchList<T> {
    pub items: HashSet<T>,
}

impl WatchList<TestResource> {
    pub fn on_update(&mut self, update: subscriptions::SubscriptionUpdate) -> Result<(), Error> {
        match update {
            subscriptions::SubscriptionUpdate::New { topic } => match topic {
                Topic::TestResource(tr) => {
                    self.items.insert(tr);
                }
                _ => {}
            },
            subscriptions::SubscriptionUpdate::Decrement {
                topic,
                subscribers_count,
            } => match topic {
                Topic::TestResource(tr) => {
                    if subscribers_count == 0 {
                        self.items.remove(&tr);
                    }
                }
                _ => {}
            },
            subscriptions::SubscriptionUpdate::Increment { topic } => match topic {
                Topic::TestResource(tr) => {
                    if !self.items.contains(&tr) {
                        self.items.insert(tr);
                    }
                }
                _ => {}
            },
        }

        Ok(())
    }
}

impl Default for WatchList<TestResource> {
    fn default() -> Self {
        WatchList {
            items: HashSet::new(),
        }
    }
}
