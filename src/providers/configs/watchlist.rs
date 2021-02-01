use crate::error::Error;
use crate::models::{ConfigFile, Topic};
use crate::subscriptions;
use std::collections::HashSet;

#[derive(Debug)]
pub struct WatchList<T> {
    pub items: HashSet<T>,
}

impl WatchList<ConfigFile> {
    pub fn on_update(&mut self, update: subscriptions::SubscriptionUpdate) -> Result<(), Error> {
        match update {
            subscriptions::SubscriptionUpdate::New { topic } => match topic {
                Topic::Config(config_file) => {
                    self.items.insert(config_file);
                }
            },
            subscriptions::SubscriptionUpdate::Decrement {
                topic,
                subscribers_count,
            } => match topic {
                Topic::Config(config_file) => {
                    if subscribers_count == 0 {
                        self.items.remove(&config_file);
                    }
                }
            },
            subscriptions::SubscriptionUpdate::Increment { topic } => match topic {
                Topic::Config(config_file) => {
                    if !self.items.contains(&config_file) {
                        self.items.insert(config_file);
                    }
                }
            },
        }

        Ok(())
    }
}

impl Default for WatchList<ConfigFile> {
    fn default() -> Self {
        WatchList {
            items: HashSet::new(),
        }
    }
}
