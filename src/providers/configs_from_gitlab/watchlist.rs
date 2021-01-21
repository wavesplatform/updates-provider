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
        match update.resource {
            Topic::ConfigFromGitlab(config_file) => match update.update_type {
                subscriptions::SubscriptionUpdateType::New => {
                    self.items.insert(config_file);
                }
                subscriptions::SubscriptionUpdateType::Decrement => {
                    if update.subscribers_count == 0 {
                        self.items.remove(&config_file);
                    }
                }
                subscriptions::SubscriptionUpdateType::Increment => {
                    if !self.items.contains(&config_file) {
                        self.items.insert(config_file);
                    }
                }
            },
            _ => {
                // nothing to do
            }
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
