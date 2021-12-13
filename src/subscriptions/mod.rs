pub mod puller;
pub mod pusher;
pub mod repo;

use std::collections::HashMap;

use crate::error::Error;
use wavesexchange_topic::Topic;

pub type Subscriptions = Vec<Subscription>;

#[derive(Clone, Debug)]
pub struct Subscription {
    pub subscription_key: String,
    pub context: Option<SubscriptionContext>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct SubscriptionContext {
    pub tracing_context: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Config {
    pub subscriptions_key: String,
}

#[derive(Clone, Debug)]
pub enum SubscriptionEvent {
    Updated {
        topic: Topic,
        context: Option<SubscriptionContext>,
    },
    Removed {
        topic: Topic,
    },
}

pub trait SubscriptionsRepo {
    fn get_existing_subscriptions(&self) -> Result<Subscriptions, Error>;
    fn get_updates_fetcher(&self) -> Result<Box<dyn SubscriptionsUpdateFetcher>, Error>;
}

pub trait SubscriptionsUpdateFetcher {
    fn updates_stream(&mut self) -> Result<Box<dyn SubscriptionsStream + '_>, Error>;
}

pub trait SubscriptionsStream {
    fn next_event(&mut self) -> Result<SubscriptionEvent, Error>;
}
