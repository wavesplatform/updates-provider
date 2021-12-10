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
    fn get_subscriptions(&self) -> Result<Subscriptions, Error>;
    fn get_subscription_context(
        &self,
        subscription_key: &str,
    ) -> Result<Option<SubscriptionContext>, Error>;
}
