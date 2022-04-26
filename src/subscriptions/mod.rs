pub mod puller;
pub mod pusher;
pub mod repo;

use crate::error::Error;
use async_trait::async_trait;
use wavesexchange_topic::Topic;

type Subscriptions = Vec<String>;

#[derive(Debug)]
pub struct Config {
    pub subscriptions_key: String,
}

#[derive(Clone, Debug)]
pub enum SubscriptionEvent {
    Updated { topic: Topic },
    Removed { topic: Topic },
}

#[async_trait]
pub trait SubscriptionsRepo {
    async fn get_subscriptions(&self) -> Result<Subscriptions, Error>;
}
