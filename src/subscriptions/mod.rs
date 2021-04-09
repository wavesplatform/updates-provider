pub mod puller;
pub mod pusher;
pub mod repo;

use crate::error::Error;
use crate::models::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{self, mpsc};

type Subscriptions = HashMap<String, i64>;
pub type SubscriptionsUpdatesObservers =
    Arc<sync::RwLock<Vec<mpsc::UnboundedSender<SubscriptionUpdate>>>>;

#[derive(Debug)]
pub struct Config {
    pub subscriptions_key: String,
}

#[derive(Clone, Debug)]
pub enum SubscriptionUpdate {
    New {
        topic: Topic,
        subscribers_count: i64,
    },
    Change {
        topic: Topic,
        subscribers_count: i64,
    },
    Delete {
        topic: Topic,
    },
}

pub trait SubscriptionsRepo {
    fn get_subscriptions(&self) -> Result<Subscriptions, Error>;
}
