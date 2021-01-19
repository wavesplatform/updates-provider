pub mod puller;
pub mod pusher;
pub mod repo;

use crate::error::Error;
use crate::models::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{self, mpsc};

type Subscriptions = HashMap<String, i32>;
pub type SubscriptionsUpdatesObservers =
    Arc<sync::RwLock<Vec<mpsc::UnboundedSender<SubscriptionUpdate>>>>;

#[derive(Debug)]
pub struct Config {
    pub subscriptions_key: String,
}

#[derive(Clone, Debug)]
pub enum SubscriptionUpdateType {
    New,
    Increment,
    Decrement,
}

#[derive(Clone, Debug)]
pub struct SubscriptionUpdate {
    pub update_type: SubscriptionUpdateType,
    pub resource: Topic,
    pub subscribers_count: i32,
}

pub trait SubscriptionsRepo {
    fn get_subscriptions(&self) -> Result<Subscriptions, Error>;
}
