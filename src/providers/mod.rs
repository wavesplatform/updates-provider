pub mod configs;
pub mod configs_from_gitlab;

use crate::{error::Error, subscriptions::SubscriptionUpdate};
use async_trait::async_trait;

#[async_trait]
pub trait UpdatesProvider {
    async fn fetch_updates(
        &self,
    ) -> Result<tokio::sync::mpsc::UnboundedSender<SubscriptionUpdate>, Error>;
}
