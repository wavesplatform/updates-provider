pub mod configs;
pub mod test_resources;

use crate::{error::Error, subscriptions::SubscriptionUpdate};
use async_trait::async_trait;

#[async_trait]
pub trait UpdatesProvider {
    async fn fetch_updates(
        &self,
    ) -> Result<tokio::sync::mpsc::UnboundedSender<SubscriptionUpdate>, Error>;
}
