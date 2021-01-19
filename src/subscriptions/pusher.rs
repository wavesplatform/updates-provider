use super::{SubscriptionUpdate, SubscriptionsUpdatesObservers};
use crate::error::Error;
use tokio::sync::mpsc;
use wavesexchange_log::error;

pub struct PusherImpl {
    subscriptions_changes_observers: SubscriptionsUpdatesObservers,
    subscriptions_changes_receiver: mpsc::UnboundedReceiver<SubscriptionUpdate>,
}

impl PusherImpl {
    pub fn new(
        subscriptions_changes_observers: SubscriptionsUpdatesObservers,
        subscriptions_changes_receiver: mpsc::UnboundedReceiver<SubscriptionUpdate>,
    ) -> Self {
        Self {
            subscriptions_changes_observers,
            subscriptions_changes_receiver,
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(subscription_update) = self.subscriptions_changes_receiver.recv().await {
            let observers = self.subscriptions_changes_observers.read().await;

            let fs: Vec<_> = observers
                .iter()
                .map(|observer| handle_subscription_update(observer, subscription_update.clone()))
                .collect();

            let res = futures::future::try_join_all(fs).await;

            if let Err(err) = res {
                error!(
                    "error while notification processing by observers: {:?}",
                    err
                );
                break;
            }
        }

        Ok(())
    }
}

async fn handle_subscription_update(
    observer_tx: &tokio::sync::mpsc::UnboundedSender<SubscriptionUpdate>,
    subscription_update: SubscriptionUpdate,
) -> Result<(), Error> {
    observer_tx
        .send(subscription_update)
        .map_err(|e| Error::from(e))
}
