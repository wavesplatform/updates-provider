use super::SubscriptionUpdate;
use crate::error::Error;
use crate::providers::watchlist::MaybeFromUpdate;
use tokio::sync::mpsc;
use wavesexchange_log::error;

pub struct PusherImpl {
    subscriptions_changes_observers: Vec<Box<dyn MaybeSend>>,
    subscriptions_changes_receiver: mpsc::UnboundedReceiver<SubscriptionUpdate>,
}

impl PusherImpl {
    pub fn new(
        subscriptions_changes_receiver: mpsc::UnboundedReceiver<SubscriptionUpdate>,
    ) -> Self {
        Self {
            subscriptions_changes_receiver,
            subscriptions_changes_observers: vec![],
        }
    }

    pub fn add_observer<T: 'static + MaybeFromUpdate>(&mut self, tx: mpsc::UnboundedSender<T>) {
        let observer = Box::new(Observer { tx });
        self.subscriptions_changes_observers.push(observer);
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(subscription_update) = self.subscriptions_changes_receiver.recv().await {
            for observer in self.subscriptions_changes_observers.iter() {
                if let Err(error) = observer.handle_subscription_update(&subscription_update) {
                    error!(
                        "error while notification processing by observers: {:?}",
                        error
                    );
                    break;
                }
            }
        }

        Ok(())
    }
}

struct Observer<T: MaybeFromUpdate> {
    tx: mpsc::UnboundedSender<T>,
}

trait MaybeSend: Send + Sync {
    fn handle_subscription_update(&self, update: &SubscriptionUpdate) -> Result<(), Error>;
}

impl<T: MaybeFromUpdate> MaybeSend for Observer<T> {
    fn handle_subscription_update(&self, update: &SubscriptionUpdate) -> Result<(), Error> {
        if let Some(update) = T::maybe_from_update(update) {
            if let Err(error) = self.tx.send(update) {
                return Err(Error::SendError(format!("{:?}", error)));
            }
        };
        Ok(())
    }
}
