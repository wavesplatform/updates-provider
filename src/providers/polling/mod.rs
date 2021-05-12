pub mod configs;
pub mod requester;
pub mod test_resources;

use super::watchlist::{WatchList, WatchListItem};
use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::error::Error;
use async_trait::async_trait;
use requester::{ErasedRequester, Requester};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};

pub struct PollProvider<T: WatchListItem> {
    requester: Box<dyn ErasedRequester<T>>,
    resources_repo: TSResourcesRepoImpl,
    polling_delay: Duration,
    watchlist: Arc<RwLock<WatchList<T>>>,
    last_values: TSUpdatesProviderLastValues<T>,
}

impl<T: WatchListItem> PollProvider<T> {
    pub fn new(
        requester: Box<(dyn ErasedRequester<T>)>,
        polling_delay: Duration,
        delete_timeout: Duration,
        resources_repo: TSResourcesRepoImpl,
    ) -> Self {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
            delete_timeout,
        )));
        Self {
            requester,
            resources_repo,
            polling_delay,
            watchlist,
            last_values,
        }
    }
}

#[async_trait]
impl<T> super::UpdatesProvider<T> for PollProvider<T>
where
    T: WatchListItem + Send + Sync + 'static,
{
    async fn fetch_updates(
        self,
    ) -> Result<mpsc::UnboundedSender<super::watchlist::WatchListUpdate<T>>, Error> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel();

        let watchlist = self.watchlist.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
            }
        });

        tokio::task::spawn(async move {
            let data_type = std::any::type_name::<T>();
            info!("starting {} updater", data_type);
            self.run().await;
        });

        Ok(subscriptions_updates_sender)
    }
}

impl<T: WatchListItem + Send + Sync + 'static> PollProvider<T> {
    async fn run(self) {
        loop {
            {
                let mut watchlist_guard = self.watchlist.write().await;
                watchlist_guard.delete_old().await;
                let mut items = watchlist_guard.into_iter();
                if let Err(error) = self
                    .requester
                    .process(&mut items, &self.resources_repo, &self.last_values)
                    .await
                {
                    error!("error occured while watchlist processing: {:?}", error);
                }
            }
            tokio::time::delay_for(self.polling_delay).await;
        }
    }
}
