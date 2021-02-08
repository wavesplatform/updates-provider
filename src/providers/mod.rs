pub mod configs;
pub mod requester;
pub mod states;
pub mod test_resources;
pub mod watchlist;

use crate::error::Error;
use crate::models::Topic;
use crate::resources::{repo::ResourcesRepoImpl, ResourcesRepo};
use crate::subscriptions::SubscriptionUpdate;
use async_trait::async_trait;
use requester::Requester;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use watchlist::{WatchList, WatchListItem};
use wavesexchange_log::{error, info};

#[async_trait]
pub trait UpdatesProvider {
    async fn fetch_updates(self) -> Result<mpsc::UnboundedSender<SubscriptionUpdate>, Error>;
}

type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;
type TSUpdatesProviderLastValues = Arc<RwLock<HashMap<String, String>>>;
type TSWatchList<T> = Arc<RwLock<WatchList<T>>>;

pub struct Provider<T: WatchListItem> {
    requester: Box<dyn Requester<T>>,
    resources_repo: TSResourcesRepoImpl,
    polling_delay: Duration,
    watchlist: TSWatchList<T>,
    last_values: TSUpdatesProviderLastValues,
}

impl<T: WatchListItem> Provider<T> {
    pub fn new(
        requester: Box<dyn Requester<T>>,
        polling_delay: Duration,
        resources_repo: TSResourcesRepoImpl,
    ) -> Self {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
        )));
        Self {
            requester,
            polling_delay,
            resources_repo,
            watchlist,
            last_values,
        }
    }
}

#[async_trait]
impl<T> UpdatesProvider for Provider<T>
where
    T: WatchListItem + Send + Sync + 'static,
{
    async fn fetch_updates(self) -> Result<mpsc::UnboundedSender<SubscriptionUpdate>, Error> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<SubscriptionUpdate>();

        let watchlist = self.watchlist.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(upd).await {
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

impl<T: WatchListItem> Provider<T> {
    async fn run(self) {
        loop {
            let watchlist = self.watchlist.read().await;
            let items_iter = watchlist.items.iter();
            if let Err(error) = self
                .requester
                .process(items_iter, &self.resources_repo, &self.last_values)
                .await
            {
                error!("error occured while watchlist processing: {:?}", error);
            }
            drop(watchlist);
            tokio::time::sleep(self.polling_delay).await;
        }
    }
}

// function used in implementations of provider
pub async fn watchlist_process<T: WatchListItem + Clone>(
    data: &T,
    current_value: String,
    resources_repo: &TSResourcesRepoImpl,
    last_values: &TSUpdatesProviderLastValues,
) -> Result<(), Error> {
    let data_key = data.to_string();
    let resource: Topic = data.clone().into();
    let last_value = last_values.read().await.get(&data_key).cloned();

    match last_value {
        Some(last_value) => {
            if current_value != last_value {
                resources_repo.set(resource.clone(), current_value.clone())?;

                last_values.write().await.insert(data_key, current_value);
            }
        }
        None => {
            last_values
                .write()
                .await
                .insert(data_key.clone(), current_value.clone());

            match resources_repo.get(&resource)? {
                Some(last_updated_value) => {
                    if current_value != last_updated_value {
                        resources_repo.set(resource, current_value)?;
                    }
                }
                None => {
                    resources_repo.set(resource, current_value)?;
                }
            }
        }
    }

    Ok(())
}
