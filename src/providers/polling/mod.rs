pub mod configs;
pub mod test_resources;

use super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::UpdatesProvider;
use crate::error::Error;
use crate::resources::ResourcesRepo;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{debug, error, info};

pub struct PollProvider<T: WatchListItem, R: ResourcesRepo> {
    requester: Box<dyn Requester<T>>,
    resources_repo: Arc<R>,
    polling_delay: Duration,
    watchlist: Arc<RwLock<WatchList<T, R>>>,
}

impl<T: WatchListItem, R: ResourcesRepo> PollProvider<T, R> {
    pub fn new(
        requester: Box<(dyn Requester<T>)>,
        polling_delay: Duration,
        delete_timeout: Duration,
        resources_repo: Arc<R>,
    ) -> Self {
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            delete_timeout,
        )));
        Self {
            requester,
            resources_repo,
            polling_delay,
            watchlist,
        }
    }
}

#[async_trait]
impl<T, R> UpdatesProvider<T> for PollProvider<T, R>
where
    T: WatchListItem + Send + Sync + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
{
    /// Run 2 async tasks: handler of subscribe/unsubscribe events & updates polling
    async fn fetch_updates(self) -> Result<mpsc::Sender<WatchListUpdate<T>>, Error> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

        // Run handler of subscribe/unsubscribe events
        let watchlist = self.watchlist.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
            }
        });

        // Run updates polling
        tokio::task::spawn(async move {
            let data_type = std::any::type_name::<T>();
            info!("starting {} polling updater", data_type);
            self.run().await;
            info!("{} polling updater stopped", data_type);
        });

        Ok(subscriptions_updates_sender)
    }
}

impl<T, R> PollProvider<T, R>
where
    T: WatchListItem + Send + Sync + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
{
    async fn run(self) {
        loop {
            let keys = self.get_polling_keys().await;
            let result = self.poll_keys(keys).await;
            if let Err(error) = result {
                error!("error occurred while watchlist processing: {:?}", error);
            }
            tokio::time::sleep(self.polling_delay).await;
        }
    }

    async fn get_polling_keys(&self) -> Vec<T> {
        let mut watchlist_guard = self.watchlist.write().await;
        watchlist_guard.delete_old().await;
        watchlist_guard
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    }

    async fn poll_keys(&self, keys: Vec<T>) -> Result<(), Error> {
        stream::iter(&keys)
            .map(|data| Ok((data, &self.requester, &self.resources_repo, &self.watchlist)))
            .try_for_each_concurrent(
                5,
                |(data, requester, resources_repo, watchlist)| async move {
                    let current_value = requester.get(data).await?;
                    Self::watchlist_process(data, current_value, resources_repo, watchlist).await?;
                    Ok::<(), Error>(())
                },
            )
            .await
    }

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &R,
        watchlist: &RwLock<WatchList<T, R>>,
    ) -> Result<(), Error> {
        let resource = data.clone().into().as_topic(); //TODO Bad design - cloning is unnecessary here
        let mut watchlist_guard = watchlist.write().await;
        if let Some(last_value) = watchlist_guard.get_value(data) {
            if &current_value != last_value {
                debug!("insert new value {:?}", resource);
                watchlist_guard.insert_value(data, current_value.clone());
                drop(watchlist_guard);
                resources_repo
                    .set_and_push(&resource, current_value)
                    .await?;
            }
        } else {
            watchlist_guard.insert_value(data, current_value.clone());
            drop(watchlist_guard);

            if let Some(last_stored_value) = resources_repo.get(&resource).await? {
                if current_value != last_stored_value {
                    debug!("update value {:?}", resource);
                    resources_repo
                        .set_and_push(&resource, current_value)
                        .await?;
                }
            } else {
                debug!("update value {:?}", resource);
                resources_repo
                    .set_and_push(&resource, current_value)
                    .await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
pub trait Requester<T: 'static>: Send + Sync {
    async fn get(&self, data: &T) -> Result<String, Error>;
}
