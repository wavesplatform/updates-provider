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
use wavesexchange_log::{error, info};

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
impl<T, R> UpdatesProvider<T, R> for PollProvider<T, R>
where
    T: WatchListItem + Send + Sync + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
{
    async fn fetch_updates(self) -> Result<mpsc::Sender<WatchListUpdate<T>>, Error> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

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
            info!("{} updater stopped", data_type);
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
            {
                let keys = {
                    let mut watchlist_guard = self.watchlist.write().await;
                    watchlist_guard.delete_old();
                    watchlist_guard
                        .into_iter()
                        .map(|x| x.to_owned())
                        .collect::<Vec<_>>()
                };

                if let Err(error) = stream::iter(&keys)
                    .map(|data| Ok((data, &self.requester, &self.resources_repo, &self.watchlist)))
                    .try_for_each_concurrent(
                        5,
                        |(data, requester, resources_repo, watchlist)| async move {
                            let current_value = requester.get(data).await?;
                            Self::watchlist_process(data, current_value, resources_repo, watchlist)
                                .await?;
                            Ok::<(), Error>(())
                        },
                    )
                    .await
                {
                    error!("error occurred while watchlist processing: {:?}", error);
                }
            }
            tokio::time::sleep(self.polling_delay).await;
        }
    }
}

#[async_trait]
pub trait Requester<T: 'static>: Send + Sync {
    async fn get(&self, data: &T) -> Result<String, Error>;
}
