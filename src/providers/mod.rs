pub mod blockchain;
pub mod polling;
pub mod watchlist;

use crate::error::Error;
use crate::resources::ResourcesRepo;
use async_trait::async_trait;
use tokio::sync::{mpsc, RwLock};
use watchlist::{WatchList, WatchListItem, WatchListUpdate};
use wavesexchange_log::info;
use wavesexchange_topic::Topic;

#[async_trait]
pub trait UpdatesProvider<T, R>
where
    T: WatchListItem + Clone + Send + Sync,
    R: ResourcesRepo + Send + Sync,
{
    async fn fetch_updates(self) -> Result<mpsc::Sender<WatchListUpdate<T>>, Error>;

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &R,
        watchlist: &RwLock<WatchList<T, R>>,
    ) -> Result<(), Error> {
        let resource: Topic = data.clone().into();
        let mut watchlist_guard = watchlist.write().await;
        if let Some(last_value) = watchlist_guard.get_value(data) {
            if &current_value != last_value {
                info!("insert new value {:?}", resource);
                watchlist_guard.insert_value(data, current_value.clone());
                drop(watchlist_guard);
                resources_repo.set_and_push(resource, current_value).await?;
            }
        } else {
            watchlist_guard.insert_value(data, current_value.clone());
            drop(watchlist_guard);

            if let Some(last_updated_value) = resources_repo.get(&resource).await? {
                if current_value != last_updated_value {
                    info!("update value {:?}", resource);
                    resources_repo.set_and_push(resource, current_value).await?;
                }
            } else {
                info!("update value {:?}", resource);
                resources_repo.set_and_push(resource, current_value).await?;
            }
        }

        Ok(())
    }
}
