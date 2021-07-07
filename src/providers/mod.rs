pub mod blockchain;
pub mod polling;
pub mod watchlist;

use crate::error::Error;
use crate::resources::{repo::ResourcesRepoImpl, ResourcesRepo};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use watchlist::WatchList;
use watchlist::{WatchListItem, WatchListUpdate};
use wavesexchange_log::info;
use wavesexchange_topic::Topic;

type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;

#[async_trait]
pub trait UpdatesProvider<T: WatchListItem + Clone + Send + Sync> {
    async fn fetch_updates(self) -> Result<mpsc::Sender<WatchListUpdate<T>>, Error>;

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        watchlist: &Arc<RwLock<WatchList<T>>>,
    ) -> Result<(), Error> {
        let resource: Topic = data.clone().into();
        let mut watchlist_guard = watchlist.write().await;
        if let Some(last_value) = watchlist_guard.get_value(data) {
            if &current_value != last_value {
                info!("insert new value {:?}", resource);
                watchlist_guard.insert_value(data, current_value.clone());
                drop(watchlist_guard);
                resources_repo.set_and_push(resource, current_value)?;
            }
        } else {
            watchlist_guard.insert_value(data, current_value.clone());
            drop(watchlist_guard);

            if let Some(last_updated_value) = resources_repo.get(&resource)? {
                if current_value != last_updated_value {
                    info!("update value {:?}", resource);
                    resources_repo.set_and_push(resource, current_value)?;
                }
            } else {
                info!("update value {:?}", resource);
                resources_repo.set_and_push(resource, current_value)?;
            }
        }

        Ok(())
    }
}
