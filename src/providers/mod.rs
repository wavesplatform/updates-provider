pub mod blockchain;
pub mod polling;
pub mod watchlist;

use crate::error::Error;
use crate::models::Topic;
use crate::resources::{repo::ResourcesRepoImpl, ResourcesRepo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use watchlist::{WatchListItem, WatchListUpdate};
use wavesexchange_log::info;

type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;
type TSUpdatesProviderLastValues<T> = Arc<RwLock<HashMap<T, String>>>;

#[async_trait]
pub trait UpdatesProvider<T: WatchListItem + Clone + Send + Sync> {
    async fn fetch_updates(self) -> Result<mpsc::UnboundedSender<WatchListUpdate<T>>, Error>;

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues<T>,
    ) -> Result<(), Error> {
        let resource: Topic = data.clone().into();
        let mut last_value_guard = last_values.write().await;
        if let Some(last_value) = last_value_guard.get(data) {
            if &current_value != last_value {
                info!("insert new value {:?}", resource);
                resources_repo.set(resource, current_value.clone())?;
                last_value_guard.insert(data.to_owned(), current_value);
            }
        } else {
            last_value_guard.insert(data.to_owned(), current_value.clone());

            if let Some(last_updated_value) = resources_repo.get(&resource)? {
                if current_value != last_updated_value {
                    info!("update value {:?}", resource);
                    resources_repo.set(resource, current_value)?;
                }
            } else {
                info!("update value {:?}", resource);
                resources_repo.set(resource, current_value)?;
            }
        }

        Ok(())
    }
}
