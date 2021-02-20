pub mod blockchain_height;
pub mod polling;
pub mod watchlist;

use crate::error::Error;
use crate::models::Topic;
use crate::resources::{repo::ResourcesRepoImpl, ResourcesRepo};
use crate::subscriptions::SubscriptionUpdate;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use watchlist::WatchListItem;
use wavesexchange_log::info;

type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;
type TSUpdatesProviderLastValues = Arc<RwLock<HashMap<String, String>>>;

#[async_trait]
pub trait UpdatesProvider {
    async fn fetch_updates(self) -> Result<mpsc::UnboundedSender<SubscriptionUpdate>, Error>;
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
                info!("insert new value {:?}", resource);
                resources_repo.set(resource, current_value.clone())?;
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
                        info!("update value {:?}", resource);
                        resources_repo.set(resource, current_value)?;
                    }
                }
                None => {
                    info!("update value {:?}", resource);
                    resources_repo.set(resource, current_value)?;
                }
            }
        }
    }

    Ok(())
}
