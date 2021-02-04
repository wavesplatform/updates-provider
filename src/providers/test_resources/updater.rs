use super::{
    TSResourcesRepoImpl, TSTestResourcesRepoImpl, TSTestResourcesUpdatesProviderLastValues,
    TSTestResourcesWatchList, TestResourcesRepo, TestResourcesRepoImpl,
};
use crate::error::Error;
use crate::models::{TestResource, Topic};
use crate::providers::UpdatesProvider;
use crate::resources::repo::ResourcesRepoImpl;
use crate::resources::ResourcesRepo;
use crate::subscriptions;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};

pub struct TestResourcesUpdaterImpl {
    test_resources_repo: TSTestResourcesRepoImpl,
    resources_repo: TSResourcesRepoImpl,
    polling_delay: Duration,
    watchlist: TSTestResourcesWatchList,
    last_values: TSTestResourcesUpdatesProviderLastValues,
}

impl TestResourcesUpdaterImpl {
    pub fn new(
        test_resources_repo: TestResourcesRepoImpl,
        resources_repo: Arc<ResourcesRepoImpl>,
        polling_delay: Duration,
    ) -> Self {
        Self {
            test_resources_repo: Arc::new(RwLock::new(test_resources_repo)),
            resources_repo: resources_repo,
            polling_delay: polling_delay,
            watchlist: TSTestResourcesWatchList::default(),
            last_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl UpdatesProvider for TestResourcesUpdaterImpl {
    async fn fetch_updates(
        &self,
    ) -> Result<mpsc::UnboundedSender<subscriptions::SubscriptionUpdate>, Error> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<subscriptions::SubscriptionUpdate>();

        let watchlist = self.watchlist.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
            }
        });

        let test_resources_repo = self.test_resources_repo.clone();
        let resources_repo = self.resources_repo.clone();
        let polling_delay = self.polling_delay.clone();
        let last_values = self.last_values.clone();
        let watchlist = self.watchlist.clone();
        tokio::task::spawn(async move {
            info!("starting configs updater");
            loop {
                let watchlist = watchlist.read().await;

                let watchlist_processing: Vec<_> = watchlist
                    .items
                    .iter()
                    .map(|config_file| {
                        watchlist_config_file_processing(
                            &test_resources_repo,
                            &resources_repo,
                            &last_values,
                            config_file,
                        )
                    })
                    .collect();

                if let Err(err) = futures::future::try_join_all(watchlist_processing).await {
                    error!("error occured while watchlist processing: {:?}", err);
                }

                tokio::time::sleep(polling_delay).await;
            }
        });

        Ok(subscriptions_updates_sender)
    }
}

async fn watchlist_config_file_processing(
    test_resource_repo: &TSTestResourcesRepoImpl,
    resources_repo: &TSResourcesRepoImpl,
    last_values: &TSTestResourcesUpdatesProviderLastValues,
    test_resource: &TestResource,
) -> Result<(), Error> {
    let current_value = test_resource_repo
        .read()
        .await
        .get(test_resource.to_owned())
        .await?;

    let test_resource_key = test_resource.to_string();
    let resource = Topic::TestResource(test_resource.to_owned());
    let last_value = last_values.read().await.get(&test_resource_key).cloned();

    match last_value {
        Some(last_value) => {
            if current_value != last_value {
                resources_repo.set(resource.clone(), current_value.clone())?;

                last_values
                    .write()
                    .await
                    .insert(test_resource_key, current_value);
            }
        }
        None => {
            last_values
                .write()
                .await
                .insert(test_resource_key.clone(), current_value.clone());

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
