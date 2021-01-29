use super::ConfigsRepoImpl;
use super::{
    ConfigsRepo, TSConfigFilesWatchList, TSConfigUpdatesProviderLastValues,
    TSConfigsRepoImpl, TSResourcesRepoImpl,
};
use crate::error::Error;
use crate::models::{ConfigFile, Topic};
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

pub struct ConfigsUpdaterImpl {
    configs_repo: TSConfigsRepoImpl,
    resources_repo: TSResourcesRepoImpl,
    polling_delay: Duration,
    watchlist: TSConfigFilesWatchList,
    last_values: TSConfigUpdatesProviderLastValues,
}

impl ConfigsUpdaterImpl {
    pub fn new(
        configs_repo: ConfigsRepoImpl,
        resources_repo: Arc<ResourcesRepoImpl>,
        polling_delay: Duration,
    ) -> ConfigsUpdaterImpl {
        Self {
            configs_repo: Arc::new(RwLock::new(configs_repo)),
            resources_repo: resources_repo,
            polling_delay: polling_delay,
            watchlist: TSConfigFilesWatchList::default(),
            last_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl UpdatesProvider for ConfigsUpdaterImpl {
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

        let configs_repo = self.configs_repo.clone();
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
                            &configs_repo,
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
    configs_repo: &TSConfigsRepoImpl,
    resources_repo: &TSResourcesRepoImpl,
    last_values: &TSConfigUpdatesProviderLastValues,
    config_file: &ConfigFile,
) -> Result<(), Error> {
    let current_value = configs_repo
        .read()
        .await
        .get(config_file.to_owned())
        .await?;

    let config_file_key = config_file.to_string();
    let resource = Topic::Config(config_file.to_owned());
    let last_value = last_values.read().await.get(&config_file_key).cloned();

    match last_value {
        Some(last_value) => {
            if current_value != last_value {
                resources_repo.set(resource.clone(), current_value.clone())?;

                last_values
                    .write()
                    .await
                    .insert(config_file_key, current_value);
            }
        }
        None => {
            last_values
                .write()
                .await
                .insert(config_file_key.clone(), current_value.clone());

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
