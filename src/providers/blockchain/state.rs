use super::super::watchlist::{WatchList, WatchListUpdate};
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues, UpdatesProvider};
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate};
use crate::utils::clean_timeout;
use crate::{
    error::Result,
    transactions::{DataEntry, TransactionsRepo},
};
use crate::{
    models::{State, Topic},
    resources::ResourcesRepo,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};

pub struct Provider {
    watchlist: Arc<RwLock<WatchList<State>>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: Arc<RwLock<HashMap<State, String>>>,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    transactions_repo: Arc<TransactionsRepoPoolImpl>,
    clean_timeout: Duration,
}

impl Provider {
    pub fn new(
        resources_repo: TSResourcesRepoImpl,
        delete_timeout: Duration,
        transactions_repo: Arc<TransactionsRepoPoolImpl>,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
            delete_timeout,
        )));
        let clean_timeout = clean_timeout(delete_timeout);
        Self {
            watchlist,
            resources_repo,
            last_values,
            rx,
            transactions_repo,
            clean_timeout,
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.clean_timeout);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    if let Some(blockchain_updates) = msg {
                        self.process_updates(blockchain_updates).await?;
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let mut watchlist_lock = self.watchlist.write().await;
                    watchlist_lock.delete_old().await;
                }
            }
        }
        Ok(())
    }

    async fn process_updates(
        &mut self,
        blockchain_updates: Arc<Vec<BlockchainUpdate>>,
    ) -> Result<()> {
        for blockchain_update in blockchain_updates.iter() {
            match blockchain_update {
                BlockchainUpdate::Block(BlockMicroblockAppend { data_entries, .. })
                | BlockchainUpdate::Microblock(BlockMicroblockAppend { data_entries, .. }) => {
                    self.check_data_entries(data_entries).await?
                }
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_data_entries(&mut self, data_entries: &[DataEntry]) -> Result<()> {
        for de in data_entries.iter() {
            let data = State {
                address: de.address.to_owned(),
                key: de.key.to_owned(),
            };
            if self.watchlist.read().await.contains_key(&data) {
                let current_value = serde_json::to_string(de)?;
                Self::watchlist_process(
                    &data,
                    current_value,
                    &self.resources_repo,
                    &self.last_values,
                )
                .await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl UpdatesProvider<State> for Provider {
    async fn fetch_updates(mut self) -> Result<mpsc::UnboundedSender<WatchListUpdate<State>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<WatchListUpdate<State>>();

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let transactions_repo = self.transactions_repo.clone();
        tokio::task::spawn(async move {
            info!("starting transactions subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::New { item, .. } = upd {
                    if let Err(err) =
                        check_and_maybe_insert(&resources_repo, &transactions_repo, item).await
                    {
                        error!("error while updating value: {:?}", err);
                    }
                }
            }
        });

        tokio::task::spawn(async move {
            info!("starting transactions provider");
            if let Err(error) = self.run().await {
                error!("transaction provider return error: {:?}", error);
            }
        });

        Ok(subscriptions_updates_sender)
    }

    async fn watchlist_process(
        data: &State,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues<State>,
    ) -> Result<()> {
        let resource: Topic = data.clone().into();
        info!("insert new value {:?}", resource);
        last_values
            .write()
            .await
            .insert(data.to_owned(), current_value.clone());
        resources_repo.set(resource, current_value)?;
        Ok(())
    }
}

async fn check_and_maybe_insert(
    resources_repo: &TSResourcesRepoImpl,
    transactions_repo: &Arc<TransactionsRepoPoolImpl>,
    value: State,
) -> Result<()> {
    let topic = value.clone().into();
    if resources_repo.get(&topic)?.is_none() {
        let new_value = get_last(transactions_repo, value)?;
        resources_repo.set(topic, new_value)?;
    }

    Ok(())
}

fn get_last(transactions_repo: &Arc<TransactionsRepoPoolImpl>, value: State) -> Result<String> {
    Ok(
        if let Some(ide) = transactions_repo.last_data_entry(value.address, value.key)? {
            let de = DataEntry::from(ide);
            serde_json::to_string(&de)?
        } else {
            serde_json::to_string(&None::<DataEntry>)?
        },
    )
}
