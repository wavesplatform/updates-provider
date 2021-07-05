pub mod leasing_balance;
pub mod state;
pub mod transaction;

use super::super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::super::{TSResourcesRepoImpl, UpdatesProvider};
use crate::error::Result;
use crate::resources::ResourcesRepo;
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate};
use crate::utils::clean_timeout;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};
use wavesexchange_topic::Topic;

pub trait Item: WatchListItem + Send + Sync + LastValue + DataFromBlock {}

pub struct Provider<T: Item> {
    watchlist: Arc<RwLock<WatchList<T>>>,
    resources_repo: TSResourcesRepoImpl,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    transactions_repo: Arc<TransactionsRepoPoolImpl>,
    clean_timeout: Duration,
}

impl<T: 'static + Item> Provider<T> {
    pub fn new(
        resources_repo: TSResourcesRepoImpl,
        delete_timeout: Duration,
        transactions_repo: Arc<TransactionsRepoPoolImpl>,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            delete_timeout,
        )));
        let clean_timeout = clean_timeout(delete_timeout);
        Self {
            watchlist,
            resources_repo,
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
                BlockchainUpdate::Block(block) | BlockchainUpdate::Microblock(block) => {
                    self.check_and_process(block).await?
                }
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_and_process(&self, block: &BlockMicroblockAppend) -> Result<()> {
        for (current_value, data) in T::data_from_block(block) {
            if self.watchlist.read().await.contains_key(&data) {
                Self::watchlist_process(
                    &data,
                    current_value,
                    &self.resources_repo,
                    &self.watchlist,
                )
                .await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T: 'static + Item> UpdatesProvider<T> for Provider<T> {
    async fn fetch_updates(mut self) -> Result<mpsc::Sender<WatchListUpdate<T>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

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
        data: &T,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        watchlist: &Arc<RwLock<WatchList<T>>>,
    ) -> Result<()> {
        let resource: Topic = data.clone().into();
        info!("insert new value {:?}", resource);
        watchlist
            .write()
            .await
            .insert_value(data, current_value.clone());
        resources_repo.set_and_push(resource, current_value)?;
        Ok(())
    }
}

async fn check_and_maybe_insert<T: Item>(
    resources_repo: &TSResourcesRepoImpl,
    transactions_repo: &Arc<TransactionsRepoPoolImpl>,
    value: T,
) -> Result<()> {
    let topic = value.clone().into();
    if resources_repo.get(&topic)?.is_none() {
        let new_value = value.get_last(transactions_repo).await?;
        resources_repo.set_and_push(topic, new_value)?;
    }

    Ok(())
}

pub trait DataFromBlock: Sized {
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)>;
}

#[async_trait]
pub trait LastValue {
    async fn get_last(self, repo: &Arc<TransactionsRepoPoolImpl>) -> Result<String>;
}
