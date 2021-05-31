use super::super::watchlist::{WatchList, WatchListUpdate};
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues, UpdatesProvider};
use crate::resources::ResourcesRepo;
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::LeasingBalance as LeasingBalanceDB;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate};
use crate::utils::clean_timeout;
use crate::{error::Result, transactions::TransactionsRepo};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};
use wavesexchange_topic::{LeasingBalance, Topic};

pub struct Provider {
    watchlist: Arc<RwLock<WatchList<LeasingBalance>>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: Arc<RwLock<HashMap<LeasingBalance, String>>>,
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
                BlockchainUpdate::Block(BlockMicroblockAppend {
                    leasing_balances, ..
                })
                | BlockchainUpdate::Microblock(BlockMicroblockAppend {
                    leasing_balances, ..
                }) => self.check_leasing_balances(leasing_balances).await?,
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_leasing_balances(
        &mut self,
        leasing_balances: &[LeasingBalanceDB],
    ) -> Result<()> {
        for lb in leasing_balances.iter() {
            self.check_leasing_balance(lb).await?;
        }

        Ok(())
    }

    async fn check_leasing_balance(&mut self, lb: &LeasingBalanceDB) -> Result<()> {
        let data = LeasingBalance {
            address: lb.address.to_owned(),
        };
        if self.watchlist.read().await.contains_key(&data) {
            let current_value = serde_json::to_string(lb)?;
            Self::watchlist_process(
                &data,
                current_value,
                &self.resources_repo,
                &self.last_values,
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl UpdatesProvider<LeasingBalance> for Provider {
    async fn fetch_updates(
        mut self,
    ) -> Result<mpsc::UnboundedSender<WatchListUpdate<LeasingBalance>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<WatchListUpdate<LeasingBalance>>();

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
        data: &LeasingBalance,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues<LeasingBalance>,
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
    value: LeasingBalance,
) -> Result<()> {
    let topic = value.clone().into();
    if resources_repo.get(&topic)?.is_none() {
        let new_value = get_last(transactions_repo, value)?;
        resources_repo.set(topic, new_value)?;
    }

    Ok(())
}

fn get_last(
    transactions_repo: &Arc<TransactionsRepoPoolImpl>,
    value: LeasingBalance,
) -> Result<String> {
    Ok(
        if let Some(ilb) = transactions_repo.last_leasing_balance(value.address)? {
            let lb = LeasingBalanceDB::from(ilb);
            serde_json::to_string(&lb)?
        } else {
            serde_json::to_string(&None::<LeasingBalanceDB>)?
        },
    )
}
